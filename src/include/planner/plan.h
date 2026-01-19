/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <cassert>
#include <cstring>
#include <memory>
#include <string>
#include <vector>
#include "common/condition.h"
#include "parser/ast.h"

#include "parser/parser.h"

#include "common/common.h"
#include "system/sm_manager.h"

namespace easydb {

/**
 * @brief 计划标签枚举
 * @note 定义了所有类型的查询计划
 */
typedef enum PlanTag {
  T_Invalid = 1,              /**< 无效计划 */
  T_Help,                     /**< HELP命令 */
  T_ShowTable,                /**< SHOW TABLES命令 */
  T_ShowIndex,                /**< SHOW INDEX命令 */
  T_DescTable,                /**< DESC TABLE命令 */
  T_CreateTable,              /**< CREATE TABLE命令 */
  T_DropTable,                /**< DROP TABLE命令 */
  T_CreateIndex,              /**< CREATE INDEX命令 */
  T_DropIndex,                /**< DROP INDEX命令 */
  T_CreateStaticCheckpoint,   /**< CREATE CHECKPOINT命令 */
  T_LoadData,                 /**< LOAD DATA命令 */
  T_SetKnob,                  /**< SET KNOB命令 */
  T_Insert,                   /**< INSERT语句 */
  T_Update,                   /**< UPDATE语句 */
  T_Delete,                   /**< DELETE语句 */
  T_select,                   /**< SELECT语句 */
  T_Transaction_begin,        /**< BEGIN TRANSACTION */
  T_Transaction_commit,       /**< COMMIT TRANSACTION */
  T_Transaction_abort,        /**< ABORT TRANSACTION */
  T_Transaction_rollback,     /**< ROLLBACK TRANSACTION */
  T_SeqScan,                  /**< 顺序扫描执行器 */
  T_IndexScan,                /**< 索引扫描执行器 */
  T_NestLoop,                 /**< 嵌套循环连接 */
  T_SortMerge,                /**< 排序归并连接（sort merge join） */
  T_IndexMerge,               /**< 使用索引的归并连接（merge join using index） */
  T_HashJoin,                 /**< 哈希连接 */
  T_Sort,                     /**< 排序执行器 */
  T_Projection,               /**< 投影执行器 */
  T_Aggregation,              /**< 聚合执行器 */
  T_Empty                     /**< 空结果集的执行计划 */
} PlanTag;

/**
 * @brief 查询执行计划基类
 * 
 * Plan 是所有查询计划的基类，定义了计划的基本接口。
 * 不同的计划类型（如ScanPlan、JoinPlan等）继承此类。
 */
class Plan {
 public:
  /** @brief 计划标签，标识计划的类型 */
  PlanTag tag;
  
  /**
   * @brief 虚析构函数
   */
  virtual ~Plan() = default;
  
  /**
   * @brief 获取条件列表
   * @return 条件向量
   * @throws std::runtime_error 如果计划类型不支持此操作
   * @note 默认实现抛出异常，只有ScanPlan等支持条件的计划会重写此方法
   */
  std::vector<Condition> get_conds() { throw std::runtime_error("not supported!"); }
};

/**
 * @brief 空计划类
 * @note 表示空结果集的执行计划，用于某些特殊情况
 */
class EmptyPlan : public Plan {
 public:
  /**
   * @brief 构造函数
   */
  EmptyPlan() { tag = T_Empty; }
  
  /**
   * @brief 析构函数
   */
  ~EmptyPlan() {}
};

/**
 * @brief 扫描计划类
 * 
 * ScanPlan 表示对表的扫描操作，可以是顺序扫描或索引扫描。
 * 包含扫描的表名、条件列表、列信息等。
 */
class ScanPlan : public Plan {
 public:
  /**
   * @brief 构造函数
   * @param tag 计划标签（T_SeqScan或T_IndexScan）
   * @param sm_manager 系统管理器指针
   * @param tab_name 表名
   * @param conds 扫描条件列表
   * @param index_col_names 索引列名列表（用于索引扫描）
   */
  ScanPlan(PlanTag tag, SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds,
           std::vector<std::string> index_col_names) {
    Plan::tag = tag;
    tab_name_ = std::move(tab_name);
    conds_ = std::move(conds);
    TabMeta &tab = sm_manager->db_.get_table(tab_name_);
    cols_ = tab.cols;
    len_ = cols_.back().offset + cols_.back().len;
    fed_conds_ = conds_;
    index_col_names_ = index_col_names;
  }
  
  /**
   * @brief 析构函数
   */
  ~ScanPlan() {}

  // bool is_index_scan(){
  //   return tag == T_IndexScan;
  // }

  /**
   * @brief 获取条件列表
   * @return 扫描条件的向量
   */
  std::vector<Condition> get_conds() { return fed_conds_; }

  // 以下变量同ScanExecutor中的变量
  
  /** @brief 表名 */
  std::string tab_name_;
  
  /** @brief 列的元数据向量 */
  std::vector<ColMeta> cols_;
  
  /** @brief 扫描条件列表 */
  std::vector<Condition> conds_;
  
  /** @brief 记录长度（字节数） */
  size_t len_;
  
  /** @brief 已传递的条件列表 */
  std::vector<Condition> fed_conds_;
  
  /** @brief 索引列名列表（用于索引扫描） */
  std::vector<std::string> index_col_names_;
};

/**
 * @brief 连接计划类
 * 
 * JoinPlan 表示两个表的连接操作，支持多种连接算法。
 */
class JoinPlan : public Plan {
 public:
  /**
   * @brief 构造函数
   * @param tag 计划标签（T_NestLoop、T_SortMerge、T_HashJoin等）
   * @param left 左子计划指针
   * @param right 右子计划指针
   * @param conds 连接条件列表
   */
  JoinPlan(PlanTag tag, std::shared_ptr<Plan> left, std::shared_ptr<Plan> right, std::vector<Condition> conds) {
    Plan::tag = tag;
    left_ = std::move(left);
    right_ = std::move(right);
    conds_ = std::move(conds);
    type = INNER_JOIN;
  }
  
  /**
   * @brief 析构函数
   */
  ~JoinPlan() {}
  
  /** @brief 左子计划指针 */
  std::shared_ptr<Plan> left_;
  
  /** @brief 右子计划指针 */
  std::shared_ptr<Plan> right_;
  
  /** @brief 连接条件列表 */
  std::vector<Condition> conds_;
  
  /**
   * @brief 连接类型
   * @note future TODO: 后续可以支持的连接类型（LEFT JOIN、RIGHT JOIN等）
   */
  JoinType type;
};

/**
 * @brief 投影计划类
 * 
 * ProjectionPlan 表示投影操作，选择指定的列。
 */
class ProjectionPlan : public Plan {
 public:
  /**
   * @brief 构造函数
   * @param tag 计划标签（T_Projection）
   * @param subplan 子计划指针
   * @param sel_cols 选择的列列表
   * @param is_unique 是否去重（默认false）
   */
  ProjectionPlan(PlanTag tag, std::shared_ptr<Plan> subplan, std::vector<TabCol> sel_cols, bool is_unique = false) {
    Plan::tag = tag;
    subplan_ = std::move(subplan);
    sel_cols_ = std::move(sel_cols);
    is_unique_ = is_unique;
  }
  
  /**
   * @brief 析构函数
   */
  ~ProjectionPlan() {}
  
  /**
   * @brief 设置是否去重
   * @param is_unique 是否去重
   */
  void SetUnique(bool is_unique) { is_unique_ = is_unique; }
  
  /** @brief 子计划指针 */
  std::shared_ptr<Plan> subplan_;
  
  /** @brief 选择的列列表 */
  std::vector<TabCol> sel_cols_;
  
  /** @brief 是否去重标志 */
  bool is_unique_;
};

/**
 * @brief 排序计划类
 * 
 * SortPlan 表示排序操作，根据指定列对结果进行排序。
 */
class SortPlan : public Plan {
 public:
  /**
   * @brief 构造函数
   * @param tag 计划标签（T_Sort）
   * @param subplan 子计划指针
   * @param sel_col 排序的列
   * @param is_desc 是否降序（true表示降序，false表示升序）
   */
  SortPlan(PlanTag tag, std::shared_ptr<Plan> subplan, TabCol sel_col, bool is_desc) {
    Plan::tag = tag;
    subplan_ = std::move(subplan);
    sel_col_ = sel_col;
    is_desc_ = is_desc;
  }
  
  /**
   * @brief 析构函数
   */
  ~SortPlan() {}
  
  /** @brief 子计划指针 */
  std::shared_ptr<Plan> subplan_;
  
  /** @brief 排序的列 */
  TabCol sel_col_;
  
  /** @brief 是否降序标志 */
  bool is_desc_;
};

/**
 * @brief 聚合计划类
 * 
 * AggregationPlan 表示聚合操作，包括GROUP BY和HAVING子句。
 */
class AggregationPlan : public Plan {
 public:
  /**
   * @brief 构造函数
   * @param tag 计划标签（T_Aggregation）
   * @param subplan 子计划指针
   * @param sel_cols 选择的列列表（包含聚合函数）
   * @param group_cols 分组列列表
   * @param having_conds HAVING条件列表
   */
  AggregationPlan(PlanTag tag, std::shared_ptr<Plan> subplan, std::vector<TabCol> sel_cols,
                  std::vector<TabCol> group_cols, std::vector<Condition> having_conds) {
    Plan::tag = tag;
    subplan_ = std::move(subplan);
    sel_cols_ = std::move(sel_cols);
    group_cols_ = std::move(group_cols);
    having_conds_ = std::move(having_conds);
  }
  
  /**
   * @brief 析构函数
   */
  ~AggregationPlan() {}
  
  /** @brief 子计划指针 */
  std::shared_ptr<Plan> subplan_;
  
  /** @brief 选择的列列表（包含聚合函数，如SUM、COUNT等） */
  std::vector<TabCol> sel_cols_;
  
  /** @brief 分组列列表 */
  std::vector<TabCol> group_cols_;
  
  /** @brief HAVING条件列表 */
  std::vector<Condition> having_conds_;
};

/**
 * @brief DML语句计划类
 * 
 * DMLPlan 表示数据操作语言（Data Manipulation Language）语句的计划，
 * 包括INSERT、DELETE、UPDATE、SELECT语句。
 */
class DMLPlan : public Plan {
 public:
  /**
   * @brief 构造函数
   * @param tag 计划标签（T_Insert、T_Update、T_Delete、T_select）
   * @param subplan 子计划指针（用于SELECT）
   * @param tab_name 表名
   * @param values 值向量（用于INSERT）
   * @param conds 条件列表（用于UPDATE、DELETE、SELECT）
   * @param set_clauses SET子句列表（用于UPDATE）
   * @param unique 是否唯一SELECT（默认false）
   */
  DMLPlan(PlanTag tag, std::shared_ptr<Plan> subplan, std::string tab_name, std::vector<Value> values,
          std::vector<Condition> conds, std::vector<SetClause> set_clauses, bool unique = false) {
    Plan::tag = tag;
    subplan_ = std::move(subplan);
    tab_name_ = std::move(tab_name);
    values_ = std::move(values);
    conds_ = std::move(conds);
    set_clauses_ = std::move(set_clauses);
    unique_ = unique;
  }
  
  /**
   * @brief 从shared_ptr构造DML计划
   * @param ptr 指向DMLPlan对象的shared_ptr
   */
  DMLPlan(std::shared_ptr<void> &ptr) {
    auto derived_ptr = std::static_pointer_cast<DMLPlan>(ptr);
    if (!derived_ptr) {
      throw std::bad_cast();
    }

    // 使用 derived_ptr 进行成员变量初始化
    Plan::tag = derived_ptr->tag;
    subplan_ = derived_ptr->subplan_;
    tab_name_ = derived_ptr->tab_name_;
    values_ = derived_ptr->values_;
    conds_ = derived_ptr->conds_;
    set_clauses_ = derived_ptr->set_clauses_;
    unique_ = derived_ptr->unique_;
  }
  
  /**
   * @brief 析构函数
   */
  ~DMLPlan() {}
  
  /** @brief 子计划指针（用于SELECT） */
  std::shared_ptr<Plan> subplan_;
  
  /** @brief 表名 */
  std::string tab_name_;
  
  /** @brief 值向量（用于INSERT） */
  std::vector<Value> values_;
  
  /** @brief 条件列表（用于UPDATE、DELETE、SELECT） */
  std::vector<Condition> conds_;
  
  /** @brief SET子句列表（用于UPDATE） */
  std::vector<SetClause> set_clauses_;
  
  /** @brief 是否唯一SELECT标志 */
  bool unique_;
};

/**
 * @brief DDL语句计划类
 * 
 * DDLPlan 表示数据定义语言（Data Definition Language）语句的计划，
 * 包括CREATE/DROP TABLE、CREATE/DROP INDEX等。
 */
class DDLPlan : public Plan {
 public:
  /**
   * @brief 构造函数
   * @param tag 计划标签（T_CreateTable、T_DropTable、T_CreateIndex、T_DropIndex）
   * @param tab_name 表名
   * @param col_names 列名向量
   * @param cols 列定义向量
   */
  DDLPlan(PlanTag tag, std::string tab_name, std::vector<std::string> col_names, std::vector<ColDef> cols) {
    Plan::tag = tag;
    tab_name_ = std::move(tab_name);
    cols_ = std::move(cols);
    tab_col_names_ = std::move(col_names);
  }
  
  /**
   * @brief 析构函数
   */
  ~DDLPlan() {}
  
  /** @brief 表名 */
  std::string tab_name_;
  
  /** @brief 表列名向量 */
  std::vector<std::string> tab_col_names_;
  
  /** @brief 列定义向量 */
  std::vector<ColDef> cols_;
};

/**
 * @brief LOAD DATA语句计划类
 * 
 * LoadDataPlan 表示从文件加载数据到表的计划。
 */
class LoadDataPlan : public Plan {
 public:
  /**
   * @brief 构造函数
   * @param tag 计划标签（T_LoadData）
   * @param file_name 数据文件名
   * @param tab_name 表名
   */
  LoadDataPlan(PlanTag tag, std::string file_name, std::string tab_name) {
    Plan::tag = tag;
    file_name_ = std::move(file_name);
    tab_name_ = std::move(tab_name);
  }
  
  /**
   * @brief 析构函数
   */
  ~LoadDataPlan() {}
  
  /** @brief 数据文件名 */
  std::string file_name_;
  
  /** @brief 表名 */
  std::string tab_name_;
};

/**
 * @brief 其他命令计划类
 * 
 * OtherPlan 表示其他类型的命令计划，包括：
 * HELP、SHOW TABLES、DESC TABLE、BEGIN、ABORT、COMMIT、ROLLBACK等。
 */
class OtherPlan : public Plan {
 public:
  /**
   * @brief 构造函数
   * @param tag 计划标签
   * @param tab_name 表名（某些命令需要）
   */
  OtherPlan(PlanTag tag, std::string tab_name) {
    Plan::tag = tag;
    tab_name_ = std::move(tab_name);
  }
  
  /**
   * @brief 析构函数
   */
  ~OtherPlan() {}
  
  /** @brief 表名 */
  std::string tab_name_;
};

/**
 * @brief SET KNOB命令计划类
 * 
 * SetKnobPlan 表示设置系统参数的命令计划。
 */
class SetKnobPlan : public Plan {
 public:
  /**
   * @brief 构造函数
   * @param knob_type 参数类型
   * @param bool_value 布尔值
   */
  SetKnobPlan(ast::SetKnobType knob_type, bool bool_value) {
    Plan::tag = T_SetKnob;
    set_knob_type_ = knob_type;
    bool_value_ = bool_value;
  }
  
  /** @brief 参数类型 */
  ast::SetKnobType set_knob_type_;
  
  /** @brief 布尔值 */
  bool bool_value_;
};

/**
 * @brief 计划器信息结构体
 * 
 * plannerInfo 存储查询计划过程中的中间信息，包括：
 * - 解析后的SELECT语句
 * - WHERE条件
 * - 选择的列
 * - 生成的计划
 * - 表扫描执行器列表
 * - SET子句列表
 */
class plannerInfo {
 public:
  /** @brief 解析后的SELECT语句 */
  std::shared_ptr<ast::SelectStmt> parse;
  
  /** @brief WHERE条件列表 */
  std::vector<Condition> where_conds;
  
  /** @brief 选择的列列表 */
  std::vector<TabCol> sel_cols;
  
  /** @brief 生成的查询计划 */
  std::shared_ptr<Plan> plan;
  
  /** @brief 表扫描执行器列表 */
  std::vector<std::shared_ptr<Plan>> table_scan_executors;
  
  /** @brief SET子句列表 */
  std::vector<SetClause> set_clauses;
  
  /**
   * @brief 构造函数
   * @param parse_ 解析后的SELECT语句指针
   */
  plannerInfo(std::shared_ptr<ast::SelectStmt> parse_) : parse(std::move(parse_)) {}
};
};  // namespace easydb