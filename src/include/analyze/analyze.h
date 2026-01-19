/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL
v2. You may obtain a copy of Mulan PSL v2 at:
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

#include "common/common.h"
#include "common/condition.h"
#include "parser/parser.h"
#include "system/sm.h"

namespace easydb {

/**
 * @brief 查询结构体
 *
 * Query 存储经过语义分析后的查询信息，将AST转换为执行器可用的格式。
 * 包含查询的所有组成部分：表、列、条件、聚合、排序等。
 */
class Query {
 public:
  /**
   * @brief 标记查询结果为空
   * @note ADDED: 用于某些特殊情况（如WHERE条件永远为false）
   */
  bool no_result = false;

  /**
   * @brief 解析后的AST根节点
   */
  std::shared_ptr<ast::TreeNode> parse;
  // TODO jointree

  /**
   * @brief WHERE条件列表
   */
  std::vector<Condition> conds;

  /**
   * @brief 投影列列表
   */
  std::vector<TabCol> cols;

  /**
   * @brief 表名列表
   */
  std::vector<std::string> tables;

  /**
   * @brief UPDATE的SET值列表
   */
  std::vector<SetClause> set_clauses;

  /**
   * @brief INSERT的VALUES值列表
   */
  std::vector<Value> values;

  /**
   * @brief GROUP BY条件列表
   */
  std::vector<TabCol> groupby_cols;

  /**
   * @brief HAVING条件列表
   */
  std::vector<Condition> having_conds;

  /**
   * @brief 是否有SELECT DISTINCT语句
   */
  bool is_unique = false;

  /**
   * @brief 在逻辑优化后确定的表连接顺序（连接重排后使用）
   */
  std::vector<std::string> optimized_table_order;

  /**
   * @brief 默认构造函数
   */
  Query() {}

  /**
   * @brief 从shared_ptr构造Query
   * @param ptr 指向Query对象的shared_ptr
   */
  Query(std::shared_ptr<void> &ptr) {
    auto queryPtr = std::static_pointer_cast<Query>(ptr);
    if (queryPtr) {
      this->parse = queryPtr->parse;
      this->conds = queryPtr->conds;
      this->cols = queryPtr->cols;
      this->tables = queryPtr->tables;
      this->set_clauses = queryPtr->set_clauses;
      this->values = queryPtr->values;
      this->groupby_cols = queryPtr->groupby_cols;
      this->having_conds = queryPtr->having_conds;
      this->is_unique = queryPtr->is_unique;
      this->optimized_table_order = queryPtr->optimized_table_order;
    }
  }
};

/**
 * @brief 语义分析器类
 *
 * 当SQL语句经过语法解析模块的处理，获得抽象语法树之后，进入分析器analyze。
 * 在分析器中需要进行语义分析，包括：
 * - 表是否存在
 * - 字段是否存在
 * - 类型是否匹配
 * - 聚合函数的合法性检查
 *
 * 并将AST改写成Query结构，供后续的查询计划器使用。
 */
class Analyze {
 private:
  /**
   * @brief 系统管理器指针
   * @note 用于访问数据库元数据，检查表和列是否存在
   */
  SmManager *sm_manager_;

 public:
  /**
   * @brief 构造函数
   * @param sm_manager 系统管理器指针
   */
  Analyze(SmManager *sm_manager) : sm_manager_(sm_manager) {}

  /**
   * @brief 析构函数
   */
  ~Analyze() {}

  /**
   * @brief 执行语义分析
   * @param root AST根节点
   * @return 分析后的Query对象指针
   * @note 这是主要的分析入口，根据AST节点类型调用相应的分析方法
   */
  std::shared_ptr<Query> do_analyze(std::shared_ptr<ast::TreeNode> root);

 private:
  /**
   * @brief 检查列是否存在并填充表名
   * @param all_cols 所有列的元数据向量
   * @param target 目标列标识
   * @return 填充了表名的列标识
   * @note 通过遍历all_cols来填充列的表名（如果列名唯一）
   */
  TabCol check_column(const std::vector<ColMeta> &all_cols, TabCol target);

  /**
   * @brief 获取所有表的列元数据
   * @param tab_names 表名列表
   * @param[out] all_cols 输出参数，存储所有列的元数据
   */
  void get_all_cols(const std::vector<std::string> &tab_names,
                    std::vector<ColMeta> &all_cols);

  /**
   * @brief 将AST条件转换为系统条件
   * @param sv_conds AST条件列表
   * @param[out] conds 输出参数，存储转换后的条件列表
   */
  void get_clause(const std::vector<std::shared_ptr<ast::BinaryExpr>> &sv_conds,
                  std::vector<Condition> &conds);

  /**
   * @brief 检查条件的合法性
   * @param tab_names 表名列表
   * @param[in,out] conds 条件列表（会被修改，填充表名等信息）
   * @note 检查条件中引用的表和列是否存在
   */
  void check_clause(const std::vector<std::string> &tab_names,
                    std::vector<Condition> &conds);

  /**
   * @brief 在列列表中查找指定列名
   * @param cols 列列表
   * @param col_name 要查找的列名
   * @return true 如果找到，false 否则
   */
  bool find_col(std::vector<std::shared_ptr<ast::Col>> &cols,
                std::string col_name);

  /**
   * @brief 检查聚合函数的合法性
   * @param x SELECT语句节点
   * @return true 如果合法，false 否则
   * @note
   * 检查聚合函数的使用是否符合SQL规范（如SELECT中不能混合聚合列和非聚合列等）
   */
  bool check_aggregation_legality(const std::shared_ptr<ast::SelectStmt> &x);

  /**
   * @brief 初始化语义值
   * @param sv_val AST值节点
   * @return 系统Value对象
   */
  Value init_sv_value(const std::shared_ptr<ast::Value> &sv_val);

  /**
   * @brief 转换语义值为系统Value
   * @param sv_val AST值节点
   * @return 系统Value对象
   */
  Value convert_sv_value(const std::shared_ptr<ast::Value> &sv_val);

  /**
   * @brief 转换AST比较运算符为系统比较运算符
   * @param op AST比较运算符
   * @return 系统比较运算符
   */
  CompOp convert_sv_comp_op(ast::SvCompOp op);

  /**
   * @brief 转换AST算术运算符为系统算术运算符
   * @param op AST算术运算符
   * @return 系统算术运算符
   */
  ArithOp convert_sv_arith_op(ast::SvArithOp op);
};
};  // namespace easydb