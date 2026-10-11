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

#include <memory>
#include <string>
#include <vector>
#include "defs.h"

/**
 * @brief 连接类型枚举
 * @note 定义了SQL中支持的连接类型
 */
enum JoinType {
  INNER_JOIN, /**< 内连接 */
  LEFT_JOIN,  /**< 左外连接 */
  RIGHT_JOIN, /**< 右外连接 */
  FULL_JOIN   /**< 全外连接 */
};

namespace ast {

/**
 * @brief 语义值类型枚举
 * @note 用于词法分析器和语法分析器中的语义值类型
 */
enum SvType {
  SV_TYPE_INT,     /**< 整数类型 */
  SV_TYPE_FLOAT,   /**< 浮点数类型 */
  SV_TYPE_STRING,  /**< 字符串类型 */
  SV_TYPE_BOOL,    /**< 布尔类型 */
  SV_TYPE_DATETIME /**< 日期时间类型 */
};

/**
 * @brief 比较运算符枚举
 * @note 用于WHERE子句和连接条件中的比较操作
 */
enum SvCompOp {
  SV_OP_EQ, /**< 等于（=） */
  SV_OP_NE, /**< 不等于（!=） */
  SV_OP_LT, /**< 小于（<） */
  SV_OP_GT, /**< 大于（>） */
  SV_OP_LE, /**< 小于等于（<=） */
  SV_OP_GE, /**< 大于等于（>=） */
  SV_OP_IN  /**< IN操作符 */
};

/**
 * @brief 算术运算符枚举
 * @note 用于算术表达式中
 */
enum SvArithOp {
  SV_OP_PLUS,  /**< 加法（+） */
  SV_OP_MINUS, /**< 减法（-） */
  SV_OP_MUL,   /**< 乘法（*） */
  SV_OP_DIV    /**< 除法（/） */
};

/**
 * @brief ORDER BY排序方向枚举
 */
enum OrderByDir {
  OrderBy_DEFAULT, /**< 默认（升序） */
  OrderBy_ASC,     /**< 升序 */
  OrderBy_DESC     /**< 降序 */
};

/**
 * @brief SET KNOB命令类型枚举
 * @note 用于设置系统参数
 */
enum SetKnobType {
  EnableNestLoop,  /**< 启用嵌套循环连接 */
  EnableSortMerge, /**< 启用排序归并连接 */
  EnableHashJoin,  /**< 启用哈希连接 */
  EnableOutput,    /**< 启用输出 */
  EnableOptimizer  /**< 启用优化器 */
};

/**
 * @brief 抽象语法树节点基类
 *
 * TreeNode 是所有AST节点的基类，使用虚析构函数支持多态。
 * 所有SQL语句的AST节点都继承自此类。
 */
struct TreeNode {
  /**
   * @brief 虚析构函数
   * @note 支持多态，确保派生类对象正确析构
   */
  virtual ~TreeNode() = default;
};

/**
 * @brief HELP命令节点
 */
struct Help : public TreeNode {};

/**
 * @brief SHOW TABLES命令节点
 */
struct ShowTables : public TreeNode {};

/**
 * @brief SHOW INDEX命令节点
 */
struct ShowIndex : public TreeNode {
  /** @brief 表名 */
  std::string tab_name;

  /**
   * @brief 构造函数
   * @param tab_name_ 表名
   */
  ShowIndex(std::string tab_name_) : tab_name(std::move(tab_name_)) {}
};

/**
 * @brief BEGIN TRANSACTION命令节点
 */
struct TxnBegin : public TreeNode {};

/**
 * @brief COMMIT TRANSACTION命令节点
 */
struct TxnCommit : public TreeNode {};

/**
 * @brief ABORT TRANSACTION命令节点
 */
struct TxnAbort : public TreeNode {};

/**
 * @brief ROLLBACK TRANSACTION命令节点
 */
struct TxnRollback : public TreeNode {};

/**
 * @brief 类型和长度节点
 * @note 用于列定义中指定数据类型和长度
 */
struct TypeLen : public TreeNode {
  /** @brief 类型 */
  SvType type;

  /** @brief 长度（对于变长类型） */
  int len;

  /**
   * @brief 构造函数
   * @param type_ 类型
   * @param len_ 长度
   */
  TypeLen(SvType type_, int len_) : type(type_), len(len_) {}
};

/**
 * @brief 字段节点基类
 */
struct Field : public TreeNode {};

/**
 * @brief 列定义节点
 * @note 用于CREATE TABLE语句中定义列
 */
struct ColDef : public Field {
  /** @brief 列名 */
  std::string col_name;

  /** @brief 类型和长度 */
  std::shared_ptr<TypeLen> type_len;

  /** @brief 是否非空标志 */
  bool not_null;

  /**
   * @brief 构造函数（默认非空）
   * @param col_name_ 列名
   * @param type_len_ 类型和长度
   */
  ColDef(std::string col_name_, std::shared_ptr<TypeLen> type_len_)
      : col_name(std::move(col_name_)),
        type_len(std::move(type_len_)),
        not_null(true) {}

  /**
   * @brief 构造函数（指定非空标志）
   * @param col_name_ 列名
   * @param type_len_ 类型和长度
   * @param not_null_ 是否非空
   */
  ColDef(std::string col_name_, std::shared_ptr<TypeLen> type_len_,
         bool not_null_)
      : col_name(std::move(col_name_)),
        type_len(std::move(type_len_)),
        not_null(not_null_) {}
};

/**
 * @brief CREATE TABLE语句节点
 */
struct CreateTable : public TreeNode {
  /** @brief 表名 */
  std::string tab_name;

  /** @brief 字段列表 */
  std::vector<std::shared_ptr<Field>> fields;

  /**
   * @brief 构造函数
   * @param tab_name_ 表名
   * @param fields_ 字段列表
   */
  CreateTable(std::string tab_name_,
              std::vector<std::shared_ptr<Field>> fields_)
      : tab_name(std::move(tab_name_)), fields(std::move(fields_)) {}
};

/**
 * @brief DROP TABLE语句节点
 */
struct DropTable : public TreeNode {
  /** @brief 表名 */
  std::string tab_name;

  /**
   * @brief 构造函数
   * @param tab_name_ 表名
   */
  DropTable(std::string tab_name_) : tab_name(std::move(tab_name_)) {}
};

/**
 * @brief DESC TABLE命令节点
 */
struct DescTable : public TreeNode {
  /** @brief 表名 */
  std::string tab_name;

  /**
   * @brief 构造函数
   * @param tab_name_ 表名
   */
  DescTable(std::string tab_name_) : tab_name(std::move(tab_name_)) {}
};

/**
 * @brief CREATE INDEX语句节点
 */
struct CreateIndex : public TreeNode {
  /** @brief 表名 */
  std::string tab_name;

  /** @brief 列名列表 */
  std::vector<std::string> col_names;

  /**
   * @brief 构造函数
   * @param tab_name_ 表名
   * @param col_names_ 列名列表
   */
  CreateIndex(std::string tab_name_, std::vector<std::string> col_names_)
      : tab_name(std::move(tab_name_)), col_names(std::move(col_names_)) {}
};

/**
 * @brief DROP INDEX语句节点
 */
struct DropIndex : public TreeNode {
  /** @brief 表名 */
  std::string tab_name;

  /** @brief 列名列表 */
  std::vector<std::string> col_names;

  /**
   * @brief 构造函数
   * @param tab_name_ 表名
   * @param col_names_ 列名列表
   */
  DropIndex(std::string tab_name_, std::vector<std::string> col_names_)
      : tab_name(std::move(tab_name_)), col_names(std::move(col_names_)) {}
};

/**
 * @brief CREATE CHECKPOINT命令节点
 */
struct CreateStaticCheckpoint : public TreeNode {};

/**
 * @brief LOAD DATA语句节点
 */
struct LoadData : public TreeNode {
  /** @brief 数据文件名 */
  std::string file_name;

  /** @brief 表名 */
  std::string tab_name;

  /**
   * @brief 构造函数
   * @param file_name_ 数据文件名
   * @param tab_name_ 表名
   */
  LoadData(std::string file_name_, std::string tab_name_)
      : file_name(std::move(file_name_)), tab_name(std::move(tab_name_)) {}
};

/**
 * @brief 表达式节点基类
 */
struct Expr : public TreeNode {};

/**
 * @brief 值节点基类
 */
struct Value : public Expr {};

/**
 * @brief 整数字面量节点
 */
struct IntLit : public Value {
  /** @brief 整数值 */
  int val;

  /**
   * @brief 构造函数
   * @param val_ 整数值
   */
  IntLit(int val_) : val(val_) {}
};

/**
 * @brief 浮点数字面量节点
 */
struct FloatLit : public Value {
  /** @brief 浮点数值 */
  float val;

  /**
   * @brief 构造函数
   * @param val_ 浮点数值
   */
  FloatLit(float val_) : val(val_) {}
};

/**
 * @brief 字符串字面量节点
 */
struct StringLit : public Value {
  /** @brief 字符串值 */
  std::string val;

  /**
   * @brief 构造函数
   * @param val_ 字符串值
   */
  StringLit(std::string val_) : val(std::move(val_)) {}
};

/**
 * @brief 布尔字面量节点
 */
struct BoolLit : public Value {
  /** @brief 布尔值 */
  bool val;

  /**
   * @brief 构造函数
   * @param val_ 布尔值
   */
  BoolLit(bool val_) : val(val_) {}
};

/**
 * @brief 列节点
 * @note 用于SELECT、WHERE、GROUP BY等子句中引用列
 */
struct Col : public Expr {
  /** @brief 表名 */
  std::string tab_name;

  /** @brief 列名 */
  std::string col_name;

  /** @brief 新列名（用于SELECT中的别名） */
  std::string new_col_name;

  /** @brief 聚合类型（如果列包含聚合函数） */
  AggregationType aggregation_type;

  /**
   * @brief 构造函数
   * @param tab_name_ 表名
   * @param col_name_ 列名
   * @param new_col_name_ 新列名（别名）
   * @param aggregation_type_ 聚合类型
   */
  Col(std::string tab_name_, std::string col_name_, std::string new_col_name_,
      AggregationType aggregation_type_)
      : tab_name(std::move(tab_name_)),
        col_name(std::move(col_name_)),
        new_col_name(std::move(new_col_name_)),
        aggregation_type(aggregation_type_) {}
};

/**
 * @brief 算术表达式节点
 * @note 用于UPDATE语句中的SET子句，如 SET col = col + 1
 */
struct ArithExpr : public TreeNode {
  /** @brief 左侧列名 */
  std::string lhs;

  /** @brief 算术运算符 */
  SvArithOp op;

  /** @brief 右侧值 */
  std::shared_ptr<Value> rhs;

  /**
   * @brief 构造函数
   * @param lhs_ 左侧列名
   * @param op_ 算术运算符
   * @param rhs_ 右侧值
   */
  ArithExpr(std::string lhs_, SvArithOp op_, std::shared_ptr<Value> rhs_)
      : lhs(std::move(lhs_)), op(op_), rhs(std::move(rhs_)) {}
};

/**
 * @brief SET子句节点
 * @note 用于UPDATE语句中指定要更新的列和新值
 */
struct SetClause : public TreeNode {
  /** @brief 列名 */
  std::string col_name;

  /** @brief 值（如果直接赋值） */
  std::shared_ptr<Value> val;

  /** @brief 算术表达式（如果使用表达式赋值） */
  std::shared_ptr<ArithExpr> rhs_expr;

  /**
   * @brief 构造函数（直接赋值）
   * @param col_name_ 列名
   * @param val_ 值
   */
  SetClause(std::string col_name_, std::shared_ptr<Value> val_)
      : col_name(std::move(col_name_)),
        val(std::move(val_)),
        rhs_expr(nullptr) {}

  /**
   * @brief 构造函数（表达式赋值）
   * @param col_name_ 列名
   * @param rhs_expr_ 算术表达式
   */
  SetClause(std::string col_name_, std::shared_ptr<ArithExpr> rhs_expr_)
      : col_name(std::move(col_name_)),
        val(nullptr),
        rhs_expr(std::move(rhs_expr_)) {}
};

/**
 * @brief 二元表达式节点（条件表达式）
 * @note 用于WHERE、HAVING、JOIN ON等子句中的条件
 */
struct BinaryExpr : public TreeNode {
  /** @brief 左侧列 */
  std::shared_ptr<Col> lhs;

  /** @brief 比较运算符 */
  SvCompOp op;

  /** @brief 右侧节点（可以是值、列或子查询） */
  std::shared_ptr<TreeNode> rhs;

  /** @brief 右侧值列表（用于IN操作） */
  std::vector<std::shared_ptr<Value>> rhs_value_list;

  /**
   * @brief 构造函数（列 vs 值/列/子查询）
   * @param lhs_ 左侧列
   * @param op_ 比较运算符
   * @param rhs_ 右侧节点
   */
  BinaryExpr(std::shared_ptr<Col> lhs_, SvCompOp op_,
             std::shared_ptr<TreeNode> rhs_)
      : lhs(std::move(lhs_)), op(op_), rhs(std::move(rhs_)) {}

  /**
   * @brief 构造函数（列 IN 值列表）
   * @param lhs_ 左侧列
   * @param op_ 比较运算符（SV_OP_IN）
   * @param rhs_value_list_ 右侧值列表
   */
  BinaryExpr(std::shared_ptr<Col> lhs_, SvCompOp op_,
             std::vector<std::shared_ptr<Value>> rhs_value_list_)
      : lhs(std::move(lhs_)),
        op(op_),
        rhs(nullptr),
        rhs_value_list(std::move(rhs_value_list_)) {}
};

/**
 * @brief GROUP BY子句节点
 * @note 允许按多个列分组
 */
struct GroupBy : public TreeNode {
  /**
   * @brief 分组列列表
   * @note 允许按多个列分组
   */
  std::vector<std::shared_ptr<Col>> cols;

  /**
   * @brief 构造函数
   * @param cols_ 分组列列表
   */
  GroupBy(std::vector<std::shared_ptr<Col>> cols_) : cols(cols_) {}
};

/**
 * @brief ORDER BY子句节点
 */
struct OrderBy : public TreeNode {
  /** @brief 排序的列 */
  std::shared_ptr<Col> cols;

  /** @brief 排序方向 */
  OrderByDir orderby_dir;

  /**
   * @brief 构造函数
   * @param cols_ 排序的列
   * @param orderby_dir_ 排序方向
   */
  OrderBy(std::shared_ptr<Col> cols_, OrderByDir orderby_dir_)
      : cols(std::move(cols_)), orderby_dir(std::move(orderby_dir_)) {}
};

/**
 * @brief INSERT语句节点
 */
struct InsertStmt : public TreeNode {
  /** @brief 表名 */
  std::string tab_name;

  /** @brief 值列表 */
  std::vector<std::shared_ptr<Value>> vals;

  /**
   * @brief 构造函数
   * @param tab_name_ 表名
   * @param vals_ 值列表
   */
  InsertStmt(std::string tab_name_, std::vector<std::shared_ptr<Value>> vals_)
      : tab_name(std::move(tab_name_)), vals(std::move(vals_)) {}
};

/**
 * @brief DELETE语句节点
 */
struct DeleteStmt : public TreeNode {
  /** @brief 表名 */
  std::string tab_name;

  /** @brief 条件列表 */
  std::vector<std::shared_ptr<BinaryExpr>> conds;

  /**
   * @brief 构造函数
   * @param tab_name_ 表名
   * @param conds_ 条件列表
   */
  DeleteStmt(std::string tab_name_,
             std::vector<std::shared_ptr<BinaryExpr>> conds_)
      : tab_name(std::move(tab_name_)), conds(std::move(conds_)) {}
};

/**
 * @brief UPDATE语句节点
 */
struct UpdateStmt : public TreeNode {
  /** @brief 表名 */
  std::string tab_name;

  /** @brief SET子句列表 */
  std::vector<std::shared_ptr<SetClause>> set_clauses;

  /** @brief 条件列表 */
  std::vector<std::shared_ptr<BinaryExpr>> conds;

  /**
   * @brief 构造函数
   * @param tab_name_ 表名
   * @param set_clauses_ SET子句列表
   * @param conds_ 条件列表
   */
  UpdateStmt(std::string tab_name_,
             std::vector<std::shared_ptr<SetClause>> set_clauses_,
             std::vector<std::shared_ptr<BinaryExpr>> conds_)
      : tab_name(std::move(tab_name_)),
        set_clauses(std::move(set_clauses_)),
        conds(std::move(conds_)) {}
};

/**
 * @brief JOIN表达式节点
 * @note 用于表示表之间的连接关系
 */
struct JoinExpr : public TreeNode {
  /** @brief 左表名 */
  std::string left;

  /** @brief 右表名 */
  std::string right;

  /** @brief 连接条件列表 */
  std::vector<std::shared_ptr<BinaryExpr>> conds;

  /** @brief 连接类型 */
  JoinType type;

  /**
   * @brief 构造函数
   * @param left_ 左表名
   * @param right_ 右表名
   * @param conds_ 连接条件列表
   * @param type_ 连接类型
   */
  JoinExpr(std::string left_, std::string right_,
           std::vector<std::shared_ptr<BinaryExpr>> conds_, JoinType type_)
      : left(std::move(left_)),
        right(std::move(right_)),
        conds(std::move(conds_)),
        type(type_) {}
};

/**
 * @brief SELECT语句节点
 * @note 表示完整的SELECT查询，包含所有子句
 */
struct SelectStmt : public TreeNode {
  /** @brief 选择的列列表 */
  std::vector<std::shared_ptr<Col>> cols;

  /** @brief 表名列表 */
  std::vector<std::string> tabs;

  /** @brief WHERE条件列表 */
  std::vector<std::shared_ptr<BinaryExpr>> conds;

  /** @brief JOIN表达式列表 */
  std::vector<std::shared_ptr<JoinExpr>> jointree;

  /** @brief 是否有ORDER BY子句 */
  bool has_sort;

  /** @brief 是否有GROUP BY子句 */
  bool has_group;

  /** @brief 是否有HAVING子句 */
  bool has_having;

  /** @brief 是否有SELECT DISTINCT */
  bool is_unique;

  /** @brief ORDER BY子句 */
  std::shared_ptr<OrderBy> order;

  /** @brief GROUP BY子句 */
  std::shared_ptr<GroupBy> group;

  /** @brief HAVING条件列表 */
  std::vector<std::shared_ptr<BinaryExpr>> having;

  SelectStmt()
      : has_sort(false),
        has_group(false),
        has_having(false),
        is_unique(false),
        order(nullptr),
        group(nullptr) {}
  SelectStmt(std::shared_ptr<void> &ptr) {
    auto selectStmtPtr = std::static_pointer_cast<SelectStmt>(ptr);
    if (selectStmtPtr) {
      this->cols = selectStmtPtr->cols;
      this->tabs = selectStmtPtr->tabs;
      this->conds = selectStmtPtr->conds;
      this->jointree = selectStmtPtr->jointree;
      this->has_sort = selectStmtPtr->has_sort;
      this->has_group = selectStmtPtr->has_group;
      this->has_having = selectStmtPtr->has_having;
      this->is_unique = selectStmtPtr->is_unique;
      this->order = selectStmtPtr->order;
      this->group = selectStmtPtr->group;
      this->having = selectStmtPtr->having;
    }
  }
  SelectStmt(std::vector<std::shared_ptr<Col>> cols_,
             std::vector<std::string> tabs_,
             std::vector<std::shared_ptr<BinaryExpr>> conds_,
             std::shared_ptr<GroupBy> group_,
             std::vector<std::shared_ptr<BinaryExpr>> having_,
             std::shared_ptr<OrderBy> order_, bool is_unique_ = false)
      : cols(std::move(cols_)),
        tabs(std::move(tabs_)),
        conds(std::move(conds_)),
        order(std::move(order_)),
        group(std::move(group_)),
        having(std::move(having_)) {
    has_sort = (bool)order;
    has_group = (bool)group;
    has_having = having.size();
    is_unique = is_unique_;
  }
  SelectStmt &operator=(const std::shared_ptr<SelectStmt> &rhs) {
    if (this != rhs.get()) {
      this->cols = rhs->cols;
      this->tabs = rhs->tabs;
      this->conds = rhs->conds;
      this->jointree = rhs->jointree;
      this->has_sort = rhs->has_sort;
      this->has_group = rhs->has_group;
      this->has_having = rhs->has_having;
      this->is_unique = rhs->is_unique;
      this->order = rhs->order;
      this->group = rhs->group;
      this->having = rhs->having;
    }
    return *this;
  }
};

/**
 * @brief SET语句节点
 * @note 用于设置系统参数，如 SET enable_nestloop = true
 */
struct SetStmt : public TreeNode {
  /** @brief SET KNOB类型 */
  SetKnobType set_knob_type_;

  /** @brief 布尔值 */
  bool bool_val_;

  /**
   * @brief 构造函数
   * @param type SET KNOB类型
   * @param bool_value 布尔值
   */
  SetStmt(SetKnobType &type, bool bool_value)
      : set_knob_type_(type), bool_val_(bool_value) {}
};

/**
 * @brief 语义值结构体
 *
 * SemValue 用于词法分析器和语法分析器之间传递语义值。
 * 包含所有可能的语义值类型，使用联合体的思想（但用结构体实现）。
 */
struct SemValue {
  /** @brief 整数值 */
  int sv_int;

  /** @brief 浮点数值 */
  float sv_float;

  /** @brief 字符串值 */
  std::string sv_str;

  /** @brief 布尔值 */
  bool sv_bool;

  /** @brief ORDER BY方向 */
  OrderByDir sv_orderby_dir;

  /** @brief 字符串向量 */
  std::vector<std::string> sv_strs;

  /** @brief 树节点指针 */
  std::shared_ptr<TreeNode> sv_node;

  /** @brief 比较运算符 */
  SvCompOp sv_comp_op;

  /** @brief 算术运算符 */
  SvArithOp sv_arith_op;

  /** @brief 类型和长度 */
  std::shared_ptr<TypeLen> sv_type_len;

  /** @brief 字段指针 */
  std::shared_ptr<Field> sv_field;

  /** @brief 字段向量 */
  std::vector<std::shared_ptr<Field>> sv_fields;

  /** @brief 表达式指针 */
  std::shared_ptr<Expr> sv_expr;

  /** @brief 算术表达式指针 */
  std::shared_ptr<ArithExpr> sv_arith_expr;

  /** @brief 值指针 */
  std::shared_ptr<Value> sv_val;

  /** @brief 值向量 */
  std::vector<std::shared_ptr<Value>> sv_vals;

  /** @brief 列指针 */
  std::shared_ptr<Col> sv_col;

  /** @brief 列向量 */
  std::vector<std::shared_ptr<Col>> sv_cols;

  /** @brief SET子句指针 */
  std::shared_ptr<SetClause> sv_set_clause;

  /** @brief SET子句向量 */
  std::vector<std::shared_ptr<SetClause>> sv_set_clauses;

  /** @brief 二元表达式（条件）指针 */
  std::shared_ptr<BinaryExpr> sv_cond;

  /** @brief 二元表达式（条件）向量 */
  std::vector<std::shared_ptr<BinaryExpr>> sv_conds;

  /** @brief GROUP BY子句指针 */
  std::shared_ptr<GroupBy> sv_groupby;

  /** @brief HAVING条件向量 */
  std::vector<std::shared_ptr<BinaryExpr>> sv_having;

  /** @brief ORDER BY子句指针 */
  std::shared_ptr<OrderBy> sv_orderby;

  /** @brief SET KNOB类型 */
  SetKnobType sv_setKnobType;
};

/**
 * @brief 解析树根节点
 * @note 全局变量，存储语法分析器生成的AST根节点
 */
extern std::shared_ptr<ast::TreeNode> parse_tree;

}  // namespace ast

#define YYSTYPE ast::SemValue
