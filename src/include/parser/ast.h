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

#include <memory>
#include <string>
#include <vector>
#include "defs.h"

enum JoinType { INNER_JOIN, LEFT_JOIN, RIGHT_JOIN, FULL_JOIN };

namespace ast {

enum SvType { SV_TYPE_INT, SV_TYPE_FLOAT, SV_TYPE_STRING, SV_TYPE_BOOL, SV_TYPE_DATETIME };

enum SvCompOp { SV_OP_EQ, SV_OP_NE, SV_OP_LT, SV_OP_GT, SV_OP_LE, SV_OP_GE, SV_OP_IN };

enum SvArithOp { SV_OP_PLUS, SV_OP_MINUS, SV_OP_MUL, SV_OP_DIV };

enum OrderByDir { OrderBy_DEFAULT, OrderBy_ASC, OrderBy_DESC };

enum SetKnobType { EnableNestLoop, EnableSortMerge, EnableOutput };

// Base class for tree nodes
struct TreeNode {
  virtual ~TreeNode() = default;  // enable polymorphism
};

struct Help : public TreeNode {};

struct ShowTables : public TreeNode {};

struct ShowIndex : public TreeNode {
  std::string tab_name;

  ShowIndex(std::string tab_name_) : tab_name(std::move(tab_name_)) {}
};

struct TxnBegin : public TreeNode {};

struct TxnCommit : public TreeNode {};

struct TxnAbort : public TreeNode {};

struct TxnRollback : public TreeNode {};

struct TypeLen : public TreeNode {
  SvType type;
  int len;

  TypeLen(SvType type_, int len_) : type(type_), len(len_) {}
};

struct Field : public TreeNode {};

struct ColDef : public Field {
  std::string col_name;
  std::shared_ptr<TypeLen> type_len;
  bool not_null;
  ColDef(std::string col_name_, std::shared_ptr<TypeLen> type_len_)
      : col_name(std::move(col_name_)), type_len(std::move(type_len_)), not_null(true) {}

  ColDef(std::string col_name_, std::shared_ptr<TypeLen> type_len_, bool not_null_)
      : col_name(std::move(col_name_)), type_len(std::move(type_len_)), not_null(not_null_) {}
};

struct CreateTable : public TreeNode {
  std::string tab_name;
  std::vector<std::shared_ptr<Field>> fields;

  CreateTable(std::string tab_name_, std::vector<std::shared_ptr<Field>> fields_)
      : tab_name(std::move(tab_name_)), fields(std::move(fields_)) {}
};

struct DropTable : public TreeNode {
  std::string tab_name;

  DropTable(std::string tab_name_) : tab_name(std::move(tab_name_)) {}
};

struct DescTable : public TreeNode {
  std::string tab_name;

  DescTable(std::string tab_name_) : tab_name(std::move(tab_name_)) {}
};

struct CreateIndex : public TreeNode {
  std::string tab_name;
  std::vector<std::string> col_names;

  CreateIndex(std::string tab_name_, std::vector<std::string> col_names_)
      : tab_name(std::move(tab_name_)), col_names(std::move(col_names_)) {}
};

struct DropIndex : public TreeNode {
  std::string tab_name;
  std::vector<std::string> col_names;

  DropIndex(std::string tab_name_, std::vector<std::string> col_names_)
      : tab_name(std::move(tab_name_)), col_names(std::move(col_names_)) {}
};

struct CreateStaticCheckpoint : public TreeNode {};

struct LoadData : public TreeNode {
  std::string file_name;
  std::string tab_name;

  LoadData(std::string file_name_, std::string tab_name_)
      : file_name(std::move(file_name_)), tab_name(std::move(tab_name_)) {}
};

struct Expr : public TreeNode {};

struct Value : public Expr {};

struct IntLit : public Value {
  int val;

  IntLit(int val_) : val(val_) {}
};

struct FloatLit : public Value {
  float val;

  FloatLit(float val_) : val(val_) {}
};

struct StringLit : public Value {
  std::string val;

  StringLit(std::string val_) : val(std::move(val_)) {}
};

struct BoolLit : public Value {
  bool val;

  BoolLit(bool val_) : val(val_) {}
};

struct Col : public Expr {
  std::string tab_name;
  std::string col_name;
  std::string new_col_name;
  AggregationType aggregation_type;

  Col(std::string tab_name_, std::string col_name_, std::string new_col_name_, AggregationType aggregation_type_)
      : tab_name(std::move(tab_name_)),
        col_name(std::move(col_name_)),
        new_col_name(std::move(new_col_name_)),
        aggregation_type(aggregation_type_) {}
};
struct ArithExpr : public TreeNode {
  std::string lhs;
  SvArithOp op;
  std::shared_ptr<Value> rhs;

  ArithExpr(std::string lhs_, SvArithOp op_, std::shared_ptr<Value> rhs_)
      : lhs(std::move(lhs_)), op(op_), rhs(std::move(rhs_)) {}
};

struct SetClause : public TreeNode {
  std::string col_name;
  std::shared_ptr<Value> val;
  std::shared_ptr<ArithExpr> rhs_expr;

  SetClause(std::string col_name_, std::shared_ptr<Value> val_)
      : col_name(std::move(col_name_)), val(std::move(val_)), rhs_expr(nullptr) {}
  SetClause(std::string col_name_, std::shared_ptr<ArithExpr> rhs_expr_)
      : col_name(std::move(col_name_)), val(nullptr), rhs_expr(std::move(rhs_expr_)) {}
};

struct BinaryExpr : public TreeNode {
  std::shared_ptr<Col> lhs;
  SvCompOp op;
  std::shared_ptr<TreeNode> rhs;
  std::vector<std::shared_ptr<Value>> rhs_value_list;

  BinaryExpr(std::shared_ptr<Col> lhs_, SvCompOp op_, std::shared_ptr<TreeNode> rhs_)
      : lhs(std::move(lhs_)), op(op_), rhs(std::move(rhs_)) {}
  BinaryExpr(std::shared_ptr<Col> lhs_, SvCompOp op_, std::vector<std::shared_ptr<Value>> rhs_value_list_)
      : lhs(std::move(lhs_)), op(op_), rhs(nullptr), rhs_value_list(std::move(rhs_value_list_)) {}
};

struct GroupBy : public TreeNode {
  std::vector<std::shared_ptr<Col>> cols;  // allow group by several cols
  GroupBy(std::vector<std::shared_ptr<Col>> cols_) : cols(cols_) {}
};

struct OrderBy : public TreeNode {
  std::shared_ptr<Col> cols;
  OrderByDir orderby_dir;
  OrderBy(std::shared_ptr<Col> cols_, OrderByDir orderby_dir_)
      : cols(std::move(cols_)), orderby_dir(std::move(orderby_dir_)) {}
};

struct InsertStmt : public TreeNode {
  std::string tab_name;
  std::vector<std::shared_ptr<Value>> vals;

  InsertStmt(std::string tab_name_, std::vector<std::shared_ptr<Value>> vals_)
      : tab_name(std::move(tab_name_)), vals(std::move(vals_)) {}
};

struct DeleteStmt : public TreeNode {
  std::string tab_name;
  std::vector<std::shared_ptr<BinaryExpr>> conds;

  DeleteStmt(std::string tab_name_, std::vector<std::shared_ptr<BinaryExpr>> conds_)
      : tab_name(std::move(tab_name_)), conds(std::move(conds_)) {}
};

struct UpdateStmt : public TreeNode {
  std::string tab_name;
  std::vector<std::shared_ptr<SetClause>> set_clauses;
  std::vector<std::shared_ptr<BinaryExpr>> conds;

  UpdateStmt(std::string tab_name_, std::vector<std::shared_ptr<SetClause>> set_clauses_,
             std::vector<std::shared_ptr<BinaryExpr>> conds_)
      : tab_name(std::move(tab_name_)), set_clauses(std::move(set_clauses_)), conds(std::move(conds_)) {}
};

struct JoinExpr : public TreeNode {
  std::string left;
  std::string right;
  std::vector<std::shared_ptr<BinaryExpr>> conds;
  JoinType type;

  JoinExpr(std::string left_, std::string right_, std::vector<std::shared_ptr<BinaryExpr>> conds_, JoinType type_)
      : left(std::move(left_)), right(std::move(right_)), conds(std::move(conds_)), type(type_) {}
};

struct SelectStmt : public TreeNode {
  std::vector<std::shared_ptr<Col>> cols;
  std::vector<std::string> tabs;
  std::vector<std::shared_ptr<BinaryExpr>> conds;
  std::vector<std::shared_ptr<JoinExpr>> jointree;

  bool has_sort;
  bool has_group;
  bool has_having;

  std::shared_ptr<OrderBy> order;
  std::shared_ptr<GroupBy> group;
  std::vector<std::shared_ptr<BinaryExpr>> having;

  SelectStmt() : has_sort(false), has_group(false), has_having(false), order(nullptr), group(nullptr) {}
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
      this->order = selectStmtPtr->order;
      this->group = selectStmtPtr->group;
      this->having = selectStmtPtr->having;
    }
  }
  SelectStmt(std::vector<std::shared_ptr<Col>> cols_, std::vector<std::string> tabs_,
             std::vector<std::shared_ptr<BinaryExpr>> conds_, std::shared_ptr<GroupBy> group_,
             std::vector<std::shared_ptr<BinaryExpr>> having_, std::shared_ptr<OrderBy> order_)
      : cols(std::move(cols_)),
        tabs(std::move(tabs_)),
        conds(std::move(conds_)),
        order(std::move(order_)),
        group(std::move(group_)),
        having(std::move(having_)) {
    has_sort = (bool)order;
    has_group = (bool)group;
    has_having = having.size();
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
      this->order = rhs->order;
      this->group = rhs->group;
      this->having = rhs->having;
    }
    return *this;
  }
};

// set enable_nestloop
struct SetStmt : public TreeNode {
  SetKnobType set_knob_type_;
  bool bool_val_;

  SetStmt(SetKnobType &type, bool bool_value) : set_knob_type_(type), bool_val_(bool_value) {}
};

// Semantic value
struct SemValue {
  int sv_int;
  float sv_float;
  std::string sv_str;
  bool sv_bool;
  OrderByDir sv_orderby_dir;
  std::vector<std::string> sv_strs;

  std::shared_ptr<TreeNode> sv_node;

  SvCompOp sv_comp_op;
  SvArithOp sv_arith_op;

  std::shared_ptr<TypeLen> sv_type_len;

  std::shared_ptr<Field> sv_field;
  std::vector<std::shared_ptr<Field>> sv_fields;

  std::shared_ptr<Expr> sv_expr;
  std::shared_ptr<ArithExpr> sv_arith_expr;

  std::shared_ptr<Value> sv_val;
  std::vector<std::shared_ptr<Value>> sv_vals;

  std::shared_ptr<Col> sv_col;
  std::vector<std::shared_ptr<Col>> sv_cols;

  std::shared_ptr<SetClause> sv_set_clause;
  std::vector<std::shared_ptr<SetClause>> sv_set_clauses;

  std::shared_ptr<BinaryExpr> sv_cond;
  std::vector<std::shared_ptr<BinaryExpr>> sv_conds;

  std::shared_ptr<GroupBy> sv_groupby;
  std::vector<std::shared_ptr<BinaryExpr>> sv_having;

  std::shared_ptr<OrderBy> sv_orderby;

  SetKnobType sv_setKnobType;
};

extern std::shared_ptr<ast::TreeNode> parse_tree;

}  // namespace ast

#define YYSTYPE ast::SemValue
