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

#include "common/common.h"
#include "common/condition.h"
#include "parser/parser.h"
#include "system/sm.h"

namespace easydb {

class Query {
 public:
  std::shared_ptr<ast::TreeNode> parse;
  // TODO jointree
  // where条件
  std::vector<Condition> conds;
  // 投影列
  std::vector<TabCol> cols;
  // 表名
  std::vector<std::string> tables;
  // update 的set 值
  std::vector<SetClause> set_clauses;
  // insert 的values值
  std::vector<Value> values;
  // group by条件
  std::vector<TabCol> groupby_cols;
  // having条件
  std::vector<Condition> having_conds;

  Query() {}
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
    }
  }
};
/*
当SQL语句经过语法解析模块的处理，获得抽象语法树之后，进⼊分析器analyze，在分析器中需要进⾏语义分
析，包括表是否存在、字段是否存在等，并把easydb改写成Query
*/
class Analyze {
 private:
  SmManager *sm_manager_;

 public:
  Analyze(SmManager *sm_manager) : sm_manager_(sm_manager) {}
  ~Analyze() {}

  std::shared_ptr<Query> do_analyze(std::shared_ptr<ast::TreeNode> root);

 private:
  TabCol check_column(const std::vector<ColMeta> &all_cols,
                      TabCol target);  // fill col's table name by traversing all_cols
  void get_all_cols(const std::vector<std::string> &tab_names, std::vector<ColMeta> &all_cols);
  void get_clause(const std::vector<std::shared_ptr<ast::BinaryExpr>> &sv_conds, std::vector<Condition> &conds);
  void check_clause(const std::vector<std::string> &tab_names, std::vector<Condition> &conds);
  bool find_col(std::vector<std::shared_ptr<ast::Col>> &cols,
                std::string col_name);                                            // check if col_name is find in cols
  bool check_aggregation_legality(const std::shared_ptr<ast::SelectStmt> &x);  // 聚集函数相关的合法性
  Value init_sv_value(const std::shared_ptr<ast::Value> &sv_val);
  Value convert_sv_value(const std::shared_ptr<ast::Value> &sv_val);
  CompOp convert_sv_comp_op(ast::SvCompOp op);
  ArithOp convert_sv_arith_op(ast::SvArithOp op);
};
};  // namespace easydb