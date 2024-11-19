/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "analyze/analyze.h"
using namespace easydb;

/**
 * @description: 分析器，进行语义分析和查询重写，需要检查不符合语义规定的部分
 * @param {shared_ptr<ast::TreeNode>} parse parser生成的结果集
 * @return {shared_ptr<Query>} Query
 */
std::shared_ptr<Query> Analyze::do_analyze(std::shared_ptr<ast::TreeNode> parse) {
  std::shared_ptr<Query> query = std::make_shared<Query>();
  // checks whether the cast was successful (i.e., whether x is not empty)
  if (auto x = std::dynamic_pointer_cast<ast::LoadData>(parse)) {
    // std::cout << "LoadData statement" << std::endl;
  } else {
    // std::cout << "Not a LoadData statement" << std::endl;
    if (sm_manager_->GetLoadStatus() == 0) {
      sm_manager_->AsyncLoadDataFinish();
    }
  }
  if (auto x = std::dynamic_pointer_cast<ast::SelectStmt>(parse)) {
    // 处理表名
    query->tables = std::move(x->tabs);
    /** TODO: 检查表是否存在 */
    for (auto tab_name : query->tables) {
      if (!sm_manager_->db_.is_table(tab_name)) {
        throw TableExistsError(tab_name);
      }
    }

    // 处理target list，再target list中添加上表名，例如 a.id
    for (auto &sv_sel_col : x->cols) {
      TabCol sel_col = {.tab_name = sv_sel_col->tab_name,
                        .col_name = sv_sel_col->col_name,
                        .aggregation_type = sv_sel_col->aggregation_type,
                        .new_col_name = sv_sel_col->new_col_name};
      // TabCol sel_col = {.tab_name = sv_sel_col->tab_name, .col_name = sv_sel_col->col_name};
      query->cols.push_back(sel_col);
    }

    std::vector<ColMeta> all_cols;
    get_all_cols(query->tables, all_cols);
    // not select any specific column(select *), then select all columns in all tables
    if (query->cols.empty()) {
      // select all columns
      for (auto &col : all_cols) {
        TabCol sel_col = {
            .tab_name = col.tab_name, .col_name = col.name, .aggregation_type = NO_AGG, .new_col_name = ""};
        query->cols.push_back(sel_col);
      }

    }
    // COUNT(*)
    //  else if (query->cols.size() == 1 && query->cols[0].col_name.empty() && query->cols[0].aggregation_type ==
    //  COUNT_AGG){
    //      query->cols[0].col_name = "*";
    //  }
    else {
      // infer table name from column name
      for (auto &sel_col : query->cols) {
        // check if sel_col.col_name legal and fill empty sel_col.tab_name
        // COUNT(*)
        if (sel_col.col_name.empty() && sel_col.aggregation_type == COUNT_AGG) {
          sel_col.col_name = "*";
        } else {
          sel_col = check_column(all_cols, sel_col);  // 列元数据校验
        }
      }
    }
    // 处理where条件
    get_clause(x->conds, query->conds);
    check_clause(query->tables, query->conds);

    // 处理group by
    if (x->has_group) {
      for (auto &sv_groupby_col : x->group->cols) {
        TabCol groupby_col = {.tab_name = sv_groupby_col->tab_name,
                              .col_name = sv_groupby_col->col_name,
                              .aggregation_type = sv_groupby_col->aggregation_type,
                              .new_col_name = sv_groupby_col->new_col_name};
        query->groupby_cols.push_back(groupby_col);
      }
      for (auto &groupby_col : query->groupby_cols) {
        groupby_col = check_column(all_cols, groupby_col);
      }
    }

    // 处理having
    if (x->has_having) {
      get_clause(x->having, query->having_conds);        // maybe buggy (lack of robustness)
      check_clause(query->tables, query->having_conds);  // fill table info for cols
    }

    // 检查聚合函数相关的合法性
    if (!check_aggregation_legality(x)) {
      throw AggregationIllegalError();
    }

  } else if (auto x = std::dynamic_pointer_cast<ast::UpdateStmt>(parse)) {  // update statements

    // 处理表名
    /** TODO: 检查表是否存在 */
    // 处理tab_name
    if (!sm_manager_->db_.is_table(x->tab_name)) {
      throw TableExistsError(x->tab_name);
    }
    query->tables = {x->tab_name};

    // 处理set_clauses
    // query->set_clauses = x->set_clauses;
    for (auto it : x->set_clauses) {
      SetClause set_clause;
      set_clause.lhs = {.tab_name = x->tab_name, .col_name = it->col_name};
      // auto c = it->val;
      auto exp = it->rhs_expr;
      bool rhs_exp = false;
      if (exp != NULL) {
        rhs_exp = true;
        set_clause.rhs_col = {.tab_name = x->tab_name, .col_name = exp->lhs};
        set_clause.op = convert_sv_arith_op(exp->op);
        set_clause.rhs = convert_sv_value(exp->rhs);
      } else {
        set_clause.rhs = convert_sv_value(it->val);
      }
      set_clause.is_rhs_exp = rhs_exp;

      query->set_clauses.push_back(set_clause);
    }

    // 处理conds
    get_clause(x->conds, query->conds);
    check_clause(query->tables, query->conds);

  } else if (auto x = std::dynamic_pointer_cast<ast::DeleteStmt>(parse)) {
    // 处理表名
    query->tables = {x->tab_name};
    // 处理where条件
    get_clause(x->conds, query->conds);
    check_clause({x->tab_name}, query->conds);
  } else if (auto x = std::dynamic_pointer_cast<ast::InsertStmt>(parse)) {
    // 处理表名
    query->tables = {x->tab_name};
    // 处理insert 的values值
    for (auto &sv_val : x->vals) {
      query->values.push_back(convert_sv_value(sv_val));
    }
  } else {
    // do nothing
  }
  query->parse = std::move(parse);
  return query;
}

// fill col's table name by traversing all_cols
TabCol Analyze::check_column(const std::vector<ColMeta> &all_cols, TabCol target) {
  if (target.tab_name.empty()) {
    // Table name not specified, infer table name from column name
    std::string tab_name;
    for (auto &col : all_cols) {
      if (col.name == target.col_name) {
        if (!tab_name.empty()) {
          throw AmbiguousColumnError(target.col_name);
        }
        tab_name = col.tab_name;
      }
    }
    if (tab_name.empty()) {
      throw ColumnNotFoundError(target.col_name);
    }
    target.tab_name = tab_name;
  } else {
    /** TODO: Make sure target column exists */
    if (!sm_manager_->db_.is_table(target.tab_name)) {
      throw TableExistsError(target.tab_name);
    }
    for (auto &col : all_cols) {
      if (col.name == target.col_name && col.tab_name == target.tab_name) {
        // found
        return target;
      }
    }
    throw ColumnNotFoundError(target.col_name);
  }
  return target;
}

void Analyze::get_all_cols(const std::vector<std::string> &tab_names, std::vector<ColMeta> &all_cols) {
  for (auto &sel_tab_name : tab_names) {
    // 这里db_不能写成get_db(), 注意要传指针
    const auto &sel_tab_cols = sm_manager_->db_.get_table(sel_tab_name).cols;
    all_cols.insert(all_cols.end(), sel_tab_cols.begin(), sel_tab_cols.end());
  }
}

void Analyze::get_clause(const std::vector<std::shared_ptr<ast::BinaryExpr>> &sv_conds, std::vector<Condition> &conds) {
  conds.clear();
  for (auto &expr : sv_conds) {
    Condition cond;
    cond.lhs_col = {.tab_name = expr->lhs->tab_name,
                    .col_name = expr->lhs->col_name,
                    .aggregation_type = expr->lhs->aggregation_type,
                    .new_col_name = expr->lhs->new_col_name};
    cond.op = convert_sv_comp_op(expr->op);
    cond.is_rhs_exe_processed = false;
    if (!expr->rhs) {
      if (cond.op != OP_IN) {
        throw SubqueryIllegalError("only allows tuples after IN");
      }
      cond.is_rhs_val = false;
      cond.is_rhs_stmt = true;
      cond.is_rhs_exe_processed = true;
      cond.rhs_stmt = nullptr;
      for (auto v : expr->rhs_value_list) {
        cond.rhs_in_col.push_back(convert_sv_value(v));
      }
    } else if (auto rhs_val = std::dynamic_pointer_cast<ast::Value>(expr->rhs)) {
      cond.is_rhs_val = true;
      cond.is_rhs_stmt = false;
      cond.rhs_val = convert_sv_value(rhs_val);
    } else if (auto rhs_col = std::dynamic_pointer_cast<ast::Col>(expr->rhs)) {
      cond.is_rhs_val = false;
      cond.is_rhs_stmt = false;
      cond.rhs_col = {.tab_name = rhs_col->tab_name,
                      .col_name = rhs_col->col_name,
                      .aggregation_type = rhs_col->aggregation_type,
                      .new_col_name = rhs_col->new_col_name};
    } else if (auto rhs_stmt = std::dynamic_pointer_cast<ast::SelectStmt>(expr->rhs)) {
      // subquery only return one column
      if (rhs_stmt->cols.size() != 1) {
        throw SubqueryIllegalError("subquery should only return one column in comparison statement");
      }
      if (rhs_stmt->tabs.empty()) {
        throw SubqueryIllegalError("Subquery should specify table name\n");
      }

      // e.g. AVG(salary) > (SELECT AVG(salary) FROM employees);
      if (expr->op != ast::SV_OP_IN) {
        // comparison stmt, sub query should return only one value
        // Also legal if subquery doesn't use aggregation function, but return only a value.
        // Check is delayed until plan_query.
        // Remember to fill in cond.rhs_val real value after carrying executor.
        cond.is_rhs_val = true;
        cond.is_rhs_stmt = true;
        cond.rhs_val = init_sv_value(rhs_val);
        cond.rhs_col = {.tab_name = rhs_stmt->tabs[0],
                        .col_name = rhs_stmt->cols[0]->col_name,
                        .aggregation_type = rhs_stmt->cols[0]->aggregation_type,
                        .new_col_name = rhs_stmt->cols[0]->new_col_name};
      } else {  // e.g. department_id IN (SELECT id FROM departments WHERE name = 'HR');
        // IN operator, return a column
        cond.is_rhs_val = false;
        cond.is_rhs_stmt = true;
        // cautious: the comparison col is newly-generated, not exist in current table
        cond.rhs_col = {.tab_name = rhs_stmt->tabs[0],
                        .col_name = rhs_stmt->cols[0]->col_name,
                        .aggregation_type = rhs_stmt->cols[0]->aggregation_type,
                        .new_col_name = rhs_stmt->cols[0]->new_col_name};
      }
      // tab_name and col_name info of rhs_col are filled in next step check_clause
      if (rhs_stmt) {
        cond.rhs_stmt = std::static_pointer_cast<void>(rhs_stmt);
      } else {
        throw NullptrError();
      }
    }  // end else if subquery
    conds.push_back(cond);
  }
}

// void Analyze::check_clause(const std::vector<std::string> &tab_names, std::vector<Condition> &conds) {
//     // auto all_cols = get_all_cols(tab_names);
//     std::vector<ColMeta> all_cols;
//     get_all_cols(tab_names, all_cols);
//     // Get raw values in where clause
//     for (auto &cond : conds) {
//         if(cond.lhs_col.col_name.empty() && cond.lhs_col.aggregation_type == COUNT_AGG){
//             //cond: having COUNT(*) < a
//             if(cond.lhs_col.col_name.empty() && cond.lhs_col.aggregation_type == COUNT_AGG){
//                 cond.lhs_col.col_name = "*";
//                 if(!cond.is_rhs_val){
//                     throw AggregationIllegalError();
//                 }
//                 cond.rhs_val.init_raw(4);
//             }
//         }
//         else{
//             // Infer table name from column name
//             cond.lhs_col = check_column(all_cols, cond.lhs_col);
//             if (!cond.is_rhs_val) {
//                 cond.rhs_col = check_column(all_cols, cond.rhs_col);
//             }
//             TabMeta &lhs_tab = sm_manager_->db_.get_table(cond.lhs_col.tab_name);
//             auto lhs_col = lhs_tab.get_col(cond.lhs_col.col_name);
//             ColType lhs_type = lhs_col->type;
//             ColType rhs_type;
//             if (cond.is_rhs_val) {
//                 cond.rhs_val.init_raw(lhs_col->len);
//                 rhs_type = cond.rhs_val.type;
//             } else {
//                 TabMeta &rhs_tab = sm_manager_->db_.get_table(cond.rhs_col.tab_name);
//                 auto rhs_col = rhs_tab.get_col(cond.rhs_col.col_name);
//                 rhs_type = rhs_col->type;
//             }
//             if (lhs_type != rhs_type && (lhs_type == TYPE_STRING || rhs_type == TYPE_STRING )) {
//                 throw IncompatibleTypeError(coltype2str(lhs_type), coltype2str(rhs_type));
//             }
//         }
//     }
// }

// add Query into subquery vector
void Analyze::check_clause(const std::vector<std::string> &tab_names, std::vector<Condition> &conds) {
  // auto all_cols = get_all_cols(tab_names);
  std::vector<ColMeta> all_cols;
  get_all_cols(tab_names, all_cols);
  // Get raw values in where clause
  for (auto &cond : conds) {
    if (cond.is_rhs_stmt && !cond.is_rhs_exe_processed) {
      // Create a shared_ptr to the SelectStmt object
      auto select_stmt_shared_ptr = std::make_shared<ast::SelectStmt>(cond.rhs_stmt);
      // Cast the shared_ptr<SelectStmt> to shared_ptr<ast::TreeNode>
      auto tree_node_shared_ptr = std::static_pointer_cast<ast::TreeNode>(select_stmt_shared_ptr);
      auto subquery = do_analyze(tree_node_shared_ptr);
      cond.rhs_stmt = std::static_pointer_cast<void>(subquery);
    }
    // check lhs_col legality
    if (cond.lhs_col.col_name.empty() && cond.lhs_col.aggregation_type == COUNT_AGG) {
      // cond: having COUNT(*) < a
      if (cond.lhs_col.col_name.empty() && cond.lhs_col.aggregation_type == COUNT_AGG) {
        cond.lhs_col.col_name = "*";
        if (!cond.is_rhs_val) {
          throw AggregationIllegalError();
        }
        cond.rhs_val.init_raw(4);
      }
    } else {
      // Infer table name from column name
      cond.lhs_col = check_column(all_cols, cond.lhs_col);
      if (!cond.is_rhs_val) {
        if (!cond.is_rhs_stmt) {
          cond.rhs_col = check_column(all_cols, cond.rhs_col);
        } else if (cond.is_rhs_exe_processed) {  // is processed IN subquery tuple
          continue;
        } else {  // rhs is subquery stmt, then table name should have been specified.
          auto temp_rhs_stmt = std::static_pointer_cast<ast::SelectStmt>(cond.rhs_stmt);

          // e.g. where a > select COUNT(*) from table
          if (cond.rhs_col.col_name.empty() && cond.rhs_col.aggregation_type == COUNT_AGG) {
            cond.rhs_col.col_name = "*";
            if (!cond.is_rhs_val) {
              throw AggregationIllegalError();
            }
          } else if (!cond.is_rhs_stmt) {
            // e.g. where a > select MIN(val) from table
            cond.rhs_col = check_column(all_cols, cond.rhs_col);
          }
        }
      }

      TabMeta &lhs_tab = sm_manager_->db_.get_table(cond.lhs_col.tab_name);
      auto lhs_col = lhs_tab.get_col(cond.lhs_col.col_name);
      ColType lhs_type = lhs_col->type;
      ColType rhs_type;
      if (cond.is_rhs_val) {
        cond.rhs_val.init_raw(lhs_col->len);
        rhs_type = cond.rhs_val.type;
      } else {
        TabMeta &rhs_tab = sm_manager_->db_.get_table(cond.rhs_col.tab_name);
        auto rhs_col = rhs_tab.get_col(cond.rhs_col.col_name);
        rhs_type = rhs_col->type;
      }
      if (lhs_type != rhs_type && (lhs_type == TYPE_STRING || rhs_type == TYPE_STRING)) {
        throw IncompatibleTypeError(coltype2str(lhs_type), coltype2str(rhs_type));
      }
    }
  }
}

bool Analyze::find_col(std::vector<std::shared_ptr<ast::Col>> &cols, std::string col_name) {
  for (auto col : cols) {
    if (col->col_name == col_name) {
      return true;
    }
  }
  return false;
}
bool Analyze::check_aggregation_legality(const std::shared_ptr<ast::SelectStmt> &x) {
  // 1.SELECT 列表中不能出现没有在 GROUP BY 子句中的非聚集列
  if (x->has_group) {
    auto group_cols = x->group->cols;
    for (auto &sel_col : x->cols) {
      if (sel_col->aggregation_type == NO_AGG && !find_col(group_cols, sel_col->col_name)) {
        return false;
      }
    }
  }
  // 2.WHERE 子句中不能用聚集函数作为条件表达式
  for (auto &cond : x->conds) {
    if (cond->lhs->aggregation_type != NO_AGG) {
      return false;
    }
  }
  return true;
}
Value Analyze::convert_sv_value(const std::shared_ptr<ast::Value> &sv_val) {
  Value val;
  if (auto int_lit = std::dynamic_pointer_cast<ast::IntLit>(sv_val)) {
    val.set_int(int_lit->val);
  } else if (auto float_lit = std::dynamic_pointer_cast<ast::FloatLit>(sv_val)) {
    val.set_float(float_lit->val);
  } else if (auto str_lit = std::dynamic_pointer_cast<ast::StringLit>(sv_val)) {
    val.set_str(str_lit->val);
  } else {
    throw InternalError("Unexpected sv value type");
  }
  return val;
}
/*
Init cond rhs val .type as TYPE_EMPTY, .raw as nullptr.
Need to fill rhs value after executor processed subquery.
*/
Value Analyze::init_sv_value(const std::shared_ptr<ast::Value> &sv_val) {
  Value val;
  val.type = TYPE_EMPTY;
  val.raw = nullptr;
  return val;
}

CompOp Analyze::convert_sv_comp_op(ast::SvCompOp op) {
  std::map<ast::SvCompOp, CompOp> m = {{ast::SV_OP_EQ, OP_EQ}, {ast::SV_OP_NE, OP_NE}, {ast::SV_OP_LT, OP_LT},
                                       {ast::SV_OP_GT, OP_GT}, {ast::SV_OP_LE, OP_LE}, {ast::SV_OP_GE, OP_GE},
                                       {ast::SV_OP_IN, OP_IN}};
  return m.at(op);
}

ArithOp Analyze::convert_sv_arith_op(ast::SvArithOp op) {
  std::map<ast::SvArithOp, ArithOp> m = {
      {ast::SV_OP_PLUS, OP_PLUS}, {ast::SV_OP_MINUS, OP_MINUS}, {ast::SV_OP_MUL, OP_MULTI}, {ast::SV_OP_DIV, OP_DIV}};
  return m.at(op);
}