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

#include "analyze/analyze.h"
#include "common/common.h"
#include "common/context.h"
// #include "execution/execution_defs.h"
// #include "execution/execution_manager.h"
#include "parser/parser.h"
#include "plan.h"
#include "record/rm.h"
#include "system/sm.h"
#include "type/type_id.h"

namespace easydb {
class Planner {
 private:
  SmManager *sm_manager_;

  bool enable_nestedloop_join = true;
  bool enable_sortmerge_join = false;
  bool enable_hash_join = false;

  bool enable_optimizer = true;
  double estimate_table_scan_cost(const std::string &tab_name);
  double estimate_join_cost(const std::string &left_table, const std::string &right_table);
  void reorder_conds_based_on_table_size(std::shared_ptr<Query> query);
  void reorder_joins(std::shared_ptr<Query> query);

  CompOp reverse_op(CompOp op) {
    switch (op) {
      case OP_EQ:
        return OP_EQ;  // 相等对称
      case OP_NE:
        return OP_NE;  // 不等对称
      case OP_LT:
        return OP_GT;  // 小于变大于
      case OP_GT:
        return OP_LT;  // 大于变小于
      case OP_LE:
        return OP_GE;  // 小于等于变大于等于
      case OP_GE:
        return OP_LE;  // 大于等于变小于等于
      default:
        return op;  // fallback
    }
  }

 public:
  Planner(SmManager *sm_manager) : sm_manager_(sm_manager) {}

  std::shared_ptr<Plan> do_planner(std::shared_ptr<Query> query, Context *context);

  void set_enable_nestedloop_join(bool set_val) { enable_nestedloop_join = set_val; }

  void set_enable_sortmerge_join(bool set_val) { enable_sortmerge_join = set_val; }

  void setEnableHashJoin(bool set_val) { enable_hash_join = set_val; }

  void SetEnableOptimizer(bool set_val) { enable_optimizer = set_val; }

  bool GetEnableOptimizer() { return enable_optimizer; }

  void deduce_conditions_via_equijoin(std::shared_ptr<Query> query);

 private:
  std::shared_ptr<Query> logical_optimization(std::shared_ptr<Query> query, Context *context);
  std::shared_ptr<Plan> physical_optimization(std::shared_ptr<Query> query, Context *context);

  std::shared_ptr<Plan> make_one_rel(std::shared_ptr<Query> query, Context *context);

  std::shared_ptr<Plan> generate_sort_plan(std::shared_ptr<Query> query, std::shared_ptr<Plan> plan);
  std::shared_ptr<Plan> generate_aggregation_plan(std::shared_ptr<Query> query, std::shared_ptr<Plan> plan);

  std::shared_ptr<Plan> generate_select_plan(std::shared_ptr<Query> query, Context *context);

  // int get_indexNo(std::string tab_name, std::vector<Condition> curr_conds);
  bool get_index_cols(std::string tab_name, std::vector<Condition> curr_conds,
                      std::vector<std::string> &index_col_names);
  bool get_index_cols_swap(std::string tab_name, std::vector<Condition> curr_conds,
                           std::vector<std::string> &index_col_names);

  ColType interp_sv_type(ast::SvType sv_type) {
    std::map<ast::SvType, ColType> m = {
        {ast::SV_TYPE_INT, TYPE_INT}, {ast::SV_TYPE_FLOAT, TYPE_FLOAT}, {ast::SV_TYPE_STRING, TYPE_VARCHAR}};
    return m.at(sv_type);
  }
};

}  // namespace easydb