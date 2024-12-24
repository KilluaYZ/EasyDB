/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "planner/planner.h"

#include <memory>

#include "common/common.h"
#include "common/errors.h"
#include "common/macros.h"
#include "planner/plan.h"
// #include "execution/executor_delete.h"
// #include "execution/executor_index_scan.h"
// #include "execution/executor_insert.h"
// #include "execution/executor_nestedloop_join.h"
// #include "execution/executor_projection.h"
// #include "execution/executor_seq_scan.h"
// #include "execution/executor_update.h"
// #include "index/ix.h"
// #include "record_printer.h"
namespace easydb {

namespace {

// 辅助结构用于简化同一列上的条件
struct ColCondRange {
  bool has_equal = false;
  Value equal_val;
  bool has_lower = false;
  Value lower_val;
  bool lower_inclusive = false;
  bool has_upper = false;
  Value upper_val;
  bool upper_inclusive = false;
  std::vector<Value> ne_values;

  bool isContradictory() {
    // 若有equal条件，则检查equal_val是否满足上下界并与NE条件无冲突
    if (has_equal) {
      // 检查与下界冲突
      if (has_lower) {
        if (lower_inclusive) {
          // equal_val必须 >= lower_val
          if (equal_val < lower_val) return true;
        } else {
          // equal_val必须 > lower_val
          if (equal_val <= lower_val) return true;
        }
      }
      // 检查与上界冲突
      if (has_upper) {
        if (upper_inclusive) {
          // equal_val必须 <= upper_val
          if (equal_val > upper_val) return true;
        } else {
          // equal_val必须 < upper_val
          if (equal_val >= upper_val) return true;
        }
      }
      // 检查不等条件冲突
      for (auto &nev : ne_values) {
        if (equal_val == nev) {
          return true;
        }
      }
    } else {
      // 无equal时检查范围
      if (has_lower && has_upper) {
        // 下界不能大于上界
        if (lower_val > upper_val) {
          return true;
        } else if (lower_val == upper_val && (!lower_inclusive || !upper_inclusive)) {
          // 下界 == 上界但没有包含这个点
          return true;
        }
      }
    }
    return false;
  }

  std::vector<Condition> toConditions(const TabCol &col) {
    std::vector<Condition> result;
    if (has_equal) {
      Condition c;
      c.op = OP_EQ;
      c.lhs_col = col;
      c.is_rhs_val = true;
      c.is_rhs_stmt = false;
      c.is_rhs_exe_processed = false;
      c.rhs_val = equal_val;
      result.push_back(c);
      return result;
    }

    if (has_lower) {
      Condition c;
      c.lhs_col = col;
      c.is_rhs_val = true;
      c.is_rhs_stmt = false;
      c.is_rhs_exe_processed = false;
      c.rhs_val = lower_val;
      c.op = lower_inclusive ? OP_GE : OP_GT;
      result.push_back(c);
    }

    if (has_upper) {
      Condition c;
      c.lhs_col = col;
      c.is_rhs_val = true;
      c.is_rhs_stmt = false;
      c.is_rhs_exe_processed = false;
      c.rhs_val = upper_val;
      c.op = upper_inclusive ? OP_LE : OP_LT;
      result.push_back(c);
    }

    for (auto &v : ne_values) {
      Condition c;
      c.lhs_col = col;
      c.is_rhs_val = true;
      c.is_rhs_stmt = false;
      c.is_rhs_exe_processed = false;
      c.rhs_val = v;
      c.op = OP_NE;
      result.push_back(c);
    }

    return result;
  }

  // 辅助函数：处理一个新的范围条件后检查矛盾
  bool tryCheckContradictory() { return isContradictory(); }
};

void simplify_conditions(std::shared_ptr<Query> query) {
  std::map<std::pair<std::string, std::string>, ColCondRange> col_map;

  for (auto &cond : query->conds) {
    // 只简化列-常量的条件
    if (!cond.is_rhs_val || cond.is_rhs_stmt) {
      continue;
    }

    auto key = std::make_pair(cond.lhs_col.tab_name, cond.lhs_col.col_name);
    auto &range = col_map[key];

    switch (cond.op) {
      case OP_EQ: {
        if (range.has_equal) {
          // 已有equal，如果值不同则无解
          if (range.equal_val != cond.rhs_val) {
            query->no_result = true;
            return;
          }
        } else {
          range.has_equal = true;
          range.equal_val = cond.rhs_val;
        }
        break;
      }
      case OP_NE: {
        if (range.has_equal && range.equal_val == cond.rhs_val) {
          query->no_result = true;
          return;
        }
        range.ne_values.push_back(cond.rhs_val);
        break;
      }
      case OP_LT: {
        if (!range.has_upper) {
          range.has_upper = true;
          range.upper_val = cond.rhs_val;
          range.upper_inclusive = false;
        } else {
          if (cond.rhs_val < range.upper_val) {
            range.upper_val = cond.rhs_val;
            range.upper_inclusive = false;
          } else if (cond.rhs_val == range.upper_val && range.upper_inclusive) {
            // 原为<=，现为<更严格，更新为<（上界更严格）
            range.upper_inclusive = false;
          }
        }
        break;
      }
      case OP_LE: {
        if (!range.has_upper) {
          range.has_upper = true;
          range.upper_val = cond.rhs_val;
          range.upper_inclusive = true;
        } else {
          if (cond.rhs_val < range.upper_val) {
            range.upper_val = cond.rhs_val;
            range.upper_inclusive = true;
          } else if (cond.rhs_val == range.upper_val && !range.upper_inclusive) {
            // 原是<，现在<=宽松，不更新为宽松的条件，保持严格的<
          }
        }
        break;
      }
      case OP_GT: {
        if (!range.has_lower) {
          range.has_lower = true;
          range.lower_val = cond.rhs_val;
          range.lower_inclusive = false;
        } else {
          if (cond.rhs_val > range.lower_val) {
            range.lower_val = cond.rhs_val;
            range.lower_inclusive = false;
          } else if (cond.rhs_val == range.lower_val && range.lower_inclusive) {
            // 原是>=，现在>更严格
            range.lower_inclusive = false;
          }
        }
        break;
      }
      case OP_GE: {
        if (!range.has_lower) {
          range.has_lower = true;
          range.lower_val = cond.rhs_val;
          range.lower_inclusive = true;
        } else {
          if (cond.rhs_val > range.lower_val) {
            range.lower_val = cond.rhs_val;
            range.lower_inclusive = true;
          } else if (cond.rhs_val == range.lower_val && !range.lower_inclusive) {
            // 原是>，新是>=更宽松，不替换为宽松的条件
          }
        }
        break;
      }
      default:
        // OP_IN等不做特殊优化
        break;
    }

    // 每添加一个条件后就检查是否矛盾
    if (range.tryCheckContradictory()) {
      query->no_result = true;
      return;
    }
  }

  // 所有条件处理完再次检查
  for (auto &[key, range] : col_map) {
    if (range.isContradictory()) {
      query->no_result = true;
      return;
    }
  }

  if (query->no_result) return;

  // 重构conds
  std::vector<Condition> new_conds;
  for (auto &cond : query->conds) {
    // 非列-常量条件保留
    if (!cond.is_rhs_val || cond.is_rhs_stmt) {
      new_conds.push_back(cond);
    }
  }
  // 添加简化后的列条件
  for (auto &[key, range] : col_map) {
    TabCol col;
    col.tab_name = key.first;
    col.col_name = key.second;
    auto cnds = range.toConditions(col);
    for (auto &c : cnds) {
      new_conds.push_back(c);
    }
  }

  query->conds = std::move(new_conds);
}
// 标识 (表名, 列名)
struct ColId {
  std::string tab_name;
  std::string col_name;

  bool operator==(const ColId &o) const { return tab_name == o.tab_name && col_name == o.col_name; }
};

struct ColIdHash {
  size_t operator()(const ColId &c) const {
    // 简易hash组合
    auto h1 = std::hash<std::string>()(c.tab_name);
    auto h2 = std::hash<std::string>()(c.col_name);
    // 大致混合
    return h1 ^ (h2 + 0x9e3779b97f4a7c15ULL + (h1 << 6) + (h1 >> 2));
  }
};

// 并查集，用于把 tableA.a, tableB.a, tableC.a 等列合并到同一个“等价类”
class ColumnUnionFind {
 public:
  ColId find(const ColId &x) {
    if (parent_.find(x) == parent_.end()) {
      parent_[x] = x;  // 若 x 不在 parent_ 里，则把它自己设为它的 parent
      return x;
    }
    if (!(parent_[x] == x)) {
      parent_[x] = find(parent_[x]);  // 路径压缩
    }
    return parent_[x];
  }

  void unite(const ColId &a, const ColId &b) {
    auto ra = find(a);
    auto rb = find(b);
    if (!(ra == rb)) {
      parent_[rb] = ra;
    }
  }

 private:
  std::unordered_map<ColId, ColId, ColIdHash> parent_;
};

}  // namespace

void Planner::deduce_conditions_via_equijoin(std::shared_ptr<Query> query) {
  // 第1步：构建并查集
  ColumnUnionFind uf;

  // 先把所有 “表列” 都在 union-find 里出现一次
  //   1) lhs_col
  //   2) 若是 col op col 的，则 rhs_col 也要
  for (auto &cond : query->conds) {
    ColId lhs{cond.lhs_col.tab_name, cond.lhs_col.col_name};
    uf.find(lhs);  // 确保它进并查集
    if (!cond.is_rhs_val && !cond.is_rhs_stmt) {
      // 说明 rhs 也是列
      ColId rhs{cond.rhs_col.tab_name, cond.rhs_col.col_name};
      uf.find(rhs);
    }
  }

  // 把多表等值条件 (tableA.a = tableB.a) 全部 union
  for (auto &cond : query->conds) {
    bool is_join_eq = (cond.op == OP_EQ && !cond.is_rhs_val &&  // rhs不是常量
                       !cond.is_rhs_stmt &&                     // rhs不是子查询
                       cond.lhs_col.tab_name != cond.rhs_col.tab_name);
    if (is_join_eq) {
      ColId c1{cond.lhs_col.tab_name, cond.lhs_col.col_name};
      ColId c2{cond.rhs_col.tab_name, cond.rhs_col.col_name};
      uf.unite(c1, c2);
    }
  }

  // 第2步：收集单表谓词
  std::unordered_map<ColId, std::vector<Condition>, ColIdHash> singleTableConds;
  for (auto &cond : query->conds) {
    // 如果是 “col op 常量” 的单表条件 (op可以是 <, <=, >, >=, =, != ...)
    if (cond.is_rhs_val && !cond.is_rhs_stmt) {
      ColId c{cond.lhs_col.tab_name, cond.lhs_col.col_name};
      ColId rep = uf.find(c);  // 找到它所在的等价类
      singleTableConds[rep].push_back(cond);
    }
  }

  // 第3步：等价类复制
  // eqClassMembers[rep] = 这个等价类下的所有列
  std::unordered_map<ColId, std::vector<ColId>, ColIdHash> eqClassMembers;
  // 枚举一下 union-find 里出现过的所有列(方法：再扫一遍 conds, 或者若有接口能直接从 union-find 里取出)
  for (auto &cond : query->conds) {
    // LHS
    ColId lhs{cond.lhs_col.tab_name, cond.lhs_col.col_name};
    ColId rep_lhs = uf.find(lhs);
    eqClassMembers[rep_lhs].push_back(lhs);

    // 如果 RHS 也是列，则同样处理
    if (!cond.is_rhs_val && !cond.is_rhs_stmt) {
      ColId rhs{cond.rhs_col.tab_name, cond.rhs_col.col_name};
      ColId rep_rhs = uf.find(rhs);
      eqClassMembers[rep_rhs].push_back(rhs);
    }
  }

  // 遍历 singleTableConds[rep] 里的所有 condition, 复制到 eqClassMembers[rep] 下的每个列
  std::vector<Condition> newConds;
  for (auto &kv : singleTableConds) {
    ColId rep = kv.first;
    auto &condsInThisRep = kv.second;
    // 该等价类的所有列
    auto &cols = eqClassMembers[rep];
    // 复制
    for (auto &oldCond : condsInThisRep) {
      for (auto &colId : cols) {
        // 如果本来就是 oldCond 那个列，就不必重复
        if (colId.tab_name == oldCond.lhs_col.tab_name && colId.col_name == oldCond.lhs_col.col_name) {
          continue;
        }
        // 新建一个 condition
        Condition c = oldCond;
        // 把 lhs 改为等价类中的另一个列
        c.lhs_col.tab_name = colId.tab_name;
        c.lhs_col.col_name = colId.col_name;
        newConds.push_back(std::move(c));
      }
    }
  }

  // 第4步：将推导出的 newConds 加入 query->conds
  if (!newConds.empty()) {
    query->conds.insert(query->conds.end(), newConds.begin(), newConds.end());
  }
}

// 目前的索引匹配规则为：完全匹配索引字段，支持范围查询(不支持NE)，不会自动调整where条件的顺序(目前是左边字段，右边值)
// OLD：完全匹配索引字段，且全部为单点查询，不会自动调整where条件的顺序
bool Planner::get_index_cols(std::string tab_name, std::vector<Condition> curr_conds,
                             std::vector<std::string> &index_col_names) {
  index_col_names.clear();
  // for (auto &cond : curr_conds) {
  //     if(cond.is_rhs_val && cond.op == OP_EQ && cond.lhs_col.tab_name.compare(tab_name) == 0)
  //         index_col_names.push_back(cond.lhs_col.col_name);
  // }
  std::unordered_set<std::string> added_cols;
  for (const auto &cond : curr_conds) {
    if (!cond.is_rhs_stmt && cond.lhs_col.tab_name.compare(tab_name) == 0) {
      if (added_cols.find(cond.lhs_col.col_name) == added_cols.end() && cond.op != OP_NE) {
        index_col_names.push_back(cond.lhs_col.col_name);
        added_cols.insert(cond.lhs_col.col_name);
      }
    }
  }
  TabMeta &tab = sm_manager_->db_.get_table(tab_name);
  if (tab.is_index(index_col_names)) return true;
  return false;
}

// 右边字段
bool Planner::get_index_cols_swap(std::string tab_name, std::vector<Condition> curr_conds,
                                  std::vector<std::string> &index_col_names) {
  index_col_names.clear();
  // for (auto &cond : curr_conds) {
  //     if(cond.is_rhs_val && cond.op == OP_EQ && cond.lhs_col.tab_name.compare(tab_name) == 0)
  //         index_col_names.push_back(cond.lhs_col.col_name);
  // }
  std::unordered_set<std::string> added_cols;
  for (const auto &cond : curr_conds) {
    if (!cond.is_rhs_val && cond.rhs_col.tab_name.compare(tab_name) == 0) {
      if (added_cols.find(cond.rhs_col.col_name) == added_cols.end() && cond.op != OP_NE) {
        index_col_names.push_back(cond.rhs_col.col_name);
        added_cols.insert(cond.rhs_col.col_name);
      }
    }
  }
  TabMeta &tab = sm_manager_->db_.get_table(tab_name);
  if (tab.is_index(index_col_names)) return true;
  return false;
}

/**
 * @brief 表算子条件谓词生成
 *
 * @param conds 条件
 * @param tab_names 表名
 * @return std::vector<Condition>
 */
std::vector<Condition> pop_conds(std::vector<Condition> &conds, std::string tab_names) {
  // auto has_tab = [&](const std::string &tab_name) {
  //     return std::find(tab_names.begin(), tab_names.end(), tab_name) != tab_names.end();
  // };
  std::vector<Condition> solved_conds;
  auto it = conds.begin();
  while (it != conds.end()) {
    if ((tab_names.compare(it->lhs_col.tab_name) == 0 && it->is_rhs_stmt) ||
        (tab_names.compare(it->lhs_col.tab_name) == 0 && it->is_rhs_val) ||
        (it->lhs_col.tab_name.compare(it->rhs_col.tab_name) == 0)) {
      solved_conds.emplace_back(std::move(*it));
      it = conds.erase(it);
    } else {
      it++;
    }
  }
  return solved_conds;
}

int push_conds(Condition *cond, std::shared_ptr<Plan> plan) {
  if (auto x = std::dynamic_pointer_cast<ScanPlan>(plan)) {
    if (x->tab_name_.compare(cond->lhs_col.tab_name) == 0) {
      return 1;
    } else if (x->tab_name_.compare(cond->rhs_col.tab_name) == 0) {
      return 2;
    } else {
      return 0;
    }
  } else if (auto x = std::dynamic_pointer_cast<JoinPlan>(plan)) {
    int left_res = push_conds(cond, x->left_);
    // 条件已经下推到左子节点
    if (left_res == 3) {
      return 3;
    }
    int right_res = push_conds(cond, x->right_);
    // 条件已经下推到右子节点
    if (right_res == 3) {
      return 3;
    }
    // 左子节点或右子节点有一个没有匹配到条件的列
    if (left_res == 0 || right_res == 0) {
      return left_res + right_res;
    }
    // 左子节点匹配到条件的右边
    if (left_res == 2) {
      // 需要将左右两边的条件变换位置
      std::map<CompOp, CompOp> swap_op = {
          {OP_EQ, OP_EQ}, {OP_NE, OP_NE}, {OP_LT, OP_GT}, {OP_GT, OP_LT}, {OP_LE, OP_GE}, {OP_GE, OP_LE},
      };
      std::swap(cond->lhs_col, cond->rhs_col);
      cond->op = swap_op.at(cond->op);
    }
    x->conds_.emplace_back(std::move(*cond));
    return 3;
  }
  return false;
}

std::shared_ptr<Plan> pop_scan(int *scantbl, std::string table, std::vector<std::string> &joined_tables,
                               std::vector<std::shared_ptr<Plan>> plans) {
  for (size_t i = 0; i < plans.size(); i++) {
    auto x = std::dynamic_pointer_cast<ScanPlan>(plans[i]);
    if (x->tab_name_.compare(table) == 0) {
      scantbl[i] = 1;
      joined_tables.emplace_back(x->tab_name_);
      return plans[i];
    }
  }
  return nullptr;
}

void Planner::reorder_conds_based_on_table_size(std::shared_ptr<Query> query) {
  std::vector<Condition> join_conds;
  std::vector<Condition> single_conds;

  // 将query->conds拆分为join条件和单表条件
  for (auto &cond : query->conds) {
    bool is_join_cond = (!cond.is_rhs_val && !cond.is_rhs_stmt && cond.lhs_col.tab_name != cond.rhs_col.tab_name);
    if (is_join_cond) {
      join_conds.push_back(cond);
    } else {
      single_conds.push_back(cond);
    }
  }

  auto get_table_size = [&](const std::string &tab_name) {
    int count = sm_manager_->GetTableCount(tab_name);
    if (count < 0) {
      count = 1000;  // 若无统计信息则假设为1000
    }
    return count;
  };

  auto get_max_distinct_size = [&](const std::string &left_tab_name, const std::string &left_col_name,
                                   const std::string &right_tab_name, const std::string &right_col_name) {
    int left_count = sm_manager_->GetTableAttrDistinct(left_tab_name, left_col_name);
    int right_count = sm_manager_->GetTableAttrDistinct(right_tab_name, right_col_name);
    if (left_count < 0 && right_count < 0) {
      return 1;  // 若无统计信息则返回1，相当于不进行distinct值统计
    }
    return left_count > right_count ? left_count : right_count;
  };

  // 根据表大小对join_conds进行排序，小表优先
  // 这里使用两表大小的乘积作为简易估计值
  std::sort(join_conds.begin(), join_conds.end(), [&](const Condition &a, const Condition &b) {
    int a_size = (get_table_size(a.lhs_col.tab_name) * get_table_size(a.rhs_col.tab_name)) /
                 get_max_distinct_size(a.lhs_col.tab_name, a.lhs_col.col_name, a.rhs_col.tab_name, a.rhs_col.col_name);
    int b_size = (get_table_size(b.lhs_col.tab_name) * get_table_size(b.rhs_col.tab_name)) /
                 get_max_distinct_size(b.lhs_col.tab_name,b.lhs_col.col_name, b.rhs_col.tab_name,b.rhs_col.col_name);
    return a_size < b_size;
  });

  // 对每个join_cond，若 lhs_table 大于 rhs_table，则交换 lhs 和 rhs
  for (auto &cond : join_conds) {
    int lhs_size = get_table_size(cond.lhs_col.tab_name);
    int rhs_size = get_table_size(cond.rhs_col.tab_name);
    if (lhs_size > rhs_size) {
      // 交换 lhs_col 和 rhs_col
      std::swap(cond.lhs_col, cond.rhs_col);
      // 翻转操作符
      cond.op = reverse_op(cond.op);
    }
  }

  // 最终将单表条件放前面，join条件放后面
  std::vector<Condition> new_conds;
  new_conds.insert(new_conds.end(), single_conds.begin(), single_conds.end());
  new_conds.insert(new_conds.end(), join_conds.begin(), join_conds.end());

  query->conds = std::move(new_conds);
}

std::shared_ptr<Query> Planner::logical_optimization(std::shared_ptr<Query> query, Context *context) {
  if (GetEnableOptimizer()) {
    // 调用reorder_joins对query->tables进行连接顺序重排
    if (query->tables.size() > 1) {
      reorder_joins(query);
    }
    reorder_conds_based_on_table_size(query);
    deduce_conditions_via_equijoin(query);
    // ADDED: 简化条件
    simplify_conditions(query);
  }
  return query;
}

void Planner::reorder_joins(std::shared_ptr<Query> query) {
  // 简单启发式：对query->tables根据其大小(行数)进行升序排序
  // 获取每个表的代价(用行数代替)
  std::vector<std::pair<std::string, double>> table_costs;
  for (auto &t : query->tables) {
    double cost = estimate_table_scan_cost(t);
    table_costs.emplace_back(t, cost);
  }

  // 按照cost从小到大排序
  std::sort(table_costs.begin(), table_costs.end(), [](auto &a, auto &b) { return a.second < b.second; });

  query->optimized_table_order.clear();
  for (auto &tc : table_costs) {
    query->optimized_table_order.push_back(tc.first);
  }
}

double Planner::estimate_table_scan_cost(const std::string &tab_name) {
  // 简单估计：行数越多，cost越高。行数从sm_manager_获取
  int count = sm_manager_->GetTableCount(tab_name);
  if (count < 0) {
    // 如果没有统计信息，假设一个默认值
    return 1000.0;
  }
  return static_cast<double>(count);
}

double Planner::estimate_join_cost(const std::string &left_table, const std::string &right_table) {
  // 简单启发式join代价估计 = 两表大小相乘 (笛卡尔积大小)
  double left_cost = estimate_table_scan_cost(left_table);
  double right_cost = estimate_table_scan_cost(right_table);
  return left_cost * right_cost;
}

std::shared_ptr<Plan> Planner::physical_optimization(std::shared_ptr<Query> query, Context *context) {
  // ADDED: 若no_result为true，直接返回EmptyPlan
  if (query->no_result) {
    return std::make_shared<EmptyPlan>();
  }
  std::shared_ptr<Plan> plan = make_one_rel(query, context);
  if (GetEnableOptimizer()) {
    // 其他物理优化
  }

  // 处理aggregation
  plan = generate_aggregation_plan(query, std::move(plan));
  // 处理orderby
  plan = generate_sort_plan(query, std::move(plan));

  return plan;
}

std::shared_ptr<Plan> Planner::make_one_rel(std::shared_ptr<Query> query, Context *context) {
  auto x = std::dynamic_pointer_cast<ast::SelectStmt>(query->parse);
  std::vector<std::string> tables = query->optimized_table_order.empty() ? query->tables : query->optimized_table_order;

  std::vector<std::shared_ptr<Plan>> table_scan_executors(tables.size());

  // std::vector<std::string> tables = query->tables;
  // // Scan table , 生成表算子列表tab_nodes
  // std::vector<std::shared_ptr<Plan>> table_scan_executors(tables.size());
  // traverse all tables, if tables[i] == left col tab, then move corresponding cond into curr_conds
  for (size_t i = 0; i < tables.size(); i++) {
    auto curr_conds = pop_conds(query->conds, tables[i]);
    for (auto &cond : curr_conds) {
      if (cond.is_rhs_stmt && !cond.is_rhs_exe_processed) {
        auto rhs_stmt_ptr = std::make_shared<Query>(cond.rhs_stmt);
        std::shared_ptr<Plan> rhs_stmt_plan = do_planner(rhs_stmt_ptr, context);
        cond.rhs_stmt = std::static_pointer_cast<void>(rhs_stmt_plan);
      }
    }
    // int index_no = get_indexNo(tables[i], curr_conds);
    std::vector<std::string> index_col_names;
    bool index_exist = get_index_cols(tables[i], curr_conds, index_col_names);
    if (index_exist == false) {  // 该表没有索引
      index_col_names.clear();
      table_scan_executors[i] =
          std::make_shared<ScanPlan>(T_SeqScan, sm_manager_, tables[i], curr_conds, index_col_names);
    } else {  // 存在索引
      table_scan_executors[i] =
          std::make_shared<ScanPlan>(T_IndexScan, sm_manager_, tables[i], curr_conds, index_col_names);
    }
  }
  // 只有一个表，不需要join。
  if (tables.size() == 1) {
    return table_scan_executors[0];
  }
  // 获取where条件
  auto conds = std::move(query->conds);
  std::shared_ptr<Plan> table_join_executors;

  int scantbl[tables.size()];
  for (size_t i = 0; i < tables.size(); i++) {
    scantbl[i] = -1;
  }
  // 假设在ast中已经添加了jointree，这里需要修改的逻辑是，先处理jointree，然后再考虑剩下的部分
  if (conds.size() >= 1) {
    // 有连接条件

    // 根据连接条件，生成第一层join
    std::vector<std::string> joined_tables(tables.size());
    auto it = conds.begin();
    while (it != conds.end()) {
      std::shared_ptr<ScanPlan> left, right;
      left = std::dynamic_pointer_cast<ScanPlan>(
          pop_scan(scantbl, it->lhs_col.tab_name, joined_tables, table_scan_executors));
      right = std::dynamic_pointer_cast<ScanPlan>(
          pop_scan(scantbl, it->rhs_col.tab_name, joined_tables, table_scan_executors));
      std::vector<Condition> join_conds{*it};
      // 建立join
      //  判断使用哪种join方式
      if (enable_nestedloop_join && enable_sortmerge_join) {
        // 默认nested loop join
        table_join_executors = std::make_shared<JoinPlan>(T_NestLoop, std::move(left), std::move(right), join_conds);
      } else if (enable_nestedloop_join) {
        table_join_executors = std::make_shared<JoinPlan>(T_NestLoop, std::move(left), std::move(right), join_conds);
      } else if (enable_sortmerge_join) {
        std::vector<std::string> index_col_name_left;
        std::vector<std::string> index_col_name_right;
        // TODO: 临时fix，后续去除
        bool left_index_exist = get_index_cols(it->lhs_col.tab_name, join_conds, index_col_name_left);
        bool right_index_exist = get_index_cols_swap(it->rhs_col.tab_name, join_conds, index_col_name_right);
        if (left_index_exist && right_index_exist) {  // join列存在索引
          // 强行将scan替换为indexscan，前面会由于涉及到多个表而没办法定义为index_scan
          // Note that we need the original condition!
          left = std::make_shared<ScanPlan>(T_IndexScan, sm_manager_, it->lhs_col.tab_name, left->get_conds(),
                                            index_col_name_left);
          right = std::make_shared<ScanPlan>(T_IndexScan, sm_manager_, it->rhs_col.tab_name, right->get_conds(),
                                             index_col_name_right);

          table_join_executors =
              std::make_shared<JoinPlan>(T_IndexMerge, std::move(left), std::move(right), join_conds);
        } else {  // 不存在索引
          table_join_executors = std::make_shared<JoinPlan>(T_SortMerge, std::move(left), std::move(right), join_conds);
        }
      } else if (enable_hash_join) {
        table_join_executors = std::make_shared<JoinPlan>(T_HashJoin, std::move(left), std::move(right), join_conds);
      } else {
        // error
        throw EASYDBError("No join executor selected!");
      }

      // table_join_executors = std::make_shared<JoinPlan>(T_NestLoop, std::move(left), std::move(right), join_conds);
      it = conds.erase(it);
      break;
    }
    // 根据连接条件，生成第2-n层join
    it = conds.begin();
    while (it != conds.end()) {
      std::shared_ptr<Plan> left_need_to_join_executors = nullptr;
      std::shared_ptr<Plan> right_need_to_join_executors = nullptr;
      bool isneedreverse = false;
      if (std::find(joined_tables.begin(), joined_tables.end(), it->lhs_col.tab_name) == joined_tables.end()) {
        left_need_to_join_executors = pop_scan(scantbl, it->lhs_col.tab_name, joined_tables, table_scan_executors);
      }
      if (std::find(joined_tables.begin(), joined_tables.end(), it->rhs_col.tab_name) == joined_tables.end()) {
        right_need_to_join_executors = pop_scan(scantbl, it->rhs_col.tab_name, joined_tables, table_scan_executors);
        isneedreverse = true;
      }

      if (left_need_to_join_executors != nullptr && right_need_to_join_executors != nullptr) {
        std::vector<Condition> join_conds{*it};
        std::shared_ptr<Plan> temp_join_executors = std::make_shared<JoinPlan>(
            T_NestLoop, std::move(left_need_to_join_executors), std::move(right_need_to_join_executors), join_conds);
        table_join_executors = std::make_shared<JoinPlan>(T_NestLoop, std::move(temp_join_executors),
                                                          std::move(table_join_executors), std::vector<Condition>());
      } else if (left_need_to_join_executors != nullptr || right_need_to_join_executors != nullptr) {
        if (isneedreverse) {
          std::map<CompOp, CompOp> swap_op = {
              {OP_EQ, OP_EQ}, {OP_NE, OP_NE}, {OP_LT, OP_GT}, {OP_GT, OP_LT}, {OP_LE, OP_GE}, {OP_GE, OP_LE},
          };
          std::swap(it->lhs_col, it->rhs_col);
          it->op = swap_op.at(it->op);
          left_need_to_join_executors = std::move(right_need_to_join_executors);
        }
        std::vector<Condition> join_conds{*it};
        table_join_executors = std::make_shared<JoinPlan>(T_NestLoop, std::move(left_need_to_join_executors),
                                                          std::move(table_join_executors), join_conds);
      } else {
        push_conds(std::move(&(*it)), table_join_executors);
      }
      it = conds.erase(it);
    }
  } else {
    table_join_executors = table_scan_executors[0];
    scantbl[0] = 1;
  }

  // 连接剩余表
  for (size_t i = 0; i < tables.size(); i++) {
    if (scantbl[i] == -1) {
      table_join_executors = std::make_shared<JoinPlan>(T_NestLoop, std::move(table_scan_executors[i]),
                                                        std::move(table_join_executors), std::vector<Condition>());
    }
  }

  return table_join_executors;
}

std::shared_ptr<Plan> Planner::generate_sort_plan(std::shared_ptr<Query> query, std::shared_ptr<Plan> plan) {
  auto x = std::dynamic_pointer_cast<ast::SelectStmt>(query->parse);
  if (!x->has_sort) {
    return plan;
  }
  std::vector<std::string> tables = query->tables;
  std::vector<ColMeta> all_cols;
  for (auto &sel_tab_name : tables) {
    // 这里db_不能写成get_db(), 注意要传指针
    const auto &sel_tab_cols = sm_manager_->db_.get_table(sel_tab_name).cols;
    all_cols.insert(all_cols.end(), sel_tab_cols.begin(), sel_tab_cols.end());
  }
  TabCol sel_col;
  for (auto &col : all_cols) {
    if (col.name.compare(x->order->cols->col_name) == 0) sel_col = {.tab_name = col.tab_name, .col_name = col.name};
  }
  return std::make_shared<SortPlan>(T_Sort, std::move(plan), sel_col, x->order->orderby_dir == ast::OrderBy_DESC);
}

std::shared_ptr<Plan> Planner::generate_aggregation_plan(std::shared_ptr<Query> query, std::shared_ptr<Plan> plan) {
  auto x = std::dynamic_pointer_cast<ast::SelectStmt>(query->parse);

  // 判断是否存在aggregation语句
  bool has_agg = false;
  std::vector<TabCol> cols = query->cols;
  for (auto &col : cols) {
    if (col.aggregation_type != AggregationType::NO_AGG) {
      has_agg = true;
    }
  }

  if (!(x->has_group || x->has_having || has_agg)) {
    return plan;
  }

  return std::make_shared<AggregationPlan>(T_Aggregation, std::move(plan), query->cols, query->groupby_cols,
                                           query->having_conds);
}

/**
 * @brief select plan 生成
 *
 * @param sel_cols select plan 选取的列
 * @param tab_names select plan 目标的表
 * @param conds select plan 选取条件
 */
std::shared_ptr<Plan> Planner::generate_select_plan(std::shared_ptr<Query> query, Context *context) {
  // 逻辑优化
  query = logical_optimization(std::move(query), context);

  // 物理优化
  auto sel_cols = query->cols;
  std::shared_ptr<Plan> plannerRoot = physical_optimization(query, context);
  if (plannerRoot->tag != T_Empty)
    plannerRoot = std::make_shared<ProjectionPlan>(T_Projection, std::move(plannerRoot), std::move(sel_cols));

  return plannerRoot;
}

// 生成DDL语句和DML语句的查询执行计划
std::shared_ptr<Plan> Planner::do_planner(std::shared_ptr<Query> query, Context *context) {
  std::shared_ptr<Plan> plannerRoot;
  if (auto x = std::dynamic_pointer_cast<ast::CreateTable>(query->parse)) {
    // create table;
    std::vector<ColDef> col_defs;
    for (auto &field : x->fields) {
      if (auto sv_col_def = std::dynamic_pointer_cast<ast::ColDef>(field)) {
        ColDef col_def = {.name = sv_col_def->col_name,
                          .type = interp_sv_type(sv_col_def->type_len->type),
                          .len = sv_col_def->type_len->len};
        col_defs.push_back(col_def);
      } else {
        throw InternalError("Unexpected field type");
      }
    }
    plannerRoot = std::make_shared<DDLPlan>(T_CreateTable, x->tab_name, std::vector<std::string>(), col_defs);
  } else if (auto x = std::dynamic_pointer_cast<ast::DropTable>(query->parse)) {
    // drop table;
    plannerRoot =
        std::make_shared<DDLPlan>(T_DropTable, x->tab_name, std::vector<std::string>(), std::vector<ColDef>());
  } else if (auto x = std::dynamic_pointer_cast<ast::CreateIndex>(query->parse)) {
    // create index;
    plannerRoot = std::make_shared<DDLPlan>(T_CreateIndex, x->tab_name, x->col_names, std::vector<ColDef>());
  } else if (auto x = std::dynamic_pointer_cast<ast::DropIndex>(query->parse)) {
    // drop index
    plannerRoot = std::make_shared<DDLPlan>(T_DropIndex, x->tab_name, x->col_names, std::vector<ColDef>());
  } else if (auto x = std::dynamic_pointer_cast<ast::InsertStmt>(query->parse)) {
    // insert;
    plannerRoot = std::make_shared<DMLPlan>(T_Insert, std::shared_ptr<Plan>(), x->tab_name, query->values,
                                            std::vector<Condition>(), std::vector<SetClause>());
  } else if (auto x = std::dynamic_pointer_cast<ast::DeleteStmt>(query->parse)) {
    // delete;
    // 生成表扫描方式
    std::shared_ptr<Plan> table_scan_executors;
    // 只有一张表，不需要进行物理优化了
    // int index_no = get_indexNo(x->tab_name, query->conds);
    std::vector<std::string> index_col_names;
    bool index_exist = get_index_cols(x->tab_name, query->conds, index_col_names);

    if (index_exist == false) {  // 该表没有索引
      index_col_names.clear();
      table_scan_executors =
          std::make_shared<ScanPlan>(T_SeqScan, sm_manager_, x->tab_name, query->conds, index_col_names);
    } else {  // 存在索引
      table_scan_executors =
          std::make_shared<ScanPlan>(T_IndexScan, sm_manager_, x->tab_name, query->conds, index_col_names);
    }

    plannerRoot = std::make_shared<DMLPlan>(T_Delete, table_scan_executors, x->tab_name, std::vector<Value>(),
                                            query->conds, std::vector<SetClause>());
  } else if (auto x = std::dynamic_pointer_cast<ast::UpdateStmt>(query->parse)) {
    // update;
    // 生成表扫描方式
    std::shared_ptr<Plan> table_scan_executors;
    // 只有一张表，不需要进行物理优化了
    // int index_no = get_indexNo(x->tab_name, query->conds);
    std::vector<std::string> index_col_names;
    bool index_exist = get_index_cols(x->tab_name, query->conds, index_col_names);

    if (index_exist == false) {  // 该表没有索引
      index_col_names.clear();
      table_scan_executors =
          std::make_shared<ScanPlan>(T_SeqScan, sm_manager_, x->tab_name, query->conds, index_col_names);
    } else {  // 存在索引
      table_scan_executors =
          std::make_shared<ScanPlan>(T_IndexScan, sm_manager_, x->tab_name, query->conds, index_col_names);
    }
    plannerRoot = std::make_shared<DMLPlan>(T_Update, table_scan_executors, x->tab_name, std::vector<Value>(),
                                            query->conds, query->set_clauses);
  } else if (auto x = std::dynamic_pointer_cast<ast::SelectStmt>(query->parse)) {
    // select;
    std::shared_ptr<plannerInfo> root = std::make_shared<plannerInfo>(x);
    // 生成select语句的查询执行计划
    std::shared_ptr<Plan> projection = generate_select_plan(std::move(query), context);
    if (projection->tag != T_Empty)
      plannerRoot = std::make_shared<DMLPlan>(T_select, projection, std::string(), std::vector<Value>(),
                                              std::vector<Condition>(), std::vector<SetClause>(), x->is_unique);
    else
      return projection;
  } else {
    throw InternalError("Unexpected AST root");
  }
  return plannerRoot;
}

}  // namespace easydb
