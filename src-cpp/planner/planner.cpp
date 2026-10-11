/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL
v2. You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

/**
 * @file planner.cpp
 * @brief 查询计划器实现
 * 
 * 本文件实现了EasyDB的查询计划器（Planner），负责将SQL查询转换为执行计划。
 * 
 * 主要功能包括：
 * 1. 逻辑优化：
 *    - 条件简化：合并同一列上的多个条件，检测矛盾条件
 *    - 连接重排序：根据表大小优化连接顺序
 *    - 条件推导：通过等值连接推导新条件
 * 
 * 2. 物理优化：
 *    - 索引选择：根据条件选择使用顺序扫描或索引扫描
 *    - 连接算法选择：嵌套循环连接、排序合并连接、哈希连接、索引合并连接
 *    - 计划生成：生成扫描、连接、聚合、排序、投影计划
 * 
 * 3. 计划生成：
 *    - DDL计划：CREATE/DROP TABLE/INDEX
 *    - DML计划：INSERT、DELETE、UPDATE、SELECT
 * 
 * 关键数据结构：
 * - ColCondRange: 列条件范围，用于条件简化
 * - ColumnUnionFind: 列等价类并查集，用于条件推导
 * - ColId: 列标识符（表名+列名）
 */

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

/**
 * @brief 列条件范围结构体
 * 
 * 用于合并和简化同一列上的多个条件谓词，将多个条件（如 a > 5, a < 10, a = 7）
 * 合并为一个范围表示，以便进行条件优化和矛盾检测。
 */
struct ColCondRange {
  bool has_equal = false;           // 是否存在等值条件（如 a = 5）
  Value equal_val;                  // 等值条件的值
  bool has_lower = false;           // 是否存在下界条件（如 a > 5 或 a >= 5）
  Value lower_val;                  // 下界值
  bool lower_inclusive = false;     // 下界是否包含边界值（true表示>=，false表示>）
  bool has_upper = false;           // 是否存在上界条件（如 a < 10 或 a <= 10）
  Value upper_val;                  // 上界值
  bool upper_inclusive = false;     // 上界是否包含边界值（true表示<=，false表示<）
  std::vector<Value> ne_values;     // 不等条件的值列表（如 a != 3, a != 4）

  /**
   * @brief 检查条件范围是否存在矛盾
   * 
   * 例如：a = 5 且 a > 10 是矛盾的；a > 5 且 a < 3 也是矛盾的
   * 
   * @return true 如果条件矛盾，查询结果为空
   * @return false 如果条件不矛盾
   */
  bool isContradictory() {
    // 若有等值条件，则检查等值是否满足上下界并与不等条件无冲突
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
        } else if (lower_val == upper_val &&
                   (!lower_inclusive || !upper_inclusive)) {
          // 下界 == 上界但没有包含这个点
          return true;
        }
      }
    }
    return false;
  }

  /**
   * @brief 将范围表示转换回条件列表
   * 
   * 将合并后的范围条件重新展开为具体的Condition对象列表
   * 
   * @param col 列标识（表名和列名）
   * @return std::vector<Condition> 转换后的条件列表
   */
  std::vector<Condition> toConditions(const TabCol &col) {
    std::vector<Condition> result;
    // 如果有等值条件，直接返回等值条件（等值条件优先级最高）
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

  /**
   * @brief 辅助函数：处理一个新的范围条件后检查矛盾
   * 
   * @return true 如果条件矛盾
   * @return false 如果条件不矛盾
   */
  bool tryCheckContradictory() { return isContradictory(); }
};

/**
 * @brief 简化查询条件
 * 
 * 将同一列上的多个条件合并为范围表示，例如：
 * - a > 5, a < 10, a != 7 可以合并为范围 [5, 10] 且排除 7
 * - a = 5, a > 3, a < 10 可以简化为 a = 5
 * 
 * 同时检测矛盾条件，如果发现矛盾（如 a = 5 且 a = 6），
 * 则设置 query->no_result = true，表示查询结果为空。
 * 
 * @param query 查询对象，其conds字段会被简化
 */
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
          } else if (cond.rhs_val == range.upper_val &&
                     !range.upper_inclusive) {
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
          } else if (cond.rhs_val == range.lower_val &&
                     !range.lower_inclusive) {
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
/**
 * @brief 列标识符结构体
 * 
 * 用于唯一标识一个列，由表名和列名组成
 */
struct ColId {
  std::string tab_name;  // 表名
  std::string col_name;  // 列名

  /**
   * @brief 相等运算符重载
   * 
   * @param o 另一个ColId对象
   * @return true 如果表名和列名都相同
   */
  bool operator==(const ColId &o) const {
    return tab_name == o.tab_name && col_name == o.col_name;
  }
};

/**
 * @brief ColId的哈希函数
 * 
 * 用于在unordered_map/unordered_set中使用ColId作为key
 */
struct ColIdHash {
  /**
   * @brief 计算ColId的哈希值
   * 
   * @param c 列标识符
   * @return size_t 哈希值
   */
  size_t operator()(const ColId &c) const {
    // 分别计算表名和列名的哈希值
    auto h1 = std::hash<std::string>()(c.tab_name);
    auto h2 = std::hash<std::string>()(c.col_name);
    // 使用位运算和黄金比例常数混合哈希值，减少冲突
    return h1 ^ (h2 + 0x9e3779b97f4a7c15ULL + (h1 << 6) + (h1 >> 2));
  }
};

// 并查集，用于把 tableA.a, tableB.a, tableC.a 等列合并到同一个“等价类”
class ColumnUnionFind {
 public:
  /**
   * @brief 查找列所在等价类的代表元素
   * 
   * 使用路径压缩优化，将查找路径上的所有节点直接连接到根节点
   * 
   * @param x 要查找的列
   * @return ColId 等价类的代表元素
   */
  ColId find(const ColId &x) {
    if (parent_.find(x) == parent_.end()) {
      parent_[x] = x;  // 若 x 不在 parent_ 里，则把它自己设为它的 parent
      return x;
    }
    if (!(parent_[x] == x)) {
      parent_[x] = find(parent_[x]);  // 路径压缩：将路径上的节点直接连接到根
    }
    return parent_[x];
  }

  /**
   * @brief 合并两个列到同一个等价类
   * 
   * @param a 第一个列
   * @param b 第二个列
   */
  void unite(const ColId &a, const ColId &b) {
    auto ra = find(a);
    auto rb = find(b);
    if (!(ra == rb)) {
      parent_[rb] = ra;  // 将rb的根节点指向ra的根节点
    }
  }

 private:
  std::unordered_map<ColId, ColId, ColIdHash> parent_;  // 并查集的父节点映射
};

}  // namespace

/**
 * @brief 通过等值连接推导条件
 * 
 * 利用等值连接（如 tableA.a = tableB.a）推导新的条件。
 * 例如：如果 tableA.a = tableB.a 且 tableA.a > 5，
 * 则可以推导出 tableB.a > 5。
 * 
 * 算法步骤：
 * 1. 构建并查集，将等值连接的列合并到同一个等价类
 * 2. 收集单表谓词（列与常量的比较条件）
 * 3. 将单表谓词复制到等价类中的其他列
 * 4. 将推导出的新条件加入查询条件列表
 * 
 * @param query 查询对象，其conds字段会被扩展
 */
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
  // 枚举一下 union-find 里出现过的所有列(方法：再扫一遍 conds,
  // 或者若有接口能直接从 union-find 里取出)
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

  // 遍历 singleTableConds[rep] 里的所有 condition, 复制到 eqClassMembers[rep]
  // 下的每个列
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
        if (colId.tab_name == oldCond.lhs_col.tab_name &&
            colId.col_name == oldCond.lhs_col.col_name) {
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

/**
 * @brief 获取可用于索引扫描的列名列表
 * 
 * 检查给定表的条件中，哪些列可以用于索引扫描。
 * 索引匹配规则：
 * - 完全匹配索引字段（列的顺序必须与索引定义一致）
 * - 支持范围查询（>, >=, <, <=, =）
 * - 不支持不等查询（!=）
 * - 不会自动调整where条件的顺序（目前是左边字段，右边值）
 * 
 * @param tab_name 表名
 * @param curr_conds 当前条件列表
 * @param index_col_names 输出参数，匹配的索引列名列表（按索引顺序）
 * @return true 如果找到匹配的索引
 * @return false 如果没有匹配的索引
 */
bool Planner::get_index_cols(std::string tab_name,
                             std::vector<Condition> curr_conds,
                             std::vector<std::string> &index_col_names) {
  index_col_names.clear();
  // for (auto &cond : curr_conds) {
  //     if(cond.is_rhs_val && cond.op == OP_EQ &&
  //     cond.lhs_col.tab_name.compare(tab_name) == 0)
  //         index_col_names.push_back(cond.lhs_col.col_name);
  // }
  std::unordered_set<std::string> added_cols;
  for (const auto &cond : curr_conds) {
    if (!cond.is_rhs_stmt && cond.lhs_col.tab_name.compare(tab_name) == 0) {
      if (added_cols.find(cond.lhs_col.col_name) == added_cols.end() &&
          cond.op != OP_NE) {
        index_col_names.push_back(cond.lhs_col.col_name);
        added_cols.insert(cond.lhs_col.col_name);
      }
    }
  }
  TabMeta &tab = sm_manager_->db_.get_table(tab_name);
  if (tab.is_index(index_col_names)) return true;
  return false;
}

/**
 * @brief 获取可用于索引扫描的列名列表（从条件右边字段匹配）
 * 
 * 与get_index_cols类似，但检查的是条件右边的列（rhs_col）。
 * 用于处理形如 "常量 op 列" 的条件（需要交换后匹配索引）。
 * 
 * @param tab_name 表名
 * @param curr_conds 当前条件列表
 * @param index_col_names 输出参数，匹配的索引列名列表（按索引顺序）
 * @return true 如果找到匹配的索引
 * @return false 如果没有匹配的索引
 */
bool Planner::get_index_cols_swap(std::string tab_name,
                                  std::vector<Condition> curr_conds,
                                  std::vector<std::string> &index_col_names) {
  index_col_names.clear();
  // for (auto &cond : curr_conds) {
  //     if(cond.is_rhs_val && cond.op == OP_EQ &&
  //     cond.lhs_col.tab_name.compare(tab_name) == 0)
  //         index_col_names.push_back(cond.lhs_col.col_name);
  // }
  std::unordered_set<std::string> added_cols;
  for (const auto &cond : curr_conds) {
    if (!cond.is_rhs_val && cond.rhs_col.tab_name.compare(tab_name) == 0) {
      if (added_cols.find(cond.rhs_col.col_name) == added_cols.end() &&
          cond.op != OP_NE) {
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
 * @brief 从条件列表中提取与指定表相关的条件
 * 
 * 提取满足以下条件之一的条件：
 * 1. 左列属于指定表且右端是子查询或常量
 * 2. 左列和右列都属于指定表（单表条件）
 * 
 * 提取的条件会从原条件列表中移除。
 * 
 * @param conds 条件列表（会被修改，提取的条件会被移除）
 * @param tab_names 表名
 * @return std::vector<Condition> 提取出的条件列表
 */
std::vector<Condition> pop_conds(std::vector<Condition> &conds,
                                 std::string tab_names) {
  // auto has_tab = [&](const std::string &tab_name) {
  //     return std::find(tab_names.begin(), tab_names.end(), tab_name) !=
  //     tab_names.end();
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

/**
 * @brief 将条件下推到计划树中
 * 
 * 递归地将条件下推到计划树中，尽可能靠近数据源。
 * 这样可以尽早过滤数据，减少后续处理的数据量。
 * 
 * 返回值说明：
 * - 0: 条件中的列不在该计划节点中
 * - 1: 条件左列匹配该计划节点
 * - 2: 条件右列匹配该计划节点
 * - 3: 条件已成功下推
 * 
 * @param cond 要下推的条件（可能会被修改，如交换左右列）
 * @param plan 计划树节点
 * @return int 下推结果（见上述说明）
 */
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
          {OP_EQ, OP_EQ}, {OP_NE, OP_NE}, {OP_LT, OP_GT},
          {OP_GT, OP_LT}, {OP_LE, OP_GE}, {OP_GE, OP_LE},
      };
      std::swap(cond->lhs_col, cond->rhs_col);
      cond->op = swap_op.at(cond->op);
    }
    x->conds_.emplace_back(std::move(*cond));
    return 3;
  }
  return false;
}

/**
 * @brief 从计划列表中提取指定表的扫描计划
 * 
 * 查找并返回指定表的扫描计划，同时标记该表已被使用。
 * 
 * @param scantbl 表使用标记数组（会被修改）
 * @param table 要查找的表名
 * @param joined_tables 已连接的表列表（会被修改，添加找到的表）
 * @param plans 扫描计划列表
 * @return std::shared_ptr<Plan> 找到的扫描计划，如果未找到则返回nullptr
 */
std::shared_ptr<Plan> pop_scan(int *scantbl, std::string table,
                               std::vector<std::string> &joined_tables,
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

/**
 * @brief 根据表大小重排序条件
 * 
 * 将条件分为单表条件和连接条件，然后：
 * 1. 根据表大小和distinct值对连接条件进行排序（小表优先）
 * 2. 对于每个连接条件，如果左表大于右表，则交换左右列
 * 3. 将单表条件放在前面，连接条件放在后面
 * 
 * 这样可以在连接时优先处理小表，减少中间结果的大小。
 * 
 * @param query 查询对象，其conds字段会被重排序
 */
void Planner::reorder_conds_based_on_table_size(std::shared_ptr<Query> query) {
  std::vector<Condition> join_conds;    // 连接条件（涉及多个表）
  std::vector<Condition> single_conds;   // 单表条件（只涉及一个表）

  // 将query->conds拆分为join条件和单表条件
  for (auto &cond : query->conds) {
    bool is_join_cond = (!cond.is_rhs_val && !cond.is_rhs_stmt &&
                         cond.lhs_col.tab_name != cond.rhs_col.tab_name);
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

  auto get_max_distinct_size = [&](const std::string &left_tab_name,
                                   const std::string &left_col_name,
                                   const std::string &right_tab_name,
                                   const std::string &right_col_name) {
    int left_count =
        sm_manager_->GetTableAttrDistinct(left_tab_name, left_col_name);
    int right_count =
        sm_manager_->GetTableAttrDistinct(right_tab_name, right_col_name);
    if (left_count < 0 && right_count < 0) {
      return 1;  // 若无统计信息则返回1，相当于不进行distinct值统计
    }
    return left_count > right_count ? left_count : right_count;
  };

  // 根据表大小对join_conds进行排序，小表优先
  // 这里使用两表大小的乘积作为简易估计值
  std::sort(join_conds.begin(), join_conds.end(),
            [&](const Condition &a, const Condition &b) {
              int a_size =
                  (get_table_size(a.lhs_col.tab_name) *
                   get_table_size(a.rhs_col.tab_name)) /
                  get_max_distinct_size(a.lhs_col.tab_name, a.lhs_col.col_name,
                                        a.rhs_col.tab_name, a.rhs_col.col_name);
              int b_size =
                  (get_table_size(b.lhs_col.tab_name) *
                   get_table_size(b.rhs_col.tab_name)) /
                  get_max_distinct_size(b.lhs_col.tab_name, b.lhs_col.col_name,
                                        b.rhs_col.tab_name, b.rhs_col.col_name);
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

/**
 * @brief 逻辑优化
 * 
 * 对查询进行逻辑层面的优化，包括：
 * 1. 重排序连接顺序（多表查询时）
 * 2. 根据表大小重排序条件
 * 3. 通过等值连接推导新条件
 * 4. 简化条件（合并同一列上的多个条件，检测矛盾）
 * 
 * @param query 查询对象
 * @param context 执行上下文
 * @return std::shared_ptr<Query> 优化后的查询对象
 */
std::shared_ptr<Query> Planner::logical_optimization(
    std::shared_ptr<Query> query, Context *context) {
  if (GetEnableOptimizer()) {
    // 调用reorder_joins对query->tables进行连接顺序重排
    if (query->tables.size() > 1) {
      reorder_joins(query);
    }
    reorder_conds_based_on_table_size(query);
    deduce_conditions_via_equijoin(query);
    // 简化条件：合并同一列上的多个条件，检测矛盾
    simplify_conditions(query);
  }
  return query;
}

/**
 * @brief 重排序连接顺序
 * 
 * 根据表的大小（行数）对表进行排序，小表优先。
 * 这是一个简单的启发式优化，目的是减少连接时的中间结果大小。
 * 
 * @param query 查询对象，其optimized_table_order字段会被设置
 */
void Planner::reorder_joins(std::shared_ptr<Query> query) {
  // 简单启发式：对query->tables根据其大小(行数)进行升序排序
  // 获取每个表的代价(用行数代替)
  std::vector<std::pair<std::string, double>> table_costs;
  for (auto &t : query->tables) {
    double cost = estimate_table_scan_cost(t);
    table_costs.emplace_back(t, cost);
  }

  // 按照cost从小到大排序
  std::sort(table_costs.begin(), table_costs.end(),
            [](auto &a, auto &b) { return a.second < b.second; });

  query->optimized_table_order.clear();
  for (auto &tc : table_costs) {
    query->optimized_table_order.push_back(tc.first);
  }
}

/**
 * @brief 估计表扫描的代价
 * 
 * 使用表的行数作为扫描代价的估计值。
 * 
 * @param tab_name 表名
 * @return double 估计的扫描代价（行数）
 */
double Planner::estimate_table_scan_cost(const std::string &tab_name) {
  // 简单估计：行数越多，cost越高。行数从sm_manager_获取
  int count = sm_manager_->GetTableCount(tab_name);
  if (count < 0) {
    // 如果没有统计信息，假设一个默认值
    return 1000.0;
  }
  return static_cast<double>(count);
}

/**
 * @brief 估计连接的代价
 * 
 * 使用两表大小的乘积作为连接代价的估计值（笛卡尔积大小）。
 * 这是一个简化的启发式，实际连接代价取决于连接类型和选择性。
 * 
 * @param left_table 左表名
 * @param right_table 右表名
 * @return double 估计的连接代价
 */
double Planner::estimate_join_cost(const std::string &left_table,
                                   const std::string &right_table) {
  // 简单启发式join代价估计 = 两表大小相乘 (笛卡尔积大小)
  double left_cost = estimate_table_scan_cost(left_table);
  double right_cost = estimate_table_scan_cost(right_table);
  return left_cost * right_cost;
}

/**
 * @brief 物理优化
 * 
 * 将逻辑查询计划转换为物理执行计划，包括：
 * 1. 生成关系计划（扫描和连接）
 * 2. 生成聚合计划（如果有GROUP BY或聚合函数）
 * 3. 生成排序计划（如果有ORDER BY）
 * 
 * @param query 查询对象
 * @param context 执行上下文
 * @return std::shared_ptr<Plan> 物理执行计划
 */
std::shared_ptr<Plan> Planner::physical_optimization(
    std::shared_ptr<Query> query, Context *context) {
  // 若no_result为true（条件矛盾），直接返回EmptyPlan
  if (query->no_result) {
    return std::make_shared<EmptyPlan>();
  }
  // 生成关系计划（扫描和连接）
  std::shared_ptr<Plan> plan = make_one_rel(query, context);
  if (GetEnableOptimizer()) {
    // 其他物理优化（可在此处添加）
  }

  // 处理聚合（GROUP BY, HAVING, 聚合函数）
  plan = generate_aggregation_plan(query, std::move(plan));
  // 处理排序（ORDER BY）
  plan = generate_sort_plan(query, std::move(plan));

  return plan;
}

/**
 * @brief 生成关系计划（扫描和连接）
 * 
 * 为查询中的所有表生成扫描计划，然后根据连接条件生成连接计划。
 * 
 * 算法步骤：
 * 1. 为每个表生成扫描计划（顺序扫描或索引扫描）
 * 2. 根据连接条件逐步构建连接计划
 * 3. 处理剩余未连接的表（笛卡尔积）
 * 
 * @param query 查询对象
 * @param context 执行上下文
 * @return std::shared_ptr<Plan> 关系计划（扫描+连接）
 */
std::shared_ptr<Plan> Planner::make_one_rel(std::shared_ptr<Query> query,
                                            Context *context) {
  auto x = std::dynamic_pointer_cast<ast::SelectStmt>(query->parse);
  // 使用优化后的表顺序（如果存在），否则使用原始顺序
  std::vector<std::string> tables = query->optimized_table_order.empty()
                                        ? query->tables
                                        : query->optimized_table_order;

  std::vector<std::shared_ptr<Plan>> table_scan_executors(tables.size());

  // 为每个表生成扫描计划
  for (size_t i = 0; i < tables.size(); i++) {
    // 提取与该表相关的条件
    auto curr_conds = pop_conds(query->conds, tables[i]);
    // 处理子查询条件：为子查询生成执行计划
    for (auto &cond : curr_conds) {
      if (cond.is_rhs_stmt && !cond.is_rhs_exe_processed) {
        auto rhs_stmt_ptr = std::make_shared<Query>(cond.rhs_stmt);
        std::shared_ptr<Plan> rhs_stmt_plan = do_planner(rhs_stmt_ptr, context);
        cond.rhs_stmt = std::static_pointer_cast<void>(rhs_stmt_plan);
      }
    }
    // 检查是否有可用的索引
    std::vector<std::string> index_col_names;
    bool index_exist = get_index_cols(tables[i], curr_conds, index_col_names);
    if (index_exist == false) {  // 该表没有匹配的索引，使用顺序扫描
      index_col_names.clear();
      table_scan_executors[i] = std::make_shared<ScanPlan>(
          T_SeqScan, sm_manager_, tables[i], curr_conds, index_col_names);
    } else {  // 存在匹配的索引，使用索引扫描
      table_scan_executors[i] = std::make_shared<ScanPlan>(
          T_IndexScan, sm_manager_, tables[i], curr_conds, index_col_names);
    }
  }
  // 只有一个表，不需要join，直接返回扫描计划
  if (tables.size() == 1) {
    return table_scan_executors[0];
  }
  // 获取剩余的where条件（主要是连接条件）
  auto conds = std::move(query->conds);
  std::shared_ptr<Plan> table_join_executors;

  // 标记哪些表已经被使用（-1表示未使用，1表示已使用）
  int scantbl[tables.size()];
  for (size_t i = 0; i < tables.size(); i++) {
    scantbl[i] = -1;
  }
  
  if (conds.size() >= 1) {
    // 有连接条件，开始构建连接计划

    // 根据连接条件，生成第一层join
    std::vector<std::string> joined_tables(tables.size());
    auto it = conds.begin();
    while (it != conds.end()) {
      // 获取连接条件涉及的两个表的扫描计划
      std::shared_ptr<ScanPlan> left, right;
      left = std::dynamic_pointer_cast<ScanPlan>(pop_scan(
          scantbl, it->lhs_col.tab_name, joined_tables, table_scan_executors));
      right = std::dynamic_pointer_cast<ScanPlan>(pop_scan(
          scantbl, it->rhs_col.tab_name, joined_tables, table_scan_executors));
      std::vector<Condition> join_conds{*it};
      // 建立join
      // 根据配置和索引情况判断使用哪种join方式
      if (enable_nestedloop_join && enable_sortmerge_join) {
        // 默认nested loop join
        table_join_executors = std::make_shared<JoinPlan>(
            T_NestLoop, std::move(left), std::move(right), join_conds);
      } else if (enable_nestedloop_join) {
        table_join_executors = std::make_shared<JoinPlan>(
            T_NestLoop, std::move(left), std::move(right), join_conds);
      } else if (enable_sortmerge_join) {
        std::vector<std::string> index_col_name_left;
        std::vector<std::string> index_col_name_right;
        // TODO: 临时fix，后续去除
        bool left_index_exist = get_index_cols(it->lhs_col.tab_name, join_conds,
                                               index_col_name_left);
        bool right_index_exist = get_index_cols_swap(
            it->rhs_col.tab_name, join_conds, index_col_name_right);
        if (left_index_exist && right_index_exist) {  // join列存在索引
          // 强行将scan替换为indexscan，前面会由于涉及到多个表而没办法定义为index_scan
          // Note that we need the original condition!
          // 重新创建索引扫描计划
          left = std::make_shared<ScanPlan>(
              T_IndexScan, sm_manager_, it->lhs_col.tab_name, left->get_conds(),
              index_col_name_left);
          right = std::make_shared<ScanPlan>(
              T_IndexScan, sm_manager_, it->rhs_col.tab_name,
              right->get_conds(), index_col_name_right);

          // 使用索引合并连接（Index Merge Join）
          table_join_executors = std::make_shared<JoinPlan>(
              T_IndexMerge, std::move(left), std::move(right), join_conds);
        } else {  // 不存在索引，使用排序合并连接
          table_join_executors = std::make_shared<JoinPlan>(
              T_SortMerge, std::move(left), std::move(right), join_conds);
        }
      } else if (enable_hash_join) {
        table_join_executors = std::make_shared<JoinPlan>(
            T_HashJoin, std::move(left), std::move(right), join_conds);
      } else {
        // error
        throw EASYDBError("No join executor selected!");
      }

      // table_join_executors = std::make_shared<JoinPlan>(T_NestLoop,
      // std::move(left), std::move(right), join_conds);
      it = conds.erase(it);
      break;
    }
    // 根据连接条件，生成第2-n层join（处理剩余的连接条件）
    it = conds.begin();
    while (it != conds.end()) {
      std::shared_ptr<Plan> left_need_to_join_executors = nullptr;
      std::shared_ptr<Plan> right_need_to_join_executors = nullptr;
      bool isneedreverse = false;  // 是否需要交换条件的左右列
      if (std::find(joined_tables.begin(), joined_tables.end(),
                    it->lhs_col.tab_name) == joined_tables.end()) {
        left_need_to_join_executors = pop_scan(
            scantbl, it->lhs_col.tab_name, joined_tables, table_scan_executors);
      }
      if (std::find(joined_tables.begin(), joined_tables.end(),
                    it->rhs_col.tab_name) == joined_tables.end()) {
        right_need_to_join_executors = pop_scan(
            scantbl, it->rhs_col.tab_name, joined_tables, table_scan_executors);
        isneedreverse = true;
      }

      if (left_need_to_join_executors != nullptr &&
          right_need_to_join_executors != nullptr) {
        // 两个表都未连接，先连接这两个表，再与已有连接结果连接
        std::vector<Condition> join_conds{*it};
        std::shared_ptr<Plan> temp_join_executors = std::make_shared<JoinPlan>(
            T_NestLoop, std::move(left_need_to_join_executors),
            std::move(right_need_to_join_executors), join_conds);
        // 与已有连接结果进行笛卡尔积连接
        table_join_executors = std::make_shared<JoinPlan>(
            T_NestLoop, std::move(temp_join_executors),
            std::move(table_join_executors), std::vector<Condition>());
      } else if (left_need_to_join_executors != nullptr ||
                 right_need_to_join_executors != nullptr) {
        // 只有一个表未连接，直接与已有连接结果连接
        if (isneedreverse) {
          // 需要交换条件的左右列和操作符
          std::map<CompOp, CompOp> swap_op = {
              {OP_EQ, OP_EQ}, {OP_NE, OP_NE}, {OP_LT, OP_GT},
              {OP_GT, OP_LT}, {OP_LE, OP_GE}, {OP_GE, OP_LE},
          };
          std::swap(it->lhs_col, it->rhs_col);
          it->op = swap_op.at(it->op);
          left_need_to_join_executors = std::move(right_need_to_join_executors);
        }
        std::vector<Condition> join_conds{*it};
        table_join_executors = std::make_shared<JoinPlan>(
            T_NestLoop, std::move(left_need_to_join_executors),
            std::move(table_join_executors), join_conds);
      } else {
        // 两个表都已连接，将条件下推到连接计划中
        push_conds(std::move(&(*it)), table_join_executors);
      }
      it = conds.erase(it);
    }
  } else {
    // 没有连接条件，从第一个表开始
    table_join_executors = table_scan_executors[0];
    scantbl[0] = 1;
  }

  // 连接剩余未连接的表（使用笛卡尔积）
  for (size_t i = 0; i < tables.size(); i++) {
    if (scantbl[i] == -1) {
      // 该表还未连接，使用笛卡尔积连接（无连接条件）
      table_join_executors = std::make_shared<JoinPlan>(
          T_NestLoop, std::move(table_scan_executors[i]),
          std::move(table_join_executors), std::vector<Condition>());
    }
  }

  return table_join_executors;
}

/**
 * @brief 生成排序计划
 * 
 * 如果查询包含ORDER BY子句，则在计划树顶部添加排序节点。
 * 
 * @param query 查询对象
 * @param plan 输入计划
 * @return std::shared_ptr<Plan> 如果有序则返回排序计划，否则返回原计划
 */
std::shared_ptr<Plan> Planner::generate_sort_plan(std::shared_ptr<Query> query,
                                                  std::shared_ptr<Plan> plan) {
  auto x = std::dynamic_pointer_cast<ast::SelectStmt>(query->parse);
  if (!x->has_sort) {
    return plan;  // 没有ORDER BY，直接返回原计划
  }
  // 收集所有表的列信息
  std::vector<std::string> tables = query->tables;
  std::vector<ColMeta> all_cols;
  for (auto &sel_tab_name : tables) {
    // 这里db_不能写成get_db(), 注意要传指针
    const auto &sel_tab_cols = sm_manager_->db_.get_table(sel_tab_name).cols;
    all_cols.insert(all_cols.end(), sel_tab_cols.begin(), sel_tab_cols.end());
  }
  // 查找排序列
  TabCol sel_col;
  for (auto &col : all_cols) {
    if (col.name.compare(x->order->cols->col_name) == 0)
      sel_col = {.tab_name = col.tab_name, .col_name = col.name};
  }
  // 创建排序计划（DESC或ASC）
  return std::make_shared<SortPlan>(T_Sort, std::move(plan), sel_col,
                                    x->order->orderby_dir == ast::OrderBy_DESC);
}

/**
 * @brief 生成聚合计划
 * 
 * 如果查询包含GROUP BY、HAVING或聚合函数（如COUNT、SUM等），
 * 则在计划树顶部添加聚合节点。
 * 
 * @param query 查询对象
 * @param plan 输入计划
 * @return std::shared_ptr<Plan> 如果有聚合则返回聚合计划，否则返回原计划
 */
std::shared_ptr<Plan> Planner::generate_aggregation_plan(
    std::shared_ptr<Query> query, std::shared_ptr<Plan> plan) {
  auto x = std::dynamic_pointer_cast<ast::SelectStmt>(query->parse);

  // 判断是否存在聚合函数（COUNT、SUM、AVG、MAX、MIN等）
  bool has_agg = false;
  std::vector<TabCol> cols = query->cols;
  for (auto &col : cols) {
    if (col.aggregation_type != AggregationType::NO_AGG) {
      has_agg = true;
    }
  }

  // 如果没有GROUP BY、HAVING和聚合函数，不需要聚合计划
  if (!(x->has_group || x->has_having || has_agg)) {
    return plan;
  }

  // 创建聚合计划
  return std::make_shared<AggregationPlan>(T_Aggregation, std::move(plan),
                                           query->cols, query->groupby_cols,
                                           query->having_conds);
}

/**
 * @brief 生成SELECT查询的执行计划
 * 
 * 完整的SELECT查询计划生成流程：
 * 1. 逻辑优化（重排序、条件推导、条件简化等）
 * 2. 物理优化（生成扫描、连接、聚合、排序计划）
 * 3. 投影（选择需要的列）
 * 
 * @param query 查询对象
 * @param context 执行上下文
 * @return std::shared_ptr<Plan> SELECT查询的执行计划
 */
std::shared_ptr<Plan> Planner::generate_select_plan(
    std::shared_ptr<Query> query, Context *context) {
  // 逻辑优化：重排序连接、推导条件、简化条件等
  query = logical_optimization(std::move(query), context);

  // 物理优化：生成扫描、连接、聚合、排序计划
  auto sel_cols = query->cols;
  std::shared_ptr<Plan> plannerRoot = physical_optimization(query, context);
  // 如果不是空计划，添加投影节点（选择需要的列）
  if (plannerRoot->tag != T_Empty)
    plannerRoot = std::make_shared<ProjectionPlan>(
        T_Projection, std::move(plannerRoot), std::move(sel_cols));

  return plannerRoot;
}

/**
 * @brief 主计划器入口函数
 * 
 * 根据查询类型（DDL或DML）生成相应的执行计划：
 * - DDL: CREATE TABLE, DROP TABLE, CREATE INDEX, DROP INDEX
 * - DML: INSERT, DELETE, UPDATE, SELECT
 * 
 * @param query 查询对象（包含解析后的AST）
 * @param context 执行上下文
 * @return std::shared_ptr<Plan> 执行计划
 */
std::shared_ptr<Plan> Planner::do_planner(std::shared_ptr<Query> query,
                                          Context *context) {
  std::shared_ptr<Plan> plannerRoot;
  if (auto x = std::dynamic_pointer_cast<ast::CreateTable>(query->parse)) {
    // CREATE TABLE语句
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
    plannerRoot = std::make_shared<DDLPlan>(
        T_CreateTable, x->tab_name, std::vector<std::string>(), col_defs);
  } else if (auto x = std::dynamic_pointer_cast<ast::DropTable>(query->parse)) {
    // DROP TABLE语句
    plannerRoot = std::make_shared<DDLPlan>(T_DropTable, x->tab_name,
                                            std::vector<std::string>(),
                                            std::vector<ColDef>());
  } else if (auto x =
                 std::dynamic_pointer_cast<ast::CreateIndex>(query->parse)) {
    // CREATE INDEX语句
    plannerRoot = std::make_shared<DDLPlan>(
        T_CreateIndex, x->tab_name, x->col_names, std::vector<ColDef>());
  } else if (auto x = std::dynamic_pointer_cast<ast::DropIndex>(query->parse)) {
    // DROP INDEX语句
    plannerRoot = std::make_shared<DDLPlan>(
        T_DropIndex, x->tab_name, x->col_names, std::vector<ColDef>());
  } else if (auto x =
                 std::dynamic_pointer_cast<ast::InsertStmt>(query->parse)) {
    // INSERT语句
    plannerRoot = std::make_shared<DMLPlan>(
        T_Insert, std::shared_ptr<Plan>(), x->tab_name, query->values,
        std::vector<Condition>(), std::vector<SetClause>());
  } else if (auto x =
                 std::dynamic_pointer_cast<ast::DeleteStmt>(query->parse)) {
    // DELETE语句
    // 生成表扫描方式（顺序扫描或索引扫描）
    std::shared_ptr<Plan> table_scan_executors;
    // 检查是否有可用的索引
    std::vector<std::string> index_col_names;
    bool index_exist =
        get_index_cols(x->tab_name, query->conds, index_col_names);

    if (index_exist == false) {  // 该表没有匹配的索引，使用顺序扫描
      index_col_names.clear();
      table_scan_executors = std::make_shared<ScanPlan>(
          T_SeqScan, sm_manager_, x->tab_name, query->conds, index_col_names);
    } else {  // 存在匹配的索引，使用索引扫描
      table_scan_executors = std::make_shared<ScanPlan>(
          T_IndexScan, sm_manager_, x->tab_name, query->conds, index_col_names);
    }

    plannerRoot = std::make_shared<DMLPlan>(
        T_Delete, table_scan_executors, x->tab_name, std::vector<Value>(),
        query->conds, std::vector<SetClause>());
  } else if (auto x =
                 std::dynamic_pointer_cast<ast::UpdateStmt>(query->parse)) {
    // UPDATE语句
    // 生成表扫描方式（顺序扫描或索引扫描）
    std::shared_ptr<Plan> table_scan_executors;
    // 检查是否有可用的索引
    std::vector<std::string> index_col_names;
    bool index_exist =
        get_index_cols(x->tab_name, query->conds, index_col_names);

    if (index_exist == false) {  // 该表没有匹配的索引，使用顺序扫描
      index_col_names.clear();
      table_scan_executors = std::make_shared<ScanPlan>(
          T_SeqScan, sm_manager_, x->tab_name, query->conds, index_col_names);
    } else {  // 存在匹配的索引，使用索引扫描
      table_scan_executors = std::make_shared<ScanPlan>(
          T_IndexScan, sm_manager_, x->tab_name, query->conds, index_col_names);
    }
    plannerRoot = std::make_shared<DMLPlan>(T_Update, table_scan_executors,
                                            x->tab_name, std::vector<Value>(),
                                            query->conds, query->set_clauses);
  } else if (auto x =
                 std::dynamic_pointer_cast<ast::SelectStmt>(query->parse)) {
    // SELECT语句
    std::shared_ptr<plannerInfo> root = std::make_shared<plannerInfo>(x);
    // 生成select语句的查询执行计划（包括逻辑优化、物理优化、投影等）
    std::shared_ptr<Plan> projection =
        generate_select_plan(std::move(query), context);
    if (projection->tag != T_Empty)
      plannerRoot = std::make_shared<DMLPlan>(
          T_select, projection, std::string(), std::vector<Value>(),
          std::vector<Condition>(), std::vector<SetClause>(), x->is_unique);
    else
      return projection;  // 空计划直接返回
  } else {
    throw InternalError("Unexpected AST root");
  }
  return plannerRoot;
}

}  // namespace easydb
