#pragma once

#include <memory>
#include <unordered_map>
#include <vector>
#include "common/common.h"
#include "common/condition.h"
#include "common/errors.h"
#include "common/hashutil.h"
#include "defs.h"
#include "executor_abstract.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace easydb {

/**
 * @brief 哈希连接键结构体
 * 
 * HashJoinKey 用于哈希连接操作，存储连接列的值。
 * 支持多列连接（多个值组成一个键）。
 */
struct HashJoinKey {
  /**
   * @brief 连接列的值向量
   * @note 每个元素对应一个连接列的值
   */
  std::vector<Value> values_;

  /**
   * @brief 相等运算符重载
   * @param other 另一个HashJoinKey对象
   * @return true 如果两个键的所有值都相等
   */
  bool operator==(const HashJoinKey &other) const {
    if (values_.size() != other.values_.size()) {
      return false;
    }
    for (size_t i = 0; i < values_.size(); ++i) {
      if (values_[i].CompareEquals(other.values_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};

}  // namespace easydb

namespace std {

/** Implements std::hash on HashJoinKey */
template <>
struct hash<easydb::HashJoinKey> {
  std::size_t operator()(const easydb::HashJoinKey &key) const {
    std::size_t curr_hash = 0;
    for (const auto &value : key.values_) {
      curr_hash = easydb::HashUtil::CombineHashes(curr_hash, easydb::HashUtil::HashValue(&value));
    }
    return curr_hash;
  }
};

}  // namespace std

namespace easydb {

/**
 * @brief 哈希连接执行器类
 * 
 * HashJoinExecutor 实现哈希连接操作，使用哈希表加速等值连接。
 * 如果连接条件不是等值条件，则回退到嵌套循环连接。
 * 
 * 算法流程：
 * 1. Build阶段：将左表的所有元组构建哈希表
 * 2. Probe阶段：遍历右表，在哈希表中查找匹配的元组
 */
class HashJoinExecutor : public AbstractExecutor {
 private:
  /**
   * @brief 左子节点执行器
   */
  std::unique_ptr<AbstractExecutor> left_;
  
  /**
   * @brief 右子节点执行器
   */
  std::unique_ptr<AbstractExecutor> right_;
  
  /**
   * @brief 左表名称
   */
  std::string left_tab_name_;
  
  /**
   * @brief 右表名称
   */
  std::string right_tab_name_;
  
  /**
   * @brief join后的表名称
   */
  std::string join_tab_name_;
  
  /**
   * @brief join后获得的每条记录的长度（字节数）
   */
  size_t len_;
  
  /**
   * @brief join后生成的记录的字段模式
   */
  Schema schema_;
  
  /**
   * @brief 连接条件列表
   */
  std::vector<Condition> conds_;
  
  /**
   * @brief 是否到达末尾标志
   */
  bool isend_;

  /**
   * @brief 哈希表数据结构
   * @note Key: 连接键，Value: 左表元组
   *       使用multimap支持一对多连接
   */
  std::unordered_multimap<HashJoinKey, Tuple> hash_table_;

  /**
   * @brief 连接列列表
   */
  std::vector<Column> left_join_cols_;   /**< 左表连接列 */
  std::vector<Column> right_join_cols_;  /**< 右表连接列 */

  /**
   * @brief 哈希连接迭代器
   */
  std::unordered_multimap<HashJoinKey, Tuple>::iterator match_iter_;  /**< 当前匹配的迭代器 */
  std::unordered_multimap<HashJoinKey, Tuple>::iterator match_end_;  /**< 匹配范围的结束迭代器 */
  
  /**
   * @brief 当前探测的右表元组
   */
  Tuple current_probe_tuple_;

  /**
   * @brief 是否使用嵌套循环连接（回退方案）
   * @note 如果连接条件不是等值条件，则使用嵌套循环连接
   */
  bool use_nested_loop_;
  
  /**
   * @brief 嵌套循环连接的索引
   */
  size_t left_idx_;   /**< 左表缓冲区索引 */
  size_t right_idx_;  /**< 右表缓冲区索引 */
  
  /**
   * @brief 嵌套循环连接的缓冲区
   */
  std::vector<Tuple> left_buffer_;   /**< 左表元组缓冲区 */
  std::vector<Tuple> right_buffer_;  /**< 右表元组缓冲区 */

 public:
  HashJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right,
                   std::vector<Condition> conds);

  void beginTuple() override;
  void nextTuple() override;
  std::unique_ptr<Tuple> Next() override;
  RID &rid() override { return _abstract_rid; }

  size_t tupleLen() const override { return len_; }
  const Schema &schema() const override { return schema_; }
  std::string getTabName() const override { return join_tab_name_; }

  bool IsEnd() const override { return isend_; }

 private:
  void BuildHashTable();
  void ProbeHashTable();
  bool predicate(const Tuple &left_tuple, const Tuple &right_tuple);
  void NestedLoopBegin();
  void NestedLoopNext();
};

HashJoinExecutor::HashJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right,
                                   std::vector<Condition> conds)
    : left_(std::move(left)),
      right_(std::move(right)),
      conds_(std::move(conds)),
      isend_(false),
      use_nested_loop_(false),
      left_idx_(0),
      right_idx_(0) {
  left_tab_name_ = left_->getTabName();
  right_tab_name_ = right_->getTabName();
  join_tab_name_ = left_tab_name_ + "_" + right_tab_name_;

  // Build the output schema by combining left and right schemas
  auto left_columns = left_->schema().GetColumns();
  auto right_columns = right_->schema().GetColumns();
  left_columns.insert(left_columns.end(), right_columns.begin(), right_columns.end());
  schema_ = Schema(left_columns);

  len_ = left_->tupleLen() + right_->tupleLen();

  // Determine join columns
  for (auto &cond : conds_) {
    if (cond.op == OP_EQ && !cond.is_rhs_val) {
      // Both sides are columns
      if (cond.lhs_col.tab_name == left_tab_name_ || cond.rhs_col.tab_name == right_tab_name_) {
        left_join_cols_.push_back(left_->schema().GetColumn(cond.lhs_col.col_name));
        right_join_cols_.push_back(right_->schema().GetColumn(cond.rhs_col.col_name));
      } else if (cond.rhs_col.tab_name == left_tab_name_ || cond.lhs_col.tab_name == right_tab_name_) {
        left_join_cols_.push_back(left_->schema().GetColumn(cond.rhs_col.col_name));
        right_join_cols_.push_back(right_->schema().GetColumn(cond.lhs_col.col_name));
      }
    }
  }

  if (left_join_cols_.empty()) {
    // No equality conditions, cannot perform hash join, use nested loop join
    use_nested_loop_ = true;
  }
}

void HashJoinExecutor::beginTuple() {
  if (use_nested_loop_) {
    // Use nested loop join
    NestedLoopBegin();
    return;
  }

  // Hash join
  BuildHashTable();
  right_->beginTuple();
  if (right_->IsEnd()) {
    isend_ = true;
    return;
  }
  do {
    current_probe_tuple_ = *(right_->Next());
    ProbeHashTable();
    while (match_iter_ != match_end_) {
      if (predicate(match_iter_->second, current_probe_tuple_)) {
        // Found a matching tuple that satisfies all conditions
        return;
      }
      ++match_iter_;
    }
    right_->nextTuple();
  } while (!right_->IsEnd());
  isend_ = true;
}

void HashJoinExecutor::nextTuple() {
  if (use_nested_loop_) {
    NestedLoopNext();
    return;
  }

  // Hash join
  while (true) {
    ++match_iter_;
    while (match_iter_ == match_end_) {
      right_->nextTuple();
      if (right_->IsEnd()) {
        isend_ = true;
        return;
      }
      current_probe_tuple_ = *(right_->Next());
      ProbeHashTable();
    }
    if (predicate(match_iter_->second, current_probe_tuple_)) {
      // Found a matching tuple that satisfies all conditions
      return;
    }
    // Else, continue to next matching tuple
  }
}

std::unique_ptr<Tuple> HashJoinExecutor::Next() {
  if (isend_) {
    return nullptr;
  }
  if (use_nested_loop_) {
    // Combine the current matching left and right tuples
    auto left_values = left_buffer_[left_idx_].GetValueVec(&left_->schema());
    auto right_values = right_buffer_[right_idx_].GetValueVec(&right_->schema());
    left_values.insert(left_values.end(), right_values.begin(), right_values.end());
    Tuple joined_tuple(left_values, &schema_);
    return std::make_unique<Tuple>(std::move(joined_tuple));
  } else {
    // Hash join
    auto left_values = match_iter_->second.GetValueVec(&left_->schema());
    auto right_values = current_probe_tuple_.GetValueVec(&right_->schema());
    left_values.insert(left_values.end(), right_values.begin(), right_values.end());
    Tuple joined_tuple(left_values, &schema_);
    return std::make_unique<Tuple>(std::move(joined_tuple));
  }
}

void HashJoinExecutor::BuildHashTable() {
  // Build the hash table from the left input
  left_->beginTuple();
  while (!left_->IsEnd()) {
    Tuple tuple = *(left_->Next());
    // Extract join keys
    std::vector<Value> key_values;
    for (const auto &col : left_join_cols_) {
      key_values.push_back(tuple.GetValue(&left_->schema(), col.GetName()));
    }
    HashJoinKey key{key_values};
    hash_table_.emplace(key, tuple);
    left_->nextTuple();
  }
}

void HashJoinExecutor::ProbeHashTable() {
  // Extract join keys from the right tuple
  std::vector<Value> key_values;
  for (const auto &col : right_join_cols_) {
    key_values.push_back(current_probe_tuple_.GetValue(&right_->schema(), col.GetName()));
  }
  HashJoinKey key{key_values};
  auto range = hash_table_.equal_range(key);
  match_iter_ = range.first;
  match_end_ = range.second;
}

bool HashJoinExecutor::predicate(const Tuple &left_tuple, const Tuple &right_tuple) {
  for (const auto &cond : conds_) {
    Value lhs_v, rhs_v;
    if (!cond.is_rhs_val) {
      // Both sides are columns
      // If the left or right is a join executor, then the table name will be join_tab_name instead of tab_name in the
      // condition. We assume that there must be a raw table name from the left or right executor, that means one side
      // must not be join executor.
      if (cond.lhs_col.tab_name == left_tab_name_ || cond.rhs_col.tab_name == right_tab_name_) {
        lhs_v = left_tuple.GetValue(&left_->schema(), cond.lhs_col.col_name);
        rhs_v = right_tuple.GetValue(&right_->schema(), cond.rhs_col.col_name);
      } else if (cond.lhs_col.tab_name == right_tab_name_ || cond.rhs_col.tab_name == left_tab_name_) {
        lhs_v = right_tuple.GetValue(&right_->schema(), cond.lhs_col.col_name);
        rhs_v = left_tuple.GetValue(&left_->schema(), cond.rhs_col.col_name);
      } else {
        throw InternalError("Unknown table in condition (lhs or rhs)");
      }
    } else {
      // Right-hand side is a value
      if (cond.lhs_col.tab_name == left_tab_name_) {
        lhs_v = left_tuple.GetValue(&left_->schema(), cond.lhs_col.col_name);
      } else if (cond.lhs_col.tab_name == right_tab_name_) {
        lhs_v = right_tuple.GetValue(&right_->schema(), cond.lhs_col.col_name);
      } else {
        throw InternalError("Unknown table in condition (lhs)");
      }
      rhs_v = cond.rhs_val;
    }

    // Evaluate the condition based on the operator
    bool condition_satisfied = false;
    switch (cond.op) {
      case OP_EQ:
        condition_satisfied = (lhs_v.CompareEquals(rhs_v) == CmpBool::CmpTrue);
        break;
      case OP_NE:
        condition_satisfied = (lhs_v.CompareNotEquals(rhs_v) == CmpBool::CmpTrue);
        break;
      case OP_LT:
        condition_satisfied = (lhs_v.CompareLessThan(rhs_v) == CmpBool::CmpTrue);
        break;
      case OP_GT:
        condition_satisfied = (lhs_v.CompareGreaterThan(rhs_v) == CmpBool::CmpTrue);
        break;
      case OP_LE:
        condition_satisfied = (lhs_v.CompareLessThanEquals(rhs_v) == CmpBool::CmpTrue);
        break;
      case OP_GE:
        condition_satisfied = (lhs_v.CompareGreaterThanEquals(rhs_v) == CmpBool::CmpTrue);
        break;
      default:
        throw InternalError("Unsupported operator in condition.");
    }
    if (!condition_satisfied) {
      return false;  // Condition not satisfied
    }
  }
  return true;  // All conditions satisfied
}

void HashJoinExecutor::NestedLoopBegin() {
  // Load all tuples from left and right into buffers
  left_buffer_.clear();
  right_buffer_.clear();

  for (left_->beginTuple(); !left_->IsEnd(); left_->nextTuple()) {
    left_buffer_.emplace_back(*(left_->Next()));
  }

  for (right_->beginTuple(); !right_->IsEnd(); right_->nextTuple()) {
    right_buffer_.emplace_back(*(right_->Next()));
  }
  left_idx_ = 0;
  right_idx_ = 0;
  isend_ = false;

  // Find the first pair that satisfies the conditions
  while (!isend_) {
    if (predicate(left_buffer_[left_idx_], right_buffer_[right_idx_])) {
      // Found a pair that satisfies the conditions
      return;
    }
    // Advance to next pair
    NestedLoopNext();
  }
}

void HashJoinExecutor::NestedLoopNext() {
  // Advance to the next tuple pair in nested loop fashion
  left_idx_++;
  if (left_idx_ >= left_buffer_.size()) {
    left_idx_ = 0;
    right_idx_++;
    if (right_idx_ >= right_buffer_.size()) {
      isend_ = true;
      return;
    }
  }
  // Check if the new pair satisfies the conditions
  while (!isend_) {
    if (predicate(left_buffer_[left_idx_], right_buffer_[right_idx_])) {
      // Found a pair that satisfies the conditions
      return;
    }
    // Advance to next pair
    left_idx_++;
    if (left_idx_ >= left_buffer_.size()) {
      left_idx_ = 0;
      right_idx_++;
      if (right_idx_ >= right_buffer_.size()) {
        isend_ = true;
        return;
      }
    }
  }
}

}  // namespace easydb