#pragma once

#include <memory>
#include <unordered_map>
#include <vector>
#include "common/condition.h"
#include "common/common.h"
#include "common/errors.h"
#include "common/hashutil.h"
#include "defs.h"
#include "executor_abstract.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace easydb {

struct HashJoinKey {
  std::vector<Value> values_;

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

class HashJoinExecutor : public AbstractExecutor {
 private:
  std::unique_ptr<AbstractExecutor> left_;
  std::unique_ptr<AbstractExecutor> right_;
  std::string left_tab_name_;
  std::string right_tab_name_;
  size_t len_;
  Schema schema_;
  std::vector<Condition> conds_;
  bool isend_;

  // Hash table data structure
  std::unordered_multimap<HashJoinKey, Tuple> hash_table_;

  // Join columns
  std::vector<Column> left_join_cols_;
  std::vector<Column> right_join_cols_;

  // Iterators for hash join
  std::unordered_multimap<HashJoinKey, Tuple>::iterator match_iter_;
  std::unordered_multimap<HashJoinKey, Tuple>::iterator match_end_;
  Tuple current_probe_tuple_;

  // For nested loop join fallback
  bool use_nested_loop_;
  size_t left_idx_;
  size_t right_idx_;
  std::vector<Tuple> left_buffer_;
  std::vector<Tuple> right_buffer_;

 public:
  HashJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right,
                   std::vector<Condition> conds);

  void beginTuple() override;
  void nextTuple() override;
  std::unique_ptr<Tuple> Next() override;
  RID &rid() override { return _abstract_rid; }

  size_t tupleLen() const override { return len_; }
  const Schema &schema() const override { return schema_; }

  bool IsEnd() const override { return isend_; }

 private:
  void BuildHashTable();
  void ProbeHashTable();
  bool predicate(const Tuple &left_tuple, const Tuple &right_tuple);
  void NestedLoopBegin();
  void NestedLoopNext();
};

HashJoinExecutor::HashJoinExecutor(std::unique_ptr<AbstractExecutor> left,
                                   std::unique_ptr<AbstractExecutor> right,
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
      if (cond.lhs_col.tab_name == left_tab_name_ && cond.rhs_col.tab_name == right_tab_name_) {
        left_join_cols_.push_back(left_->schema().GetColumn(cond.lhs_col.col_name));
        right_join_cols_.push_back(right_->schema().GetColumn(cond.rhs_col.col_name));
      } else if (cond.rhs_col.tab_name == left_tab_name_ && cond.lhs_col.tab_name == right_tab_name_) {
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
      if (cond.lhs_col.tab_name == left_tab_name_) {
        lhs_v = left_tuple.GetValue(&left_->schema(), cond.lhs_col.col_name);
      } else if (cond.lhs_col.tab_name == right_tab_name_) {
        lhs_v = right_tuple.GetValue(&right_->schema(), cond.lhs_col.col_name);
      } else {
        throw InternalError("Unknown table in condition (lhs)");
      }
      if (cond.rhs_col.tab_name == left_tab_name_) {
        rhs_v = left_tuple.GetValue(&left_->schema(), cond.rhs_col.col_name);
      } else if (cond.rhs_col.tab_name == right_tab_name_) {
        rhs_v = right_tuple.GetValue(&right_->schema(), cond.rhs_col.col_name);
      } else {
        throw InternalError("Unknown table in condition (rhs)");
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