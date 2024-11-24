#pragma once

#include <memory>
#include <unordered_map>
#include <vector>

#include "common/utils/hashutil.h"
#include "execution/executor_abstract.h"
#include "storage/table/tuple.h"

#include "common/condition.h"

namespace easydb {

struct HashJoinKey {
  Value value_;

  bool operator==(const HashJoinKey &other) const { return value_.CompareEquals(other.value_) == CmpBool::CmpTrue; }
};

}  // namespace easydb

namespace std {

/** Implements std::hash on HashJoinKey */
template <>
struct hash<easydb::HashJoinKey> {
  std::size_t operator()(const easydb::HashJoinKey &key) const { return easydb::HashUtil::HashValue(&key.value_); }
};

}  // namespace std

namespace easydb {

/**
 * HashJoinExecutor executes a hash join on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 private:
  std::unique_ptr<AbstractExecutor> left_child_;   // Left child executor
  std::unique_ptr<AbstractExecutor> right_child_;  // Right child executor
  std::string left_tab_name_;
  std::string right_tab_name_;
  size_t tuple_len_;    // Length of the joined tuple
  Schema schema_;       // Schema of the joined tuple
  std::vector<Condition> join_conditions_;

  // Hash table for the join
  std::unordered_multimap<HashJoinKey, Tuple> hash_table_;

  // Iterator for the current matching tuples
  std::vector<Tuple> current_matches_;
  size_t match_index_;

  // Current right tuple
  Tuple right_tuple_;

  bool is_end_;

 public:
  HashJoinExecutor(std::unique_ptr<AbstractExecutor> left_child, std::unique_ptr<AbstractExecutor> right_child,
                   std::vector<Condition> join_conditions);

  void beginTuple() override;

  // Remove the declaration of nextTuple()
  // void nextTuple() override;

  std::unique_ptr<Tuple> Next() override;

  RID &rid() override { return _abstract_rid; }

  size_t tupleLen() const override { return tuple_len_; }

  const Schema &schema() const override { return schema_; }

  bool IsEnd() const override { return is_end_; }

  std::string getTabName() const override { return ""; }

  Column get_col_offset(Schema sche, const TabCol &target);

 private:
  void BuildHashTable();

  void ProbeHashTable();

  Tuple ConcatenateTuples(const Tuple &left_tuple, const Tuple &right_tuple);
};

HashJoinExecutor::HashJoinExecutor(std::unique_ptr<AbstractExecutor> left_child,
                                   std::unique_ptr<AbstractExecutor> right_child,
                                   std::vector<Condition> join_conditions)
    : left_child_(std::move(left_child)),
      right_child_(std::move(right_child)),
      join_conditions_(std::move(join_conditions)),
      is_end_(false),
      match_index_(0) {
  left_tab_name_ = left_child_->getTabName();
  right_tab_name_ = right_child_->getTabName();

  // Combine schemas from both children
  auto left_schema = left_child_->schema();
  auto right_schema = right_child_->schema();
  schema_ = left_schema;
  auto right_columns = right_schema.GetColumns();
  for (auto &col : right_columns) {
    col.AddOffset(schema_.GetInlinedStorageSize());
  }
  schema_.Append(right_columns);

  tuple_len_ = schema_.GetInlinedStorageSize();
}

void HashJoinExecutor::beginTuple() {
  // Build the hash table on the left child
  BuildHashTable();

  // Initialize the right child
  right_child_->beginTuple();

  // Get the first tuple from the right child
  if (!right_child_->IsEnd()) {
    auto right_tuple_ptr = right_child_->Next();
    if (right_tuple_ptr != nullptr) {
      right_tuple_ = *right_tuple_ptr;
      ProbeHashTable();
    } else {
      is_end_ = true;
    }
  } else {
    is_end_ = true;
  }
}

// Remove the implementation of nextTuple(), since it's no longer needed
// If AbstractExecutor requires it, you can provide an empty implementation
/*
void HashJoinExecutor::nextTuple() {
  // No longer needed as iteration is handled within Next()
}
*/

std::unique_ptr<Tuple> HashJoinExecutor::Next() {
  while (true) {
    if (is_end_) {
      return nullptr;
    }

    if (match_index_ < current_matches_.size()) {
      // Return the current match
      auto result_tuple = ConcatenateTuples(current_matches_[match_index_], right_tuple_);
      match_index_++;
      return std::make_unique<Tuple>(std::move(result_tuple));
    } else {
      // Advance to the next right tuple
      auto right_tuple_ptr = right_child_->Next();
      if (right_tuple_ptr == nullptr) {
        is_end_ = true;
        return nullptr;
      }
      right_tuple_ = *right_tuple_ptr;
      // Probe hash table for the new right tuple
      ProbeHashTable();
      // Reset match_index_
      match_index_ = 0;
    }
  }
}

Column HashJoinExecutor::get_col_offset(Schema sche, const TabCol &target) {
  auto cols = sche.GetColumns();
  for (auto &col : cols) {
    if (target.col_name == col.GetName() && target.tab_name == col.GetTabName()) {
      return col;
    }
  }
  throw ColumnNotFoundError(target.col_name);
}

void HashJoinExecutor::BuildHashTable() {
  // Build hash table from the left child
  left_child_->beginTuple();
  while (!left_child_->IsEnd()) {
    auto left_tuple_ptr = left_child_->Next();
    if (left_tuple_ptr == nullptr) {
      left_child_->nextTuple();
      continue;
    }
    auto left_tuple = *left_tuple_ptr;

    // Get join key from the left tuple
    for (const auto &cond : join_conditions_) {
      if (!cond.is_rhs_val && cond.lhs_col.tab_name == left_tab_name_ &&
          cond.rhs_col.tab_name == right_tab_name_) {
        auto left_col = get_col_offset(left_child_->schema(), cond.lhs_col);
        auto left_value = left_tuple.GetValue(&left_child_->schema(), left_col.GetName());

        HashJoinKey key{left_value};
        hash_table_.emplace(key, left_tuple);
      } else if (!cond.is_rhs_val && cond.rhs_col.tab_name == left_tab_name_ &&
                 cond.lhs_col.tab_name == right_tab_name_) {
        auto left_col = get_col_offset(left_child_->schema(), cond.rhs_col);
        auto left_value = left_tuple.GetValue(&left_child_->schema(), left_col.GetName());

        HashJoinKey key{left_value};
        hash_table_.emplace(key, left_tuple);
      }
    }
    left_child_->nextTuple();
  }
}

void HashJoinExecutor::ProbeHashTable() {
  // Reset matches
  current_matches_.clear();
  match_index_ = 0;

  // Get join key from the right tuple
  for (const auto &cond : join_conditions_) {
    if (!cond.is_rhs_val && cond.lhs_col.tab_name == left_tab_name_ &&
        cond.rhs_col.tab_name == right_tab_name_) {
      auto right_col = get_col_offset(right_child_->schema(), cond.rhs_col);
      auto right_value = right_tuple_.GetValue(&right_child_->schema(), right_col.GetName());

      HashJoinKey key{right_value};
      auto range = hash_table_.equal_range(key);
      for (auto it = range.first; it != range.second; ++it) {
        current_matches_.push_back(it->second);
      }
    } else if (!cond.is_rhs_val && cond.rhs_col.tab_name == left_tab_name_ &&
               cond.lhs_col.tab_name == right_tab_name_) {
      auto right_col = get_col_offset(right_child_->schema(), cond.lhs_col);
      auto right_value = right_tuple_.GetValue(&right_child_->schema(), right_col.GetName());

      HashJoinKey key{right_value};
      auto range = hash_table_.equal_range(key);
      for (auto it = range.first; it != range.second; ++it) {
        current_matches_.push_back(it->second);
      }
    }
  }
}

Tuple HashJoinExecutor::ConcatenateTuples(const Tuple &left_tuple, const Tuple &right_tuple) {
  size_t left_len = left_tuple.GetLength();
  size_t right_len = right_tuple.GetLength();
  size_t total_len = left_len + right_len;

  char *data_cat = new char[total_len];
  memcpy(data_cat, left_tuple.GetData(), left_len);
  memcpy(data_cat + left_len, right_tuple.GetData(), right_len);

  Tuple result_tuple(total_len, data_cat);
  delete[] data_cat;
  return result_tuple;
}

}  // namespace easydb