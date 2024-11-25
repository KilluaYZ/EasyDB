#pragma once

#include <memory>
#include <unordered_map>
#include <vector>

#include "common/hashutil.h"
#include "execution/executor_abstract.h"
#include "storage/table/tuple.h"

#include "common/condition.h"

namespace easydb {

struct HashJoinKey {
  Value value_;

  bool operator==(const HashJoinKey &other) const { 
    return value_.CompareEquals(other.value_) == CmpBool::CmpTrue; 
  }
};

}  // namespace easydb

namespace std {

/** Implements std::hash on HashJoinKey */
template <>
struct hash<easydb::HashJoinKey> {
  std::size_t operator()(const easydb::HashJoinKey &key) const { 
    return easydb::HashUtil::HashValue(&key.value_); 
  }
};

}  // namespace std

#pragma once

#include <memory>
#include <unordered_map>
#include <vector>
#include "common/common.h"
#include "common/errors.h"
#include "common/hashutil.h"
#include "defs.h"
#include "executor_abstract.h"
#include "storage/table/tuple.h"
#include "type/value.h"

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
  Column left_join_col_;
  Column right_join_col_;

  // Iterators for output
  std::unordered_multimap<HashJoinKey, Tuple>::iterator match_iter_;
  std::unordered_multimap<HashJoinKey, Tuple>::iterator match_end_;
  Tuple current_probe_tuple_;

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
};

HashJoinExecutor::HashJoinExecutor(std::unique_ptr<AbstractExecutor> left,
                                   std::unique_ptr<AbstractExecutor> right,
                                   std::vector<Condition> conds)
    : left_(std::move(left)),
      right_(std::move(right)),
      conds_(std::move(conds)),
      isend_(false) {
  left_tab_name_ = left_->getTabName();
  right_tab_name_ = right_->getTabName();

  // Build the output schema
  auto left_columns = left_->schema().GetColumns();
  auto right_columns = right_->schema().GetColumns();
  left_columns.insert(left_columns.end(), right_columns.begin(), right_columns.end());
  schema_ = Schema(left_columns);

  len_ = left_->tupleLen() + right_->tupleLen();

  // Determine join columns
  if (conds_.size() > 0) {
    for (auto &cond : conds_) {
      if (cond.op == OP_EQ && !cond.is_rhs_val) {
        if (cond.lhs_col.tab_name == left_tab_name_ && cond.rhs_col.tab_name == right_tab_name_) {
          left_join_col_ = left_->schema().GetColumn(cond.lhs_col.col_name);
          right_join_col_ = right_->schema().GetColumn(cond.rhs_col.col_name);
          break;
        } else if (cond.rhs_col.tab_name == left_tab_name_ && cond.lhs_col.tab_name == right_tab_name_) {
          left_join_col_ = left_->schema().GetColumn(cond.rhs_col.col_name);
          right_join_col_ = right_->schema().GetColumn(cond.lhs_col.col_name);
          break;
        }
      }
    }
  } else {
    throw InternalError("HashJoinExecutor requires at least one equality condition for join.");
  }
}

void HashJoinExecutor::beginTuple() {
  BuildHashTable();
  // Initialize right iterator
  right_->beginTuple();
  if (right_->IsEnd()) {
    isend_ = true;
    return;
  }
  // Initialize match iterators
  do {
    current_probe_tuple_ = *(right_->Next());
    ProbeHashTable();
    if (match_iter_ != match_end_) {
      // Found matches
      return;
    }
    right_->nextTuple();
  } while (!right_->IsEnd());
  // No matches found
  isend_ = true;
}

void HashJoinExecutor::nextTuple() {
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
}

std::unique_ptr<Tuple> HashJoinExecutor::Next() {
  if (isend_) {
    return nullptr;
  }
  // Combine the current left and right tuples
  auto left_values = match_iter_->second.GetValueVec(&left_->schema());
  auto right_values = current_probe_tuple_.GetValueVec(&right_->schema());
  left_values.insert(left_values.end(), right_values.begin(), right_values.end());
  Tuple joined_tuple(left_values, &schema_);
  return std::make_unique<Tuple>(std::move(joined_tuple));
}

void HashJoinExecutor::BuildHashTable() {
  // Build the hash table from the left input
  left_->beginTuple();
  while (!left_->IsEnd()) {
    Tuple tuple = *(left_->Next());
    // Extract join key
    Value key_value = tuple.GetValue(&left_->schema(), left_join_col_.GetName());
    HashJoinKey key{key_value};
    hash_table_.emplace(key, tuple);
    left_->nextTuple();
  }
}

void HashJoinExecutor::ProbeHashTable() {
  // Probe the hash table with current_probe_tuple_
  Value key_value = current_probe_tuple_.GetValue(&right_->schema(), right_join_col_.GetName());
  HashJoinKey key{key_value};
  auto range = hash_table_.equal_range(key);
  match_iter_ = range.first;
  match_end_ = range.second;
}

}  // namespace easydb