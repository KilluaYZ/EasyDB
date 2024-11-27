/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 *
 *-------------------------------------------------------------------------
 */

#pragma once
#include "execution/executor_nestedloop_join.h"

namespace easydb {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(std::unique_ptr<AbstractExecutor> left,
                                               std::unique_ptr<AbstractExecutor> right, std::vector<Condition> conds) {
  left_ = std::move(left);
  right_ = std::move(right);

  left_tab_name_ = left_->getTabName();
  right_tab_name_ = right_->getTabName();
  join_tab_name_ = left_tab_name_ + "_" + right_tab_name_;
  left_len_ = left_->tupleLen();
  right_len_ = right_->tupleLen();
  len_ = left_len_ + right_len_;
  // buffer_record_count = block_size / len_;
  buffer_record_count = block_size / left_len_;
  // cols_ = left_->cols();
  // schema_ = left_->schema();

  auto left_columns = left_->schema().GetColumns();
  auto right_colums = right_->schema().GetColumns();
  left_columns.insert(left_columns.end(), right_colums.begin(), right_colums.end());
  schema_ = Schema(left_columns);

  // cols_.insert(cols_.end(), right_cols.begin(), right_cols.end());
  isend = false;
  fed_conds_ = std::move(conds);

  if (fed_conds_.size() > 0) {
    need_sort_ = false;
    // get selected connection col's colmeta information
    for (auto &cond : fed_conds_) {
      // op must be OP_EQ and right hand must also be a col
      if (cond.op == OP_EQ && !cond.is_rhs_val) {
        if (cond.lhs_col.tab_name == left_->getTabName() && cond.rhs_col.tab_name == right_->getTabName()) {
          left_sel_colu_ = left_->schema().GetColumn(cond.lhs_col.col_name);
          right_sel_colu_ = right_->schema().GetColumn(cond.rhs_col.col_name);
        } else if (cond.rhs_col.tab_name == left_->getTabName() && cond.lhs_col.tab_name == right_->getTabName()) {
          left_sel_colu_ = left_->schema().GetColumn(cond.lhs_col.col_name);
          right_sel_colu_ = right_->schema().GetColumn(cond.rhs_col.col_name);
        }
      }
    }

    leftSorter_ = std::make_unique<MergeSorter>(left_sel_colu_, left_->schema().GetColumns(), left_len_, false);
  }
}

void NestedLoopJoinExecutor::beginTuple() {
  // Load left and right buffers
  for (left_->beginTuple(); !left_->IsEnd(); left_->nextTuple()) {
    left_buffer_.emplace_back(*(left_->Next()));
  }
  for (right_->beginTuple(); !right_->IsEnd(); right_->nextTuple()) {
    right_buffer_.emplace_back(*(right_->Next()));
  }

  left_idx_ = 0;
  right_idx_ = 0;
  isend = false;
  iterate_helper();
  if (isend) {
    return;
  }
  joined_records_ = concat_records();
}

void NestedLoopJoinExecutor::nextTuple() {
  left_idx_++;
  if (left_idx_ >= left_buffer_.size()) {
    left_idx_ = 0;
    right_idx_++;
  }
  iterate_helper();
  if (isend) {
    return;
  }
  joined_records_ = concat_records();
}

void NestedLoopJoinExecutor::sorted_iterate_helper() {
  Value lhs_v, rhs_v;
  lhs_v = left_buffer_[left_idx_].GetValue(&left_->schema(), left_sel_colu_.GetName());
  rhs_v = right_buffer_[right_idx_].GetValue(&right_->schema(), right_sel_colu_.GetName());

  // lhs_v.get_value_from_record(left_buffer_[left_idx_], left_sel_col_);
  // rhs_v.get_value_from_record(right_buffer_[right_idx_], right_sel_col_);

  while (left_idx_ + 1 < left_buffer_.size() && rhs_v > lhs_v) {
    left_idx_++;
    lhs_v = left_buffer_[left_idx_].GetValue(&schema_, left_sel_colu_.GetName());
    // lhs_v.get_value_from_record(left_buffer_[left_idx_], left_sel_col_);
  }

  if (rhs_v == lhs_v) {
    return;
  } else {
    left_idx_ = 0;
    right_idx_++;
    if (right_idx_ >= right_buffer_.size()) {
      isend = true;
    } else {
      sorted_iterate_helper();
    }
  }
}

void NestedLoopJoinExecutor::iterate_helper() {
  while (right_idx_ < right_buffer_.size()) {
    while (left_idx_ < left_buffer_.size()) {
      if (predicate(left_buffer_[left_idx_], right_buffer_[right_idx_])) {
        // Found a matching pair
        return;
      }
      left_idx_++;
    }
    left_idx_ = 0;
    right_idx_++;
  }
  // No matching pair found
  isend = true;
}

void NestedLoopJoinExecutor::iterate_next() {
  left_idx_++;
  if (left_idx_ >= left_buffer_.size()) {
    right_idx_++;
    left_idx_ = 0;
    if (right_idx_ >= right_buffer_.size()) {
      isend = true;
    }
  }
}

Tuple NestedLoopJoinExecutor::concat_records() {
  auto left_value_vec = left_buffer_[left_idx_].GetValueVec(&left_->schema());
  auto right_value_vec = right_buffer_[right_idx_].GetValueVec(&right_->schema());
  left_value_vec.insert(left_value_vec.end(), right_value_vec.begin(), right_value_vec.end());
  return Tuple(left_value_vec, &schema_);
}

bool NestedLoopJoinExecutor::predicate(const Tuple &left_tuple, const Tuple &right_tuple) {
  for (const auto &cond : fed_conds_) {
    Value lhs_v, rhs_v;
    // Determine the values based on whether RHS is a column or a value
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
      // RHS is a value
      if (cond.lhs_col.tab_name == left_tab_name_) {
        lhs_v = left_tuple.GetValue(&left_->schema(), cond.lhs_col.col_name);
      } else if (cond.lhs_col.tab_name == right_tab_name_) {
        lhs_v = right_tuple.GetValue(&right_->schema(), cond.lhs_col.col_name);
      } else {
        throw InternalError("Unknown table in condition (lhs)");
      }
      rhs_v = cond.rhs_val;
    }

    // Evaluate the condition
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

}  // namespace easydb