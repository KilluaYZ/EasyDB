/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 *
 *-------------------------------------------------------------------------
 */

#pragma once
#include "execution/executor_merge_join.h"
#include "storage/table/tuple.h"

namespace easydb {

MergeJoinExecutor::MergeJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right,
                                     std::vector<Condition> conds, bool use_index) {
  left_ = std::move(left);
  right_ = std::move(right);
  len_ = left_->tupleLen() + right_->tupleLen();

  use_index_ = use_index;

  left_tab_name_ = left_->getTabName();
  right_tab_name_ = right_->getTabName();
  join_tab_name_ = left_tab_name_ + "_" + right_tab_name_;

  auto left_columns = left_->schema().GetColumns();
  auto right_colums = right_->schema().GetColumns();
  left_columns.insert(left_columns.end(), right_colums.begin(), right_colums.end());
  schema_ = Schema(left_columns);

  isend = false;
  fed_conds_ = std::move(conds);

  // get selected connection col's colmeta information
  for (auto &cond : fed_conds_) {
    // op must be OP_EQ and right hand must also be a col
    if (cond.op == OP_EQ && !cond.is_rhs_val) {
      if (cond.lhs_col.tab_name == left_->getTabName() || cond.rhs_col.tab_name == right_->getTabName()) {
        left_sel_colu_ = left_->schema().GetColumn(cond.lhs_col.col_name);
        right_sel_colu_ = right_->schema().GetColumn(cond.rhs_col.col_name);
      } else if (cond.rhs_col.tab_name == left_->getTabName() || cond.lhs_col.tab_name == right_->getTabName()) {
        left_sel_colu_ = left_->schema().GetColumn(cond.lhs_col.col_name);
        right_sel_colu_ = right_->schema().GetColumn(cond.rhs_col.col_name);
      }
    }
  }
  
  if (!use_index_) {
    left_size_ = left_->schema().GetPhysicalSize();
    right_size_ = right_->schema().GetPhysicalSize();
    leftSorter_ = std::make_unique<MergeSorter>(left_sel_colu_, left_->schema().GetColumns(), left_size_, false);
    rightSorter_ = std::make_unique<MergeSorter>(right_sel_colu_, right_->schema().GetColumns(), right_size_, false);
  }

  current_left_data_ = new char[left_size_];
  current_right_data_ = new char[right_size_];
}

MergeJoinExecutor::~MergeJoinExecutor() {
  delete[] current_left_data_;
  delete[] current_right_data_;
}

void MergeJoinExecutor::beginTuple() {
  if (use_index_) {
    for (left_->beginTuple(); !left_->IsEnd(); left_->nextTuple()) {
      left_buffer_.emplace_back(*(left_->Next()));
    }

    for (right_->beginTuple(); !right_->IsEnd(); right_->nextTuple()) {
      right_buffer_.emplace_back(*(right_->Next()));
    }

    left_idx_ = 0;
    right_idx_ = 0;

  } else {
    for (left_->beginTuple(); !left_->IsEnd(); left_->nextTuple()) {
      leftSorter_->writeBuffer(*(left_->Next()));
    }
    for (right_->beginTuple(); !right_->IsEnd(); right_->nextTuple()) {
      rightSorter_->writeBuffer(*(right_->Next()));
    }

    leftSorter_->clearBuffer();
    rightSorter_->clearBuffer();

    leftSorter_->initializeMergeListAndConstructTree();
    rightSorter_->initializeMergeListAndConstructTree();
  }
  nextTuple();
}

void MergeJoinExecutor::nextTuple() {
  if (use_index_) {
    index_iterate_helper();
  } else {
    iterate_helper();
  }
  if (isend) {
    return;
  }
  joined_records_ = concat_records();
}

void MergeJoinExecutor::iterate_helper() {
  char *tp;
  if (!initialize_flag_) {
    tp = rightSorter_->getOneRecord();
    if (tp != NULL) {
      memcpy(current_right_data_, tp, right_size_);
      free(tp);
    } else {
      isend = true;
      return;
    }
    initialize_flag_ = true;
  }

  tp = leftSorter_->getOneRecord();
  if (tp != NULL) {
    memcpy(current_left_data_, tp, left_size_);
    free(tp);
  } else {
    isend = true;
    return;
  }

  Tuple left_tuple;
  left_tuple.DeserializeFrom(current_left_data_);
  Tuple right_tuple;
  right_tuple.DeserializeFrom(current_right_data_);
  Value lhs_v, rhs_v;

  lhs_v = left_tuple.GetValue(&left_->schema(), left_sel_colu_.GetName());
  rhs_v = right_tuple.GetValue(&right_->schema(), right_sel_colu_.GetName());

  while ((!leftSorter_->IsEnd() && !rightSorter_->IsEnd())) {
    if (lhs_v == rhs_v) {
      break;
    } else if (lhs_v < rhs_v) {
      tp = leftSorter_->getOneRecord();
      memcpy(current_left_data_, tp, left_size_);
      free(tp);
      left_tuple.DeserializeFrom(current_left_data_);
      lhs_v = left_tuple.GetValue(&left_->schema(), left_sel_colu_.GetName());
    } else {
      tp = rightSorter_->getOneRecord();
      memcpy(current_right_data_, tp, right_size_);
      free(tp);
      right_tuple.DeserializeFrom(current_right_data_);
      rhs_v = right_tuple.GetValue(&right_->schema(), right_sel_colu_.GetName());
    }
  }

  while (lhs_v > rhs_v && !rightSorter_->IsEnd()) {
    tp = rightSorter_->getOneRecord();
    memcpy(current_right_data_, tp, right_size_);
    free(tp);
    right_tuple.DeserializeFrom(current_right_data_);
    rhs_v = right_tuple.GetValue(&right_->schema(), right_sel_colu_.GetName());
  }

  if (lhs_v != rhs_v) {
    isend = true;
  }
}

void MergeJoinExecutor::index_iterate_helper() {
  if (left_idx_ >= left_buffer_.size() || right_idx_ >= right_buffer_.size()) {
    isend = true;
    return;
  }
  if (!initialize_flag_) {
    current_right_tup_ = right_buffer_[right_idx_];
    right_idx_++;
  }
  current_left_tup_ = left_buffer_[left_idx_];
  left_idx_++;

  Value lhs_v, rhs_v;

  lhs_v = current_left_tup_.GetValue(&left_->schema(), left_sel_colu_.GetName());
  rhs_v = current_right_tup_.GetValue(&right_->schema(), right_sel_colu_.GetName());

  while (left_idx_ < left_buffer_.size() && right_idx_ < right_buffer_.size()) {
    if (lhs_v == rhs_v) {
      break;
    } else if (lhs_v < rhs_v) {
      current_left_tup_ = left_buffer_[left_idx_];
      lhs_v = current_left_tup_.GetValue(&left_->schema(), left_sel_colu_.GetName());
      left_idx_++;
    } else {
      current_right_tup_ = right_buffer_[right_idx_];
      rhs_v = current_right_tup_.GetValue(&right_->schema(), right_sel_colu_.GetName());
      right_idx_++;
    }
  }

  while (lhs_v > rhs_v && right_idx_ < right_buffer_.size()) {
    current_right_tup_ = right_buffer_[right_idx_];
    rhs_v = current_right_tup_.GetValue(&right_->schema(), right_sel_colu_.GetName());
    right_idx_++;
  }

  if (lhs_v != rhs_v) {
    isend = true;
  }
}

Tuple MergeJoinExecutor::concat_records() {
  if (use_index_) {
    auto left_value_vec = current_left_tup_.GetValueVec(&left_->schema());
    auto right_value_vec = current_right_tup_.GetValueVec(&right_->schema());
    left_value_vec.insert(left_value_vec.end(), right_value_vec.begin(), right_value_vec.end());
    return Tuple(left_value_vec, &schema_);
  } else {
    Tuple left_tuple_tp;
    left_tuple_tp.DeserializeFrom(current_left_data_);
    auto left_value_vec = left_tuple_tp.GetValueVec(&left_->schema());

    Tuple right_tuple_tp;
    right_tuple_tp.DeserializeFrom(current_right_data_);
    auto right_value_vec = right_tuple_tp.GetValueVec(&right_->schema());

    left_value_vec.insert(left_value_vec.end(), right_value_vec.begin(), right_value_vec.end());

    return Tuple(left_value_vec, &schema_);
  }
}

}  // namespace easydb
