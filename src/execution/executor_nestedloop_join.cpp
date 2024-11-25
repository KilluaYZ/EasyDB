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
          // left_sel_col_ = get_col_offset(left_->cols(), cond.lhs_col);
          // right_sel_col_ = get_col_offset(right_->cols(), cond.rhs_col);
          left_sel_colu_ = left_->schema().GetColumn(cond.lhs_col.col_name);
          right_sel_colu_ = right_->schema().GetColumn(cond.rhs_col.col_name);
        } else if (cond.rhs_col.tab_name == left_->getTabName() && cond.lhs_col.tab_name == right_->getTabName()) {
          // left_sel_col_ = get_col_offset(left_->cols(), cond.rhs_col);
          // right_sel_col_ = get_col_offset(right_->cols(), cond.lhs_col);
          // left_sel_colu_ = get_col_offset(left_->schema(), cond.rhs_col);
          // right_sel_colu_ = get_col_offset(right_->schema(), cond.lhs_col);
          left_sel_colu_ = left_->schema().GetColumn(cond.lhs_col.col_name);
          right_sel_colu_ = right_->schema().GetColumn(cond.rhs_col.col_name);
        }
      }
    }

    leftSorter_ = std::make_unique<MergeSorter>(left_sel_colu_, left_->schema().GetColumns(), left_len_, false);
  }
}

void NestedLoopJoinExecutor::beginTuple() {
  if (need_sort_) {
    for (left_->beginTuple(); !left_->IsEnd(); left_->nextTuple()) {
      leftSorter_->writeBuffer(*(left_->Next()));
    }
    leftSorter_->clearBuffer();
    leftSorter_->initializeMergeListAndConstructTree();
    while (!leftSorter_->IsEnd()) {
      char *storage_tp = leftSorter_->getOneRecord();
      Tuple tp(left_len_, storage_tp);
      // tp.DeserializeFrom(storage_tp);
      left_buffer_.emplace_back(tp);
    }
  } else {
    for (left_->beginTuple(); !left_->IsEnd(); left_->nextTuple()) {
      // printRecord(*(left_->Next()),left_->cols());
      left_buffer_.emplace_back(*(left_->Next()));
    }
  }

  for (right_->beginTuple(); !right_->IsEnd(); right_->nextTuple()) {
    // printRecord(*(right_->Next()),right_->cols());
    right_buffer_.emplace_back(*(right_->Next()));
  }
  left_idx_ = 0;
  right_idx_ = 0;
  if (!isend && fed_conds_.size() > 0) {
    if (need_sort_) {
      sorted_iterate_helper();
    } else {
      iterate_helper();
    }
  }
  if (isend) {
    return;
  }
  joined_records_ = concat_records();
}

void NestedLoopJoinExecutor::nextTuple() {
  iterate_next();
  if (!isend && fed_conds_.size() > 0) {
    if (need_sort_) {
      sorted_iterate_helper();
    } else {
      iterate_helper();
    }
  }
  if (isend) {
    return;
  }
  joined_records_ = concat_records();
}

// bool NestedLoopJoinExecutor::predicate() {
//   bool satisfy = true;
//   // return true only all the conditions were true
//   // i.e. all conditions are connected with 'and' operator
//   for (auto &cond : fed_conds_) {
//     if (cond.op == OP_EQ && !cond.is_rhs_val) {
//       continue;
//     } else {
//       Value lhs_v, rhs_v;
//       if (lhs_v.get_value_from_record(left_buffer_[left_idx_], left_->cols(), cond.lhs_col.col_name) == nullptr) {
//         throw InternalError("target column not found.");
//       }
//       rhs_v = cond.rhs_val;
//       if (!cond.satisfy(lhs_v, rhs_v)) {
//         satisfy = false;
//         break;
//       }
//     }
//   }
//   return satisfy;
// }

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
  Value lhs_v, rhs_v;
  lhs_v = left_buffer_[left_idx_].GetValue(&left_->schema(), left_sel_colu_.GetName());
  rhs_v = right_buffer_[right_idx_].GetValue(&right_->schema(), right_sel_colu_.GetName());

  if (rhs_v == lhs_v) {
    return;
  } else {
    left_idx_++;
    if (left_idx_ >= left_buffer_.size()) {
      left_idx_ = 0;
      right_idx_++;
      if (right_idx_ >= right_buffer_.size()) {
        isend = true;
      } else {
        iterate_helper();
      }
    }else{
      iterate_helper();
    }
  }
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

// RmRecord NestedLoopJoinExecutor::concat_records() {
//   char *data_cat = new char[len_];
//   memcpy(data_cat, left_buffer_[left_idx_].data, left_len_);
//   memcpy(data_cat + left_len_, right_buffer_[right_idx_].data, right_len_);
//   return RmRecord(len_, data_cat);
// }

Tuple NestedLoopJoinExecutor::concat_records() {
  auto left_value_vec = left_buffer_[left_idx_].GetValueVec(&left_->schema());
  auto right_value_vec = right_buffer_[right_idx_].GetValueVec(&right_->schema());
  left_value_vec.insert(left_value_vec.end(), right_value_vec.begin(), right_value_vec.end());

  return Tuple(left_value_vec, &schema_);
}

}  // namespace easydb