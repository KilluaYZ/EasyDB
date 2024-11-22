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
  schema_ = left_->schema;
  
  // auto right_cols = right_->cols();
  auto right_colums = right->schema().GetColumns();
  // for (auto &col : right_cols) {
  //   col.offset += left_->tupleLen();
  // }
  for (auto &colu : right_colums){
    colu.AddOffset(schema_.GetInlinedStorageSize());
  }
  schema_.Append(right_colums);

  // cols_.insert(cols_.end(), right_cols.begin(), right_cols.end());
  isend = false;
  fed_conds_ = std::move(conds);

  if (fed_conds_.size() > 0) {
    need_sort_ = true;
    // get selected connection col's colmeta information
    for (auto &cond : fed_conds_) {
      // op must be OP_EQ and right hand must also be a col
      if (cond.op == OP_EQ && !cond.is_rhs_val) {
        if (cond.lhs_col.tab_name == left_->getTabName() && cond.rhs_col.tab_name == right_->getTabName()) {
          // left_sel_col_ = get_col_offset(left_->cols(), cond.lhs_col);
          // right_sel_col_ = get_col_offset(right_->cols(), cond.rhs_col);
          left_sel_colu_ = get_col_offset(left_->schema(), cond.lhs_col);
          right_sel_colu_ = get_col_offset(right_->schema(), cond.rhs_col);
        } else if (cond.rhs_col.tab_name == left_->getTabName() && cond.lhs_col.tab_name == right_->getTabName()) {
          // left_sel_col_ = get_col_offset(left_->cols(), cond.rhs_col);
          // right_sel_col_ = get_col_offset(right_->cols(), cond.lhs_col);
          left_sel_colu_ = get_col_offset(left_->schema(), cond.rhs_col);
          right_sel_colu_ = get_col_offset(right_->chema(), cond.lhs_col);
        }
      }
    }

    leftSorter_ = std::make_unique<MergeSorter>(left_sel_col_, left_->shema().GetColumns(), left_len_, false);
  }
}

void NestedLoopJoinExecutor::beginTuple() override {
  if (need_sort_) {
    for (left_->beginTuple(); !left_->IsEnd(); left_->nextTuple()) {
      leftSorter_->writeBuffer(*(left_->Next()));
    }
    leftSorter_->clearBuffer();
    leftSorter_->initializeMergeListAndConstructTree();
    while (!leftSorter_->IsEnd()) {
      RmRecord tp(left_len_, leftSorter_->getOneRecord());
      // printRecord(tp,left_->cols());
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
  if (!isend && need_sort_) {
    iterate_helper();
  }
  if (isend) {
    return;
  }
  joined_records_ = concat_records();
}

void NestedLoopJoinExecutor::printRecord(RmRecord record, std::vector<ColMeta> cols) {
  std::string str;
  int str_size = 0;
  char *data = record.data;
  for (auto &col : cols) {
    switch (col.type) {
      case TYPE_INT:
        printf(" %d  ", *(int *)(data + col.offset));
        break;
      case TYPE_FLOAT:
        printf(" %f   ", *(float *)(data + col.offset));
        break;
      case TYPE_VARCHAR:
      case TYPE_CHAR:
        str_size = col.len < strlen(data + col.offset) ? col.len : strlen(data + col.offset);
        str.assign(data + col.offset, str_size);
        str[str_size] = '\0';
        printf(" %s  ", str.c_str());
        break;
      default:
        throw InternalError("unsupported data type.");
    }
  }
  printf("\n");
}

void NestedLoopJoinExecutor::printRecord(char *data, std::vector<ColMeta> cols) {
  std::string str;
  int str_size = 0;
  for (auto &col : cols) {
    switch (col.type) {
      case TYPE_INT:
        printf(" %d  ", *(int *)(data + col.offset));
        break;
      case TYPE_FLOAT:
        printf(" %f   ", *(float *)(data + col.offset));
        break;
      case TYPE_VARCHAR:
      case TYPE_CHAR:
        str_size = col.len < strlen(data + col.offset) ? col.len : strlen(data + col.offset);
        str.assign(data + col.offset, str_size);
        str[str_size] = '\0';
        printf(" %s  ", str.c_str());
        break;
      default:
        throw InternalError("unsupported data type.");
    }
  }
  printf("\n");
}

void NestedLoopJoinExecutor::printRecord(std::unique_ptr<RmRecord> &Tuple, const std::vector<ColMeta> &cols) {
  std::vector<std::string> columns;
  for (auto &col : cols) {
    std::string col_str;
    char *rec_buf = Tuple->data + col.offset;
    switch (col.type) {
      case TYPE_INT:
        col_str = std::to_string(*(int *)rec_buf);
        std::cout << col_str << " ";
        break;
      case TYPE_FLOAT:
        col_str = std::to_string(*(float *)rec_buf);
        std::cout << col_str << " ";
        break;
      case TYPE_VARCHAR:
      case TYPE_CHAR:
        col_str = std::string((char *)rec_buf, col.len);
        col_str.resize(strlen(col_str.c_str()));
        std::cout << col_str << " ";
        break;
      default:
        throw InternalError("unsupported data type.");
    }
    columns.push_back(col_str);
  }
  std::cout << std::endl;
}

void NestedLoopJoinExecutor::nextTuple() override {
  iterate_next();
  if (!isend && need_sort_) {
    iterate_helper();
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

void NestedLoopJoinExecutor::iterate_helper() {
  Value lhs_v, rhs_v;
  lhs_v = left_buffer_[left_idx_].GetValue(schema_,left_sel_colu_.GetName());
  rhs_v = right_buffer_[right_idx_].GetValue(schema_,right_sel_colu_.GetName());
  
  // lhs_v.get_value_from_record(left_buffer_[left_idx_], left_sel_col_);
  // rhs_v.get_value_from_record(right_buffer_[right_idx_], right_sel_col_);

  while (left_idx_ + 1 < left_buffer_.size() && rhs_v > lhs_v) {
    left_idx_++;
    lhs_v = left_buffer_[left_idx_].GetValue(schema_,left_sel_colu_.GetName());
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
  char *data_cat = new char[len_];
  memcpy(data_cat, left_buffer_[left_idx_].GetData(), left_len_);
  memcpy(data_cat + left_len_, right_buffer_[right_idx_].GetData(), right_len_);
  // std::vector<char> vec_tp;
  // vec_tp.assign(data_cat,data_cat+len_);
  // vec_tp.emplace_back('\0');
  return Tuple(len_,data_cat);
}

}  // namespace easydb
