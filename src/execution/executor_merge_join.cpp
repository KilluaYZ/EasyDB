/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 *
 *-------------------------------------------------------------------------
 */

#pragma once
#include "execution/executor_merge_join.h"

namespace easydb {

MergeJoinExecutor::MergeJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right,
                                     std::vector<Condition> conds, bool use_index) {
  left_ = std::move(left);
  right_ = std::move(right);
  len_ = left_->tupleLen() + right_->tupleLen();
  cols_ = left_->cols();
  use_index_ = use_index;
  auto right_cols = right_->cols();
  for (auto &col : right_cols) {
    col.offset += left_->tupleLen();
  }

  cols_.insert(cols_.end(), right_cols.begin(), right_cols.end());
  isend = false;
  fed_conds_ = std::move(conds);

  // get selected connection col's colmeta information
  for (auto &cond : fed_conds_) {
    // op must be OP_EQ and right hand must also be a col
    if (cond.op == OP_EQ && !cond.is_rhs_val) {
      if (cond.lhs_col.tab_name == left_->getTabName() && cond.rhs_col.tab_name == right_->getTabName()) {
        left_sel_col_ = get_col_offset(left_->cols(), cond.lhs_col);
        right_sel_col_ = get_col_offset(right_->cols(), cond.rhs_col);
      } else if (cond.rhs_col.tab_name == left_->getTabName() && cond.lhs_col.tab_name == right_->getTabName()) {
        left_sel_col_ = get_col_offset(left_->cols(), cond.rhs_col);
        right_sel_col_ = get_col_offset(right_->cols(), cond.lhs_col);
      }
    }
  }

  if (!use_index_) {
    leftSorter_ = std::make_unique<MergeSorter>(left_sel_col_, left_->cols(), left_->tupleLen(), false);
    rightSorter_ = std::make_unique<MergeSorter>(right_sel_col_, right_->cols(), right_->tupleLen(), false);
  }

  fd_left.open("sorted_results_left.txt", std::ios::out | std::ios::trunc);
  fd_right.open("sorted_results.txt", std::ios::out | std::ios::trunc);
  if (!fd_left.is_open()) {
    printf("sorted_results_left.txt open failed.\n");
  }
  if (!fd_right.is_open()) {
    printf("sorted_results.txt open failed.\n");
  }
  writeHeader(fd_left, left_->cols());
  writeHeader(fd_right, right_->cols());
}

void MergeJoinExecutor::beginTuple() override {
  if (use_index_) {
    // left_->beginTuple();
    // right_->beginTuple();

    for (left_->beginTuple(); !left_->is_end(); left_->nextTuple()) {
      // printRecord(*(left_->Next()),left_->cols());
      left_buffer_.emplace_back(*(left_->Next()));
    }

    for (right_->beginTuple(); !right_->is_end(); right_->nextTuple()) {
      // printRecord(*(left_->Next()),left_->cols());
      right_buffer_.emplace_back(*(right_->Next()));
    }

    left_idx_ = 0;
    right_idx_ = 0;

  } else {
    // sleep(1);
    for (left_->beginTuple(); !left_->is_end(); left_->nextTuple()) {
      leftSorter_->writeBuffer(*(left_->Next()));
    }
    for (right_->beginTuple(); !right_->is_end(); right_->nextTuple()) {
      rightSorter_->writeBuffer(*(right_->Next()));
    }

    leftSorter_->clearBuffer();
    rightSorter_->clearBuffer();

    leftSorter_->initializeMergeListAndConstructTree();
    rightSorter_->initializeMergeListAndConstructTree();
  }
  nextTuple();
}

void MergeJoinExecutor::nextTuple() override {
  if (use_index_) {
    index_iterate_helper();
  } else {
    iterate_helper();
  }
  // }while(!isend && !predicate());
  if (isend) {
    return;
  }
  joined_records_ = concat_records();
}

// attention : statement with additional conds is not supported yet.
bool MergeJoinExecutor::predicate() {
  if (fed_conds_.size() == 1) {
    return true;
  }
  bool satisfy = true;
  // return true only all the conditions were true
  // i.e. all conditions are connected with 'and' operator
  for (auto &cond : fed_conds_) {
    assert(!cond.is_rhs_val);
    Value lhs_v, rhs_v;
    if (lhs_v.get_value_from_record(*current_left_data_, left_->cols(), cond.lhs_col.col_name) == nullptr) {
      throw InternalError("target column not found.");
    }
    if (rhs_v.get_value_from_record(*current_right_data_, right_->cols(), cond.rhs_col.col_name) == nullptr) {
      throw InternalError("target column not found.");
    }
    if (!cond.satisfy(lhs_v, rhs_v)) {
      satisfy = false;
      break;
    }
  }
  return satisfy;
}

void MergeJoinExecutor::iterate_helper() {
  current_left_data_ = leftSorter_->getOneRecord();
  current_right_data_ = rightSorter_->getOneRecord();
  if (current_left_data_ == NULL || current_right_data_ == NULL) {
    isend = true;
    completeWriting();
    return;
  }

  writeRecord(fd_left, current_left_data_, left_->cols());
  writeRecord(fd_right, current_right_data_, right_->cols());
  Value lhs_v, rhs_v;
  lhs_v.get_value_from_record(current_left_data_, left_sel_col_);
  rhs_v.get_value_from_record(current_right_data_, right_sel_col_);

  while (!leftSorter_->is_end() && !rightSorter_->is_end()) {
    if (lhs_v == rhs_v) {
      break;
    } else if (lhs_v < rhs_v) {
      current_left_data_ = leftSorter_->getOneRecord();
      writeRecord(fd_left, current_left_data_, left_->cols());
      lhs_v.get_value_from_record(current_left_data_, left_sel_col_);
    } else {
      current_right_data_ = rightSorter_->getOneRecord();
      writeRecord(fd_right, current_right_data_, right_->cols());
      rhs_v.get_value_from_record(current_right_data_, right_sel_col_);
    }
  }

  if (lhs_v != rhs_v) {
    isend = true;
    completeWriting();
  }
}

void MergeJoinExecutor::index_iterate_helper() {
  if (left_idx_ >= left_buffer_.size() || right_idx_ >= right_buffer_.size()) {
    isend = true;
    completeWriting();
    return;
  }
  current_left_rec_ = left_buffer_[left_idx_];
  // left_->nextTuple();
  left_idx_++;
  current_right_rec_ = right_buffer_[right_idx_];
  // right_->nextTuple();
  right_idx_++;

  writeRecord(fd_left, current_left_rec_, left_->cols());
  writeRecord(fd_right, current_right_rec_, right_->cols());
  Value lhs_v, rhs_v;
  lhs_v.get_value_from_record(current_left_rec_, left_sel_col_);
  rhs_v.get_value_from_record(current_right_rec_, right_sel_col_);

  while (left_idx_ < left_buffer_.size() && right_idx_ < right_buffer_.size()) {
    if (lhs_v == rhs_v) {
      break;
    } else if (lhs_v < rhs_v) {
      current_left_rec_ = left_buffer_[left_idx_];
      writeRecord(fd_left, current_left_rec_, left_->cols());
      lhs_v.get_value_from_record(current_left_rec_, left_sel_col_);
      left_idx_++;
    } else {
      current_right_rec_ = right_buffer_[right_idx_];
      writeRecord(fd_right, current_right_rec_, right_->cols());
      rhs_v.get_value_from_record(current_right_rec_, right_sel_col_);
      right_idx_++;
    }
  }

  if (lhs_v != rhs_v) {
    isend = true;
    completeWriting();
  }
}

RmRecord MergeJoinExecutor::concat_records() {
  if (use_index_) {
    RmRecord left = (current_left_rec_);
    RmRecord right = (current_right_rec_);
    left += right;

    return left;
  } else {
    char *data_cat = new char[len_];

    memcpy(data_cat, current_left_data_, left_->tupleLen());
    memcpy(data_cat + left_->tupleLen(), current_right_data_, right_->tupleLen());

    return RmRecord(len_, data_cat);
  }
}

void MergeJoinExecutor::writeRecord(std::fstream &fd, char *data, std::vector<ColMeta> cols) {
  std::vector<std::string> columns;
  for (auto &col : cols) {
    std::string col_str;
    char *rec_buf = data + col.offset;
    switch (col.type) {
      case TYPE_INT:
        col_str = std::to_string(*(int *)rec_buf);
        break;
      case TYPE_FLOAT:
        col_str = std::to_string(*(float *)rec_buf);
        break;
      case TYPE_STRING:
        col_str = std::string((char *)rec_buf, col.len);
        col_str.resize(strlen(col_str.c_str()));
        break;
      default:
        throw InternalError("unsupported data type.");
    }
    columns.push_back(col_str);
  }
  fd << "|";
  for (auto &col_str : columns) {
    fd << " " << col_str << " |";
  }
  fd << "\n";
}

void MergeJoinExecutor::writeRecord(std::fstream &fd, std::unique_ptr<RmRecord> &Tuple,
                                    const std::vector<ColMeta> &cols) {
  std::vector<std::string> columns;
  for (auto &col : cols) {
    std::string col_str;
    char *rec_buf = Tuple->data + col.offset;
    switch (col.type) {
      case TYPE_INT:
        col_str = std::to_string(*(int *)rec_buf);
        break;
      case TYPE_FLOAT:
        col_str = std::to_string(*(float *)rec_buf);
        break;
      case TYPE_STRING:
        col_str = std::string((char *)rec_buf, col.len);
        col_str.resize(strlen(col_str.c_str()));
        break;
      default:
        throw InternalError("unsupported data type.");
    }
    columns.push_back(col_str);
  }
  fd << "|";
  for (auto &col_str : columns) {
    fd << " " << col_str << " |";
  }
  fd << "\n";
}

void MergeJoinExecutor::writeRecord(std::fstream &fd, RmRecord &Tuple, const std::vector<ColMeta> &cols) {
  std::vector<std::string> columns;
  for (auto &col : cols) {
    std::string col_str;
    char *rec_buf = Tuple.data + col.offset;
    switch (col.type) {
      case TYPE_INT:
        col_str = std::to_string(*(int *)rec_buf);
        break;
      case TYPE_FLOAT:
        col_str = std::to_string(*(float *)rec_buf);
        break;
      case TYPE_STRING:
        col_str = std::string((char *)rec_buf, col.len);
        col_str.resize(strlen(col_str.c_str()));
        break;
      default:
        throw InternalError("unsupported data type.");
    }
    columns.push_back(col_str);
  }
  fd << "|";
  for (auto &col_str : columns) {
    fd << " " << col_str << " |";
  }
  fd << "\n";
}

void MergeJoinExecutor::completeWriting() {
  if (use_index_) {
    while (left_idx_ < left_buffer_.size()) {
      left_idx_++;
      current_left_rec_ = left_buffer_[left_idx_];
      current_left_data_ = current_left_rec_.data;
      writeRecord(fd_left, current_left_data_, left_->cols());
    }
    while (right_idx_ < right_buffer_.size()) {
      right_idx_++;
      current_right_rec_ = right_buffer_[right_idx_];
      current_right_data_ = current_right_rec_.data;
      writeRecord(fd_right, current_right_data_, right_->cols());
    }
  } else {
    while (!leftSorter_->is_end()) {
      current_left_data_ = leftSorter_->getOneRecord();
      writeRecord(fd_left, current_left_data_, left_->cols());
    }
    while (!rightSorter_->is_end()) {
      current_right_data_ = rightSorter_->getOneRecord();
      writeRecord(fd_right, current_right_data_, right_->cols());
    }
  }

  fd_left.close();
  fd_left.open("sorted_results_left.txt", std::ios::in);
  char tp;
  while (fd_left.get(tp)) {
    fd_right << tp;
  }
  fd_right.close();
  fd_left.close();
  if (std::remove("sorted_results_left.txt") != 0) {  // delete file
    printf("Failed to delete file sorted_results_left.txt.\n");
  }
}

}  // namespace easydb
