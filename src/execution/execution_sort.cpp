/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 *
 *-------------------------------------------------------------------------
 */

#pragma once
#include "execution/execution_sort.h"

namespace easydb {

SortExecutor::SortExecutor(std::unique_ptr<AbstractExecutor> prev, TabCol sel_cols, bool is_desc) {
  prev_ = std::move(prev);
  cols_ = prev_->get_col_offset(sel_cols);
  is_desc_ = is_desc;
  tuple_num = 0;
  used_tuple.clear();
  len_ = prev_->tupleLen();
  sorter = std::make_unique<MergeSorter>(cols_, prev_->cols(), len_, is_desc_);
}

void SortExecutor::beginTuple() override {
  for (prev_->beginTuple(); !prev_->IsEnd(); prev_->nextTuple()) {
    current_tuple = prev_->Next();
    sorter->writeBuffer(*current_tuple);
  }
  sorter->clearBuffer();
  sorter->initializeMergeListAndConstructTree();
  nextTuple();
}

void SortExecutor::nextTuple() override {
  if (!sorter->IsEnd()) {
    current_tuple = std::make_unique<RmRecord>(len_, sorter->getOneRecord());
  } else {
    isend_ = true;
    current_tuple = NULL;
  }
}

void SortExecutor::printRecord(RmRecord record, std::vector<ColMeta> cols) {
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

void SortExecutor::printRecord(char *data, std::vector<ColMeta> cols) {
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

}  // namespace easydb
