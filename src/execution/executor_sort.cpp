/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 *
 *-------------------------------------------------------------------------
 */

#include "execution/executor_sort.h"

namespace easydb {

SortExecutor::SortExecutor(std::unique_ptr<AbstractExecutor> prev, TabCol sel_cols, bool is_desc) {
  prev_ = std::move(prev);
  schema_ = prev_->schema();
  colus_ = schema_.GetColumn(sel_cols.col_name);
  is_desc_ = is_desc;
  tuple_num = 0;
  used_tuple.clear();
  len_ = prev_->tupleLen();
  max_physical_len_ = schema_.GetPhysicalSize();
  current_data_ = new char[max_physical_len_];
  sorter = std::make_unique<MergeSorter>(colus_, prev_->schema().GetColumns(), max_physical_len_, is_desc_);
}

SortExecutor::~SortExecutor() { delete[] current_data_; }

void SortExecutor::beginTuple() {
  std::unique_ptr<Tuple> current_tuple;
  for (prev_->beginTuple(); !prev_->IsEnd(); prev_->nextTuple()) {
    current_tuple = prev_->Next();
    sorter->writeBuffer(*current_tuple);
  }
  sorter->clearBuffer();
  sorter->initializeMergeListAndConstructTree();
  nextTuple();
}

void SortExecutor::nextTuple() {
  if (!sorter->IsEnd()) {
    char *tp = sorter->getOneRecord();
    memcpy(current_data_, tp, max_physical_len_);
    free(tp);
  } else {
    isend_ = true;
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
