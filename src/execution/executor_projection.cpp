/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "execution/executor_projection.h"

namespace easydb {

ProjectionExecutor::ProjectionExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<TabCol> &sel_cols) {
  prev_ = std::move(prev);

  size_t curr_offset = 0;
  auto &prev_cols = prev_->cols();
  auto schema_ = prev_->schema();
  for (auto &sel_col : sel_cols) {
    std::string new_name = sel_col.col_name;
    if (sel_col.aggregation_type != AggregationType::NO_AGG) {
      new_name = generate_new_name(sel_col);
    }
    auto pos = get_col(prev_cols, sel_col.tab_name, new_name);
    sel_idxs_.push_back(pos - prev_cols.begin());
    auto col = *pos;
    if (sel_col.aggregation_type != AggregationType::NO_AGG) {
      col.name = new_name;
    }
    col.offset = curr_offset;
    curr_offset += col.len;
    cols_.push_back(col);
  }
  len_ = curr_offset;
}

void ProjectionExecutor::beginTuple() override {
  prev_->beginTuple();
  if (!IsEnd()) {
    projection_records_ = projectRecord();
  }
}

void ProjectionExecutor::nextTuple() override {
  prev_->nextTuple();
  if (!IsEnd()) {
    projection_records_ = projectRecord();
  }
}

RmRecord ProjectionExecutor::projectRecord() {
  char *projected_record = new char[len_];
  size_t temp_pos = 0;
  auto prev_record = prev_->Next();
  auto prev_data = prev_record->data;
  for (int i = 0; i < sel_idxs_.size(); i++) {
    auto col_tmp = prev_->cols().at(sel_idxs_[i]);
    memcpy(projected_record + temp_pos, prev_data + col_tmp.offset, col_tmp.len);
    temp_pos += col_tmp.len;
  }
  return RmRecord(temp_pos, projected_record);
}

std::string ProjectionExecutor::generate_new_name(TabCol col) {
  if (col.new_col_name != "") {
    return col.new_col_name;
  }
  switch (col.aggregation_type) {
    case MAX_AGG:
      return "MAX(" + col.col_name + ")";
    case MIN_AGG:
      return "MIN(" + col.col_name + ")";
    case COUNT_AGG:
      return "COUNT(" + col.col_name + ")";
    case SUM_AGG:
      return "SUM(" + col.col_name + ")";
    default:
      throw InternalError("unsupported aggregation type.");
  }
}

}  // namespace easydb
