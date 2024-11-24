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
  // auto &prev_cols = prev_->cols();
  schema_ = prev_->schema();
  std::vector<Column> prev_colus_ = schema_.GetColumns();
  for (auto &sel_col : sel_cols) {
    std::string new_name = sel_col.col_name;
    if (sel_col.aggregation_type != AggregationType::NO_AGG) {
      new_name = generate_new_name(sel_col);
    }
    auto pos = get_col(prev_colus_, sel_col.tab_name, new_name);
    sel_ids_.emplace_back(schema_.GetColIdx(new_name));
    auto col = *pos;
    if (sel_col.aggregation_type != AggregationType::NO_AGG) {
      col.SetName(new_name);
    }
    col.SetOffset(curr_offset);
    curr_offset += col.GetStorageSize();
    prev_colus_.push_back(col);
  }
  len_ = curr_offset;
}

void ProjectionExecutor::beginTuple() {
  prev_->beginTuple();
  if (!IsEnd()) {
    projection_records_ = projectRecord();
  }
}

void ProjectionExecutor::nextTuple() {
  prev_->nextTuple();
  if (!IsEnd()) {
    projection_records_ = projectRecord();
  }
}

Tuple ProjectionExecutor::projectRecord() {
  auto prev_tuple = prev_->Next();
  auto prev_schema = prev_->schema();
  auto proj_schema = prev_schema.CopySchema(&prev_schema, sel_ids_);
  auto proj_tuple = prev_tuple->KeyFromTuple(prev_schema, proj_schema, sel_ids_);

  return proj_tuple;
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
