/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * executor_projection.cpp
 *
 * Identification: src/execution/executor_projection.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "execution/executor_projection.h"

#include <sstream>  // For tuple serialization

namespace easydb {

ProjectionExecutor::ProjectionExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<TabCol> &sel_cols,
                                       bool is_unique) {
  prev_ = std::move(prev);
  is_unique_ = is_unique;

  size_t curr_offset = 0;
  auto prev_schema = prev_->schema();
  std::vector<Column> prev_columns = prev_schema.GetColumns();

  for (const auto &sel_col : sel_cols) {
    std::string new_name = sel_col.col_name;
    if (sel_col.aggregation_type != AggregationType::NO_AGG) {
      new_name = generate_new_name(sel_col);
    }
    auto pos = get_col(prev_columns, sel_col.tab_name, new_name);
    if (pos == prev_columns.end()) {
      throw ColumnNotFoundError(new_name);
    }
    sel_ids_.emplace_back(prev_schema.GetColIdx(new_name));
    Column col = *pos;
    if (sel_col.aggregation_type != AggregationType::NO_AGG) {
      col.SetName(new_name);
    }
    col.SetOffset(curr_offset);
    curr_offset += col.GetStorageSize();
  }

  schema_ = prev_schema.CopySchema(&prev_schema, sel_ids_);
  len_ = curr_offset;

  if (is_unique_) {
    seen_.clear();
  }
}

void ProjectionExecutor::beginTuple() {
  prev_->beginTuple();
  if (is_unique_) {
    seen_.clear();
  }

  if (!IsEnd()) {
    projection_records_ = projectRecord();

    if (is_unique_) {
      std::string serialized = projection_records_.ToString(&schema_);
      while (seen_.find(serialized) != seen_.end()) {
        nextTuple();
        if (IsEnd()) {
          break;
        }
        projection_records_ = projectRecord();
        serialized = projection_records_.ToString(&schema_);
      }

      if (!IsEnd()) {
        seen_.insert(serialized);
      }
    }
  }
}

void ProjectionExecutor::nextTuple() {
  prev_->nextTuple();
  if (IsEnd()) {
    return;
  }

  projection_records_ = projectRecord();

  if (is_unique_) {
    std::string serialized = projection_records_.ToString(&schema_);
    while (seen_.find(serialized) != seen_.end()) {
      // Duplicate found, fetch next tuple
      prev_->nextTuple();
      if (prev_->IsEnd()) {
        break;
      }
      projection_records_ = projectRecord();
      serialized = projection_records_.ToString(&schema_);
    }

    if (!IsEnd()) {
      seen_.insert(serialized);
    }
  }
}

std::unique_ptr<Tuple> ProjectionExecutor::Next() {
  if (IsEnd()) {
    return nullptr;
  }
  return std::make_unique<Tuple>(projection_records_);
}

Tuple ProjectionExecutor::projectRecord() {
  auto prev_tuple_ptr = prev_->Next();
  if (!prev_tuple_ptr) {
    throw InternalError("Previous executor returned null tuple.");
  }
  const Tuple &prev_tuple = *prev_tuple_ptr;
  const Schema &prev_schema = prev_->schema();
  Tuple proj_tuple = prev_tuple.KeyFromTuple(prev_schema, schema_, sel_ids_);
  return proj_tuple;
}

std::string ProjectionExecutor::generate_new_name(const TabCol &col) {
  if (!col.new_col_name.empty()) {
    return col.new_col_name;
  }
  switch (col.aggregation_type) {
    case AggregationType::MAX_AGG:
      return "MAX(" + col.col_name + ")";
    case AggregationType::MIN_AGG:
      return "MIN(" + col.col_name + ")";
    case AggregationType::COUNT_AGG:
      return "COUNT(" + col.col_name + ")";
    case AggregationType::SUM_AGG:
      return "SUM(" + col.col_name + ")";
    default:
      throw InternalError("Unsupported aggregation type.");
  }
}

}  // namespace easydb