/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * schema.cpp
 *
 * Identification: src/catalog/schema.cpp
 *
 *-------------------------------------------------------------------------
 */

/*
 * Original copyright:
 * Copyright (c) 2015-2019, Carnegie Mellon University Database Group
 */

#include "catalog/schema.h"

#include <sstream>
#include <string>
#include <vector>

namespace easydb {

Schema::Schema(const std::vector<Column> &columns) {
  uint32_t curr_offset = 0;
  for (uint32_t index = 0; index < columns.size(); index++) {
    Column column = columns[index];
    // handle uninlined column
    if (!column.IsInlined()) {
      tuple_is_inlined_ = false;
      uninlined_columns_.push_back(index);
    }
    // set column offset
    column.column_offset_ = curr_offset;
    if (column.IsInlined()) {
      curr_offset += column.GetStorageSize();
    } else {
      curr_offset += sizeof(uint32_t);
    }

    // add column
    this->columns_.push_back(column);
  }
  // set tuple length
  length_ = curr_offset;

  SetPhysicalSize();
}

auto Schema::GetPhysicalSize() const -> uint32_t {
  // if(physical_size_ == 0){ // incase physical_size_ not initialized.
  //   SetPhysicalSize();
  // }
  return physical_size_;
}

void Schema::SetPhysicalSize() {
  physical_size_ = length_;
  for(auto &colu: columns_){
    if(!colu.IsInlined()){
      physical_size_ += colu.GetStorageSize() + sizeof(uint32_t);
    }
  }
}

auto Schema::ToString(bool simplified) const -> std::string {
  if (simplified) {
    std::ostringstream os;
    bool first = true;
    os << "(";
    for (uint32_t i = 0; i < GetColumnCount(); i++) {
      if (first) {
        first = false;
      } else {
        os << ", ";
      }
      os << columns_[i].ToString(simplified);
    }
    os << ")";
    return (os.str());
  }

  std::ostringstream os;

  os << "Schema[" << "NumColumns:" << GetColumnCount() << ", " << "IsInlined:" << tuple_is_inlined_ << ", "
     << "Length:" << length_ << "]";

  bool first = true;
  os << " :: (";
  for (uint32_t i = 0; i < GetColumnCount(); i++) {
    if (first) {
      first = false;
    } else {
      os << ", ";
    }
    os << columns_[i].ToString();
  }
  os << ")";

  return os.str();
}

}  // namespace easydb