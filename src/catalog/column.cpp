/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * column.cpp
 *
 * Identification: src/catalog/column.cpp
 *
 *-------------------------------------------------------------------------
 */

/*
 * Original copyright:
 * Copyright (c) 2015-2019, Carnegie Mellon University Database Group
 */

#include "catalog/column.h"

#include <sstream>
#include <string>
#include "system/sm_meta.h"
#include "type/type_id.h"

namespace easydb {

/**
 * @brief 获取列的字符串表示
 * @param simplified 如果为true，返回简化格式（列名:类型）；如果为false，返回详细格式
 * @return 列的字符串表示
 * 
 * 简化格式示例：name:INT, age:LONG, description:VARCHAR(100)
 * 详细格式示例：Column[name, INT, Offset:0, Length:4]
 */
auto Column::ToString(bool simplified) const -> std::string {
  if (simplified) {
    // 简化格式：列名:类型(长度)
    std::ostringstream os;
    os << column_name_ << ":" << Type::TypeIdToString(column_type_);
    // 对于变长类型，显示长度信息
    if (column_type_ == TYPE_VARCHAR) {
      os << "(" << length_ << ")";
    }
    // if (column_type_ == VECTOR) {
    //   os << "(" << length_ / sizeof(double) << ")";
    // }
    return (os.str());
  }

  // 详细格式：包含所有元数据信息
  std::ostringstream os;

  os << "Column[" << column_name_ << ", " << Type::TypeIdToString(column_type_) << ", " << "Offset:" << column_offset_
     << ", ";
  os << "Length:" << length_;
  os << "]";
  return (os.str());
}

}  // namespace easydb