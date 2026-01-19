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

/**
 * @brief 根据列向量构造模式
 * @param columns 描述模式中各个列的列向量
 * 
 * 构造过程：
 * 1. 遍历所有列，计算每个列在元组中的偏移量
 * 2. 对于内联列，偏移量增加列的实际大小
 * 3. 对于非内联列，偏移量只增加指针大小（sizeof(uint32_t)），并记录其索引
 * 4. 设置元组的总长度（内联部分）
 * 5. 计算并设置元组的物理大小（包括非内联列的实际数据）
 */
Schema::Schema(const std::vector<Column> &columns) {
  uint32_t curr_offset = 0;
  
  // 遍历所有列，计算偏移量并设置列属性
  for (uint32_t index = 0; index < columns.size(); index++) {
    Column column = columns[index];
    
    // 处理非内联列：记录其索引，并设置tuple_is_inlined_标志
    if (!column.IsInlined()) {
      tuple_is_inlined_ = false;
      uninlined_columns_.push_back(index);
    }
    
    // 设置列的偏移量
    column.column_offset_ = curr_offset;
    
    // 根据列是否为内联，更新偏移量
    if (column.IsInlined()) {
      // 内联列：偏移量增加列的实际大小
      curr_offset += column.GetStorageSize();
    } else {
      // 非内联列：偏移量只增加指针大小（元组中存储的是指向实际数据的指针）
      curr_offset += sizeof(uint32_t);
    }

    // 将列添加到模式中
    this->columns_.push_back(column);
  }
  
  // 设置元组的总长度（内联部分的长度）
  length_ = curr_offset;

  // 计算并设置元组的物理大小
  SetPhysicalSize();
}

/**
 * @brief 获取元组的物理大小
 * @return 元组的物理大小（字节数）
 * @note 物理大小包括：
 *   - 内联部分的大小（length_）
 *   - 非内联列的指针大小
 *   - 非内联列的实际数据大小
 */
auto Schema::GetPhysicalSize() const -> uint32_t {
  // if(physical_size_ == 0){ // incase physical_size_ not initialized.
  //   SetPhysicalSize();
  // }
  return physical_size_;
}

/**
 * @brief 计算并设置元组的物理大小
 * 
 * 物理大小的计算：
 * - 基础大小 = 内联部分长度 + sizeof(uint32_t)（用于存储非内联列的数量？）
 * - 对于每个非内联列，增加：列的实际数据大小 + sizeof(uint32_t)（指针大小）
 */
void Schema::SetPhysicalSize() {
  // 基础大小：内联部分 + 一个uint32_t（可能用于存储元数据）
  physical_size_ = length_ + sizeof(uint32_t);
  
  // 对于每个非内联列，增加其实际数据大小和指针大小
  for (auto &colu : columns_) {
    if (!colu.IsInlined()) {
      physical_size_ += colu.GetStorageSize() + sizeof(uint32_t);
    }
  }
}

/**
 * @brief 获取模式的字符串表示
 * @param simplified 如果为true，返回简化格式；如果为false，返回详细格式
 * @return 模式的字符串表示
 * 
 * 简化格式示例：(id:INT, name:VARCHAR(50), age:LONG)
 * 详细格式示例：Schema[NumColumns:3, IsInlined:false, Length:20] :: (Column[id, INT, Offset:0, Length:4], ...)
 */
auto Schema::ToString(bool simplified) const -> std::string {
  if (simplified) {
    // 简化格式：只显示列名和类型
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

  // 详细格式：显示所有元数据信息
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