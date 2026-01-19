/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * tuple.cpp
 *
 * Identification: src/storage/table/tuple.cpp
 *
 *-------------------------------------------------------------------------
 */

/*
 * Original copyright:
 * Copyright (c) 2015-2019, Carnegie Mellon University Database Group
 */

#include <cassert>
#include <cstdlib>
#include <sstream>
#include <string>
#include <vector>

#include "storage/table/tuple.h"

namespace easydb {

// TODO(Amadou): It does not look like nulls are supported. Add a null bitmap?
/**
 * @brief 根据输入值创建新元组
 * @param values 列值的向量
 * @param schema 元组的模式（列定义）
 *
 * 构造过程：
 * 1. 计算元组的总大小（包括内联列和非内联列）
 * 2. 分配内存并初始化为0
 * 3. 序列化每个属性：
 *    - 对于内联列：直接在偏移位置序列化值
 *    - 对于非内联列：在偏移位置存储相对偏移量，在末尾存储实际数据
 */
Tuple::Tuple(std::vector<Value> values, const Schema *schema) {
  assert(values.size() == schema->GetColumnCount());

  // 1. Calculate the size of the tuple.
  uint32_t tuple_size = schema->GetInlinedStorageSize();
  for (auto &i : schema->GetUnlinedColumns()) {
    auto len = values[i].GetStorageSize();
    if (len == EASYDB_VALUE_NULL) {
      len = 0;
    }
    tuple_size += sizeof(uint32_t) + len;
  }

  // 2. Allocate memory.
  data_.resize(tuple_size);
  std::fill(data_.begin(), data_.end(), 0);

  // 3. Serialize each attribute based on the input value.
  uint32_t column_count = schema->GetColumnCount();
  uint32_t offset = schema->GetInlinedStorageSize();

  for (uint32_t i = 0; i < column_count; i++) {
    const auto &col = schema->GetColumn(i);
    if (!col.IsInlined()) {
      // Serialize relative offset, where the actual varchar data is stored.
      *reinterpret_cast<uint32_t *>(data_.data() + col.GetOffset()) = offset;
      // Serialize varchar value, in place (size+data).
      values[i].SerializeTo(data_.data() + offset);
      auto len = values[i].GetStorageSize();
      if (len == EASYDB_VALUE_NULL) {
        len = 0;
      }
      offset += sizeof(uint32_t) + len;
    } else {
      values[i].SerializeTo(data_.data() + col.GetOffset());
    }
  }
}

/**
 * @brief 根据字符向量构造元组
 * @param data 元组数据的字符向量
 * @note 直接复制数据向量，不进行解析
 */
Tuple::Tuple(std::vector<char> data) {
  // // 1. Calculate the size of the tuple.
  // uint32_t tuple_size = schema->GetInlinedStorageSize();
  // for (auto &i : schema->GetUnlinedColumns()) {
  //   auto len = values[i].GetStorageSize();
  //   if (len == EASYDB_VALUE_NULL) {
  //     len = 0;
  //   }
  //   tuple_size += sizeof(uint32_t) + len;
  // }

  // // 2. Allocate memory.
  // data_.resize(tuple_size);
  // std::fill(data_.begin(), data_.end(), 0);

  data_ = data;

  // // 3. Serialize each attribute based on the input value.
  // uint32_t column_count = schema->GetColumnCount();
  // uint32_t offset = schema->GetInlinedStorageSize();

  // for (uint32_t i = 0; i < column_count; i++) {
  //   const auto &col = schema->GetColumn(i);
  //   if (!col.IsInlined()) {
  //     // Serialize relative offset, where the actual varchar data is stored.
  //     *reinterpret_cast<uint32_t *>(data_.data() + col.GetOffset()) = offset;
  //     // Serialize varchar value, in place (size+data).
  //     values[i].SerializeTo(data_.data() + offset);
  //     auto len = values[i].GetStorageSize();
  //     if (len == EASYDB_VALUE_NULL) {
  //       len = 0;
  //     }
  //     offset += sizeof(uint32_t) + len;
  //   } else {
  //     values[i].SerializeTo(data_.data() + col.GetOffset());
  //   }
  // }
}

/**
 * @brief 根据大小和数据指针构造元组
 * @param size 元组数据的大小（字节数）
 * @param data 元组数据的指针
 */
Tuple::Tuple(int size, char *data) {
  this->data_.resize(size);
  memcpy(this->data_.data(), data, size);
}

/**
 * @brief 根据大小和常量数据指针构造元组
 * @param size 元组数据的大小（字节数）
 * @param data 元组数据的常量指针
 */
Tuple::Tuple(int size, const char *data) {
  this->data_.resize(size);
  memcpy(this->data_.data(), data, size);
}

/**
 * @brief 获取指定列的值（根据列索引）
 * @param schema 元组的模式
 * @param column_idx 列的索引
 * @return 列的值
 */
auto Tuple::GetValue(const Schema *schema, const uint32_t column_idx) const
    -> Value {
  assert(schema);
  const TypeId column_type = schema->GetColumn(column_idx).GetType();
  const char *data_ptr = GetDataPtr(schema, column_idx);
  // the third parameter "is_inlined" is unused
  return Value::DeserializeFrom(data_ptr, column_type);
}

/**
 * @brief 获取指定列的值（根据列名）
 * @param schema 元组的模式
 * @param column_name 列的名称
 * @return 列的值
 */
auto Tuple::GetValue(const Schema *schema, std::string column_name) const
    -> Value {
  assert(schema);
  const TypeId column_type = schema->GetColumn(column_name).GetType();
  const char *data_ptr = GetDataPtr(schema, column_name);
  // the third parameter "is_inlined" is unused
  return Value::DeserializeFrom(data_ptr, column_type);
}

/**
 * @brief 获取指定列的值（根据列对象）
 * @param col 列对象
 * @return 列的值
 */
auto Tuple::GetValue(const Column col) const -> Value {
  const TypeId column_type = col.GetType();
  const char *data_ptr = GetDataPtr(col);
  // the third parameter "is_inlined" is unused
  return Value::DeserializeFrom(data_ptr, column_type);
}

/**
 * @brief 获取元组中所有列的值向量
 * @param schema 元组的模式
 * @return 所有列的值向量
 */
auto Tuple::GetValueVec(const Schema *schema) const -> std::vector<Value> {
  std::vector<Value> values;
  values.reserve(schema->GetColumnCount());
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    values.emplace_back(this->GetValue(schema, i));
  }
  return values;
}

/**
 * @brief 根据模式和属性列表从元组生成键元组
 * @param schema 完整元组的模式
 * @param key_schema 键的模式（只包含键列）
 * @param key_attrs 键属性的索引列表
 * @return 键元组
 * @note 用于索引操作，从完整元组中提取键列
 */
auto Tuple::KeyFromTuple(const Schema &schema, const Schema &key_schema,
                         const std::vector<uint32_t> &key_attrs) const
    -> Tuple {
  std::vector<Value> values;
  values.reserve(key_attrs.size());
  for (auto idx : key_attrs) {
    values.emplace_back(this->GetValue(&schema, idx));
  }
  return {values, &key_schema};
}

/**
 * @brief 获取指定列的起始存储地址（根据列索引）
 * @param schema 元组的模式
 * @param column_idx 列的索引
 * @return 指向列数据的常量指针
 *
 * 实现逻辑：
 * - 对于内联类型：直接返回列偏移位置的地址
 * - 对于非内联类型：从列偏移位置读取相对偏移量，返回实际数据的地址
 */
auto Tuple::GetDataPtr(const Schema *schema, const uint32_t column_idx) const
    -> const char * {
  assert(schema);
  const auto &col = schema->GetColumn(column_idx);
  bool is_inlined = col.IsInlined();
  // For inline type, data is stored where it is.
  if (is_inlined) {
    return (data_.data() + col.GetOffset());
  }
  // We read the relative offset from the tuple data.
  int32_t offset =
      *reinterpret_cast<const int32_t *>(data_.data() + col.GetOffset());
  // And return the beginning address of the real data for the VARCHAR type.
  return (data_.data() + offset);
}

auto Tuple::GetDataPtr(const Schema *schema,
                       const std::string column_name) const -> const char * {
  assert(schema);
  const auto &col = schema->GetColumn(column_name);
  bool is_inlined = col.IsInlined();
  // For inline type, data is stored where it is.
  if (is_inlined) {
    return (data_.data() + col.GetOffset());
  }
  // We read the relative offset from the tuple data.
  int32_t offset =
      *reinterpret_cast<const int32_t *>(data_.data() + col.GetOffset());
  // And return the beginning address of the real data for the VARCHAR type.
  return (data_.data() + offset);
}

auto Tuple::GetDataPtr(const Column col) const -> const char * {
  bool is_inlined = col.IsInlined();
  // For inline type, data is stored where it is.
  if (is_inlined) {
    return (data_.data() + col.GetOffset());
  }
  // We read the relative offset from the tuple data.
  int32_t offset =
      *reinterpret_cast<const int32_t *>(data_.data() + col.GetOffset());
  // And return the beginning address of the real data for the VARCHAR type.
  return (data_.data() + offset);
}

/**
 * @brief 获取元组的字符串表示
 * @param schema 元组的模式
 * @return 格式为"(val1, val2, ...)"的字符串
 * @note NULL值显示为"<NULL>"
 */
auto Tuple::ToString(const Schema *schema) const -> std::string {
  std::stringstream os;

  int column_count = schema->GetColumnCount();
  bool first = true;
  os << "(";
  for (int column_itr = 0; column_itr < column_count; column_itr++) {
    if (first) {
      first = false;
    } else {
      os << ", ";
    }
    if (IsNull(schema, column_itr)) {
      os << "<NULL>";
    } else {
      Value val = (GetValue(schema, column_itr));
      os << val.ToString();
    }
  }
  os << ")";

  return os.str();
}

/**
 * @brief 序列化元组数据到存储
 * @param storage 存储缓冲区指针
 * @note 格式：前4字节存储大小（int32_t），之后是实际数据
 */
void Tuple::SerializeTo(char *storage) const {
  int32_t sz = data_.size();
  memcpy(storage, &sz, sizeof(int32_t));
  memcpy(storage + sizeof(int32_t), data_.data(), sz);
}

/**
 * @brief 从存储反序列化元组数据（深拷贝）
 * @param storage 存储缓冲区指针
 * @note 从storage读取大小（前4字节），然后读取实际数据
 */
void Tuple::DeserializeFrom(const char *storage) {
  uint32_t size = *reinterpret_cast<const uint32_t *>(storage);
  this->data_.resize(size);
  memcpy(this->data_.data(), storage + sizeof(int32_t), size);
}

}  // namespace easydb
