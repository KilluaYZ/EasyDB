/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * column.h
 *
 * Identification: src/include/catalog/column.h
 *
 *-------------------------------------------------------------------------
 */

/*
 * Original copyright:
 * Copyright (c) 2015-2019, Carnegie Mellon University Database Group
 */

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

// #include "fmt/format.h"

#include "common/exception.h"
#include "common/macros.h"
// #include "system/sm_meta.h"
#include "type/type.h"
#include "type/type_id.h"

namespace easydb {
class AbstractExpression;

class Column {
  friend class Schema;

 public:
  Column() {}
  /**
   * Non-variable-length constructor for creating a Column.
   * @param column_name name of the column
   * @param type type of the column
   */
  Column(std::string column_name, TypeId type)
      : column_name_(std::move(column_name)), column_type_(type), length_(TypeSize(type)) {
    EASYDB_ASSERT(type != TypeId::TYPE_CHAR, "Wrong constructor for CHAR type.");
    EASYDB_ASSERT(type != TypeId::TYPE_VARCHAR, "Wrong constructor for VARCHAR type.");
    // EASYDB_ASSERT(type != TypeId::VECTOR, "Wrong constructor for VECTOR type.");
  }

  /**
   * Variable-length constructor for creating a Column.
   * @param column_name name of the column
   * @param type type of column
   * @param length length of the varlen
   * @param expr expression used to create this column
   */
  Column(std::string column_name, TypeId type, uint32_t length)
      : column_name_(std::move(column_name)), column_type_(type), length_(TypeSize(type, length)) {
    EASYDB_ASSERT(type == TypeId::TYPE_CHAR || type == TypeId::TYPE_VARCHAR, "Wrong constructor for fixed-size type.");
    // EASYDB_ASSERT(type == TypeId::TYPE_VARCHAR || type == TypeId::VECTOR, "Wrong constructor for fixed-size type.");
  }

  Column(std::string column_name, std::string tab_name, TypeId type, uint32_t length)
      : column_name_(std::move(column_name)), tab_name_(tab_name), column_type_(type), length_(TypeSize(type, length)) {
    EASYDB_ASSERT(type == TypeId::TYPE_CHAR || type == TypeId::TYPE_VARCHAR, "Wrong constructor for fixed-size type.");
    // EASYDB_ASSERT(type == TypeId::TYPE_VARCHAR || type == TypeId::VECTOR, "Wrong constructor for fixed-size type.");
  }

  /**
   * Replicate a Column with a different name.
   * @param column_name name of the column
   * @param column the original column
   */
  Column(std::string column_name, const Column &column)
      : column_name_(std::move(column_name)),
        column_type_(column.column_type_),
        length_(column.length_),
        column_offset_(column.column_offset_) {}

  auto WithColumnName(std::string column_name) -> Column {
    Column c = *this;
    c.column_name_ = std::move(column_name);
    return c;
  }


  Column(std::string column_name, std::string tab_name, TypeId type)
    : column_name_(std::move(column_name)),
      tab_name_(std::move(tab_name)),
      column_type_(type),
      length_(TypeSize(type)) {
  EASYDB_ASSERT(type != TypeId::TYPE_CHAR && type != TypeId::TYPE_VARCHAR,
                "Wrong constructor for variable-length types.");
}

  /** @return column name */
  auto GetName() const -> std::string { return column_name_; }

  void SetName(std::string new_name) { column_name_ = new_name; }

  auto GetTabName() const -> std::string { return tab_name_; }

  /** @return column length */
  auto GetStorageSize() const -> uint32_t { return length_; }

  /** @return column's offset in the tuple */
  auto GetOffset() const -> uint32_t { return column_offset_; }

  void SetOffset(uint32_t new_offset) { column_offset_ = new_offset; }

  /** @return column type */
  auto GetType() const -> TypeId { return column_type_; }

  /** @return true if column is inlined, false otherwise */
  auto IsInlined() const -> bool {
    return (column_type_ != TypeId::TYPE_CHAR) && (column_type_ != TypeId::TYPE_VARCHAR);
  }

  /** @return a string representation of this column */
  auto ToString(bool simplified = true) const -> std::string;

  void AddOffset(int off) { column_offset_ += off; };

  Column &operator=(const Column &other) {
    column_name_ = other.GetName();
    tab_name_ = other.GetTabName();
    column_type_ = other.GetType();
    length_ = other.GetStorageSize();
    column_offset_ = other.GetOffset();
    return *this;
  };

 private:
  /**
   * Return the size in bytes of the type.
   * @param type type whose size is to be determined
   * @return size in bytes
   */
  static auto TypeSize(TypeId type, uint32_t length = 0) -> uint8_t {
    switch (type) {
      // case TypeId::BOOLEAN:
      // case TypeId::TINYINT:
      // return 1;
      // case TypeId::SMALLINT:
      //   return 2;
      case TypeId::TYPE_INT:
        return 4;
      // case TypeId::BIGINT:
      // case TypeId::DECIMAL:
      case TypeId::TYPE_LONG:
      case TypeId::TYPE_FLOAT:
      case TypeId::TYPE_DOUBLE:
      case TypeId::TYPE_DATE:
        return 8;
      case TypeId::TYPE_CHAR:
      case TypeId::TYPE_VARCHAR:
        return length;
      // case TypeId::VECTOR:
      //   return length * sizeof(double);
      default: {
        UNREACHABLE("Cannot get size of invalid type");
      }
    }
  }

  /** Column name. */
  std::string column_name_;

  // TODO: Better to remove this field
  std::string tab_name_;

  /** Column value's type. */
  TypeId column_type_;

  /** The size of the column. */
  uint32_t length_;

  /** Column offset in the tuple. */
  uint32_t column_offset_{0};
};

}  // namespace easydb

// template <typename T>
// struct fmt::formatter<T, std::enable_if_t<std::is_base_of<easydb::Column, T>::value, char>>
//     : fmt::formatter<std::string> {
//   template <typename FormatCtx>
//   auto format(const easydb::Column &x, FormatCtx &ctx) const {
//     return fmt::formatter<std::string>::format(x.ToString(), ctx);
//   }
// };

// template <typename T>
// struct fmt::formatter<std::unique_ptr<T>, std::enable_if_t<std::is_base_of<easydb::Column, T>::value, char>>
//     : fmt::formatter<std::string> {
//   template <typename FormatCtx>
//   auto format(const std::unique_ptr<easydb::Column> &x, FormatCtx &ctx) const {
//     return fmt::formatter<std::string>::format(x->ToString(), ctx);
//   }
// };
