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
#include "defs.h"
// #include "system/sm_meta.h"
#include "type/type.h"
#include "type/type_id.h"

namespace easydb {
class AbstractExpression;

/**
 * @brief 列（Column）类，表示数据库表中的一个列（字段）
 * 
 * Column 类封装了数据库列的所有元数据信息，包括：
 * - 列名和表名
 * - 列的数据类型
 * - 列的存储大小
 * - 列在元组中的偏移量
 * - 聚合类型（用于聚合查询）
 * 
 * 该类提供了多个构造函数以支持不同类型的列（定长和变长）。
 */
class Column {
  friend class Schema;

 public:
  /**
   * @brief 默认构造函数，创建一个空的列对象
   */
  Column() {}
  
  /**
   * @brief 定长类型列的构造函数
   * @param column_name 列的名称
   * @param type 列的数据类型（必须是定长类型，如INT、LONG、FLOAT等）
   * @note 此构造函数用于创建非变长类型的列，如整数、浮点数、日期等
   */
  Column(std::string column_name, TypeId type)
      : column_name_(std::move(column_name)), column_type_(type), length_(TypeSize(type)) {
    EASYDB_ASSERT(type != TypeId::TYPE_CHAR, "Wrong constructor for CHAR type.");
    EASYDB_ASSERT(type != TypeId::TYPE_VARCHAR, "Wrong constructor for VARCHAR type.");
    // EASYDB_ASSERT(type != TypeId::VECTOR, "Wrong constructor for VECTOR type.");
  }

  /**
   * @brief 变长类型列的构造函数
   * @param column_name 列的名称
   * @param type 列的数据类型（必须是变长类型，如CHAR或VARCHAR）
   * @param length 变长类型的最大长度（字节数）
   * @note 此构造函数用于创建变长字符串类型的列
   */
  Column(std::string column_name, TypeId type, uint32_t length)
      : column_name_(std::move(column_name)), column_type_(type), length_(TypeSize(type, length)) {
    EASYDB_ASSERT(type == TypeId::TYPE_CHAR || type == TypeId::TYPE_VARCHAR, "Wrong constructor for fixed-size type.");
    // EASYDB_ASSERT(type == TypeId::TYPE_VARCHAR || type == TypeId::VECTOR, "Wrong constructor for fixed-size type.");
  }

  /**
   * @brief 带表名的变长类型列构造函数
   * @param column_name 列的名称
   * @param tab_name 所属表的名称
   * @param type 列的数据类型（必须是变长类型）
   * @param length 变长类型的最大长度（字节数）
   */
  Column(std::string column_name, std::string tab_name, TypeId type, uint32_t length)
      : column_name_(std::move(column_name)), tab_name_(tab_name), column_type_(type), length_(TypeSize(type, length)) {
    EASYDB_ASSERT(type == TypeId::TYPE_CHAR || type == TypeId::TYPE_VARCHAR, "Wrong constructor for fixed-size type.");
    // EASYDB_ASSERT(type == TypeId::TYPE_VARCHAR || type == TypeId::VECTOR, "Wrong constructor for fixed-size type.");
  }

  /**
   * @brief 带偏移量和聚合类型的变长列构造函数
   * @param tab_name 所属表的名称
   * @param column_name 列的名称
   * @param type 列的数据类型（必须是变长类型）
   * @param length 变长类型的最大长度（字节数）
   * @param offset 列在元组中的偏移量
   * @param agg_type 聚合类型（用于聚合查询，如SUM、COUNT等）
   * @note 此构造函数主要用于聚合查询中创建聚合列
   */
  Column(std::string tab_name, std::string column_name, TypeId type, uint32_t length, uint32_t offset,
         AggregationType agg_type)
      : tab_name_(tab_name),
        column_name_(std::move(column_name)),
        column_type_(type),
        length_(TypeSize(type, length)),
        column_offset_(offset),
        agg_type_(agg_type) {
    EASYDB_ASSERT(type == TypeId::TYPE_CHAR || type == TypeId::TYPE_VARCHAR, "Wrong constructor for fixed-size type.");
    // EASYDB_ASSERT(type == TypeId::TYPE_VARCHAR || type == TypeId::VECTOR, "Wrong constructor for fixed-size type.");
  }

  /**
   * @brief 复制构造函数：复制一个列但使用新的列名
   * @param column_name 新的列名称
   * @param column 要复制的原始列对象
   * @note 用于创建列的副本，通常用于查询计划中的列重命名
   */
  Column(std::string column_name, const Column &column)
      : column_name_(std::move(column_name)),
        column_type_(column.column_type_),
        length_(column.length_),
        column_offset_(column.column_offset_) {}

  /**
   * @brief 创建一个具有新列名的列副本
   * @param column_name 新的列名称
   * @return 具有新列名的列对象副本
   */
  auto WithColumnName(std::string column_name) -> Column {
    Column c = *this;
    c.column_name_ = std::move(column_name);
    return c;
  }

  /**
   * @brief 带表名的定长类型列构造函数
   * @param column_name 列的名称
   * @param tab_name 所属表的名称
   * @param type 列的数据类型（必须是定长类型）
   * @note 此构造函数用于创建定长类型的列，并指定所属表名
   */
  Column(std::string column_name, std::string tab_name, TypeId type)
      : column_name_(std::move(column_name)),
        tab_name_(std::move(tab_name)),
        column_type_(type),
        length_(TypeSize(type)) {
    EASYDB_ASSERT(type != TypeId::TYPE_CHAR && type != TypeId::TYPE_VARCHAR,
                  "Wrong constructor for variable-length types.");
  }

  // Column &operator=(Column &c) {
  //   tab_name_ = c.GetTabName();
  //   column_name_ = c.GetName();
  //   column_type_ = c.GetType();
  //   length_ = c.GetStorageSize();
  //   column_offset_ = c.GetOffset();
  //   agg_type_ = c.GetAggregationType();
  // }

  /**
   * @brief 获取列的名称
   * @return 列名称的字符串
   */
  auto GetName() const -> std::string { return column_name_; }

  /**
   * @brief 设置列的名称
   * @param new_name 新的列名称
   */
  void SetName(std::string new_name) { column_name_ = new_name; }

  /**
   * @brief 获取列所属表的名称
   * @return 表名称的字符串
   */
  auto GetTabName() const -> std::string { return tab_name_; }

  /**
   * @brief 设置列所属表的名称
   * @param tab_name 表名称
   */
  void SetTabName(std::string tab_name) { tab_name_ = tab_name; }

  /**
   * @brief 获取列的存储大小（字节数）
   * @return 列的存储大小
   */
  auto GetStorageSize() const -> uint32_t { return length_; }

  /**
   * @brief 设置列的存储大小
   * @param length 新的存储大小（字节数）
   * @note 注意：此函数实现有误，参数和成员变量赋值反了
   */
  void SetStorageSize(uint32_t length) { length = length_; }

  /**
   * @brief 获取列在元组中的偏移量（字节偏移）
   * @return 列的偏移量
   * @note 偏移量表示该列的数据在元组字节流中的起始位置
   */
  auto GetOffset() const -> uint32_t { return column_offset_; }

  /**
   * @brief 设置列在元组中的偏移量
   * @param new_offset 新的偏移量（字节偏移）
   */
  void SetOffset(uint32_t new_offset) { column_offset_ = new_offset; }

  /**
   * @brief 获取列的数据类型
   * @return 列的数据类型（TypeId枚举值）
   */
  auto GetType() const -> TypeId { return column_type_; }

  /**
   * @brief 设置列的数据类型
   * @param column_type 新的数据类型
   */
  void SetType(TypeId column_type) { column_type_ = column_type; }

  /**
   * @brief 获取列的聚合类型
   * @return 聚合类型（AggregationType枚举值）
   * @note 用于聚合查询，如SUM、COUNT、AVG等
   */
  auto GetAggregationType() const -> AggregationType { return agg_type_; }

  /**
   * @brief 设置列的聚合类型
   * @param agg_type 新的聚合类型
   */
  void SetAggregationType(AggregationType agg_type) { agg_type_ = agg_type; }

  /**
   * @brief 判断列是否为内联（inlined）类型
   * @return true 如果列是内联类型（定长类型），false 如果是非内联类型（变长类型）
   * @note 
   *   - 内联类型：数据直接存储在元组中，如INT、LONG、FLOAT等
   *   - 非内联类型：数据存储在元组外部，元组中只存储指针，如CHAR、VARCHAR等
   */
  auto IsInlined() const -> bool {
    return (column_type_ != TypeId::TYPE_CHAR) && (column_type_ != TypeId::TYPE_VARCHAR);
  }

  /**
   * @brief 获取列的字符串表示
   * @param simplified 如果为true，返回简化格式；如果为false，返回详细格式
   * @return 列的字符串表示
   */
  auto ToString(bool simplified = true) const -> std::string;

  /**
   * @brief 增加列的偏移量
   * @param off 要增加的偏移量（可以是负数）
   */
  void AddOffset(int off) { column_offset_ += off; };

  /**
   * @brief 赋值运算符，从另一个列对象复制所有属性
   * @param other 要复制的源列对象
   * @return 当前对象的引用
   */
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
   * @brief 静态方法：根据类型ID返回该类型的存储大小（字节数）
   * @param type 数据类型ID
   * @param length 对于变长类型，指定长度；对于定长类型，此参数被忽略
   * @return 该类型的存储大小（字节数）
   * @note 
   *   - TYPE_INT: 4字节
   *   - TYPE_LONG, TYPE_FLOAT, TYPE_DOUBLE, TYPE_DATE: 8字节
   *   - TYPE_CHAR, TYPE_VARCHAR: 由length参数指定
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

  /** @brief 列的名称 */
  std::string column_name_;

  /** @brief 列所属表的名称（TODO: 建议移除此字段，通过Schema管理） */
  std::string tab_name_;

  /** @brief 列的数据类型 */
  TypeId column_type_;

  /** @brief 列的存储大小（字节数） */
  uint32_t length_;

  /** @brief 列在元组中的偏移量（字节偏移），表示该列数据在元组字节流中的起始位置 */
  uint32_t column_offset_{0};

  /** @brief 列的聚合类型，用于聚合查询（如SUM、COUNT、AVG等），默认为NO_AGG */
  AggregationType agg_type_{AggregationType::NO_AGG};
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
