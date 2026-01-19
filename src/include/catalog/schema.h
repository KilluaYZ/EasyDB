/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * Schema.h
 *
 * Identification: src/include/catalog/schema.h
 *
 *-------------------------------------------------------------------------
 */

/*
 * Original copyright:
 * Copyright (c) 2015-2019, Carnegie Mellon University Database Group
 */

#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "catalog/column.h"
#include "common/exception.h"
#include "type/type.h"

namespace easydb {

class Schema;
/** @brief Schema的智能指针类型别名，用于共享只读的Schema对象 */
using SchemaRef = std::shared_ptr<const Schema>;

/**
 * @brief 模式（Schema）类，表示数据库表或查询结果的列结构定义
 * 
 * Schema 类封装了表或查询结果的元数据信息，包括：
 * - 所有列的列表
 * - 每个列的偏移量（在元组中的位置）
 * - 元组的存储大小
 * - 哪些列是内联的，哪些是非内联的
 * 
 * Schema 用于：
 * - 定义表的结构（CREATE TABLE）
 * - 定义查询结果的列结构（SELECT查询）
 * - 在查询计划中传递列信息
 */
class Schema {
 public:
  /**
   * @brief 默认构造函数，创建一个空的模式
   */
  Schema() {}

  /**
   * @brief 根据列向量构造模式
   * @param columns 描述模式中各个列的列向量（从左到右读取）
   * @note 
   *   - 构造函数会自动计算每个列的偏移量
   *   - 识别并记录非内联列的索引
   *   - 计算元组的总长度
   */
  explicit Schema(const std::vector<Column> &columns);
  
  /**
   * @brief 从现有模式中复制指定的列，创建新的模式
   * @param from 源模式指针
   * @param attrs 要复制的列的索引列表
   * @return 包含指定列的新模式对象
   * @note 用于查询计划中的投影操作，选择需要的列
   */
  static auto CopySchema(const Schema *from, const std::vector<uint32_t> &attrs) -> Schema {
    std::vector<Column> cols;
    cols.reserve(attrs.size());
    for (const auto i : attrs) {
      cols.emplace_back(from->columns_[i]);
    }
    return Schema{cols};
  }

  /**
   * @brief 获取模式中的所有列
   * @return 列向量的常量引用
   */
  auto GetColumns() const -> const std::vector<Column> & { return columns_; }

  /**
   * @brief 根据索引获取模式中的特定列
   * @param col_idx 请求的列的索引（从0开始）
   * @return 请求的列的常量引用
   */
  auto GetColumn(const uint32_t col_idx) const -> const Column & { return columns_[col_idx]; }

  /**
   * @brief 根据列名获取模式中的特定列
   * @param col_name 列的名称
   * @return 请求的列的常量引用
   * @note 如果列不存在，会抛出异常
   */
  auto GetColumn(const std::string col_name) const -> const Column & {
    uint32_t col_idx = GetColIdx(col_name);
    return columns_[col_idx];
  }

  /**
   * @brief 查找并返回具有指定名称的第一个列在模式中的索引
   * @param col_name 要查找的列名
   * @return 具有给定名称的列的索引，如果不存在则抛出异常
   * @note 如果多个列具有相同的名称，返回第一个匹配的索引
   */
  auto GetColIdx(const std::string &col_name) const -> uint32_t {
    if (auto col_idx = TryGetColIdx(col_name)) {
      return *col_idx;
    }
    UNREACHABLE("Column does not exist");
  }

  /**
   * @brief 查找并返回具有指定名称的第一个列在模式中的索引（安全版本）
   * @param col_name 要查找的列名
   * @return 如果找到，返回列的索引（包装在optional中）；如果不存在，返回std::nullopt
   * @note 
   *   - 如果多个列具有相同的名称，返回第一个匹配的索引
   *   - 这是GetColIdx的安全版本，不会抛出异常
   */
  auto TryGetColIdx(const std::string &col_name) const -> std::optional<uint32_t> {
    for (uint32_t i = 0; i < columns_.size(); ++i) {
      if (columns_[i].GetName() == col_name) {
        return std::optional{i};
      }
    }
    return std::nullopt;
  }

  /**
   * @brief 获取所有非内联列的索引列表
   * @return 非内联列索引的常量引用
   * @note 非内联列（如VARCHAR）的数据存储在元组外部，元组中只存储指针
   */
  auto GetUnlinedColumns() const -> const std::vector<uint32_t> & { return uninlined_columns_; }

  /**
   * @brief 获取模式中的列数量
   * @return 元组中列的数量
   */
  auto GetColumnCount() const -> uint32_t { return static_cast<uint32_t>(columns_.size()); }

  /**
   * @brief 获取非内联列的数量
   * @return 非内联列的数量
   */
  auto GetUnlinedColumnCount() const -> uint32_t { return static_cast<uint32_t>(uninlined_columns_.size()); }

  /**
   * @brief 获取一个元组使用的内联存储字节数
   * @return 元组的内联部分占用的字节数
   * @note 不包括非内联列的实际数据大小，只包括内联数据和指向非内联数据的指针
   */
  inline auto GetInlinedStorageSize() const -> uint32_t { return length_; }

  /**
   * @brief 判断所有列是否都是内联的
   * @return true 如果所有列都是内联的，false 如果存在非内联列
   */
  inline auto IsInlined() const -> bool { return tuple_is_inlined_; }

  /**
   * @brief 获取模式的字符串表示
   * @param simplified 如果为true，返回简化格式；如果为false，返回详细格式
   * @return 模式的字符串表示
   */
  auto ToString(bool simplified = true) const -> std::string;

  /**
   * @brief 向模式中追加列
   * @param app 要追加的列向量
   * @note 用于合并多个模式的列（如JOIN操作）
   */
  void Append(std::vector<Column> app) { columns_.insert(columns_.end(), app.begin(), app.end()); }

  /**
   * @brief 获取元组的物理大小（包括内联数据和非内联数据的实际大小）
   * @return 元组的物理大小（字节数）
   * @note 物理大小包括所有内联列的大小、指向非内联列的指针大小，以及非内联列的实际数据大小
   */
  auto GetPhysicalSize() const -> uint32_t;

  /**
   * @brief 设置元组的物理大小
   * @note 根据当前列的信息计算并设置physical_size_
   */
  void SetPhysicalSize();

 private:
  /**
   * @brief 定长列的大小，即一个元组使用的字节数（内联部分）
   * @note 这是元组内联部分的长度，不包括非内联列的实际数据大小
   */
  uint32_t length_;

  /**
   * @brief 元组的物理大小（包括内联数据和非内联数据的实际大小）
   * @note 物理大小 = 内联部分大小 + 非内联列的指针大小 + 非内联列的实际数据大小
   */
  uint32_t physical_size_{0};

  /**
   * @brief 模式中的所有列（包括内联和非内联列）
   * @note 列的顺序与元组中数据的布局顺序一致
   */
  std::vector<Column> columns_;

  /**
   * @brief 如果所有列都是内联的则为true，否则为false
   * @note 如果存在任何非内联列（如VARCHAR），此标志为false
   */
  bool tuple_is_inlined_{true};

  /**
   * @brief 所有非内联列的索引列表
   * @note 用于快速访问非内联列，这些列的数据存储在元组外部
   */
  std::vector<uint32_t> uninlined_columns_;
};

}  // namespace easydb

// template <typename T>
// struct fmt::formatter<T, std::enable_if_t<std::is_base_of<easydb::Schema, T>::value, char>>
//     : fmt::formatter<std::string> {
//   template <typename FormatCtx>
//   auto format(const easydb::Schema &x, FormatCtx &ctx) const {
//     return fmt::formatter<std::string>::format(x.ToString(), ctx);
//   }
// };

// template <typename T>
// struct fmt::formatter<std::shared_ptr<T>, std::enable_if_t<std::is_base_of<easydb::Schema, T>::value, char>>
//     : fmt::formatter<std::string> {
//   template <typename FormatCtx>
//   auto format(const std::shared_ptr<T> &x, FormatCtx &ctx) const {
//     if (x != nullptr) {
//       return fmt::formatter<std::string>::format(x->ToString(), ctx);
//     }
//     return fmt::formatter<std::string>::format("", ctx);
//   }
// };

// template <typename T>
// struct fmt::formatter<std::unique_ptr<T>, std::enable_if_t<std::is_base_of<easydb::Schema, T>::value, char>>
//     : fmt::formatter<std::string> {
//   template <typename FormatCtx>
//   auto format(const std::unique_ptr<T> &x, FormatCtx &ctx) const {
//     if (x != nullptr) {
//       return fmt::formatter<std::string>::format(x->ToString(), ctx);
//     }
//     return fmt::formatter<std::string>::format("", ctx);
//   }
// };
