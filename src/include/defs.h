/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <iostream>
#include <map>

/**
 * @brief 聚合类型枚举
 * @note 用于SELECT查询中的聚合函数
 */
enum AggregationType {
  MAX_AGG,   /**< MAX聚合函数 */
  MIN_AGG,   /**< MIN聚合函数 */
  COUNT_AGG, /**< COUNT聚合函数 */
  SUM_AGG,   /**< SUM聚合函数 */
  NO_AGG     /**< 非聚合列 */
};

/**
 * @brief 为枚举类型重载<<操作符
 * @tparam T 枚举类型
 * @param os 输出流
 * @param enum_val 枚举值
 * @return 输出流的引用
 * @note 此处重载了<<操作符，在ColMeta中进行了调用
 */
template <typename T,
          typename = typename std::enable_if<std::is_enum<T>::value, T>::type>
std::ostream &operator<<(std::ostream &os, const T &enum_val) {
  os << static_cast<int>(enum_val);
  return os;
}

/**
 * @brief 为枚举类型重载>>操作符
 * @tparam T 枚举类型
 * @param is 输入流
 * @param enum_val 枚举值（输出参数）
 * @return 输入流的引用
 */
template <typename T,
          typename = typename std::enable_if<std::is_enum<T>::value, T>::type>
std::istream &operator>>(std::istream &is, T &enum_val) {
  int int_val;
  is >> int_val;
  enum_val = static_cast<T>(int_val);
  return is;
}

/**
 * @brief 列类型枚举
 * @note 定义了系统中支持的所有列数据类型
 */
enum ColType {
  TYPE_INT,     /**< 32位整数类型 */
  TYPE_LONG,    /**< 64位整数类型 */
  TYPE_FLOAT,   /**< 单精度浮点数类型 */
  TYPE_DOUBLE,  /**< 双精度浮点数类型 */
  TYPE_VARCHAR, /**< 变长字符串类型 */
  TYPE_CHAR,    /**< 定长字符串类型 */
  TYPE_DATE,    /**< 日期类型 */
  TYPE_EMPTY    /**< 空类型 */
};

/**
 * @brief 将列类型转换为字符串
 * @param type 列类型枚举值
 * @return 列类型的字符串表示
 */
inline std::string coltype2str(ColType type) {
  std::map<ColType, std::string> m = {
      {TYPE_INT, "INT"},       {TYPE_LONG, "LONG"},       {TYPE_FLOAT, "FLOAT"},
      {TYPE_DOUBLE, "DOUBLE"}, {TYPE_VARCHAR, "VARCHAR"}, {TYPE_CHAR, "CHAR"},
      {TYPE_DATE, "DATE"},     {TYPE_EMPTY, "EMPTY"},
  };
  return m.at(type);
}
