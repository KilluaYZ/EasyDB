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
enum AggregationType { MAX_AGG, MIN_AGG, COUNT_AGG, SUM_AGG, NO_AGG };
// 此处重载了<<操作符，在ColMeta中进行了调用
template <typename T, typename = typename std::enable_if<std::is_enum<T>::value, T>::type>
std::ostream &operator<<(std::ostream &os, const T &enum_val) {
  os << static_cast<int>(enum_val);
  return os;
}

template <typename T, typename = typename std::enable_if<std::is_enum<T>::value, T>::type>
std::istream &operator>>(std::istream &is, T &enum_val) {
  int int_val;
  is >> int_val;
  enum_val = static_cast<T>(int_val);
  return is;
}

enum ColType { TYPE_INT, TYPE_LONG, TYPE_FLOAT, TYPE_DOUBLE, TYPE_VARCHAR, TYPE_CHAR, TYPE_DATE, TYPE_EMPTY };

inline std::string coltype2str(ColType type) {
  std::map<ColType, std::string> m = {
      {TYPE_INT, "INT"},         {TYPE_LONG, "LONG"}, {TYPE_FLOAT, "FLOAT"}, {TYPE_DOUBLE, "DOUBLE"},
      {TYPE_VARCHAR, "VARCHAR"}, {TYPE_CHAR, "CHAR"}, {TYPE_DATE, "DATE"},   {TYPE_EMPTY, "EMPTY"},
  };
  return m.at(type);
}
