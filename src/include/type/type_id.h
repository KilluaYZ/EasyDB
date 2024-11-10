/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * type_id.h
 *
 * Identification: src/include/type/type_id.h
 *
 *-------------------------------------------------------------------------
 */

/*
 * Original copyright:
 * Copyright (c) 2015-2019, Carnegie Mellon University Database Group
 */

#pragma once
#include <map>
#include <string>

namespace easydb {
// Every possible SQL type ID
// enum TypeId { INVALID = 0, BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, DECIMAL, VARCHAR, TIMESTAMP, VECTOR };
//                   |                                    /        /       /   +  |
//    equal to { TYPE_EMPTY,      ,      ,     , TYPE_INT, TYPE_LONG, TYPE_FLOAT, TYPE_DOUBLE, TYPE_VARCHAR, TYPE_DATE,
//    };

enum ColType { TYPE_EMPTY, TYPE_INT, TYPE_LONG, TYPE_FLOAT, TYPE_DOUBLE, TYPE_CHAR, TYPE_VARCHAR, TYPE_DATE };

using TypeId = ColType;

inline std::string coltype2str(ColType type) {
  std::map<ColType, std::string> m = {
      {TYPE_EMPTY, "EMPTY"},   {TYPE_INT, "INT"},   {TYPE_LONG, "LONG"},       {TYPE_FLOAT, "FLOAT"},
      {TYPE_DOUBLE, "DOUBLE"}, {TYPE_CHAR, "CHAR"}, {TYPE_VARCHAR, "VARCHAR"}, {TYPE_DATE, "DATE"},
  };
  return m.at(type);
}

}  // namespace easydb
