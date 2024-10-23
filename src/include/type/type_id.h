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

namespace easydb {
// Every possible SQL type ID
enum TypeId { INVALID = 0, BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, DECIMAL, VARCHAR, TIMESTAMP, VECTOR };
}  // namespace easydb
