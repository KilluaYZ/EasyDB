/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * type_util.h
 *
 * Identification: src/include/type/type_util.h
 *
 *-------------------------------------------------------------------------
 */

/*
 * Original copyright:
 * Copyright (c) 2015-2019, Carnegie Mellon University Database Group
 */

#pragma once

#include <algorithm>
#include <cassert>
#include <cstring>

#include "type/type.h"

namespace easydb {
/**
 * Type Utility Functions
 */
class TypeUtil {
 public:
  /**
   * Use memcmp to evaluate two strings
   * This does not work with VARBINARY attributes.
   */
  static inline auto CompareStrings(const char *str1, int len1, const char *str2, int len2) -> int {
    assert(str1 != nullptr);
    assert(len1 >= 0);
    assert(str2 != nullptr);
    assert(len2 >= 0);
    // PAVLO: 2017-04-04
    // The reason why we use memcmp here is that our inputs are
    // not null-terminated strings, so we can't use strncmp
    int ret = memcmp(str1, str2, static_cast<size_t>(std::min(len1, len2)));
    if (ret == 0 && len1 != len2) {
      ret = len1 - len2;
    }
    return ret;
  }
};

}  // namespace easydb