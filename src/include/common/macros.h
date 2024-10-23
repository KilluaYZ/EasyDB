/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * macros.h
 *
 * Identification: src/include/common/macros.h
 *
 *-------------------------------------------------------------------------
 */

/*
 * Original copyright:
 * Copyright (c) 2015-2019, Carnegie Mellon University Database Group
 */

#pragma once

#include <cassert>
#include <exception>
#include <stdexcept>
#include <optional>

namespace easydb {

#define EASYDB_ASSERT(expr, message) assert((expr) && (message))

#define UNIMPLEMENTED(message) throw std::logic_error(message)

#define EASYDB_ENSURE(expr, message)                  \
  if (!(expr)) {                                      \
    std::cerr << "ERROR: " << (message) << std::endl; \
    std::terminate();                                 \
  }

#define UNREACHABLE(message) throw std::logic_error(message)

// Macros to disable copying and moving
#define DISALLOW_COPY(cname)     \
  cname(const cname &) = delete; \
  auto operator=(const cname &)->cname & = delete;

#define DISALLOW_MOVE(cname) \
  cname(cname &&) = delete;  \
  auto operator=(cname &&)->cname & = delete;

#define DISALLOW_COPY_AND_MOVE(cname) \
  DISALLOW_COPY(cname);               \
  DISALLOW_MOVE(cname);

}  // namespace easydb
