/*
 * Catalog types shared by the compute layer and storage implementations.
 */
#pragma once

#include <string>

#include "defs.h"

namespace easydb {

/**
 * @brief Column definition used by the storage catalog.
 *
 * This type intentionally lives outside SmManager so that clients of the
 * storage-engine interface do not need to include the concrete manager.
 */
struct ColDef {
  std::string name;
  ColType type;
  int len;
};

}  // namespace easydb
