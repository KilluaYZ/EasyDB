#pragma once

#include <cstdint>
#include <optional>
#include <utility>
#include <vector>

#include "catalog/schema.h"
#include "storage/easystore/index/clustered_index_handle.h"
#include "storage/easystore/table/tuple.h"

namespace easydb {

/** Table-facing adapter for the clustered record layout.
 *
 * It deliberately owns no file descriptor. File creation/opening remains a
 * catalog/table-manager concern; this class only translates Tuple objects to
 * the key+tuple byte representation used by ClusteredIndexHandle.
 */
class ClusteredTableHandle {
 public:
  ClusteredTableHandle(ClusteredIndexHandle *index, const Schema *schema,
                       const Schema *key_schema,
                       std::vector<uint32_t> key_attrs)
      : index_(index),
        schema_(schema),
        key_schema_(key_schema),
        key_attrs_(std::move(key_attrs)) {}

  auto Insert(const Tuple &tuple) -> bool;
  auto Get(const Tuple &key_tuple) -> std::optional<Tuple>;
  auto Update(const Tuple &key_tuple, const Tuple &tuple) -> bool;
  auto Delete(const Tuple &key_tuple) -> bool;
  auto Scan() -> std::vector<Tuple>;

 private:
  auto MakeKey(const Tuple &tuple) const -> std::vector<char>;

  ClusteredIndexHandle *index_;
  const Schema *schema_;
  const Schema *key_schema_;
  std::vector<uint32_t> key_attrs_;
};

}  // namespace easydb
