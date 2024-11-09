/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * rid.h
 *
 * Identification: src/include/common/rid.h
 *
 *-------------------------------------------------------------------------
 */

/*
 * Original copyright:
 * Copyright (c) 2015-2019, Carnegie Mellon University Database Group
 */

#pragma once

#include <cstdint>
#include <sstream>
#include <string>

#include "common/config.h"

namespace easydb {

class RID {
 public:
  /** The default constructor creates an invalid RID! */
  RID() = default;

  /**
   * Creates a new Record Identifier for the given page identifier and slot number.
   * @param page_no page identifier
   * @param slot_no slot number
   */
  RID(page_id_t page_no, uint32_t slot_no) : page_no_(page_no), slot_no_(slot_no) {}

  explicit RID(int64_t rid) : page_no_(static_cast<page_id_t>(rid >> 32)), slot_no_(static_cast<uint32_t>(rid)) {}

  inline auto Get() const -> int64_t { return (static_cast<int64_t>(page_no_)) << 32 | slot_no_; }

  inline auto GetPageId() const -> page_id_t { return page_no_; }

  inline auto GetSlotNum() const -> uint32_t { return slot_no_; }

  inline void Set(page_id_t page_no, uint32_t slot_no) {
    page_no_ = page_no;
    slot_no_ = slot_no;
  }

  inline auto ToString() const -> std::string {
    std::stringstream os;
    os << "page_no: " << page_no_;
    os << " slot_no: " << slot_no_ << "\n";

    return os.str();
  }

  friend auto operator<<(std::ostream &os, const RID &rid) -> std::ostream & {
    os << rid.ToString();
    return os;
  }

  auto operator==(const RID &other) const -> bool { return page_no_ == other.page_no_ && slot_no_ == other.slot_no_; }

 private:
  page_id_t page_no_{INVALID_PAGE_ID};
  uint32_t slot_no_{0};  // logical offset from 0, 1...
};

}  // namespace easydb

namespace std {
template <>
struct hash<easydb::RID> {
  auto operator()(const easydb::RID &obj) const -> size_t { return hash<int64_t>()(obj.Get()); }
};
}  // namespace std
