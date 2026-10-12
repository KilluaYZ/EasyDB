#pragma once

#include <cstdint>
#include <functional>
#include <optional>
#include <vector>

#include "common/config.h"
#include "storage/page/page.h"
#include "storage/easystore/table/tuple.h"

namespace easydb {

struct ClusteredLeafEntry {
  std::vector<char> key;
  TupleMeta meta;
  std::vector<char> tuple;
};

/** Page-specific header. Page::SIZE_PAGE_HEADER precedes this structure. */
struct ClusteredLeafPageHeader {
  uint32_t magic;
  uint16_t version;
  uint16_t slot_count;
  uint16_t free_start;
  uint16_t free_end;
  page_id_t prev_leaf;
  page_id_t next_leaf;
};

static constexpr uint32_t CLUSTERED_LEAF_PAGE_MAGIC = 0x434C5052;  // CLPR
static constexpr uint16_t CLUSTERED_LEAF_PAGE_VERSION = 2;

/** One slot points at a key+tuple payload in the page. */
struct ClusteredLeafSlot {
  uint16_t offset;
  uint16_t key_size;
  uint16_t tuple_size;
  uint16_t reserved;
};

static_assert(sizeof(ClusteredLeafPageHeader) == 20);
static_assert(sizeof(ClusteredLeafSlot) == 8);

/** Slotted page for records stored directly in clustered-index leaves. */
class ClusteredLeafPage {
 public:
  using KeyComparator = std::function<int(const char *, const char *)>;

  static void Init(Page *page, page_id_t prev_leaf = INVALID_PAGE_ID,
                   page_id_t next_leaf = INVALID_PAGE_ID);
  static auto IsValid(const Page *page) -> bool;

  explicit ClusteredLeafPage(Page *page) : page_(page) {}

  auto GetSize() const -> uint16_t;
  auto GetFreeSpace() const -> uint16_t;
  auto GetPrevLeaf() const -> page_id_t;
  auto GetNextLeaf() const -> page_id_t;
  void SetPrevLeaf(page_id_t page_id);
  void SetNextLeaf(page_id_t page_id);

  auto GetEntry(uint16_t slot) const -> ClusteredLeafEntry;
  auto LowerBound(const std::vector<char> &key,
                  const KeyComparator &compare) const -> uint16_t;

  /** Insert before slot. Passing slot == GetSize() appends the entry. */
  auto Insert(uint16_t slot, const std::vector<char> &key,
              const std::vector<char> &tuple,
              const TupleMeta &meta = TupleMeta{}) -> bool;
  auto Erase(uint16_t slot) -> bool;

  /** Rebuild the page after a variable-length tuple update. */
  auto Update(uint16_t slot, const std::vector<char> &tuple) -> bool;
  auto Update(uint16_t slot, const std::vector<char> &tuple,
              const TupleMeta &meta) -> bool;

  // Used by the clustered-tree split path after entries have been
  // repartitioned.
  auto RebuildFromEntries(const std::vector<ClusteredLeafEntry> &entries)
      -> bool;

 private:
  auto Header() -> ClusteredLeafPageHeader *;
  auto Header() const -> const ClusteredLeafPageHeader *;
  auto Slots() -> ClusteredLeafSlot *;
  auto Slots() const -> const ClusteredLeafSlot *;
  auto ReadEntries() const -> std::vector<ClusteredLeafEntry>;
  auto Rebuild(const std::vector<ClusteredLeafEntry> &entries) -> bool;

  Page *page_;
};

}  // namespace easydb
