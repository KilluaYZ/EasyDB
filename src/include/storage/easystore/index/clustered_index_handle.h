#pragma once

#include <functional>
#include <memory>
#include <optional>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "storage/easystore/index/clustered_leaf_page.h"

namespace easydb {

/** A first, deliberately small clustered B+ tree implementation.
 *
 * The leaf pages contain key+tuple bytes.  The first implementation supports
 * a leaf root and a one-level internal root; this gives the new storage layout
 * a usable insertion/lookup path for clustered table storage.
 */
class ClusteredIndexHandle {
 public:
  using KeyComparator = ClusteredLeafPage::KeyComparator;

  ClusteredIndexHandle(int fd, BufferPoolManager *buffer_pool_manager,
                       uint16_t key_size, KeyComparator comparator);

  static auto Initialize(int fd, DiskManager *disk_manager, uint16_t key_size)
      -> bool;

  auto Insert(const std::vector<char> &key, const std::vector<char> &tuple)
      -> bool;
  auto Insert(const std::vector<char> &key, const std::vector<char> &tuple,
              const TupleMeta &meta) -> bool;
  auto Get(const std::vector<char> &key) -> std::optional<std::vector<char>>;
  auto GetRecord(const std::vector<char> &key)
      -> std::optional<ClusteredLeafEntry>;
  auto Update(const std::vector<char> &key, const std::vector<char> &tuple)
      -> bool;
  auto Update(const std::vector<char> &key, const std::vector<char> &tuple,
              const TupleMeta &meta) -> bool;
  auto Delete(const std::vector<char> &key) -> bool;
  auto ScanAll() -> std::vector<ClusteredLeafEntry>;

  void Flush();
  auto GetRootPage() const -> page_id_t { return root_page_; }
  auto GetFirstLeaf() const -> page_id_t { return first_leaf_; }

 private:
  auto FindLeaf(const std::vector<char> &key, page_id_t *leaf_page,
                uint16_t *child_index) -> bool;
  auto InsertIntoRoot(page_id_t left_page, page_id_t right_page,
                      const std::vector<char> &separator) -> bool;
  auto InsertIntoInternalRoot(page_id_t right_page,
                              const std::vector<char> &separator,
                              uint16_t child_index) -> bool;
  auto RefreshRootSeparators() -> bool;
  auto UpdateFileHeader() -> bool;

  int fd_;
  BufferPoolManager *buffer_pool_manager_;
  uint16_t key_size_;
  KeyComparator comparator_;
  page_id_t root_page_{INVALID_PAGE_ID};
  page_id_t first_leaf_{INVALID_PAGE_ID};
  page_id_t last_leaf_{INVALID_PAGE_ID};
  bool root_is_leaf_{true};
};

}  // namespace easydb
