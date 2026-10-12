#include "storage/easystore/index/clustered_index_handle.h"

#include <algorithm>
#include <cstring>

namespace easydb {

namespace {
constexpr uint32_t FILE_MAGIC = 0x434C4944;  // CLID
constexpr uint16_t FILE_VERSION = 1;
constexpr uint32_t INTERNAL_MAGIC = 0x434C494E;  // CLIN
constexpr size_t PAGE_HEADER_OFFSET = Page::OFFSET_PAGE_HDR;

struct FileHeader {
  uint32_t magic;
  uint16_t version;
  uint16_t key_size;
  page_id_t root_page;
  page_id_t first_leaf;
  page_id_t last_leaf;
  uint8_t root_is_leaf;
  uint8_t reserved[3];
};

struct InternalHeader {
  uint32_t magic;
  uint16_t version;
  uint16_t key_count;
};

static_assert(sizeof(FileHeader) == 24);
static_assert(sizeof(InternalHeader) == 8);

auto InternalDataBegin() -> size_t {
  return PAGE_HEADER_OFFSET + sizeof(InternalHeader);
}

auto Children(const Page *page) -> const page_id_t * {
  return reinterpret_cast<const page_id_t *>(page->GetData() +
                                             InternalDataBegin());
}

auto MutableChildren(Page *page) -> page_id_t * {
  return reinterpret_cast<page_id_t *>(page->GetData() + InternalDataBegin());
}

auto Separator(const Page *page, uint16_t key_count, uint16_t key_size,
               uint16_t index) -> const char * {
  const auto offset = InternalDataBegin() +
                      (static_cast<size_t>(key_count) + 1) * sizeof(page_id_t) +
                      static_cast<size_t>(index) * key_size;
  return page->GetData() + offset;
}

auto MutableSeparator(Page *page, uint16_t key_count, uint16_t key_size,
                      uint16_t index) -> char * {
  const auto offset = InternalDataBegin() +
                      (static_cast<size_t>(key_count) + 1) * sizeof(page_id_t) +
                      static_cast<size_t>(index) * key_size;
  return page->GetData() + offset;
}

auto InternalCapacity(uint16_t key_size) -> uint16_t {
  const size_t fixed = Page::OFFSET_PAGE_HDR + sizeof(InternalHeader);
  if (key_size == 0 || fixed + sizeof(page_id_t) > PAGE_SIZE) return 0;
  return static_cast<uint16_t>((PAGE_SIZE - fixed - sizeof(page_id_t)) /
                               (sizeof(page_id_t) + key_size));
}
}  // namespace

ClusteredIndexHandle::ClusteredIndexHandle(
    int fd, BufferPoolManager *buffer_pool_manager, uint16_t key_size,
    KeyComparator comparator)
    : fd_(fd),
      buffer_pool_manager_(buffer_pool_manager),
      key_size_(key_size),
      comparator_(std::move(comparator)) {
  const auto file_size =
      buffer_pool_manager_->GetDiskManager()->GetFileSize(
          buffer_pool_manager_->GetDiskManager()->GetFileName(fd_));
  const auto page_count =
      static_cast<page_id_t>((file_size + PAGE_SIZE - 1) / PAGE_SIZE);
  buffer_pool_manager_->GetDiskManager()->SetFd2Pageno(fd_, page_count);
  PageId page_id{fd_, 0};
  auto *page = buffer_pool_manager_->FetchPage(page_id);
  if (page == nullptr) return;
  const auto *header = reinterpret_cast<const FileHeader *>(
      page->GetData() + PAGE_HEADER_OFFSET);
  if (header->magic == FILE_MAGIC && header->version == FILE_VERSION) {
    key_size_ = header->key_size;
    root_page_ = header->root_page;
    first_leaf_ = header->first_leaf;
    last_leaf_ = header->last_leaf;
    root_is_leaf_ = header->root_is_leaf != 0;
  }
  buffer_pool_manager_->UnpinPage(page_id, false);
}

auto ClusteredIndexHandle::Initialize(int fd, DiskManager *disk_manager,
                                      uint16_t key_size) -> bool {
  if (disk_manager == nullptr || key_size == 0) return false;
  Page page;
  std::memset(page.GetData(), 0, PAGE_SIZE);
  auto *file_header = reinterpret_cast<FileHeader *>(
      page.GetData() + PAGE_HEADER_OFFSET);
  file_header->magic = FILE_MAGIC;
  file_header->version = FILE_VERSION;
  file_header->key_size = key_size;
  file_header->root_page = 1;
  file_header->first_leaf = 1;
  file_header->last_leaf = 1;
  file_header->root_is_leaf = 1;
  disk_manager->WritePage(fd, 0, page.GetData(), PAGE_SIZE);
  ClusteredLeafPage::Init(&page);
  disk_manager->WritePage(fd, 1, page.GetData(), PAGE_SIZE);
  disk_manager->SetFd2Pageno(fd, 2);
  return true;
}

auto ClusteredIndexHandle::UpdateFileHeader() -> bool {
  PageId page_id{fd_, 0};
  auto *page = buffer_pool_manager_->FetchPage(page_id);
  if (page == nullptr) return false;
  auto *header = reinterpret_cast<FileHeader *>(page->GetData() +
                                                 PAGE_HEADER_OFFSET);
  header->magic = FILE_MAGIC;
  header->version = FILE_VERSION;
  header->key_size = key_size_;
  header->root_page = root_page_;
  header->first_leaf = first_leaf_;
  header->last_leaf = last_leaf_;
  header->root_is_leaf = root_is_leaf_ ? 1 : 0;
  buffer_pool_manager_->UnpinPage(page_id, true);
  return true;
}

auto ClusteredIndexHandle::FindLeaf(const std::vector<char> &key,
                                    page_id_t *leaf_page,
                                    uint16_t *child_index) -> bool {
  if (key.size() != key_size_ || root_page_ == INVALID_PAGE_ID) return false;
  if (root_is_leaf_) {
    *leaf_page = root_page_;
    *child_index = 0;
    return true;
  }
  PageId page_id{fd_, root_page_};
  auto *page = buffer_pool_manager_->FetchPage(page_id);
  if (page == nullptr) return false;
  const auto *header = reinterpret_cast<const InternalHeader *>(
      page->GetData() + PAGE_HEADER_OFFSET);
  if (header->magic != INTERNAL_MAGIC || header->version != FILE_VERSION) {
    buffer_pool_manager_->UnpinPage(page_id, false);
    return false;
  }
  uint16_t left = 0;
  uint16_t right = header->key_count;
  while (left < right) {
    const auto middle = static_cast<uint16_t>(left + (right - left) / 2);
    if (comparator_(Separator(page, header->key_count, key_size_, middle),
                    key.data()) <= 0) {
      left = static_cast<uint16_t>(middle + 1);
    } else {
      right = middle;
    }
  }
  *child_index = left;
  *leaf_page = Children(page)[left];
  buffer_pool_manager_->UnpinPage(page_id, false);
  return true;
}

auto ClusteredIndexHandle::Get(const std::vector<char> &key)
    -> std::optional<std::vector<char>> {
  page_id_t leaf_page = INVALID_PAGE_ID;
  uint16_t ignored = 0;
  if (!FindLeaf(key, &leaf_page, &ignored)) return std::nullopt;
  PageId page_id{fd_, leaf_page};
  auto *page = buffer_pool_manager_->FetchPage(page_id);
  if (page == nullptr) return std::nullopt;
  ClusteredLeafPage leaf(page);
  const auto slot = leaf.LowerBound(key, comparator_);
  std::optional<std::vector<char>> result;
  if (slot < leaf.GetSize()) {
    auto entry = leaf.GetEntry(slot);
    if (comparator_(entry.key.data(), key.data()) == 0) result = entry.tuple;
  }
  buffer_pool_manager_->UnpinPage(page_id, false);
  return result;
}

auto ClusteredIndexHandle::Insert(const std::vector<char> &key,
                                  const std::vector<char> &tuple) -> bool {
  if (key.size() != key_size_ || root_page_ == INVALID_PAGE_ID) return false;
  page_id_t leaf_page = INVALID_PAGE_ID;
  uint16_t child_index = 0;
  if (!FindLeaf(key, &leaf_page, &child_index)) return false;
  PageId page_id{fd_, leaf_page};
  auto *page = buffer_pool_manager_->FetchPage(page_id);
  if (page == nullptr) return false;
  ClusteredLeafPage leaf(page);
  const auto slot = leaf.LowerBound(key, comparator_);
  if (slot < leaf.GetSize() &&
      comparator_(leaf.GetEntry(slot).key.data(), key.data()) == 0) {
    buffer_pool_manager_->UnpinPage(page_id, false);
    return false;
  }
  if (leaf.Insert(slot, key, tuple)) {
    buffer_pool_manager_->UnpinPage(page_id, true);
    return true;
  }

  std::vector<ClusteredLeafEntry> entries;
  entries.reserve(leaf.GetSize() + 1);
  for (uint16_t i = 0; i < leaf.GetSize(); ++i) entries.push_back(leaf.GetEntry(i));
  entries.insert(entries.begin() + slot, {key, tuple});
  const auto middle = entries.size() / 2;
  std::vector<ClusteredLeafEntry> left(entries.begin(), entries.begin() + middle);
  std::vector<ClusteredLeafEntry> right(entries.begin() + middle, entries.end());
  PageId right_id{fd_};
  auto *right_page = buffer_pool_manager_->NewPage(&right_id);
  if (right_page == nullptr || !leaf.RebuildFromEntries(left)) {
    if (right_page != nullptr) buffer_pool_manager_->UnpinPage(right_id, false);
    buffer_pool_manager_->UnpinPage(page_id, false);
    return false;
  }
  ClusteredLeafPage::Init(right_page, leaf_page, leaf.GetNextLeaf());
  ClusteredLeafPage right_leaf(right_page);
  const bool right_ok = right_leaf.RebuildFromEntries(right);
  const auto old_next = leaf.GetNextLeaf();
  leaf.SetNextLeaf(right_id.page_no);
  buffer_pool_manager_->UnpinPage(page_id, true);
  buffer_pool_manager_->UnpinPage(right_id, right_ok);
  if (!right_ok) return false;

  if (old_next != INVALID_PAGE_ID) {
    PageId next_id{fd_, old_next};
    auto *next_page = buffer_pool_manager_->FetchPage(next_id);
    if (next_page == nullptr) return false;
    ClusteredLeafPage next_leaf(next_page);
    next_leaf.SetPrevLeaf(right_id.page_no);
    buffer_pool_manager_->UnpinPage(next_id, true);
  } else {
    last_leaf_ = right_id.page_no;
  }
  if (root_is_leaf_) {
    if (!InsertIntoRoot(leaf_page, right_id.page_no, right.front().key)) return false;
  } else if (!InsertIntoInternalRoot(right_id.page_no, right.front().key,
                                     child_index)) {
    return false;
  }
  return UpdateFileHeader();
}

auto ClusteredIndexHandle::InsertIntoRoot(
    page_id_t left_page, page_id_t right_page,
    const std::vector<char> &separator) -> bool {
  PageId root_id{fd_};
  auto *root = buffer_pool_manager_->NewPage(&root_id);
  if (root == nullptr) return false;
  std::memset(root->GetData(), 0, PAGE_SIZE);
  auto *header = reinterpret_cast<InternalHeader *>(root->GetData() +
                                                     PAGE_HEADER_OFFSET);
  header->magic = INTERNAL_MAGIC;
  header->version = FILE_VERSION;
  header->key_count = 1;
  MutableChildren(root)[0] = left_page;
  MutableChildren(root)[1] = right_page;
  std::memcpy(MutableSeparator(root, 1, key_size_, 0), separator.data(), key_size_);
  root_page_ = root_id.page_no;
  root_is_leaf_ = false;
  buffer_pool_manager_->UnpinPage(root_id, true);
  return true;
}

auto ClusteredIndexHandle::InsertIntoInternalRoot(
    page_id_t right_page, const std::vector<char> &separator,
    uint16_t child_index) -> bool {
  PageId root_id{fd_, root_page_};
  auto *root = buffer_pool_manager_->FetchPage(root_id);
  if (root == nullptr) return false;
  auto *header = reinterpret_cast<InternalHeader *>(root->GetData() +
                                                     PAGE_HEADER_OFFSET);
  if (header->key_count >= InternalCapacity(key_size_)) {
    buffer_pool_manager_->UnpinPage(root_id, false);
    return false;
  }
  std::vector<char> separators;
  separators.resize(static_cast<size_t>(header->key_count) * key_size_);
  for (uint16_t i = 0; i < header->key_count; ++i) {
    std::memcpy(separators.data() + static_cast<size_t>(i) * key_size_,
                Separator(root, header->key_count, key_size_, i), key_size_);
  }
  separators.insert(separators.begin() + static_cast<size_t>(child_index) * key_size_,
                   separator.begin(), separator.end());
  auto *children = MutableChildren(root);
  for (int i = header->key_count + 1; i > child_index + 1; --i) {
    children[i] = children[i - 1];
  }
  children[child_index + 1] = right_page;
  ++header->key_count;
  for (uint16_t i = 0; i < header->key_count; ++i) {
    std::memcpy(MutableSeparator(root, header->key_count, key_size_, i),
                separators.data() + static_cast<size_t>(i) * key_size_, key_size_);
  }
  buffer_pool_manager_->UnpinPage(root_id, true);
  return true;
}

void ClusteredIndexHandle::Flush() {
  buffer_pool_manager_->FlushAllPages(fd_);
}

}  // namespace easydb
