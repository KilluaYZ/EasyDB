#include "storage/easystore/index/clustered_leaf_page.h"

#include <cstring>

namespace easydb {

namespace {
constexpr size_t PAGE_HEADER_OFFSET = Page::OFFSET_PAGE_HDR;

auto DataBegin() -> size_t {
  return PAGE_HEADER_OFFSET + sizeof(ClusteredLeafPageHeader);
}
}  // namespace

void ClusteredLeafPage::Init(Page *page, page_id_t prev_leaf,
                             page_id_t next_leaf) {
  std::memset(page->GetData(), 0, PAGE_SIZE);
  page->SetLSN(INVALID_LSN);
  auto *header = reinterpret_cast<ClusteredLeafPageHeader *>(
      page->GetData() + PAGE_HEADER_OFFSET);
  header->magic = CLUSTERED_LEAF_PAGE_MAGIC;
  header->version = CLUSTERED_LEAF_PAGE_VERSION;
  header->free_start = static_cast<uint16_t>(DataBegin());
  header->free_end = PAGE_SIZE;
  header->prev_leaf = prev_leaf;
  header->next_leaf = next_leaf;
}

auto ClusteredLeafPage::IsValid(const Page *page) -> bool {
  const auto *header = reinterpret_cast<const ClusteredLeafPageHeader *>(
      page->GetData() + PAGE_HEADER_OFFSET);
  return header->magic == CLUSTERED_LEAF_PAGE_MAGIC &&
         header->version == CLUSTERED_LEAF_PAGE_VERSION &&
         header->free_start >= DataBegin() && header->free_end <= PAGE_SIZE &&
         header->free_start <= header->free_end;
}

auto ClusteredLeafPage::Header() -> ClusteredLeafPageHeader * {
  return reinterpret_cast<ClusteredLeafPageHeader *>(page_->GetData() +
                                                     PAGE_HEADER_OFFSET);
}

auto ClusteredLeafPage::Header() const -> const ClusteredLeafPageHeader * {
  return reinterpret_cast<const ClusteredLeafPageHeader *>(page_->GetData() +
                                                           PAGE_HEADER_OFFSET);
}

auto ClusteredLeafPage::Slots() -> ClusteredLeafSlot * {
  return reinterpret_cast<ClusteredLeafSlot *>(page_->GetData() + DataBegin());
}

auto ClusteredLeafPage::Slots() const -> const ClusteredLeafSlot * {
  return reinterpret_cast<const ClusteredLeafSlot *>(page_->GetData() +
                                                     DataBegin());
}

auto ClusteredLeafPage::GetSize() const -> uint16_t {
  return Header()->slot_count;
}

auto ClusteredLeafPage::GetFreeSpace() const -> uint16_t {
  return Header()->free_end - Header()->free_start;
}

auto ClusteredLeafPage::GetPrevLeaf() const -> page_id_t {
  return Header()->prev_leaf;
}

auto ClusteredLeafPage::GetNextLeaf() const -> page_id_t {
  return Header()->next_leaf;
}

void ClusteredLeafPage::SetPrevLeaf(page_id_t page_id) {
  Header()->prev_leaf = page_id;
}

void ClusteredLeafPage::SetNextLeaf(page_id_t page_id) {
  Header()->next_leaf = page_id;
}

auto ClusteredLeafPage::GetEntry(uint16_t slot) const -> ClusteredLeafEntry {
  if (slot >= GetSize()) return {};
  const auto &entry = Slots()[slot];
  ClusteredLeafEntry result;
  result.key.resize(entry.key_size);
  result.tuple.resize(entry.tuple_size);
  const char *payload = page_->GetData() + entry.offset;
  std::memcpy(result.key.data(), payload, entry.key_size);
  std::memcpy(result.tuple.data(), payload + entry.key_size, entry.tuple_size);
  return result;
}

auto ClusteredLeafPage::ReadEntries() const -> std::vector<ClusteredLeafEntry> {
  std::vector<ClusteredLeafEntry> entries;
  entries.reserve(GetSize());
  for (uint16_t i = 0; i < GetSize(); ++i) {
    entries.push_back(GetEntry(i));
  }
  return entries;
}

auto ClusteredLeafPage::LowerBound(const std::vector<char> &key,
                                   const KeyComparator &compare) const
    -> uint16_t {
  const auto entries = ReadEntries();
  uint16_t left = 0;
  uint16_t right = GetSize();
  while (left < right) {
    uint16_t middle = static_cast<uint16_t>(left + (right - left) / 2);
    if (compare(entries[middle].key.data(), key.data()) < 0) {
      left = static_cast<uint16_t>(middle + 1);
    } else {
      right = middle;
    }
  }
  return left;
}

auto ClusteredLeafPage::Rebuild(const std::vector<ClusteredLeafEntry> &entries)
    -> bool {
  size_t required_slots =
      DataBegin() + entries.size() * sizeof(ClusteredLeafSlot);
  size_t payload_size = 0;
  for (const auto &entry : entries) {
    if (entry.key.size() > UINT16_MAX || entry.tuple.size() > UINT16_MAX ||
        entry.key.size() + entry.tuple.size() > UINT16_MAX) {
      return false;
    }
    payload_size += entry.key.size() + entry.tuple.size();
  }
  if (entries.size() > UINT16_MAX ||
      required_slots + payload_size > PAGE_SIZE) {
    return false;
  }

  const auto prev_leaf = Header()->prev_leaf;
  const auto next_leaf = Header()->next_leaf;
  const auto old_lsn = page_->GetLSN();
  std::memset(page_->GetData() + PAGE_HEADER_OFFSET, 0,
              PAGE_SIZE - PAGE_HEADER_OFFSET);
  auto *header = Header();
  header->magic = CLUSTERED_LEAF_PAGE_MAGIC;
  header->version = CLUSTERED_LEAF_PAGE_VERSION;
  header->slot_count = static_cast<uint16_t>(entries.size());
  header->free_start = static_cast<uint16_t>(required_slots);
  header->free_end = PAGE_SIZE;
  header->prev_leaf = prev_leaf;
  header->next_leaf = next_leaf;

  auto *slots = Slots();
  for (size_t i = 0; i < entries.size(); ++i) {
    const auto payload_size_for_entry =
        entries[i].key.size() + entries[i].tuple.size();
    header->free_end =
        static_cast<uint16_t>(header->free_end - payload_size_for_entry);
    slots[i] = {
        header->free_end,
        static_cast<uint16_t>(entries[i].key.size()),
        static_cast<uint16_t>(entries[i].tuple.size()),
        0,
    };
    char *payload = page_->GetData() + header->free_end;
    std::memcpy(payload, entries[i].key.data(), entries[i].key.size());
    std::memcpy(payload + entries[i].key.size(), entries[i].tuple.data(),
                entries[i].tuple.size());
  }
  page_->SetLSN(old_lsn);
  return true;
}

auto ClusteredLeafPage::Insert(uint16_t slot, const std::vector<char> &key,
                               const std::vector<char> &tuple) -> bool {
  if (slot > GetSize()) return false;
  auto entries = ReadEntries();
  entries.insert(entries.begin() + slot, ClusteredLeafEntry{key, tuple});
  return Rebuild(entries);
}

auto ClusteredLeafPage::Erase(uint16_t slot) -> bool {
  if (slot >= GetSize()) return false;
  auto entries = ReadEntries();
  entries.erase(entries.begin() + slot);
  return Rebuild(entries);
}

auto ClusteredLeafPage::Update(uint16_t slot, const std::vector<char> &tuple)
    -> bool {
  if (slot >= GetSize()) return false;
  auto entries = ReadEntries();
  entries[slot].tuple = tuple;
  return Rebuild(entries);
}

auto ClusteredLeafPage::RebuildFromEntries(
    const std::vector<ClusteredLeafEntry> &entries) -> bool {
  return Rebuild(entries);
}

}  // namespace easydb
