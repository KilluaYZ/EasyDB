#include "storage/easystore/table/clustered_table_manager.h"

#include <cstring>

#include "common/exception.h"

namespace easydb {

namespace {
template <typename T>
auto CompareNumber(const char *lhs, const char *rhs) -> int {
  T left;
  T right;
  std::memcpy(&left, lhs, sizeof(T));
  std::memcpy(&right, rhs, sizeof(T));
  return (left > right) - (left < right);
}
}  // namespace

auto ClusteredTableManager::FileName(const std::string &table_name) const
    -> std::string {
  return table_name + ".clustered";
}

auto ClusteredTableManager::MakeComparator(const Column &column)
    -> ClusteredIndexHandle::KeyComparator {
  switch (column.GetType()) {
    case TYPE_INT:
      return CompareNumber<int32_t>;
    case TYPE_LONG:
      return CompareNumber<int64_t>;
    case TYPE_FLOAT:
      return CompareNumber<float>;
    case TYPE_DOUBLE:
      return CompareNumber<double>;
    default:
      return
          [size = column.GetStorageSize()](const char *lhs, const char *rhs) {
            return std::memcmp(lhs, rhs, size);
          };
  }
}

void ClusteredTableManager::Create(const std::string &table_name,
                                   const Schema &schema) {
  if (schema.GetColumns().empty()) {
    throw Exception("clustered table requires a primary key column");
  }
  const auto path = FileName(table_name);
  disk_manager_->CreateFile(path);
  const auto fd = disk_manager_->OpenFile(path);
  if (!ClusteredIndexHandle::Initialize(fd, disk_manager_,
                                        schema.GetColumn(0).GetStorageSize())) {
    disk_manager_->CloseFile(fd);
    throw Exception("failed to initialize clustered table");
  }
  disk_manager_->CloseFile(fd);
}

void ClusteredTableManager::Open(const std::string &table_name,
                                 const Schema &schema) {
  if (schema.GetColumns().empty()) {
    throw Exception("clustered table requires a primary key column");
  }
  auto entry = std::make_unique<Entry>();
  entry->fd = disk_manager_->OpenFile(FileName(table_name));
  entry->schema = schema;
  entry->key_schema = Schema::CopySchema(&entry->schema, {0});
  entry->index = std::make_unique<ClusteredIndexHandle>(
      entry->fd, buffer_pool_manager_,
      static_cast<uint16_t>(entry->schema.GetColumn(0).GetStorageSize()),
      MakeComparator(entry->schema.GetColumn(0)));
  entry->table = std::make_unique<ClusteredTableHandle>(
      entry->index.get(), &entry->schema, &entry->key_schema,
      std::vector<uint32_t>{0});
  entries_[table_name] = std::move(entry);
}

void ClusteredTableManager::Close(const std::string &table_name) {
  auto it = entries_.find(table_name);
  if (it == entries_.end()) return;
  it->second->index->Flush();
  disk_manager_->CloseFile(it->second->fd);
  entries_.erase(it);
}

void ClusteredTableManager::Destroy(const std::string &table_name) {
  disk_manager_->DestroyFile(FileName(table_name));
}

auto ClusteredTableManager::Get(const std::string &table_name) const
    -> ClusteredTableHandle * {
  return entries_.at(table_name)->table.get();
}

auto ClusteredTableManager::GetFd(const std::string &table_name) const -> int {
  return entries_.at(table_name)->fd;
}

}  // namespace easydb
