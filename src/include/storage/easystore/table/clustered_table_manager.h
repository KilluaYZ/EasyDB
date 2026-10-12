#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "buffer/buffer_pool_manager.h"
#include "catalog/schema.h"
#include "storage/disk/disk_manager.h"
#include "storage/easystore/table/clustered_table_handle.h"

namespace easydb {

/** Owns clustered table files and their clustered primary-key handles. */
class ClusteredTableManager {
 public:
  ClusteredTableManager(DiskManager *disk_manager,
                        BufferPoolManager *buffer_pool_manager)
      : disk_manager_(disk_manager),
        buffer_pool_manager_(buffer_pool_manager) {}

  void Create(const std::string &table_name, const Schema &schema);
  void Open(const std::string &table_name, const Schema &schema);
  void Close(const std::string &table_name);
  void Destroy(const std::string &table_name);

  auto Get(const std::string &table_name) const -> ClusteredTableHandle *;
  auto GetFd(const std::string &table_name) const -> int;

 private:
  struct Entry {
    int fd;
    Schema schema;
    Schema key_schema;
    std::unique_ptr<ClusteredIndexHandle> index;
    std::unique_ptr<ClusteredTableHandle> table;
  };

  auto FileName(const std::string &table_name) const -> std::string;
  auto MakeComparator(const Column &column)
      -> ClusteredIndexHandle::KeyComparator;

  DiskManager *disk_manager_;
  BufferPoolManager *buffer_pool_manager_;
  std::unordered_map<std::string, std::unique_ptr<Entry>> entries_;
};

}  // namespace easydb
