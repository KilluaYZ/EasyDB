#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "storage/easystore/record/rm_file_handle.h"
#include "storage/easystore/record/rm_manager.h"

namespace easydb {

class TableManager {
 public:
  TableManager(RmManager *record_manager, BufferPoolManager *buffer_pool)
      : record_manager_(record_manager), buffer_pool_manager_(buffer_pool) {}

  std::unordered_map<std::string, std::unique_ptr<RmFileHandle>> handles_;

  RmFileHandle *Get(const std::string &table_name) const {
    return handles_.at(table_name).get();
  }

  void Create(const std::string &table_name, int record_size) {
    record_manager_->CreateFile(table_name, record_size);
  }

  void Open(const std::string &table_name) {
    handles_.emplace(table_name, record_manager_->OpenFile(table_name));
  }

  void Close(const std::string &table_name) {
    auto it = handles_.find(table_name);
    if (it == handles_.end()) return;
    record_manager_->CloseFile(it->second.get());
    handles_.erase(it);
  }

  void Destroy(const std::string &table_name) {
    record_manager_->DestoryFile(table_name);
  }

 private:
  RmManager *record_manager_;
  BufferPoolManager *buffer_pool_manager_;
};

}  // namespace easydb
