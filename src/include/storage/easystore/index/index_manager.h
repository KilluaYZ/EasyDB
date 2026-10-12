#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "storage/easystore/index/ix_index_handle.h"
#include "storage/easystore/index/ix_manager.h"

namespace easydb {

class IndexManager {
 public:
  explicit IndexManager(IxManager *index_manager)
      : index_manager_(index_manager) {}

  std::unordered_map<std::string, std::unique_ptr<IxIndexHandle>> handles_;

  IxIndexHandle *Get(const std::string &index_name) const {
    return handles_.at(index_name).get();
  }

  std::string GetIndexName(
      const std::string &table_name,
      const std::vector<std::string> &columns) const {
    return index_manager_->GetIndexName(table_name, columns);
  }

  std::string GetIndexName(const std::string &table_name,
                           const std::vector<ColMeta> &columns) const {
    return index_manager_->GetIndexName(table_name, columns);
  }

  void Open(const std::string &table_name,
            const std::vector<ColMeta> &columns) {
    auto name = index_manager_->GetIndexName(table_name, columns);
    handles_.emplace(name, index_manager_->OpenIndex(table_name, columns));
  }

  void Close(const std::string &index_name) {
    auto it = handles_.find(index_name);
    if (it == handles_.end()) return;
    index_manager_->CloseIndex(it->second.get());
    handles_.erase(it);
  }

  void Destroy(const std::string &table_name,
               const std::vector<ColMeta> &columns) {
    index_manager_->DestroyIndex(table_name, columns);
  }

 private:
  IxManager *index_manager_;
};

}  // namespace easydb
