#pragma once

#include <future>
#include <string>
#include <vector>

#include "storage/easystore/catalog/catalog_manager.h"
#include "storage/easystore/statistics/statistics_manager.h"
#include "storage/easystore/table/table_manager.h"

namespace easydb {

class BufferPoolManager;
class Context;

class LoadManager {
 public:
  LoadManager(CatalogManager *catalog_manager, TableManager *table_manager,
              BufferPoolManager *buffer_pool_manager,
              StatisticsManager *statistics_manager)
      : catalog_manager_(catalog_manager),
        table_manager_(table_manager),
        buffer_pool_manager_(buffer_pool_manager),
        statistics_manager_(statistics_manager) {}

  void AsyncLoadData(const std::string &file_name,
                     const std::string &table_name, Context *context);
  int GetStatus() const { return load_status_; }
  void Finish();

 private:
  void LoadData(const std::string &file_name, const std::string &table_name);

  CatalogManager *catalog_manager_;
  TableManager *table_manager_;
  BufferPoolManager *buffer_pool_manager_;
  StatisticsManager *statistics_manager_;
  int load_status_ = -1;
  std::vector<std::future<void>> futures_;
};

}  // namespace easydb
