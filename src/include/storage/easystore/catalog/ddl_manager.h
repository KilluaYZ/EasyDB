#pragma once

#include <string>
#include <vector>

#include "catalog/schema.h"
#include "storage/easystore/catalog/catalog_manager.h"
#include "storage/easystore/statistics/statistics_manager.h"
#include "storage/easystore/table/clustered_table_manager.h"

namespace easydb {

class BufferPoolManager;
class Context;
// EasyStore-specific DDL coordinator. It composes the independent catalog,
// table, index, and statistics components without exposing SmManager.
class DdlManager {
 public:
  DdlManager(CatalogManager *catalog_manager,
             BufferPoolManager *buffer_pool_manager,
             StatisticsManager *statistics_manager,
             ClusteredTableManager *clustered_table_manager)
      : catalog_manager_(catalog_manager),
        buffer_pool_manager_(buffer_pool_manager),
        statistics_manager_(statistics_manager),
        clustered_table_manager_(clustered_table_manager) {}

  void CreateTable(const std::string &table_name,
                   const std::vector<ColDef> &column_defs, Context *context);
  void DropTable(const std::string &table_name, Context *context);
  void CreateIndex(const std::string &table_name,
                   const std::vector<std::string> &column_names,
                   Context *context);
  void DropIndex(const std::string &table_name,
                 const std::vector<std::string> &column_names,
                 Context *context);

 private:
  CatalogManager *catalog_manager_;
  BufferPoolManager *buffer_pool_manager_;
  StatisticsManager *statistics_manager_;
  ClusteredTableManager *clustered_table_manager_;
};

}  // namespace easydb
