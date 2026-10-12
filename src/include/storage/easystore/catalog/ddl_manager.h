#pragma once

#include <string>
#include <vector>

#include "catalog/schema.h"
#include "storage/easystore/catalog/catalog_manager.h"
#include "storage/easystore/index/index_manager.h"
#include "storage/easystore/record/rm_manager.h"
#include "storage/easystore/statistics/statistics_manager.h"
#include "storage/easystore/table/table_manager.h"

namespace easydb {

class BufferPoolManager;
class Context;
class IxManager;

// EasyStore-specific DDL coordinator. It composes the independent catalog,
// table, index, and statistics components without exposing SmManager.
class DdlManager {
 public:
  DdlManager(CatalogManager *catalog_manager, TableManager *table_manager,
             IndexManager *index_manager, IxManager *ix_manager,
             BufferPoolManager *buffer_pool_manager,
             StatisticsManager *statistics_manager)
      : catalog_manager_(catalog_manager),
        table_manager_(table_manager),
        index_manager_(index_manager),
        ix_manager_(ix_manager),
        buffer_pool_manager_(buffer_pool_manager),
        statistics_manager_(statistics_manager) {}

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
  TableManager *table_manager_;
  IndexManager *index_manager_;
  IxManager *ix_manager_;
  BufferPoolManager *buffer_pool_manager_;
  StatisticsManager *statistics_manager_;
};

}  // namespace easydb
