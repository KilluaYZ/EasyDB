#pragma once

#include "storage/easystore/catalog/catalog_manager.h"
#include "storage/easystore/table/clustered_table_manager.h"
#include "storage/easystore/transaction/txn_defs.h"

namespace easydb {

class Context;
class RollbackManager {
 public:
  RollbackManager(CatalogManager *catalog_manager,
                  ClusteredTableManager *table_manager)
      : catalog_manager_(catalog_manager), table_manager_(table_manager) {}

  void Rollback(WriteRecord *write_record, Context *context);

 private:
  void RollbackInsert(const std::string &table_name, Tuple &key,
                      Context *context);
  void RollbackDelete(const std::string &table_name, Tuple &key, Tuple &tuple,
                      Context *context);
  void RollbackUpdate(const std::string &table_name, Tuple &key, Tuple &tuple,
                      Context *context);

  CatalogManager *catalog_manager_;
  ClusteredTableManager *table_manager_;
};

}  // namespace easydb
