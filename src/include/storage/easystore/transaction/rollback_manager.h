#pragma once

#include "storage/easystore/catalog/catalog_manager.h"
#include "storage/easystore/index/index_manager.h"
#include "storage/easystore/table/table_manager.h"
#include "storage/easystore/transaction/txn_defs.h"

namespace easydb {

class Context;
class IxManager;

class RollbackManager {
 public:
  RollbackManager(CatalogManager *catalog_manager, TableManager *table_manager,
                  IndexManager *index_manager)
      : catalog_manager_(catalog_manager),
        table_manager_(table_manager),
        index_manager_(index_manager) {}

  void Rollback(WriteRecord *write_record, Context *context);

 private:
  void RollbackInsert(const std::string &table_name, RID rid,
                      Context *context);
  void RollbackDelete(const std::string &table_name, RID rid, Tuple &tuple,
                      Context *context);
  void RollbackUpdate(const std::string &table_name, RID rid, Tuple &tuple,
                      Context *context);

  CatalogManager *catalog_manager_;
  TableManager *table_manager_;
  IndexManager *index_manager_;
};

}  // namespace easydb
