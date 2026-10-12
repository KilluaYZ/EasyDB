#include "storage/easystore/transaction/rollback_manager.h"

#include "common/context.h"
#include "common/errors.h"
#include "storage/easystore/table/tuple.h"

namespace easydb {

void RollbackManager::Rollback(WriteRecord *write_record, Context *context) {
  switch (write_record->GetWriteType()) {
    case WType::INSERT_TUPLE:
      RollbackInsert(write_record->GetTableName(), write_record->GetKey(),
                     context);
      return;
    case WType::DELETE_TUPLE:
      RollbackDelete(write_record->GetTableName(), write_record->GetKey(),
                     write_record->GetTuple(), context);
      return;
    case WType::UPDATE_TUPLE:
      RollbackUpdate(write_record->GetTableName(), write_record->GetKey(),
                     write_record->GetTuple(), context);
      return;
    default:
      throw InternalError("RollbackManager: invalid write type");
  }
}

void RollbackManager::RollbackInsert(const std::string &table_name, Tuple &key,
                                     Context *context) {
  (void)context;
  table_manager_->Get(table_name)->Delete(key);
}

void RollbackManager::RollbackDelete(const std::string &table_name, Tuple &key,
                                     Tuple &tuple, Context *context) {
  (void)context;
  table_manager_->Get(table_name)->Insert(tuple);
}

void RollbackManager::RollbackUpdate(const std::string &table_name, Tuple &key,
                                     Tuple &tuple, Context *context) {
  (void)context;
  table_manager_->Get(table_name)->Update(key, tuple);
}

}  // namespace easydb
