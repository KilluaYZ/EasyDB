#include "storage/easystore/transaction/rollback_manager.h"

#include <cstring>

#include "common/context.h"
#include "common/errors.h"
#include "storage/easystore/record/rm_file_handle.h"
#include "storage/easystore/table/tuple.h"

namespace easydb {

namespace {

std::vector<char> BuildKey(RmFileHandle *file, const TabMeta &table,
                           const IndexMeta &index, const RID &rid,
                           Context *context) {
  auto key_schema = Schema::CopySchema(&table.schema, index.col_ids);
  auto key_tuple =
      file->GetKeyTuple(table.schema, key_schema, index.col_ids, rid, context);
  std::vector<char> key(index.col_tot_len);
  int offset = 0;
  for (int i = 0; i < index.col_num; ++i) {
    auto value = key_tuple.GetValue(&key_schema, i);
    ix_memcpy(key.data() + offset, value, index.cols[i].len);
    offset += index.cols[i].len;
  }
  return key;
}

}  // namespace

void RollbackManager::Rollback(WriteRecord *write_record, Context *context) {
  switch (write_record->GetWriteType()) {
    case WType::INSERT_TUPLE:
      RollbackInsert(write_record->GetTableName(), write_record->GetRid(),
                     context);
      return;
    case WType::DELETE_TUPLE:
      RollbackDelete(write_record->GetTableName(), write_record->GetRid(),
                     write_record->GetTuple(), context);
      return;
    case WType::UPDATE_TUPLE:
      RollbackUpdate(write_record->GetTableName(), write_record->GetRid(),
                     write_record->GetTuple(), context);
      return;
    default:
      throw InternalError("RollbackManager: invalid write type");
  }
}

void RollbackManager::RollbackInsert(const std::string &table_name, RID rid,
                                     Context *context) {
  auto *file = table_manager_->Get(table_name);
  auto table = catalog_manager_->GetTable(table_name);
  for (const auto &index : table.indexes) {
    auto index_name = index_manager_->GetIndexName(table_name, index.cols);
    auto key = BuildKey(file, table, index, rid, context);
    index_manager_->Get(index_name)->DeleteEntry(key.data(), context->txn_);
  }
  file->DeleteTuple(rid, context);
}

void RollbackManager::RollbackDelete(const std::string &table_name, RID rid,
                                     Tuple &tuple, Context *context) {
  auto *file = table_manager_->Get(table_name);
  file->InsertTuple(rid, TupleMeta{0, false}, tuple, context);
  file->SetPageLSN(rid.GetPageId(), context->txn_->GetPrevLsn());

  auto table = catalog_manager_->GetTable(table_name);
  for (const auto &index : table.indexes) {
    auto index_name = index_manager_->GetIndexName(table_name, index.cols);
    auto key = BuildKey(file, table, index, rid, context);
    if (index_manager_->Get(index_name)->InsertEntry(
            key.data(), rid, context->txn_) == -1) {
      throw InternalError("RollbackManager: index entry already exists");
    }
  }
}

void RollbackManager::RollbackUpdate(const std::string &table_name, RID rid,
                                     Tuple &tuple, Context *context) {
  auto *file = table_manager_->Get(table_name);
  auto table = catalog_manager_->GetTable(table_name);
  auto new_tuple = file->GetTupleValue(rid, context);
  auto new_values = new_tuple->GetValueVec(&table.schema);
  auto old_values = tuple.GetValueVec(&table.schema);
  file->UpdateTupleInPlace(TupleMeta{0, false}, tuple, rid, context);
  file->SetPageLSN(rid.GetPageId(), context->txn_->GetPrevLsn());

  for (const auto &index : table.indexes) {
    auto index_name = index_manager_->GetIndexName(table_name, index.cols);
    auto *index_handle = index_manager_->Get(index_name);
    std::vector<char> new_key(index.col_tot_len);
    std::vector<char> old_key(index.col_tot_len);
    int offset = 0;
    for (int i = 0; i < index.col_num; ++i) {
      ix_memcpy(new_key.data() + offset, new_values[index.col_ids[i]],
                index.cols[i].len);
      ix_memcpy(old_key.data() + offset, old_values[index.col_ids[i]],
                index.cols[i].len);
      offset += index.cols[i].len;
    }
    if (memcmp(new_key.data(), old_key.data(), index.col_tot_len) == 0) {
      continue;
    }
    if (index_handle->InsertEntry(old_key.data(), rid, context->txn_) == -1) {
      throw InternalError("RollbackManager: index entry already exists");
    }
    index_handle->DeleteEntry(new_key.data(), context->txn_);
  }
}

}  // namespace easydb
