#include "storage/easystore/catalog/ddl_manager.h"

#include <cassert>
#include <cstring>

#include "buffer/buffer_pool_manager.h"
#include "common/context.h"
#include "common/errors.h"
#include "common/exception.h"
#include "storage/easystore/index/ix_manager.h"
#include "storage/easystore/record/rm_file_handle.h"
#include "storage/easystore/record/rm_scan.h"
#include "storage/easystore/table/tuple.h"
#include "type/type_id.h"

namespace easydb {

void DdlManager::CreateTable(const std::string &table_name,
                             const std::vector<ColDef> &column_defs,
                             Context *context) {
  if (catalog_manager_->IsTable(table_name)) {
    throw TableExistsError(table_name);
  }

  int current_offset = 0;
  TabMeta table;
  table.name = table_name;
  std::vector<Column> schema_columns;
  for (const auto &column_def : column_defs) {
    table.cols.emplace_back(table_name, column_def.name, column_def.type,
                            column_def.len, current_offset, false);
    current_offset += column_def.len;

    Column column;
    switch (column_def.type) {
      case TypeId::TYPE_INT:
      case TypeId::TYPE_LONG:
      case TypeId::TYPE_FLOAT:
      case TypeId::TYPE_DOUBLE:
        column = Column(column_def.name, column_def.type);
        break;
      case TypeId::TYPE_CHAR:
      case TypeId::TYPE_VARCHAR:
        column = Column(column_def.name, column_def.type, column_def.len);
        break;
      default:
        throw Exception("unsupported type\n");
    }
    column.SetTabName(table_name);
    schema_columns.emplace_back(column);
  }
  table.schema = Schema(schema_columns);

  table_manager_->Create(table_name, current_offset);
  catalog_manager_->SetTable(table_name, table);
  table_manager_->Open(table_name);

  if (context != nullptr) {
    context->lock_mgr_->LockExclusiveOnTable(
        context->txn_, table_manager_->Get(table_name)->GetFd());
  }
  statistics_manager_->SetTableCount(table_name, 0);
  catalog_manager_->Flush();
}

void DdlManager::DropTable(const std::string &table_name, Context *context) {
  if (!catalog_manager_->IsTable(table_name)) {
    throw TableNotFoundError(table_name);
  }

  if (context != nullptr) {
    context->lock_mgr_->LockExclusiveOnTable(
        context->txn_, table_manager_->Get(table_name)->GetFd());
  }

  auto indexes = catalog_manager_->GetTable(table_name).indexes;
  for (const auto &index : indexes) {
    std::vector<std::string> column_names;
    for (const auto &column : index.cols) column_names.push_back(column.name);
    DropIndex(table_name, column_names, context);
  }

  const int table_fd = table_manager_->Get(table_name)->GetFd();
  table_manager_->Close(table_name);
  buffer_pool_manager_->RemoveAllPages(table_fd);
  table_manager_->Destroy(table_name);
  catalog_manager_->RemoveTable(table_name);
  catalog_manager_->Flush();
}

void DdlManager::CreateIndex(const std::string &table_name,
                             const std::vector<std::string> &column_names,
                             Context *context) {
  if (!catalog_manager_->IsTable(table_name)) {
    throw TableNotFoundError(table_name);
  }

  auto &table = catalog_manager_->GetTable(table_name);
  if (table.is_index(column_names)) {
    throw IndexExistsError(table_name, column_names);
  }
  if (context != nullptr) {
    context->lock_mgr_->LockSharedOnTable(
        context->txn_, table_manager_->Get(table_name)->GetFd());
  }

  std::vector<ColMeta> index_columns;
  std::vector<uint32_t> key_ids;
  int key_length = 0;
  for (const auto &column_name : column_names) {
    auto column = *table.get_col(column_name);
    index_columns.push_back(column);
    key_ids.push_back(table.GetColId(column_name));
    key_length += column.len;
  }

  IndexMeta index_meta = {.tab_name = table_name,
                          .col_tot_len = key_length,
                          .col_num = static_cast<int>(column_names.size()),
                          .cols = index_columns,
                          .col_ids = key_ids};
  auto key_schema = Schema::CopySchema(&table.schema, key_ids);

  ix_manager_->CreateIndex(table_name, index_columns);
  index_manager_->Open(table_name, index_columns);
  auto *index = index_manager_->Get(
      index_manager_->GetIndexName(table_name, column_names));
  auto *file = table_manager_->Get(table_name);
  RmScan scan(file);
  while (!scan.IsEnd()) {
    const auto rid = scan.GetRid();
    auto key_tuple =
        file->GetKeyTuple(table.schema, key_schema, key_ids, rid, context);
    std::vector<char> key(key_length);
    int offset = 0;
    for (int i = 0; i < index_meta.col_num; ++i) {
      auto value = key_tuple.GetValue(&key_schema, i);
      if (value.GetTypeId() == TYPE_CHAR || value.GetTypeId() == TYPE_VARCHAR) {
        memcpy(key.data() + offset, value.GetData(), index_meta.cols[i].len);
      } else {
        assert(static_cast<uint32_t>(index_meta.cols[i].len) ==
               Type(value.GetTypeId()).GetTypeSize(value.GetTypeId()));
        value.SerializeTo(key.data() + offset);
      }
      offset += index_meta.cols[i].len;
    }
    if (index->InsertEntry(key.data(), rid,
                           context == nullptr ? nullptr : context->txn_) == -1) {
      throw Exception("Insert index entry failed(duplicate key). Is the index unique?");
    }
    scan.Next();
  }

  table.indexes.emplace_back(index_meta);
  catalog_manager_->Flush();
}

void DdlManager::DropIndex(const std::string &table_name,
                           const std::vector<std::string> &column_names,
                           Context *context) {
  if (!ix_manager_->Exists(table_name, column_names)) {
    throw IndexEntryNotFoundError();
  }
  if (context != nullptr) {
    context->lock_mgr_->LockSharedOnTable(
        context->txn_, table_manager_->Get(table_name)->GetFd());
  }

  const auto index_name = index_manager_->GetIndexName(table_name, column_names);
  auto it = index_manager_->handles_.find(index_name);
  if (it != index_manager_->handles_.end()) {
    buffer_pool_manager_->RemoveAllPages(it->second->GetFd());
    index_manager_->Close(index_name);
  }

  auto &table = catalog_manager_->GetTable(table_name);
  table.indexes.erase(table.get_index_meta(column_names));
  ix_manager_->DestroyIndex(table_name, column_names);
  catalog_manager_->Flush();
}

}  // namespace easydb
