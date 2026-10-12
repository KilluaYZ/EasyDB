#include "storage/easystore/catalog/ddl_manager.h"

#include "buffer/buffer_pool_manager.h"
#include "common/context.h"
#include "common/errors.h"
#include "common/exception.h"
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

  catalog_manager_->SetTable(table_name, table);
  clustered_table_manager_->Create(table_name, table.schema);
  clustered_table_manager_->Open(table_name, table.schema);

  if (context != nullptr) {
    context->lock_mgr_->LockExclusiveOnTable(
        context->txn_, clustered_table_manager_->GetFd(table_name));
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
        context->txn_, clustered_table_manager_->GetFd(table_name));
  }

  auto indexes = catalog_manager_->GetTable(table_name).indexes;
  for (const auto &index : indexes) {
    std::vector<std::string> column_names;
    for (const auto &column : index.cols) column_names.push_back(column.name);
    DropIndex(table_name, column_names, context);
  }

  const int table_fd = clustered_table_manager_->GetFd(table_name);
  clustered_table_manager_->Close(table_name);
  buffer_pool_manager_->RemoveAllPages(table_fd);
  clustered_table_manager_->Destroy(table_name);
  catalog_manager_->RemoveTable(table_name);
  catalog_manager_->Flush();
}

void DdlManager::CreateIndex(const std::string &table_name,
                             const std::vector<std::string> &column_names,
                             Context *context) {
  (void)table_name;
  (void)column_names;
  (void)context;
  throw Exception(
      "secondary RID indexes have been removed; use the clustered primary key");
}

void DdlManager::DropIndex(const std::string &table_name,
                           const std::vector<std::string> &column_names,
                           Context *context) {
  (void)table_name;
  (void)column_names;
  (void)context;
  throw Exception(
      "secondary RID indexes have been removed; there is no independent index "
      "to drop");
}

}  // namespace easydb
