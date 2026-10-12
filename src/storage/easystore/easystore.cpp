#include "storage/easystore/easystore.h"

#include <stdexcept>
#include <fstream>
#include <filesystem>
#include <unistd.h>

#include "buffer/buffer_pool_manager.h"
#include "common/errors.h"
#include "storage/disk/disk_manager.h"
#include "storage/easystore/concurrency/lock_manager.h"
#include "storage/easystore/catalog/catalog_manager.h"
#include "storage/easystore/catalog/ddl_manager.h"
#include "storage/easystore/catalog/load_manager.h"
#include "storage/easystore/record/record_printer.h"
#include "storage/easystore/record/rm_file_handle.h"
#include "storage/easystore/record/rm_manager.h"
#include "storage/easystore/record/rm_scan.h"
#include "storage/easystore/index/ix_index_handle.h"
#include "storage/easystore/index/index_manager.h"
#include "storage/easystore/index/ix_manager.h"
#include "storage/easystore/index/ix_scan.h"
#include "storage/easystore/recovery/log_manager.h"
#include "storage/easystore/recovery/log_recovery.h"
#include "storage/easystore/statistics/statistics_manager.h"
#include "storage/easystore/table/table_manager.h"
#include "storage/easystore/transaction/transaction_manager.h"
#include "storage/easystore/transaction/rollback_manager.h"

namespace easydb {

class EasyStore::Runtime {
 public:
  Runtime(const std::string &db_name, bool enable_output)
      : disk_manager(std::make_unique<DiskManager>(db_name)),
        buffer_pool_manager(std::make_unique<BufferPoolManager>(
            BUFFER_POOL_SIZE, disk_manager.get())),
        record_manager(std::make_unique<RmManager>(
            disk_manager.get(), buffer_pool_manager.get())),
        index_manager(std::make_unique<IxManager>(
            disk_manager.get(), buffer_pool_manager.get())),
        table_manager(std::make_unique<TableManager>(
            record_manager.get(), buffer_pool_manager.get())),
        easy_index_manager(std::make_unique<IndexManager>(
            index_manager.get())),
        catalog_manager(std::make_unique<CatalogManager>()),
        statistics_manager(std::make_unique<StatisticsManager>()),
        load_manager(std::make_unique<LoadManager>(
            catalog_manager.get(), table_manager.get(), buffer_pool_manager.get(),
            statistics_manager.get())),
        ddl_manager(std::make_unique<DdlManager>(
            catalog_manager.get(), table_manager.get(), easy_index_manager.get(),
            index_manager.get(), buffer_pool_manager.get(),
            statistics_manager.get())),
        rollback_manager(std::make_unique<RollbackManager>(
            catalog_manager.get(), table_manager.get(),
            easy_index_manager.get())),
        lock_manager(std::make_unique<LockManager>()),
        transaction_manager(std::make_unique<TransactionManager>(
            lock_manager.get(), rollback_manager.get(),
            disk_manager.get(), buffer_pool_manager.get(), catalog_manager.get(),
            table_manager.get(), ddl_manager.get())),
        log_manager(std::make_unique<LogManager>(disk_manager.get())),
        recovery_manager(std::make_unique<RecoveryManager>(
            disk_manager.get(), buffer_pool_manager.get(),
            catalog_manager.get(), table_manager.get(), ddl_manager.get(),
            transaction_manager.get(),
            log_manager.get())) {}

  std::unique_ptr<DiskManager> disk_manager;
  std::unique_ptr<BufferPoolManager> buffer_pool_manager;
  std::unique_ptr<RmManager> record_manager;
  std::unique_ptr<IxManager> index_manager;
  std::unique_ptr<TableManager> table_manager;
  std::unique_ptr<IndexManager> easy_index_manager;
  std::unique_ptr<CatalogManager> catalog_manager;
  std::unique_ptr<StatisticsManager> statistics_manager;
  std::unique_ptr<DdlManager> ddl_manager;
  std::unique_ptr<RollbackManager> rollback_manager;
  std::unique_ptr<LoadManager> load_manager;
  std::unique_ptr<LockManager> lock_manager;
  std::unique_ptr<TransactionManager> transaction_manager;
  std::unique_ptr<LogManager> log_manager;
  std::unique_ptr<RecoveryManager> recovery_manager;
};

EasyStore::EasyStore(const std::string &db_name, bool enable_output)
    : runtime_(std::make_unique<Runtime>(db_name, enable_output)),
      enable_output_(enable_output) {}

EasyStore::~EasyStore() = default;

bool EasyStore::IsDir(const std::string &db_name) const {
  return std::filesystem::is_directory(db_name);
}

void EasyStore::CreateDB(const std::string &db_name) {
  if (IsDir(db_name)) throw DatabaseExistsError(db_name);
  runtime_->disk_manager->CreateDir(db_name);
  if (chdir(db_name.c_str()) < 0) throw UnixError();
  runtime_->catalog_manager->Initialize(db_name);
  if (chdir("..") < 0) throw UnixError();
}

void EasyStore::OpenDB(const std::string &db_name) {
  if (!IsDir(db_name)) CreateDB(db_name);
  if (chdir(db_name.c_str()) < 0) throw UnixError();
  runtime_->catalog_manager->Load();
  for (const auto &entry : runtime_->catalog_manager->Tables()) {
    runtime_->table_manager->Open(entry.first);
    if (runtime_->index_manager->Exists(entry.first, entry.second.cols)) {
      runtime_->easy_index_manager->Open(entry.first, entry.second.cols);
    }
  }
}

void EasyStore::CloseDB() {
  // WAL must be durable before closing table files and the database handle.
  runtime_->log_manager->flush_log_to_disk();
  for (const auto &entry : runtime_->catalog_manager->Tables()) {
    runtime_->table_manager->Close(entry.first);
    if (runtime_->index_manager->Exists(entry.first, entry.second.cols)) {
      runtime_->easy_index_manager->Close(
          runtime_->easy_index_manager->GetIndexName(entry.first,
                                                     entry.second.cols));
    }
  }
  if (chdir("..") < 0) throw UnixError();
}

DiskManager *EasyStore::GetDiskManager() const {
  return runtime_->disk_manager.get();
}

BufferPoolManager *EasyStore::GetBufferPoolManager() const {
  return runtime_->buffer_pool_manager.get();
}

RmManager *EasyStore::GetRecordManager() const {
  return runtime_->record_manager.get();
}

IxManager *EasyStore::GetIndexManager() const {
  return runtime_->index_manager.get();
}

LockManager *EasyStore::GetLockManager() const {
  return runtime_->lock_manager.get();
}

TransactionManager *EasyStore::GetTransactionManager() const {
  return runtime_->transaction_manager.get();
}

LogManager *EasyStore::GetLogManager() const {
  return runtime_->log_manager.get();
}

RecoveryManager *EasyStore::GetRecoveryManager() const {
  return runtime_->recovery_manager.get();
}

CatalogManager *EasyStore::GetCatalogManager() const {
  return runtime_->catalog_manager.get();
}

TableManager *EasyStore::GetTableManager() const {
  return runtime_->table_manager.get();
}

IndexManager *EasyStore::GetIndexComponent() const {
  return runtime_->easy_index_manager.get();
}

StatisticsManager *EasyStore::GetStatisticsManager() const {
  return runtime_->statistics_manager.get();
}

namespace {

class EasyDBTableScan final : public TableScan {
 public:
  explicit EasyDBTableScan(RmFileHandle *file)
      : scan_(std::make_unique<RmScan>(file)) {}

  void Next() override { scan_->Next(); }
  bool IsEnd() const override { return scan_->IsEnd(); }
  RID GetRid() const override { return scan_->GetRid(); }

 private:
  std::unique_ptr<RmScan> scan_;
};

class EasyDBIndexScan final : public IndexScan {
 public:
  EasyDBIndexScan(IxIndexHandle *index, const Iid &lower, const Iid &upper,
                  BufferPoolManager *buffer_pool, Context *context, int fd)
      : scan_(std::make_unique<IxScan>(index, lower, upper, buffer_pool)) {
    if (context == nullptr) return;

    const auto first = scan_->GetIid();
    while (!scan_->IsEnd()) {
      context->lock_mgr_->LockGapOnIndex(context->txn_, scan_->GetIid(), fd);
      scan_->Next();
    }
    scan_->set_lower(first);
  }

  void Next() override { scan_->Next(); }
  bool IsEnd() const override { return scan_->IsEnd(); }
  RID GetRid() const override { return scan_->GetRid(); }

 private:
  std::unique_ptr<IxScan> scan_;
};

}  // namespace

bool EasyStore::HasTable(const std::string &name) const {
  return runtime_->catalog_manager->IsTable(name);
}

TabMeta &EasyStore::GetTableMeta(const std::string &name) {
  return runtime_->catalog_manager->GetTable(name);
}

int EasyStore::GetTableLockId(const std::string &name) const {
  return runtime_->table_manager->Get(name)->GetFd();
}

std::unique_ptr<TableScan> EasyStore::ScanTable(const std::string &name,
                                                Context *) {
  return std::make_unique<EasyDBTableScan>(runtime_->table_manager->Get(name));
}

std::unique_ptr<IndexScan> EasyStore::ScanIndex(
    const std::string &table, const std::vector<std::string> &columns,
    const std::vector<char> &lower_key, const std::vector<char> &upper_key,
    Context *context) {
  auto index_name = runtime_->easy_index_manager->GetIndexName(table, columns);
  auto *index = runtime_->easy_index_manager->Get(index_name);
  auto lower = index->LowerBound(lower_key.data());
  auto upper = index->UpperBound(upper_key.data());
  return std::make_unique<EasyDBIndexScan>(index, lower, upper,
                                           runtime_->buffer_pool_manager.get(),
                                           context,
                                           runtime_->table_manager->Get(table)->GetFd());
}

std::unique_ptr<Tuple> EasyStore::ReadTuple(const std::string &table,
                                            const RID &rid, Context *context) {
  return runtime_->table_manager->Get(table)->GetTupleValue(rid, context);
}

RID EasyStore::InsertTuple(const std::string &table, const Tuple &tuple,
                           Context *context) {
  auto rid = runtime_->table_manager->Get(table)->InsertTuple(
      TupleMeta{0, false}, tuple, context);
  if (!rid.has_value()) throw InternalError("failed to insert tuple");
  return *rid;
}

bool EasyStore::DeleteTuple(const std::string &table, const RID &rid,
                            Context *context) {
  return runtime_->table_manager->Get(table)->DeleteTuple(rid, context);
}

bool EasyStore::UpdateTuple(const std::string &table, const RID &rid,
                            const Tuple &tuple, Context *context) {
  return runtime_->table_manager->Get(table)->UpdateTupleInPlace(
      TupleMeta{0, false}, tuple, rid, context);
}

bool EasyStore::InsertIndexEntry(const std::string &table,
                                 const std::vector<std::string> &columns,
                                 const std::vector<char> &key, const RID &rid,
                                 Context *context) {
  auto index_name = runtime_->easy_index_manager->GetIndexName(table, columns);
  return runtime_->easy_index_manager->Get(index_name)
             ->InsertEntry(key.data(), rid,
                           context == nullptr ? nullptr : context->txn_) != -1;
}

void EasyStore::DeleteIndexEntry(const std::string &table,
                                 const std::vector<std::string> &columns,
                                 const std::vector<char> &key,
                                 Context *context) {
  auto index_name = runtime_->easy_index_manager->GetIndexName(table, columns);
  runtime_->easy_index_manager->Get(index_name)
      ->DeleteEntry(key.data(), context == nullptr ? nullptr : context->txn_);
}

void EasyStore::WaitForIndexGap(const std::string &table,
                                const std::vector<std::string> &columns,
                                const std::vector<char> &key,
                                Context *context) {
  if (context == nullptr) return;
  auto index_name = runtime_->easy_index_manager->GetIndexName(table, columns);
  auto iid = runtime_->easy_index_manager->Get(index_name)->LowerBound(key.data());
  context->lock_mgr_->HandleIndexGapWaitDie(context->txn_, iid,
                                            runtime_->table_manager->Get(table)->GetFd());
}

void EasyStore::CreateTable(const std::string &name,
                            const std::vector<ColDef> &columns,
                            Context *context) {
  runtime_->ddl_manager->CreateTable(name, columns, context);
}

void EasyStore::DropTable(const std::string &name, Context *context) {
  runtime_->ddl_manager->DropTable(name, context);
}

void EasyStore::CreateIndex(const std::string &name,
                            const std::vector<std::string> &columns,
                            Context *context) {
  runtime_->ddl_manager->CreateIndex(name, columns, context);
}

void EasyStore::DropIndex(const std::string &name,
                          const std::vector<std::string> &columns,
                          Context *context) {
  runtime_->ddl_manager->DropIndex(name, columns, context);
}

void EasyStore::ShowTables(Context *context) {
  std::fstream outfile;
  if (enable_output_) {
    outfile.open("output.txt", std::ios::out | std::ios::app);
    outfile << "| Tables |\n";
  }
  RecordPrinter printer(1);
  printer.print_separator(context);
  printer.print_record({"Tables"}, context);
  printer.print_separator(context);
  for (const auto &entry : runtime_->catalog_manager->Tables()) {
    printer.print_record({entry.second.name}, context);
    if (enable_output_) outfile << "| " << entry.second.name << " |\n";
  }
  printer.print_separator(context);
  if (outfile.is_open()) outfile.close();
}

void EasyStore::ShowIndex(const std::string &name, Context *context) {
  const auto &table = runtime_->catalog_manager->GetTable(name);
  std::fstream outfile;
  if (enable_output_) outfile.open("output.txt", std::ios::out | std::ios::app);
  RecordPrinter printer(3);
  for (const auto &index : table.indexes) {
    std::string index_name = "(" + index.cols[0].name;
    for (int i = 1; i < index.col_num; ++i) {
      index_name += "," + index.cols[i].name;
    }
    index_name += ")";
    printer.print_record({name, "unique", index_name}, context);
    if (enable_output_) {
      outfile << "| " << name << " | unique | " << index_name << " |\n";
    }
  }
  if (outfile.is_open()) outfile.close();
}

void EasyStore::DescTable(const std::string &name, Context *context) {
  const auto &table = runtime_->catalog_manager->GetTable(name);
  std::vector<std::string> captions = {"Field", "Type", "Index"};
  RecordPrinter printer(captions.size());
  printer.print_separator(context);
  printer.print_record(captions, context);
  printer.print_separator(context);
  for (const auto &column : table.cols) {
    printer.print_record(
        {column.name, coltype2str(column.type), column.index ? "YES" : "NO"},
        context);
  }
  printer.print_separator(context);
}

void EasyStore::AsyncLoadData(const std::string &file_name,
                              const std::string &table_name, Context *context) {
  runtime_->load_manager->AsyncLoadData(file_name, table_name, context);
}

int EasyStore::GetLoadStatus() const { return runtime_->load_manager->GetStatus(); }

void EasyStore::AsyncLoadDataFinish() { runtime_->load_manager->Finish(); }

void EasyStore::SetEnableOutput(bool enabled) {
  enable_output_ = enabled;
}

bool EasyStore::IsEnableOutput() const {
  return enable_output_;
}

int EasyStore::GetTableCount(const std::string &name) const {
  return runtime_->statistics_manager->GetTableCount(name);
}

int EasyStore::GetTableAttrDistinct(const std::string &table,
                                    const std::string &column) const {
  return runtime_->statistics_manager->GetTableAttrDistinct(table, column);
}

float EasyStore::GetTableAttrSum(const std::string &table,
                                 const std::string &column) const {
  return runtime_->statistics_manager->GetTableAttrSum(table, column);
}

float EasyStore::GetTableAttrMax(const std::string &table,
                                 const std::string &column) const {
  return runtime_->statistics_manager->GetTableAttrMax(table, column);
}

float EasyStore::GetTableAttrMin(const std::string &table,
                                 const std::string &column) const {
  return runtime_->statistics_manager->GetTableAttrMin(table, column);
}

void EasyStore::UpdateTableCount(const std::string &name, int delta) {
  runtime_->statistics_manager->UpdateTableCount(name, delta);
}

}  // namespace easydb
