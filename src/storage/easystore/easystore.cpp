#include "storage/easystore/easystore.h"

#include <algorithm>
#include <unistd.h>
#include <filesystem>
#include <fstream>
#include <stdexcept>

#include "buffer/buffer_pool_manager.h"
#include "common/errors.h"
#include "storage/disk/disk_manager.h"
#include "storage/easystore/catalog/catalog_manager.h"
#include "storage/easystore/catalog/ddl_manager.h"
#include "storage/easystore/catalog/load_manager.h"
#include "storage/easystore/concurrency/lock_manager.h"
#include "storage/easystore/record/record_printer.h"
#include "storage/easystore/recovery/log_manager.h"
#include "storage/easystore/recovery/recovery_manager.h"
#include "storage/easystore/statistics/statistics_manager.h"
#include "storage/easystore/table/clustered_table_manager.h"
#include "storage/easystore/transaction/transaction_manager.h"

namespace easydb {

class EasyStore::Runtime {
 public:
  Runtime(const std::string &db_name, bool enable_output)
      : disk_manager(std::make_unique<DiskManager>(db_name)),
        buffer_pool_manager(std::make_unique<BufferPoolManager>(
            BUFFER_POOL_SIZE, disk_manager.get())),
        clustered_table_manager(std::make_unique<ClusteredTableManager>(
            disk_manager.get(), buffer_pool_manager.get(),
            std::filesystem::absolute(db_name))),
        catalog_manager(std::make_unique<CatalogManager>()),
        statistics_manager(std::make_unique<StatisticsManager>()),
        load_manager(std::make_unique<LoadManager>(
            catalog_manager.get(), clustered_table_manager.get(),
            buffer_pool_manager.get(), statistics_manager.get())),
        ddl_manager(std::make_unique<DdlManager>(
            catalog_manager.get(), buffer_pool_manager.get(),
            statistics_manager.get(), clustered_table_manager.get())),
        lock_manager(std::make_unique<LockManager>()),
        transaction_manager(std::make_unique<TransactionManager>(
            lock_manager.get(), disk_manager.get(), buffer_pool_manager.get(),
            catalog_manager.get(), ddl_manager.get(),
            clustered_table_manager.get())),
        log_manager(std::make_unique<LogManager>(disk_manager.get())) {
    catalog_manager->SetDatabasePath(std::filesystem::absolute(db_name));
  }

  std::unique_ptr<DiskManager> disk_manager;
  std::unique_ptr<BufferPoolManager> buffer_pool_manager;
  std::unique_ptr<ClusteredTableManager> clustered_table_manager;
  std::unique_ptr<CatalogManager> catalog_manager;
  std::unique_ptr<StatisticsManager> statistics_manager;
  std::unique_ptr<DdlManager> ddl_manager;
  std::unique_ptr<LoadManager> load_manager;
  std::unique_ptr<LockManager> lock_manager;
  std::unique_ptr<TransactionManager> transaction_manager;
  std::unique_ptr<LogManager> log_manager;
};

EasyStore::EasyStore(const std::string &db_name, bool enable_output)
    : runtime_(std::make_unique<Runtime>(db_name, enable_output)),
      database_path_(std::filesystem::absolute(db_name)),
      enable_output_(enable_output) {}

EasyStore::~EasyStore() = default;

bool EasyStore::IsDir(const std::string &db_name) const {
  return std::filesystem::is_directory(db_name);
}

void EasyStore::CreateDB(const std::string &db_name) {
  const auto path = std::filesystem::absolute(db_name);
  if (std::filesystem::exists(path / DB_META_NAME)) {
    throw DatabaseExistsError(db_name);
  }
  std::filesystem::create_directories(path);
  runtime_->catalog_manager->Initialize(db_name);
}

void EasyStore::OpenDB(const std::string &db_name) {
  const auto path = std::filesystem::absolute(db_name);
  if (!std::filesystem::is_directory(path) ||
      !std::filesystem::exists(path / DB_META_NAME)) {
    std::filesystem::create_directories(path);
    runtime_->catalog_manager->Initialize(db_name);
  }
  runtime_->catalog_manager->Load();
  for (const auto &entry : runtime_->catalog_manager->Tables()) {
    runtime_->clustered_table_manager->Open(entry.first, entry.second.schema);
  }
  RecoveryManager recovery(runtime_->clustered_table_manager.get(),
                           runtime_->log_manager.get());
  recovery.Recover();
}

void EasyStore::CloseDB() {
  // WAL must be durable before closing table files and the database handle.
  runtime_->log_manager->flush_log_to_disk();
  for (const auto &entry : runtime_->catalog_manager->Tables()) {
    runtime_->clustered_table_manager->Close(entry.first);
  }
}

DiskManager *EasyStore::GetDiskManager() const {
  return runtime_->disk_manager.get();
}

BufferPoolManager *EasyStore::GetBufferPoolManager() const {
  return runtime_->buffer_pool_manager.get();
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

CatalogManager *EasyStore::GetCatalogManager() const {
  return runtime_->catalog_manager.get();
}

ClusteredTableManager *EasyStore::GetClusteredTableManager() const {
  return runtime_->clustered_table_manager.get();
}

StatisticsManager *EasyStore::GetStatisticsManager() const {
  return runtime_->statistics_manager.get();
}

namespace {

class ClusteredScan final : public TableScan, public IndexScan {
 public:
  explicit ClusteredScan(std::vector<Tuple> tuples)
      : tuples_(std::move(tuples)) {}

  void Next() override {
    if (position_ < tuples_.size()) ++position_;
  }
  bool IsEnd() const override { return position_ >= tuples_.size(); }
  std::unique_ptr<Tuple> GetTuple() const override {
    if (IsEnd()) return nullptr;
    return std::make_unique<Tuple>(tuples_[position_]);
  }

 private:
  std::vector<Tuple> tuples_;
  size_t position_{0};
};

}  // namespace

bool EasyStore::HasTable(const std::string &name) const {
  return runtime_->catalog_manager->IsTable(name);
}

TabMeta &EasyStore::GetTableMeta(const std::string &name) {
  return runtime_->catalog_manager->GetTable(name);
}

int EasyStore::GetTableLockId(const std::string &name) const {
  return runtime_->clustered_table_manager->GetFd(name);
}

std::unique_ptr<TableScan> EasyStore::ScanTable(const std::string &name,
                                                Context *context) {
  auto records = runtime_->clustered_table_manager->Get(name)->ScanRecords();
  if (context != nullptr && context->txn_ != nullptr) {
    if (context->read_view_ == nullptr) {
      context->read_view_ =
          runtime_->transaction_manager->GetReadView(context->txn_);
    }
    records.erase(std::remove_if(records.begin(), records.end(),
                                 [&](const auto &record) {
                                   return !context->read_view_->IsVisible(
                                       record.meta);
                                 }),
                  records.end());
  }
  std::vector<Tuple> tuples;
  tuples.reserve(records.size());
  for (const auto &record : records) tuples.emplace_back(record.tuple);
  return std::make_unique<ClusteredScan>(std::move(tuples));
}

std::unique_ptr<IndexScan> EasyStore::ScanIndex(
    const std::string &table, const std::vector<std::string> &columns,
    const std::vector<char> &lower_key, const std::vector<char> &upper_key,
    Context *context) {
  auto records = runtime_->clustered_table_manager->Get(table)->ScanRecords();
  if (context != nullptr && context->txn_ != nullptr) {
    if (context->read_view_ == nullptr) {
      context->read_view_ =
          runtime_->transaction_manager->GetReadView(context->txn_);
    }
    records.erase(std::remove_if(records.begin(), records.end(),
                                 [&](const auto &record) {
                                   return !context->read_view_->IsVisible(
                                       record.meta);
                                 }),
                  records.end());
  }
  std::vector<Tuple> tuples;
  tuples.reserve(records.size());
  for (const auto &record : records) tuples.emplace_back(record.tuple);
  return std::make_unique<ClusteredScan>(std::move(tuples));
}

std::unique_ptr<Tuple> EasyStore::ReadTuple(const std::string &table,
                                            const Tuple &key,
                                            Context *context) {
  auto record = runtime_->clustered_table_manager->Get(table)->GetRecord(key);
  if (!record.has_value()) return nullptr;
  if (context != nullptr && context->txn_ != nullptr) {
    if (context->read_view_ == nullptr) {
      context->read_view_ =
          runtime_->transaction_manager->GetReadView(context->txn_);
    }
    if (!context->read_view_->IsVisible(record->meta)) return nullptr;
  }
  return std::make_unique<Tuple>(record->tuple);
}

bool EasyStore::InsertTuple(const std::string &table, const Tuple &tuple,
                            Context *context) {
  TupleMeta meta;
  if (context != nullptr && context->txn_ != nullptr) {
    meta.trx_id_ = context->txn_->GetTransactionId();
  }
  return runtime_->clustered_table_manager->Get(table)->Insert(tuple, meta);
}

bool EasyStore::DeleteTuple(const std::string &table, const Tuple &key,
                            Context *) {
  return runtime_->clustered_table_manager->Get(table)->Delete(key);
}

bool EasyStore::UpdateTuple(const std::string &table, const Tuple &key,
                            const Tuple &tuple, Context *context) {
  TupleMeta meta;
  if (context != nullptr && context->txn_ != nullptr) {
    meta.trx_id_ = context->txn_->GetTransactionId();
  }
  return runtime_->clustered_table_manager->Get(table)->Update(key, tuple,
                                                                meta);
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
    outfile.open(database_path_ / "output.txt", std::ios::out | std::ios::app);
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
  if (enable_output_)
    outfile.open(database_path_ / "output.txt", std::ios::out | std::ios::app);
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

int EasyStore::GetLoadStatus() const {
  return runtime_->load_manager->GetStatus();
}

void EasyStore::AsyncLoadDataFinish() { runtime_->load_manager->Finish(); }

void EasyStore::SetEnableOutput(bool enabled) { enable_output_ = enabled; }

bool EasyStore::IsEnableOutput() const { return enable_output_; }

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
