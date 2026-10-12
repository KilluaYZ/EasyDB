#include "storage/easystore/easystore.h"

#include <stdexcept>

#include "buffer/buffer_pool_manager.h"
#include "storage/disk/disk_manager.h"
#include "storage/easystore/concurrency/lock_manager.h"
#include "storage/easystore/record/rm_file_handle.h"
#include "storage/easystore/record/rm_manager.h"
#include "storage/easystore/record/rm_scan.h"
#include "storage/easystore/index/ix_index_handle.h"
#include "storage/easystore/index/ix_manager.h"
#include "storage/easystore/index/ix_scan.h"
#include "storage/easystore/recovery/log_manager.h"
#include "storage/easystore/recovery/log_recovery.h"
#include "storage/easystore/transaction/transaction_manager.h"
#include "system/sm_manager.h"

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
        system_manager(std::make_unique<SmManager>(
            disk_manager.get(), buffer_pool_manager.get(),
            record_manager.get(), index_manager.get(), enable_output)),
        lock_manager(std::make_unique<LockManager>()),
        transaction_manager(std::make_unique<TransactionManager>(
            lock_manager.get(), system_manager.get())),
        log_manager(std::make_unique<LogManager>(disk_manager.get())),
        recovery_manager(std::make_unique<RecoveryManager>(
            disk_manager.get(), buffer_pool_manager.get(),
            system_manager.get(), transaction_manager.get(),
            log_manager.get())) {}

  std::unique_ptr<DiskManager> disk_manager;
  std::unique_ptr<BufferPoolManager> buffer_pool_manager;
  std::unique_ptr<RmManager> record_manager;
  std::unique_ptr<IxManager> index_manager;
  std::unique_ptr<SmManager> system_manager;
  std::unique_ptr<LockManager> lock_manager;
  std::unique_ptr<TransactionManager> transaction_manager;
  std::unique_ptr<LogManager> log_manager;
  std::unique_ptr<RecoveryManager> recovery_manager;
};

EasyStore::EasyStore(const std::string &db_name, bool enable_output)
    : runtime_(std::make_unique<Runtime>(db_name, enable_output)) {}

EasyStore::~EasyStore() = default;

bool EasyStore::IsDir(const std::string &db_name) const {
  return runtime_->system_manager->IsDir(db_name);
}

void EasyStore::CreateDB(const std::string &db_name) {
  runtime_->system_manager->CreateDB(db_name);
}

void EasyStore::OpenDB(const std::string &db_name) {
  runtime_->system_manager->OpenDB(db_name);
}

void EasyStore::CloseDB() { runtime_->system_manager->CloseDB(); }

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

SmManager *EasyStore::GetSystemManager() const {
  return runtime_->system_manager.get();
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
  return runtime_->system_manager->db_.is_table(name);
}

TabMeta &EasyStore::GetTableMeta(const std::string &name) {
  return runtime_->system_manager->db_.get_table(name);
}

int EasyStore::GetTableLockId(const std::string &name) const {
  return runtime_->system_manager->fhs_.at(name)->GetFd();
}

std::unique_ptr<TableScan> EasyStore::ScanTable(const std::string &name,
                                                Context *) {
  return std::make_unique<EasyDBTableScan>(runtime_->system_manager->fhs_.at(name).get());
}

std::unique_ptr<IndexScan> EasyStore::ScanIndex(
    const std::string &table, const std::vector<std::string> &columns,
    const std::vector<char> &lower_key, const std::vector<char> &upper_key,
    Context *context) {
  auto index_name = runtime_->system_manager->GetIxManager()->GetIndexName(table, columns);
  auto *index = runtime_->system_manager->ihs_.at(index_name).get();
  auto lower = index->LowerBound(lower_key.data());
  auto upper = index->UpperBound(upper_key.data());
  return std::make_unique<EasyDBIndexScan>(index, lower, upper,
                                           runtime_->system_manager->GetBpm(), context,
                                           runtime_->system_manager->fhs_.at(table)->GetFd());
}

std::unique_ptr<Tuple> EasyStore::ReadTuple(const std::string &table,
                                            const RID &rid, Context *context) {
  return runtime_->system_manager->fhs_.at(table)->GetTupleValue(rid, context);
}

RID EasyStore::InsertTuple(const std::string &table, const Tuple &tuple,
                           Context *context) {
  auto rid = runtime_->system_manager->fhs_.at(table)->InsertTuple(TupleMeta{0, false}, tuple,
                                                   context);
  if (!rid.has_value()) throw InternalError("failed to insert tuple");
  return *rid;
}

bool EasyStore::DeleteTuple(const std::string &table, const RID &rid,
                            Context *context) {
  return runtime_->system_manager->fhs_.at(table)->DeleteTuple(rid, context);
}

bool EasyStore::UpdateTuple(const std::string &table, const RID &rid,
                            const Tuple &tuple, Context *context) {
  return runtime_->system_manager->fhs_.at(table)->UpdateTupleInPlace(TupleMeta{0, false},
                                                      tuple, rid, context);
}

bool EasyStore::InsertIndexEntry(const std::string &table,
                                 const std::vector<std::string> &columns,
                                 const std::vector<char> &key, const RID &rid,
                                 Context *context) {
  auto index_name = runtime_->system_manager->GetIxManager()->GetIndexName(table, columns);
  return runtime_->system_manager->ihs_.at(index_name)
             ->InsertEntry(key.data(), rid,
                           context == nullptr ? nullptr : context->txn_) != -1;
}

void EasyStore::DeleteIndexEntry(const std::string &table,
                                 const std::vector<std::string> &columns,
                                 const std::vector<char> &key,
                                 Context *context) {
  auto index_name = runtime_->system_manager->GetIxManager()->GetIndexName(table, columns);
  runtime_->system_manager->ihs_.at(index_name)
      ->DeleteEntry(key.data(), context == nullptr ? nullptr : context->txn_);
}

void EasyStore::WaitForIndexGap(const std::string &table,
                                const std::vector<std::string> &columns,
                                const std::vector<char> &key,
                                Context *context) {
  if (context == nullptr) return;
  auto index_name = runtime_->system_manager->GetIxManager()->GetIndexName(table, columns);
  auto iid = runtime_->system_manager->ihs_.at(index_name)->LowerBound(key.data());
  context->lock_mgr_->HandleIndexGapWaitDie(context->txn_, iid,
                                            runtime_->system_manager->fhs_.at(table)->GetFd());
}

void EasyStore::CreateTable(const std::string &name,
                            const std::vector<ColDef> &columns,
                            Context *context) {
  runtime_->system_manager->CreateTable(name, columns, context);
}

void EasyStore::DropTable(const std::string &name, Context *context) {
  runtime_->system_manager->DropTable(name, context);
}

void EasyStore::CreateIndex(const std::string &name,
                            const std::vector<std::string> &columns,
                            Context *context) {
  runtime_->system_manager->CreateIndex(name, columns, context);
}

void EasyStore::DropIndex(const std::string &name,
                          const std::vector<std::string> &columns,
                          Context *context) {
  runtime_->system_manager->DropIndex(name, columns, context);
}

void EasyStore::ShowTables(Context *context) { runtime_->system_manager->ShowTables(context); }

void EasyStore::ShowIndex(const std::string &name, Context *context) {
  runtime_->system_manager->ShowIndex(name, context);
}

void EasyStore::DescTable(const std::string &name, Context *context) {
  runtime_->system_manager->DescTable(name, context);
}

void EasyStore::AsyncLoadData(const std::string &file_name,
                              const std::string &table_name, Context *context) {
  runtime_->system_manager->AsyncLoadData(file_name, table_name, context);
}

int EasyStore::GetLoadStatus() const { return runtime_->system_manager->GetLoadStatus(); }

void EasyStore::AsyncLoadDataFinish() { runtime_->system_manager->AsyncLoadDataFinish(); }

void EasyStore::SetEnableOutput(bool enabled) {
  runtime_->system_manager->SetEnableOutput(enabled);
}

bool EasyStore::IsEnableOutput() const {
  return runtime_->system_manager->IsEnableOutput();
}

int EasyStore::GetTableCount(const std::string &name) const {
  return runtime_->system_manager->GetTableCount(name);
}

int EasyStore::GetTableAttrDistinct(const std::string &table,
                                    const std::string &column) const {
  return runtime_->system_manager->GetTableAttrDistinct(table, column);
}

float EasyStore::GetTableAttrSum(const std::string &table,
                                 const std::string &column) const {
  return runtime_->system_manager->GetTableAttrSum(table, column);
}

float EasyStore::GetTableAttrMax(const std::string &table,
                                 const std::string &column) const {
  return runtime_->system_manager->GetTableAttrMax(table, column);
}

float EasyStore::GetTableAttrMin(const std::string &table,
                                 const std::string &column) const {
  return runtime_->system_manager->GetTableAttrMin(table, column);
}

void EasyStore::UpdateTableCount(const std::string &name, int delta) {
  runtime_->system_manager->UpdateTableCount(name, delta);
}

}  // namespace easydb
