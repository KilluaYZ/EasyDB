#include "storage/easydb_storage_engine.h"

#include <stdexcept>

#include "record/rm_file_handle.h"
#include "record/rm_scan.h"
#include "storage/index/ix_index_handle.h"
#include "storage/index/ix_manager.h"
#include "storage/index/ix_scan.h"
#include "system/sm_manager.h"

namespace easydb {

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

bool EasyDBStorageEngine::HasTable(const std::string &name) const {
  return manager_->db_.is_table(name);
}

TabMeta &EasyDBStorageEngine::GetTableMeta(const std::string &name) {
  return manager_->db_.get_table(name);
}

int EasyDBStorageEngine::GetTableLockId(const std::string &name) const {
  return manager_->fhs_.at(name)->GetFd();
}

std::unique_ptr<TableScan> EasyDBStorageEngine::ScanTable(
    const std::string &name, Context *) {
  return std::make_unique<EasyDBTableScan>(manager_->fhs_.at(name).get());
}

std::unique_ptr<IndexScan> EasyDBStorageEngine::ScanIndex(
    const std::string &table, const std::vector<std::string> &columns,
    const std::vector<char> &lower_key, const std::vector<char> &upper_key,
    Context *context) {
  auto index_name = manager_->GetIxManager()->GetIndexName(table, columns);
  auto *index = manager_->ihs_.at(index_name).get();
  auto lower = index->LowerBound(lower_key.data());
  auto upper = index->UpperBound(upper_key.data());
  return std::make_unique<EasyDBIndexScan>(index, lower, upper,
                                           manager_->GetBpm(), context,
                                           manager_->fhs_.at(table)->GetFd());
}

std::unique_ptr<Tuple> EasyDBStorageEngine::ReadTuple(const std::string &table,
                                                      const RID &rid,
                                                      Context *context) {
  return manager_->fhs_.at(table)->GetTupleValue(rid, context);
}

RID EasyDBStorageEngine::InsertTuple(const std::string &table,
                                     const Tuple &tuple, Context *context) {
  auto rid = manager_->fhs_.at(table)->InsertTuple(TupleMeta{0, false}, tuple,
                                                   context);
  if (!rid.has_value()) throw InternalError("failed to insert tuple");
  return *rid;
}

bool EasyDBStorageEngine::DeleteTuple(const std::string &table, const RID &rid,
                                      Context *context) {
  return manager_->fhs_.at(table)->DeleteTuple(rid, context);
}

bool EasyDBStorageEngine::UpdateTuple(const std::string &table, const RID &rid,
                                      const Tuple &tuple, Context *context) {
  return manager_->fhs_.at(table)->UpdateTupleInPlace(TupleMeta{0, false},
                                                      tuple, rid, context);
}

bool EasyDBStorageEngine::InsertIndexEntry(
    const std::string &table, const std::vector<std::string> &columns,
    const std::vector<char> &key, const RID &rid, Context *context) {
  auto index_name = manager_->GetIxManager()->GetIndexName(table, columns);
  return manager_->ihs_.at(index_name)
             ->InsertEntry(key.data(), rid,
                           context == nullptr ? nullptr : context->txn_) != -1;
}

void EasyDBStorageEngine::DeleteIndexEntry(
    const std::string &table, const std::vector<std::string> &columns,
    const std::vector<char> &key, Context *context) {
  auto index_name = manager_->GetIxManager()->GetIndexName(table, columns);
  manager_->ihs_.at(index_name)
      ->DeleteEntry(key.data(), context == nullptr ? nullptr : context->txn_);
}

void EasyDBStorageEngine::WaitForIndexGap(
    const std::string &table, const std::vector<std::string> &columns,
    const std::vector<char> &key, Context *context) {
  if (context == nullptr) return;
  auto index_name = manager_->GetIxManager()->GetIndexName(table, columns);
  auto iid = manager_->ihs_.at(index_name)->LowerBound(key.data());
  context->lock_mgr_->HandleIndexGapWaitDie(context->txn_, iid,
                                            manager_->fhs_.at(table)->GetFd());
}

void EasyDBStorageEngine::CreateTable(const std::string &name,
                                      const std::vector<ColDef> &columns,
                                      Context *context) {
  manager_->CreateTable(name, columns, context);
}

void EasyDBStorageEngine::DropTable(const std::string &name, Context *context) {
  manager_->DropTable(name, context);
}

void EasyDBStorageEngine::CreateIndex(const std::string &name,
                                      const std::vector<std::string> &columns,
                                      Context *context) {
  manager_->CreateIndex(name, columns, context);
}

void EasyDBStorageEngine::DropIndex(const std::string &name,
                                    const std::vector<std::string> &columns,
                                    Context *context) {
  manager_->DropIndex(name, columns, context);
}

void EasyDBStorageEngine::ShowTables(Context *context) {
  manager_->ShowTables(context);
}

void EasyDBStorageEngine::ShowIndex(const std::string &name, Context *context) {
  manager_->ShowIndex(name, context);
}

void EasyDBStorageEngine::DescTable(const std::string &name, Context *context) {
  manager_->DescTable(name, context);
}

void EasyDBStorageEngine::AsyncLoadData(const std::string &file_name,
                                        const std::string &table_name,
                                        Context *context) {
  manager_->AsyncLoadData(file_name, table_name, context);
}

int EasyDBStorageEngine::GetLoadStatus() const {
  return manager_->GetLoadStatus();
}

void EasyDBStorageEngine::AsyncLoadDataFinish() {
  manager_->AsyncLoadDataFinish();
}

void EasyDBStorageEngine::SetEnableOutput(bool enabled) {
  manager_->SetEnableOutput(enabled);
}

bool EasyDBStorageEngine::IsEnableOutput() const {
  return const_cast<SmManager *>(manager_)->IsEnableOutput();
}

int EasyDBStorageEngine::GetTableCount(const std::string &name) const {
  return const_cast<SmManager *>(manager_)->GetTableCount(name);
}

int EasyDBStorageEngine::GetTableAttrDistinct(const std::string &table,
                                              const std::string &column) const {
  return const_cast<SmManager *>(manager_)->GetTableAttrDistinct(table, column);
}

float EasyDBStorageEngine::GetTableAttrSum(const std::string &table,
                                           const std::string &column) const {
  return const_cast<SmManager *>(manager_)->GetTableAttrSum(table, column);
}

float EasyDBStorageEngine::GetTableAttrMax(const std::string &table,
                                           const std::string &column) const {
  return const_cast<SmManager *>(manager_)->GetTableAttrMax(table, column);
}

float EasyDBStorageEngine::GetTableAttrMin(const std::string &table,
                                           const std::string &column) const {
  return const_cast<SmManager *>(manager_)->GetTableAttrMin(table, column);
}

void EasyDBStorageEngine::UpdateTableCount(const std::string &name, int delta) {
  manager_->UpdateTableCount(name, delta);
}

}  // namespace easydb
