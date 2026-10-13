/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * executor_insert.cpp
 *
 * Identification: src/execution/executor_insert.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "execution/executor_insert.h"
#include <sys/stat.h>
#include <vector>

namespace easydb {

namespace {

auto ClusteredKeyBytes(const Tuple &tuple, const TabMeta &table)
    -> std::vector<char> {
  // ClusteredTableManager currently defines the first column as the primary
  // key. Keep the lock key identical to the key used by clustered storage.
  const auto key_schema = Schema::CopySchema(&table.schema, {0});
  const auto key_tuple = tuple.KeyFromTuple(table.schema, key_schema, {0});
  return {key_tuple.GetData(), key_tuple.GetData() + key_tuple.GetLength()};
}

}  // namespace

/**
 * @description: 插入执行器的构造函数
 * @param storage_engine 系统管理器指针
 * @param tab_name 表名
 * @param values 要插入的值列表
 * @param context 上下文指针
 */
InsertExecutor::InsertExecutor(StorageEngine *storage_engine,
                               const std::string &tab_name,
                               std::vector<Value> values, Context *context) {
  storage_engine_ = storage_engine;
  tab_ = storage_engine_->GetTableMeta(tab_name);
  values_ = values;
  tab_name_ = tab_name;
  if (values.size() != tab_.cols.size()) {
    throw InvalidValueCountError();
  }
  context_ = context;

  // lock table
  if (context_ != nullptr) {
    context_->lock_mgr_->LockIXOnTable(
        context_->txn_, storage_engine_->GetTableLockId(tab_name));
  }
};

/**
 * @description: 执行插入操作，将元组插入到表中并更新相关索引
 * @return 返回nullptr，因为插入操作不返回元组
 */
std::unique_ptr<Tuple> InsertExecutor::Next() {
  // 构造元组
  Tuple tuple{values_, &tab_.schema};
  TupleMeta new_meta;
  if (context_ != nullptr && context_->txn_ != nullptr) {
    const auto key = ClusteredKeyBytes(tuple, tab_);
    const auto gap = storage_engine_->FindInsertGap(tab_name_, tuple);
    if (!gap.has_value()) {
      throw InternalError("failed to locate clustered insert gap");
    }
    if (!context_->lock_mgr_->LockInsertIntention(
            context_->txn_, storage_engine_->GetTableLockId(tab_name_),
            gap->first, gap->second)) {
      throw InternalError("failed to acquire clustered insert intention lock");
    }
    if (!context_->lock_mgr_->LockExclusiveOnRecord(
            context_->txn_, storage_engine_->GetTableLockId(tab_name_), key)) {
      throw InternalError("failed to acquire clustered record X lock");
    }
  }

  if (context_ != nullptr && context_->txn_ != nullptr) {
    new_meta.trx_id_ = context_->txn_->GetTransactionId();
  }

  if (context_ != nullptr && context_->txn_ != nullptr &&
      context_->log_mgr_ != nullptr) {
    RedoLogRecord redo(LogKind::REDO, INSERT,
                       context_->txn_->GetTransactionId(),
                       context_->txn_->GetPrevLsn(), tab_name_, tuple, Tuple{},
                       tuple, TupleMeta{}, new_meta);
    auto lsn = context_->log_mgr_->add_log_to_buffer(&redo);
    context_->txn_->SetPrevLsn(lsn);

    UndoLogRecord undo(LogKind::UNDO, INSERT,
                       context_->txn_->GetTransactionId(),
                       context_->txn_->GetPrevLsn(), tab_name_, tuple, Tuple{},
                       tuple, TupleMeta{}, new_meta);
    lsn = context_->log_mgr_->add_log_to_buffer(&undo);
    context_->txn_->SetPrevLsn(lsn);
    context_->current_undo_lsn_ = lsn;
    // The clustered page must not become durable before both records are durable.
    context_->log_mgr_->flush_log_to_disk();
  }

  if (!storage_engine_->InsertTuple(tab_name_, tuple, context_)) {
    throw InternalError("failed to insert tuple into clustered table");
  }

  storage_engine_->UpdateTableCount(tab_name_, 1);

  return nullptr;
}

}  // namespace easydb
