/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * executor_delete.cpp
 *
 * Identification: src/execution/executor_delete.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "execution/executor_delete.h"
#include "storage/easystore/table/tuple.h"

namespace easydb {

/**
 * @description: 删除执行器的构造函数
 * @param storage_engine 系统管理器指针
 * @param tab_name 表名
 * @param conds 删除条件
 * @param tuples 要删除的聚簇表主键元组列表
 * @param context 上下文指针
 */
DeleteExecutor::DeleteExecutor(StorageEngine *storage_engine,
                               const std::string &tab_name,
                               std::vector<Condition> conds,
                               std::vector<Tuple> tuples, Context *context) {
  storage_engine_ = storage_engine;
  tab_name_ = tab_name;
  tab_ = storage_engine_->GetTableMeta(tab_name);
  conds_ = conds;
  tuples_ = std::move(tuples);
  context_ = context;

  // lock table
  if (context_ != nullptr) {
    context_->lock_mgr_->LockIXOnTable(
        context_->txn_, storage_engine_->GetTableLockId(tab_name));
  }
}

/**
 * @description: 执行删除操作，删除指定的记录并更新相关索引
 * @return 返回nullptr，因为删除操作不返回元组
 */
std::unique_ptr<Tuple> DeleteExecutor::Next() {
  // 遍历要删除的记录
  int tuple_count = tuples_.size();
  for (int i = 0; i < tuple_count; i++) {
    const auto &key_tuple = tuples_[i];

    // get records
    auto rec = storage_engine_->ReadTuple(tab_name_, key_tuple, context_);
    if (rec == nullptr) continue;

    if (context_ != nullptr && context_->txn_ != nullptr &&
        context_->log_mgr_ != nullptr) {
      RedoLogRecord redo(LogKind::REDO, DELETE,
                         context_->txn_->GetTransactionId(),
                         context_->txn_->GetPrevLsn(), tab_name_, key_tuple,
                         *rec, Tuple{});
      auto lsn = context_->log_mgr_->add_log_to_buffer(&redo);
      context_->txn_->SetPrevLsn(lsn);

      UndoLogRecord undo(LogKind::UNDO, DELETE,
                         context_->txn_->GetTransactionId(),
                         context_->txn_->GetPrevLsn(), tab_name_, key_tuple,
                         *rec, Tuple{});
      lsn = context_->log_mgr_->add_log_to_buffer(&undo);
      context_->txn_->SetPrevLsn(lsn);
      context_->log_mgr_->flush_log_to_disk();
    }

    // delete records
    storage_engine_->DeleteTuple(tab_name_, key_tuple, context_);

    // Update context_ for rollback
    WriteRecord *write_record =
        new WriteRecord(WType::DELETE_TUPLE, tab_name_, key_tuple, *rec);
    context_->txn_->AppendWriteRecord(write_record);

    storage_engine_->UpdateTableCount(tab_name_, -1);
  }

  return nullptr;
}

}  // namespace easydb
