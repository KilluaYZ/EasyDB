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

  if (context_ != nullptr && context_->txn_ != nullptr &&
      context_->log_mgr_ != nullptr) {
    RedoLogRecord redo(LogKind::REDO, INSERT,
                       context_->txn_->GetTransactionId(),
                       context_->txn_->GetPrevLsn(), tab_name_, tuple, Tuple{},
                       tuple);
    auto lsn = context_->log_mgr_->add_log_to_buffer(&redo);
    context_->txn_->SetPrevLsn(lsn);

    UndoLogRecord undo(LogKind::UNDO, INSERT,
                       context_->txn_->GetTransactionId(),
                       context_->txn_->GetPrevLsn(), tab_name_, tuple, Tuple{},
                       tuple);
    lsn = context_->log_mgr_->add_log_to_buffer(&undo);
    context_->txn_->SetPrevLsn(lsn);
    // The clustered page must not become durable before both records are durable.
    context_->log_mgr_->flush_log_to_disk();
  }

  if (!storage_engine_->InsertTuple(tab_name_, tuple, context_)) {
    throw InternalError("failed to insert tuple into clustered table");
  }

  // Update context_ for rollback (be sure to update after record insert)
  WriteRecord *write_record =
      new WriteRecord(WType::INSERT_TUPLE, tab_name_, tuple);
  context_->txn_->AppendWriteRecord(write_record);

  storage_engine_->UpdateTableCount(tab_name_, 1);

  return nullptr;
}

}  // namespace easydb
