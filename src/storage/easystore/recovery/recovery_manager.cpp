#include "storage/easystore/recovery/recovery_manager.h"

#include <algorithm>
#include <unordered_set>

#include "common/errors.h"
#include "storage/easystore/table/clustered_table_manager.h"

namespace easydb {

namespace {

bool SameTuple(const Tuple &lhs, const Tuple &rhs) {
  return IsTupleContentEqual(lhs, rhs);
}

}  // namespace

void RecoveryManager::Recover() {
  if (table_manager_ == nullptr || log_manager_ == nullptr) {
    throw InternalError("recovery manager is not initialized");
  }

  auto logs = log_manager_->ReadAllLogs();
  std::unordered_set<txn_id_t> committed;

  for (const auto &log : logs) {
    if (log->log_type_ == COMMIT) committed.insert(log->log_tid_);
  }

  // Redo all physical changes first. Undo below makes incomplete transactions
  // disappear, which also makes this procedure safe after a partial crash.
  for (const auto &log : logs) {
    if (log->log_kind_ != LogKind::REDO) continue;
    const auto *tuple_log = dynamic_cast<const TupleLogRecord *>(log.get());
    if (tuple_log != nullptr) ApplyRedo(*tuple_log);
  }

  // Undo in reverse LSN order. A committed transaction is never undone.
  for (auto it = logs.rbegin(); it != logs.rend(); ++it) {
    const auto &log = *it;
    if (log->log_kind_ != LogKind::UNDO ||
        committed.count(log->log_tid_) != 0) {
      continue;
    }
    const auto *tuple_log = dynamic_cast<const TupleLogRecord *>(log.get());
    if (tuple_log != nullptr) ApplyUndo(*tuple_log);
  }

  table_manager_->Flush();
}

void RecoveryManager::ApplyRedo(const TupleLogRecord &record) {
  if (!table_manager_->Has(record.GetTableName())) return;
  auto *table = table_manager_->Get(record.GetTableName());
  const auto &key = record.GetKey();
  switch (record.GetOperation()) {
    case INSERT: {
      const auto current = table->Get(key);
      if (!current.has_value()) {
        table->Insert(record.GetNewTuple());
      } else if (!SameTuple(*current, record.GetNewTuple())) {
        table->Update(key, record.GetNewTuple());
      }
      break;
    }
    case DELETE:
      table->Delete(key);
      break;
    case UPDATE:
      if (const auto current = table->Get(key);
          !current.has_value()) {
        table->Insert(record.GetNewTuple());
      } else if (!SameTuple(*current, record.GetNewTuple())) {
        table->Update(key, record.GetNewTuple());
      }
      break;
    default:
      break;
  }
}

void RecoveryManager::ApplyUndo(const TupleLogRecord &record) {
  if (!table_manager_->Has(record.GetTableName())) return;
  auto *table = table_manager_->Get(record.GetTableName());
  const auto &key = record.GetKey();
  switch (record.GetOperation()) {
    case INSERT:
      table->Delete(key);
      break;
    case DELETE: {
      const auto current = table->Get(key);
      if (!current.has_value()) {
        table->Insert(record.GetOldTuple());
      } else if (!SameTuple(*current, record.GetOldTuple())) {
        table->Update(key, record.GetOldTuple());
      }
      break;
    }
    case UPDATE:
      if (const auto current = table->Get(key);
          !current.has_value()) {
        table->Insert(record.GetOldTuple());
      } else if (!SameTuple(*current, record.GetOldTuple())) {
        table->Update(key, record.GetOldTuple());
      }
      break;
    default:
      break;
  }
}

}  // namespace easydb
