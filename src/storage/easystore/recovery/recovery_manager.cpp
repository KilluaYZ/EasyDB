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

void RecoveryManager::UndoTransaction(txn_id_t txn_id) {
  if (table_manager_ == nullptr || log_manager_ == nullptr) {
    throw InternalError("recovery manager is not initialized");
  }

  auto logs = log_manager_->ReadAllLogs();
  for (auto it = logs.rbegin(); it != logs.rend(); ++it) {
    const auto &log = *it;
    if (log->log_tid_ != txn_id || log->log_kind_ != LogKind::UNDO) {
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
        table->Insert(record.GetNewTuple(), record.GetNewMeta());
      } else if (!SameTuple(*current, record.GetNewTuple())) {
        table->Update(key, record.GetNewTuple(), record.GetNewMeta());
      }
      break;
    }
    case DELETE: {
      auto current = table->GetRecord(key);
      if (current.has_value()) {
        table->MarkDelete(key, record.GetNewMeta());
      }
      break;
    }
    case UPDATE:
      if (const auto current = table->Get(key);
          !current.has_value()) {
        table->Insert(record.GetNewTuple(), record.GetNewMeta());
      } else if (!SameTuple(*current, record.GetNewTuple())) {
        table->Update(key, record.GetNewTuple(), record.GetNewMeta());
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
      const auto current = table->GetRecord(key);
      if (!current.has_value()) {
        table->Insert(record.GetOldTuple(), record.GetOldMeta());
      } else if (!SameTuple(current->tuple, record.GetOldTuple()) ||
                 current->meta != record.GetOldMeta()) {
        table->Update(key, record.GetOldTuple(), record.GetOldMeta());
      }
      break;
    }
    case UPDATE:
      if (const auto current = table->Get(key);
          !current.has_value()) {
        table->Insert(record.GetOldTuple(), record.GetOldMeta());
      } else if (!SameTuple(*current, record.GetOldTuple())) {
        table->Update(key, record.GetOldTuple(), record.GetOldMeta());
      }
      break;
    default:
      break;
  }
}

}  // namespace easydb
