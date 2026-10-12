#pragma once

#include <memory>
#include <unordered_set>
#include <vector>

#include "storage/easystore/recovery/log_manager.h"

namespace easydb {

class ClusteredTableManager;

/** Replays the clustered-table Redo/Undo log during database startup. */
class RecoveryManager {
 public:
  RecoveryManager(ClusteredTableManager *table_manager,
                  LogManager *log_manager)
      : table_manager_(table_manager), log_manager_(log_manager) {}

  void Recover();
  void UndoTransaction(txn_id_t txn_id);

 private:
  void ApplyRedo(const TupleLogRecord &record);
  void ApplyUndo(const TupleLogRecord &record);

  ClusteredTableManager *table_manager_;
  LogManager *log_manager_;
};

}  // namespace easydb
