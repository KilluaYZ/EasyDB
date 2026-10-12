#pragma once

#include <filesystem>
#include <memory>
#include <mutex>
#include <unordered_set>
#include <vector>

#include "common/config.h"

namespace easydb {

class DiskManager;
class LogRecord;

/** Persistent append-only store for MVCC undo records. */
class UndoManager {
 public:
  explicit UndoManager(DiskManager *disk_manager);

  auto Append(LogRecord *record) -> lsn_t;
  auto ReadAll() const -> std::vector<std::unique_ptr<LogRecord>>;
  void Purge(const std::unordered_set<lsn_t> &obsolete_lsns);

 private:
  DiskManager *disk_manager_;
  std::filesystem::path path_;
  mutable std::mutex latch_;
};

}  // namespace easydb
