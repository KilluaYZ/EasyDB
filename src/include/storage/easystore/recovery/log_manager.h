#pragma once

#include <atomic>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <mutex>
#include <string>

#include "common/config.h"
#include "log_defs.h"
#include "storage/disk/disk_manager.h"
#include "storage/easystore/table/tuple.h"

namespace easydb {

enum class LogKind : uint8_t { REDO = 0, UNDO = 1, SYSTEM = 2 };

enum LogType : int {
  UPDATE = 0,
  INSERT,
  DELETE,
  BEGIN,
  COMMIT,
  ABORT,
  CHECKPOINT
};

static const char *LogTypeStr[] = {"UPDATE", "INSERT", "DELETE", "BEGIN",
                                   "COMMIT", "ABORT", "CHECKPOINT"};

class LogRecord {
 public:
  LogKind log_kind_{LogKind::SYSTEM};
  LogType log_type_{BEGIN};
  lsn_t lsn_{INVALID_LSN};
  uint32_t log_tot_len_{LOG_HEADER_SIZE};
  txn_id_t log_tid_{INVALID_TXN_ID};
  lsn_t prev_lsn_{INVALID_LSN};

  virtual void serialize(char *dest) const {
    memcpy(dest + OFFSET_LOG_TYPE, &log_type_, sizeof(log_type_));
    const auto kind = static_cast<uint8_t>(log_kind_);
    memcpy(dest + OFFSET_LOG_KIND, &kind, sizeof(kind));
    memcpy(dest + OFFSET_LSN, &lsn_, sizeof(lsn_));
    memcpy(dest + OFFSET_LOG_TOT_LEN, &log_tot_len_, sizeof(log_tot_len_));
    memcpy(dest + OFFSET_LOG_TID, &log_tid_, sizeof(log_tid_));
    memcpy(dest + OFFSET_PREV_LSN, &prev_lsn_, sizeof(prev_lsn_));
  }

  virtual void deserialize(const char *src) {
    memcpy(&log_type_, src + OFFSET_LOG_TYPE, sizeof(log_type_));
    uint8_t kind = 0;
    memcpy(&kind, src + OFFSET_LOG_KIND, sizeof(kind));
    log_kind_ = static_cast<LogKind>(kind);
    memcpy(&lsn_, src + OFFSET_LSN, sizeof(lsn_));
    memcpy(&log_tot_len_, src + OFFSET_LOG_TOT_LEN, sizeof(log_tot_len_));
    memcpy(&log_tid_, src + OFFSET_LOG_TID, sizeof(log_tid_));
    memcpy(&prev_lsn_, src + OFFSET_PREV_LSN, sizeof(prev_lsn_));
  }

  virtual void format_print() const {
    std::cout << "log type: " << LogTypeStr[log_type_]
              << ", lsn: " << lsn_ << ", txn: " << log_tid_ << '\n';
  }

  virtual ~LogRecord() = default;
};

class TransactionLogRecord : public LogRecord {
 public:
  explicit TransactionLogRecord(LogType type) {
    log_type_ = type;
    log_kind_ = LogKind::SYSTEM;
  }

  TransactionLogRecord(LogType type, txn_id_t txn_id, lsn_t prev_lsn)
      : TransactionLogRecord(type) {
    log_tid_ = txn_id;
    prev_lsn_ = prev_lsn;
  }
};

class BeginLogRecord : public TransactionLogRecord {
 public:
  BeginLogRecord() : TransactionLogRecord(BEGIN) {}
  explicit BeginLogRecord(txn_id_t txn_id) : BeginLogRecord() {
    log_tid_ = txn_id;
  }
};

class CommitLogRecord : public TransactionLogRecord {
 public:
  CommitLogRecord() : TransactionLogRecord(COMMIT) {}
  CommitLogRecord(txn_id_t txn_id, lsn_t prev_lsn)
      : TransactionLogRecord(COMMIT, txn_id, prev_lsn) {}
};

class AbortLogRecord : public TransactionLogRecord {
 public:
  AbortLogRecord() : TransactionLogRecord(ABORT) {}
  AbortLogRecord(txn_id_t txn_id, lsn_t prev_lsn)
      : TransactionLogRecord(ABORT, txn_id, prev_lsn) {}
};

inline uint32_t TupleLogSize(const Tuple &tuple) {
  return sizeof(uint32_t) + tuple.GetLength();
}

inline void SerializeTupleLog(char *dest, int &offset, const Tuple &tuple) {
  const auto size = tuple.GetLength();
  memcpy(dest + offset, &size, sizeof(size));
  offset += sizeof(size);
  if (size != 0) {
    memcpy(dest + offset, tuple.GetData(), size);
    offset += size;
  }
}

inline Tuple DeserializeTupleLog(const char *src, int &offset) {
  uint32_t size = 0;
  memcpy(&size, src + offset, sizeof(size));
  offset += sizeof(size);
  Tuple tuple(static_cast<int>(size), src + offset);
  offset += size;
  return tuple;
}

/** Row-level redo/undo record for a clustered table. */
class TupleLogRecord : public LogRecord {
 public:
  TupleLogRecord() = default;

  TupleLogRecord(LogKind kind, LogType type, txn_id_t txn_id, lsn_t prev_lsn,
                 const std::string &table_name, const Tuple &key,
                 const Tuple &old_tuple = Tuple{},
                 const Tuple &new_tuple = Tuple{})
      : table_name_(table_name), key_(key), old_tuple_(old_tuple),
        new_tuple_(new_tuple) {
    log_kind_ = kind;
    log_type_ = type;
    log_tid_ = txn_id;
    prev_lsn_ = prev_lsn;
    log_tot_len_ = LOG_HEADER_SIZE + sizeof(uint32_t) + table_name_.size() +
                   TupleLogSize(key_) + TupleLogSize(old_tuple_) +
                   TupleLogSize(new_tuple_);
  }

  void serialize(char *dest) const override {
    LogRecord::serialize(dest);
    int offset = OFFSET_LOG_DATA;
    const auto table_size = static_cast<uint32_t>(table_name_.size());
    memcpy(dest + offset, &table_size, sizeof(table_size));
    offset += sizeof(table_size);
    memcpy(dest + offset, table_name_.data(), table_size);
    offset += table_size;
    SerializeTupleLog(dest, offset, key_);
    SerializeTupleLog(dest, offset, old_tuple_);
    SerializeTupleLog(dest, offset, new_tuple_);
  }

  void deserialize(const char *src) override {
    LogRecord::deserialize(src);
    int offset = OFFSET_LOG_DATA;
    uint32_t table_size = 0;
    memcpy(&table_size, src + offset, sizeof(table_size));
    offset += sizeof(table_size);
    table_name_.assign(src + offset, table_size);
    offset += table_size;
    key_ = DeserializeTupleLog(src, offset);
    old_tuple_ = DeserializeTupleLog(src, offset);
    new_tuple_ = DeserializeTupleLog(src, offset);
  }

  LogKind GetKind() const { return log_kind_; }
  LogType GetOperation() const { return log_type_; }
  const std::string &GetTableName() const { return table_name_; }
  const Tuple &GetKey() const { return key_; }
  const Tuple &GetOldTuple() const { return old_tuple_; }
  const Tuple &GetNewTuple() const { return new_tuple_; }

  std::string table_name_;
  Tuple key_;
  Tuple old_tuple_;
  Tuple new_tuple_;
};

using RedoLogRecord = TupleLogRecord;
using UndoLogRecord = TupleLogRecord;

class CheckpointLogRecord : public TransactionLogRecord {
 public:
  CheckpointLogRecord() : TransactionLogRecord(CHECKPOINT) {}
  CheckpointLogRecord(txn_id_t txn_id, lsn_t prev_lsn)
      : TransactionLogRecord(CHECKPOINT, txn_id, prev_lsn) {}
};

class LogBuffer {
 public:
  LogBuffer() { memset(buffer_, 0, sizeof(buffer_)); }
  bool is_full(int append_size) const {
    return offset_ + append_size > LOG_BUFFER_SIZE;
  }
  char buffer_[LOG_BUFFER_SIZE + 1]{};
  int offset_{0};
};

class LogManager {
 public:
  explicit LogManager(DiskManager *disk_manager);
  ~LogManager() noexcept;

  lsn_t add_log_to_buffer(LogRecord *log_record);
  void flush_log_to_disk();
  lsn_t GetPersistentLsn() const;
  lsn_t GetNextLsn() const;
  LogBuffer *get_log_buffer() { return &log_buffer_; }

 private:
  void flush_log_to_disk_unlocked();
  void recover_lsn_state();

  std::atomic<lsn_t> global_lsn_{0};
  mutable std::mutex latch_;
  LogBuffer log_buffer_;
  lsn_t persist_lsn_{INVALID_LSN};
  size_t log_file_offset_{0};
  DiskManager *disk_manager_;
};

}  // namespace easydb
