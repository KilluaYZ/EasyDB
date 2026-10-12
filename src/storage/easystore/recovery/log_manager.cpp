/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL
v2. You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "storage/easystore/recovery/log_manager.h"

#include <algorithm>
#include <array>
#include <cstring>
#include "common/errors.h"

namespace easydb {

LogManager::LogManager(DiskManager *disk_manager)
    : disk_manager_(disk_manager) {
  if (disk_manager_ == nullptr) {
    throw InternalError("LogManager requires a DiskManager");
  }
  recover_lsn_state();
}

LogManager::~LogManager() noexcept {
  try {
    flush_log_to_disk();
  } catch (...) {
    // Destructors must not throw. The explicit Flush API remains the place
    // where callers can observe an I/O failure.
  }
}

/**
 * @description: 添加日志记录到日志缓冲区中，并返回日志记录号
 * @param {LogRecord*} log_record 要写入缓冲区的日志记录
 * @return {lsn_t} 返回该日志的日志记录号
 * @note 该函数会serialize log_record并写入log_buffer_中，要求 log_record
 * 后续不变
 */
lsn_t LogManager::add_log_to_buffer(LogRecord *log_record) {
  std::scoped_lock lock{latch_};

  // Assign a new LSN to the log record
  lsn_t new_lsn = global_lsn_.fetch_add(1);
  log_record->lsn_ = new_lsn;

  // Get the log record size
  int log_size = log_record->log_tot_len_;

  // Check if there is enough space in the log buffer
  if (log_size > static_cast<int>(sizeof(log_buffer_.buffer_))) {
    throw InternalError("log record is larger than the log buffer");
  }
  if (log_buffer_.is_full(log_size)) {
    flush_log_to_disk_unlocked();
  }

  // Serialize the log record directly to the log buffer
  char *buffer_dest = log_buffer_.buffer_ + log_buffer_.offset_;
  log_record->serialize(buffer_dest);

  // Update the buffer offset
  log_buffer_.offset_ += log_size;

  return new_lsn;
}

/**
 * @description:
 * 把日志缓冲区的内容刷到磁盘中，由于目前只设置了一个缓冲区，因此需要阻塞其他日志操作
 */
void LogManager::flush_log_to_disk() {
  std::scoped_lock lock{latch_};
  flush_log_to_disk_unlocked();
}

void LogManager::flush_log_to_disk_unlocked() {
  if (log_buffer_.offset_ == 0) return;

  disk_manager_->WriteLog(log_file_offset_, log_buffer_.buffer_,
                          static_cast<size_t>(log_buffer_.offset_));
  disk_manager_->FlushLog();
  log_file_offset_ += static_cast<size_t>(log_buffer_.offset_);

  // Update the persistent LSN
  persist_lsn_ = global_lsn_.load() == 0 ? INVALID_LSN : global_lsn_.load() - 1;

  // Clear the buffer
  log_buffer_.offset_ = 0;
  memset(log_buffer_.buffer_, 0, sizeof(log_buffer_.buffer_));
}

lsn_t LogManager::GetPersistentLsn() const {
  std::scoped_lock lock{latch_};
  return persist_lsn_;
}

lsn_t LogManager::GetNextLsn() const {
  std::scoped_lock lock{latch_};
  return global_lsn_.load();
}

auto LogManager::ReadAllLogs() const
    -> std::vector<std::unique_ptr<LogRecord>> {
  std::scoped_lock lock{latch_};
  std::vector<std::unique_ptr<LogRecord>> logs;
  size_t offset = 0;
  const auto log_size = disk_manager_->GetLogSize();

  while (offset + LOG_HEADER_SIZE <= log_size) {
    std::array<char, LOG_HEADER_SIZE> header{};
    disk_manager_->ReadLog(offset, header.data(), header.size());

    LogRecord header_record;
    header_record.deserialize(header.data());
    const auto record_size = header_record.log_tot_len_;
    if (record_size < LOG_HEADER_SIZE || offset + record_size > log_size) {
      break;
    }

    std::vector<char> bytes(record_size);
    disk_manager_->ReadLog(offset, bytes.data(), bytes.size());

    std::unique_ptr<LogRecord> record;
    const bool is_tuple_operation =
        header_record.log_kind_ != LogKind::SYSTEM &&
        header_record.log_type_ <= DELETE;
    if (is_tuple_operation) {
      record = std::make_unique<TupleLogRecord>();
    } else {
      record = std::make_unique<LogRecord>();
    }
    record->deserialize(bytes.data());
    logs.emplace_back(std::move(record));
    offset += record_size;
  }

  return logs;
}

void LogManager::recover_lsn_state() {
  std::scoped_lock lock{latch_};
  log_file_offset_ = disk_manager_->GetLogSize();
  size_t offset = 0;
  lsn_t last_lsn = INVALID_LSN;
  std::array<char, LOG_HEADER_SIZE> header{};

  while (offset + LOG_HEADER_SIZE <= log_file_offset_) {
    disk_manager_->ReadLog(offset, header.data(), header.size());
    LogRecord record;
    record.deserialize(header.data());
    if (record.log_tot_len_ < LOG_HEADER_SIZE ||
        offset + record.log_tot_len_ > log_file_offset_) {
      break;
    }
    last_lsn = std::max(last_lsn, record.lsn_);
    offset += record.log_tot_len_;
  }

  // A crash can leave a partial tail. It must not be placed before the next
  // valid record when the next process appends new WAL records.
  if (offset != log_file_offset_) {
    disk_manager_->TruncateLog(offset);
    log_file_offset_ = offset;
  }
  persist_lsn_ = last_lsn;
  global_lsn_.store(last_lsn == INVALID_LSN ? 0 : last_lsn + 1);
}

}  // namespace easydb
