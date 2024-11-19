/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "recovery/log_manager.h"

#include <cstring>
namespace easydb{

/**
 * @description: 添加日志记录到日志缓冲区中，并返回日志记录号
 * @param {LogRecord*} log_record 要写入缓冲区的日志记录
 * @return {lsn_t} 返回该日志的日志记录号
 * @note 该函数会serialize log_record并写入log_buffer_中，要求 log_record 后续不变
 */
lsn_t LogManager::add_log_to_buffer(LogRecord* log_record) {
  std::scoped_lock lock{latch_};

  // Assign a new LSN to the log record
  lsn_t new_lsn = global_lsn_.fetch_add(1);
  log_record->lsn_ = new_lsn;

  // Get the log record size
  int log_size = log_record->log_tot_len_;

  // Check if there is enough space in the log buffer
  if (log_buffer_.is_full(log_size)) {
    flush_log_to_disk();
  }

  // Serialize the log record directly to the log buffer
  char* buffer_dest = log_buffer_.buffer_ + log_buffer_.offset_;
  log_record->serialize(buffer_dest);

  // Update the buffer offset
  log_buffer_.offset_ += log_size;

  return new_lsn;
}

/**
 * @description: 把日志缓冲区的内容刷到磁盘中，由于目前只设置了一个缓冲区，因此需要阻塞其他日志操作
 */
void LogManager::flush_log_to_disk() {
  std::scoped_lock lock{latch_};

  // Write the buffer to disk
  disk_manager_->write_log(log_buffer_.buffer_, log_buffer_.offset_);

  // Update the persistent LSN
  persist_lsn_ = global_lsn_.load() - 1;

  // Clear the buffer
  log_buffer_.offset_ = 0;
  memset(log_buffer_.buffer_, 0, sizeof(log_buffer_.buffer_));
}

};