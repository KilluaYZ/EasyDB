/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "recovery/log_recovery.h"

#include <queue>
#include <string>

#include "common/config.h"
#include "common/errors.h"
#include "recovery/log_manager.h"
#include "storage/page/page.h"

namespace easydb {

/**
 * @description: analyze阶段，需要获得脏页表（DPT）和未完成的事务列表（ATT）
 */
void RecoveryManager::analyze() {
  // Read log records from the checkpoint
  int file_offset = analyze_checkpoint();
  int read_size = 0;

  LogRecord *log_record = new LogRecord();
  InsertLogRecord *insert_log = new InsertLogRecord();
  DeleteLogRecord *delete_log = new DeleteLogRecord();
  UpdateLogRecord *update_log = new UpdateLogRecord();

  while (true) {
    // 1. Read logs
    // read_size =
    //     disk_manager_->read_log(buffer_.buffer_ + buffer_.offset_, LOG_BUFFER_SIZE - buffer_.offset_, file_offset);
    // no more logs to read
    if (read_size <= 0) {
      break;
    }
    int start_offset = file_offset - buffer_.offset_;
    int processed_offset = 0;
    file_offset += read_size;
    buffer_.offset_ += read_size;

    // 2. Process complete log records
    while (buffer_.offset_ - processed_offset >= LOG_HEADER_SIZE) {
      log_record->deserialize(buffer_.buffer_ + processed_offset);

      // Check if the buffer contains the entire log record
      if (buffer_.offset_ - processed_offset < log_record->log_tot_len_) {
        // Incomplete log record, wait for more data
        break;
      }

      // Record the log's offset and size in lsn_mapping_
      lsn_mapping_[log_record->lsn_] = {start_offset + processed_offset, log_record->log_tot_len_};
      last_lsn_ = std::max(last_lsn_, log_record->lsn_);

      // 3. Parse log records
      switch (log_record->log_type_) {
        case LogType::BEGIN:
          att_[log_record->log_tid_] = log_record->lsn_;
          last_txn_id_ = std::max(last_txn_id_, log_record->log_tid_);
          break;
        case LogType::COMMIT:
          att_.erase(log_record->log_tid_);
          break;
        case LogType::ABORT:
          // undo the aborted txn because we don't write CLR
          att_[log_record->log_tid_] = log_record->lsn_;
          // aborted_txns_[log_record->log_tid_] = log_record->prev_lsn_;
          aborted_txns_.insert(log_record->log_tid_);
          break;
        case LogType::INSERT: {
          insert_log->deserialize(buffer_.buffer_ + processed_offset);
          std::string table_name(insert_log->table_name_, insert_log->table_name_size_);
          int fd = disk_manager_->GetFileFd(table_name);
          PageId page_id{fd, insert_log->rid_.GetPageId()};
          analyze_process(log_record, page_id);
          break;
        }
        case LogType::DELETE: {
          delete_log->deserialize(buffer_.buffer_ + processed_offset);
          std::string table_name(delete_log->table_name_, delete_log->table_name_size_);
          int fd = disk_manager_->GetFileFd(table_name);
          PageId page_id{fd, delete_log->rid_.GetPageId()};
          analyze_process(log_record, page_id);
          break;
        }
        case LogType::UPDATE: {
          update_log->deserialize(buffer_.buffer_ + processed_offset);
          std::string table_name(update_log->table_name_, update_log->table_name_size_);
          int fd = disk_manager_->GetFileFd(table_name);
          PageId page_id{fd, update_log->rid_.GetPageId()};
          analyze_process(log_record, page_id);
          break;
        }
        case LogType::CHECKPOINT: {
          // ignore the untracked checkpoints
          std::cout << "[warning] LogRecovery: untracked checkpoint found, ignored" << std::endl;
          break;
        }
        default:
          throw InternalError("RecoveryManager::analyze: Invalid log type");
      }

      // 4. Update processed_offset to move to the next log record
      processed_offset += log_record->log_tot_len_;
    }

    // If there is unprocessed data at the end of the buffer, move it to the beginning
    if (processed_offset > 0 && processed_offset < buffer_.offset_) {
      memmove(buffer_.buffer_, buffer_.buffer_ + processed_offset, buffer_.offset_ - processed_offset);
    }
    buffer_.offset_ -= processed_offset;
  }
  delete log_record;
  delete insert_log;
  delete delete_log;
  delete update_log;

  analyze_finish();
}

/**
 * @description: Analyze the checkpoint and process the log records.
 *
 * This function reads the checkpoint from the disk and processes the log records
 * to recover the system state. It starts from the beginning and reads the
 * log records from the log file. If the checkpoint LSN is found,
 * it recovers the att dpt(etc) from the checkpoint log record.
 * Finally, it updates the file offset and returns it.
 *
 * @return The file offset after the checkpoint.
 */
int RecoveryManager::analyze_checkpoint() {
  int file_offset = 0;
  int read_size = 0;

  // Read from checkpoint
  lsn_t checkpoint_lsn = INVALID_LSN;
  // read_size = disk_manager_->read_checkpoint(checkpoint_lsn);
  if (checkpoint_lsn == INVALID_LSN) {
    // No checkpoint found, start from the beginning of the log file
    return 0;
  }
  bool found = false;

  LogRecord *log_record = new LogRecord();
  CheckpointLogRecord *checkpoint_log = new CheckpointLogRecord();

  while (true) {
    // 1. Read logs
    // read_size =
    //     disk_manager_->read_log(buffer_.buffer_ + buffer_.offset_, LOG_BUFFER_SIZE - buffer_.offset_, file_offset);
    // no more logs to read
    if (read_size <= 0) {
      break;
    }
    int start_offset = file_offset - buffer_.offset_;
    int processed_offset = 0;
    file_offset += read_size;
    buffer_.offset_ += read_size;

    // 2. Process complete log records
    while (buffer_.offset_ - processed_offset >= LOG_HEADER_SIZE) {
      log_record->deserialize(buffer_.buffer_ + processed_offset);

      // Check if the buffer contains the entire log record
      if (buffer_.offset_ - processed_offset < log_record->log_tot_len_) {
        // Incomplete log record, wait for more data
        break;
      }

      // Record the log's offset and size in lsn_mapping_
      lsn_mapping_[log_record->lsn_] = {start_offset + processed_offset, log_record->log_tot_len_};
      // last_lsn_ = std::max(last_lsn_, log_record->lsn_);

      // 3. Parse log records
      if (log_record->lsn_ == checkpoint_lsn) {
        assert(log_record->log_type_ == LogType::CHECKPOINT);
        checkpoint_log->deserialize(buffer_.buffer_ + processed_offset);
        // recover att and dpt
        for (size_t i = 0; i < checkpoint_log->att_size_; i++) {
          att_[checkpoint_log->att_vec_[i].first] = checkpoint_log->att_vec_[i].second;
        }
        for (size_t i = 0; i < checkpoint_log->aborted_txns_size_; i++) {
          aborted_txns_.insert(checkpoint_log->aborted_txns_vec_[i]);
        }
        for (size_t i = 0; i < checkpoint_log->dpt_size_; i++) {
          std::string table_name = checkpoint_log->tab_name_str_.substr(
              checkpoint_log->tab_name_offset_vec_[i],
              checkpoint_log->tab_name_offset_vec_[i + 1] - checkpoint_log->tab_name_offset_vec_[i]);
          int fd = disk_manager_->GetFileFd(table_name);
          PageId page_id{fd, checkpoint_log->dpt_vec_[i].first};
          dpt_[page_id] = checkpoint_log->dpt_vec_[i].second;
        }
        min_rec_lsn_ = checkpoint_log->min_rec_lsn_;

        last_lsn_ = checkpoint_log->lsn_;
        last_txn_id_ = checkpoint_log->log_tid_;
        file_offset = start_offset + processed_offset + log_record->log_tot_len_;
        found = true;
        // // debug
        // checkpoint_log->format_print();
      }

      // 4. Update processed_offset to move to the next log record
      processed_offset += log_record->log_tot_len_;

      if (found) break;
    }

    // If there is unprocessed data at the end of the buffer, move it to the beginning
    if (processed_offset > 0 && processed_offset < buffer_.offset_) {
      memmove(buffer_.buffer_, buffer_.buffer_ + processed_offset, buffer_.offset_ - processed_offset);
    }
    buffer_.offset_ -= processed_offset;

    if (found) break;
  }

  delete log_record;
  delete checkpoint_log;

  return file_offset;
}

/**
 * Process the log record for INSERT, DELETE, UPDATE and updates the recovery manager's data structures.
 *
 * @param log_record The log record to be analyzed.
 * @param page_id The ID of the page associated with the log record.
 */
void RecoveryManager::analyze_process(LogRecord *log_record, PageId &page_id) {
  att_[log_record->log_tid_] = log_record->lsn_;
  if (dpt_.find(page_id) == dpt_.end()) {
    dpt_[page_id] = log_record->lsn_;
    if (min_rec_lsn_ == INVALID_LSN) {
      min_rec_lsn_ = log_record->lsn_;
    }
  }
}

/**
 * @brief Performs the final steps of the analyze process.
 *
 * This function is responsible for recovering the system by updating various
 * variables and persisting the last transaction ID and log sequence number (LSN).
 *
 * @note This function assumes that the last transaction ID and LSN have been correctly set.
 */
void RecoveryManager::analyze_finish() {
  // recover
  if (last_txn_id_ != INVALID_TXN_ID && last_lsn_ != INVALID_LSN) {
    // std::cout << "[last]txn_id: " << last_txn_id_ << ", lsn: " << last_lsn_ << std::endl;
    // std::cout << "[next]txn_id: " << last_txn_id_ + 1 << ", lsn: " << last_lsn_ + 1 << std::endl;
    txn_manager_->next_txn_id_.store(last_txn_id_ + 1);
    txn_manager_->next_timestamp_.store(last_txn_id_ + 1);
    log_manager_->global_lsn_.store(last_lsn_ + 1);
    log_manager_->persist_lsn_ = last_lsn_;
  }
  // // debug
  // format_print();
}

/**
 * @description: Analyze the log records and set checkpoint.
 *
 * @param checkpoint A pointer to the CheckpointLogRecord object.
 */
void RecoveryManager::analyze4chkpt(CheckpointLogRecord *checkpoint) {
  // temp result for checkpoint
  std::unordered_map<PageId, std::string, PageIdHash> page2tab_name;

  // Read log records from the checkpoint
  int file_offset = analyze_checkpoint();
  int read_size = 0;

  LogRecord *log_record = new LogRecord();
  InsertLogRecord *insert_log = new InsertLogRecord();
  DeleteLogRecord *delete_log = new DeleteLogRecord();
  UpdateLogRecord *update_log = new UpdateLogRecord();

  lsn_t checkpoint_lsn = last_lsn_;
  // Note: Reset min_rec_lsn_
  // if there are logs after the checkpoint, checkpoint is flushed suceesfully.
  // so we need to reset min_rec_lsn_ to get the correct min_rec_lsn_ after the checkpoint.
  min_rec_lsn_ = INVALID_LSN;
  while (true) {
    // 1. Read logs
    // read_size =
    //     disk_manager_->read_log(buffer_.buffer_ + buffer_.offset_, LOG_BUFFER_SIZE - buffer_.offset_, file_offset);
    // no more logs to read
    if (read_size <= 0) {
      break;
    }
    int start_offset = file_offset - buffer_.offset_;
    int processed_offset = 0;
    file_offset += read_size;
    buffer_.offset_ += read_size;

    // 2. Process complete log records
    while (buffer_.offset_ - processed_offset >= LOG_HEADER_SIZE) {
      log_record->deserialize(buffer_.buffer_ + processed_offset);

      // Check if the buffer contains the entire log record
      if (buffer_.offset_ - processed_offset < log_record->log_tot_len_) {
        // Incomplete log record, wait for more data
        break;
      }

      // Record the log's offset and size in lsn_mapping_
      lsn_mapping_[log_record->lsn_] = {start_offset + processed_offset, log_record->log_tot_len_};
      last_lsn_ = std::max(last_lsn_, log_record->lsn_);

      // 3. Parse log records
      switch (log_record->log_type_) {
        case LogType::BEGIN:
          att_[log_record->log_tid_] = log_record->lsn_;
          last_txn_id_ = std::max(last_txn_id_, log_record->log_tid_);
          break;
        case LogType::COMMIT:
          att_.erase(log_record->log_tid_);
          break;
        case LogType::ABORT:
          // undo the aborted txn because we don't write CLR
          att_[log_record->log_tid_] = log_record->lsn_;
          aborted_txns_.insert(log_record->log_tid_);
          break;
        case LogType::INSERT: {
          insert_log->deserialize(buffer_.buffer_ + processed_offset);
          std::string table_name(insert_log->table_name_, insert_log->table_name_size_);
          int fd = disk_manager_->GetFileFd(table_name);
          PageId page_id{fd, insert_log->rid_.GetPageId()};
          analyze_process(log_record, page_id);
          page2tab_name.emplace(page_id, table_name);
          break;
        }
        case LogType::DELETE: {
          delete_log->deserialize(buffer_.buffer_ + processed_offset);
          std::string table_name(delete_log->table_name_, delete_log->table_name_size_);
          int fd = disk_manager_->GetFileFd(table_name);
          PageId page_id{fd, delete_log->rid_.GetPageId()};
          analyze_process(log_record, page_id);
          page2tab_name.emplace(page_id, table_name);
          break;
        }
        case LogType::UPDATE: {
          update_log->deserialize(buffer_.buffer_ + processed_offset);
          std::string table_name(update_log->table_name_, update_log->table_name_size_);
          int fd = disk_manager_->GetFileFd(table_name);
          PageId page_id{fd, update_log->rid_.GetPageId()};
          analyze_process(log_record, page_id);
          page2tab_name.emplace(page_id, table_name);
          break;
        }
        default:
          throw InternalError("RecoveryManager::analyze: Invalid log type");
      }

      // 4. Update processed_offset to move to the next log record
      processed_offset += log_record->log_tot_len_;
    }

    // If there is unprocessed data at the end of the buffer, move it to the beginning
    if (processed_offset > 0 && processed_offset < buffer_.offset_) {
      memmove(buffer_.buffer_, buffer_.buffer_ + processed_offset, buffer_.offset_ - processed_offset);
    }
    buffer_.offset_ -= processed_offset;
  }
  delete log_record;
  delete insert_log;
  delete delete_log;
  delete update_log;

  // analyze_finish();

  // Note: get the correct min_rec_lsn_ after the checkpoint.
  min_rec_lsn_ = std::max(min_rec_lsn_, checkpoint_lsn);

  // // set log
  // checkpoint->set_att(att_);
  // checkpoint->set_aborted_txns(aborted_txns_);
  // checkpoint->set_dpt_with_tab_name(dpt_, page2tab_name);
  // checkpoint->set_min_rec_lsn(min_rec_lsn_);

  clean_up();
}

/**
 * @description: 重做所有未落盘的操作
 */
void RecoveryManager::redo() {
  // 1. Start scanning from the smallest recLSN in the DPT
  if (min_rec_lsn_ == INVALID_LSN) {
    return;
  }
  int file_offset = lsn_mapping_[min_rec_lsn_].first;
  // int log_size = lsn_mapping_[min_rec_lsn_].second;
  int read_size = 0;

  LogRecord *log_record = new LogRecord();
  InsertLogRecord *insert_log = new InsertLogRecord();
  DeleteLogRecord *delete_log = new DeleteLogRecord();
  UpdateLogRecord *update_log = new UpdateLogRecord();

  while (true) {
    // Read log records from the file
    // read_size =
    //     disk_manager_->read_log(buffer_.buffer_ + buffer_.offset_, LOG_BUFFER_SIZE - buffer_.offset_, file_offset);
    // no more logs to read
    if (read_size <= 0) {
      break;
    }
    int processed_offset = 0;
    file_offset += read_size;
    buffer_.offset_ += read_size;

    while (buffer_.offset_ - processed_offset >= LOG_HEADER_SIZE) {
      log_record->deserialize(buffer_.buffer_ + processed_offset);

      // Check if the buffer contains the entire log record
      if (buffer_.offset_ - processed_offset < log_record->log_tot_len_) {
        // Incomplete log record, wait for more data
        break;
      }

      // 2. Redo log records
      switch (log_record->log_type_) {
        case LogType::INSERT:
          insert_log->deserialize(buffer_.buffer_ + processed_offset);
          redo_insert(insert_log);
          break;
        case LogType::DELETE:
          delete_log->deserialize(buffer_.buffer_ + processed_offset);
          redo_delete(delete_log);
          break;
        case LogType::UPDATE:
          update_log->deserialize(buffer_.buffer_ + processed_offset);
          redo_update(update_log);
          break;
        default:
          break;
      }

      // Update processed_offset to move to the next log record
      processed_offset += log_record->log_tot_len_;
    }

    // If there is unprocessed data at the end of the buffer, move it to the beginning
    if (processed_offset > 0 && processed_offset < buffer_.offset_) {
      memmove(buffer_.buffer_, buffer_.buffer_ + processed_offset, buffer_.offset_ - processed_offset);
    }
    buffer_.offset_ -= processed_offset;
  }
  delete log_record;
  delete insert_log;
  delete delete_log;
  delete update_log;

  redo_index();
}

bool RecoveryManager::redo_skip(LogRecord *log_record, PageId &page_id, Page *page) {
  // Skip if not in DPT or LSN < recLSN
  auto dpt_entry = dpt_.find(page_id);
  if (dpt_entry == dpt_.end() || dpt_entry->second > log_record->lsn_) {
    return true;
  }

  // Skip if pageLSN >= LSN
  if (page->GetLSN() >= log_record->lsn_) {
    buffer_pool_manager_->UnpinPage(page_id, false);
    return true;
  }

  return false;
}

void RecoveryManager::redo_insert(InsertLogRecord *insert_log) {
  std::string tab_name(insert_log->table_name_, insert_log->table_name_size_);
  int fd = disk_manager_->GetFileFd(tab_name);
  PageId page_id{fd, insert_log->rid_.GetPageId()};

  auto fh = sm_manager_->fhs_.at(tab_name).get();
  Page *page = buffer_pool_manager_->RecoverPage(page_id);

  // 1. Skip or not
  if (redo_skip(insert_log, page_id, page)) {
    return;
  }

  // 2. Redo the operation
  auto &tab = sm_manager_->db_.get_table(tab_name);
  int index_len = tab.indexes.size();
  auto &rec = insert_log->insert_value_;
  RID rid = insert_log->rid_;
  // Insert into table
  fh->InsertRecord(rid, rec.data);
  // Insert into index
  if (index_len > 0) {
    tab_name_with_index_.emplace(tab_name);
  }

  // 3. Update the pageLSN
  page->SetLSN(insert_log->lsn_);
  buffer_pool_manager_->UnpinPage(page_id, true);
}

void RecoveryManager::redo_delete(DeleteLogRecord *delete_log) {
  std::string tab_name(delete_log->table_name_, delete_log->table_name_size_);
  int fd = disk_manager_->GetFileFd(tab_name);
  PageId page_id{fd, delete_log->rid_.GetPageId()};

  auto fh = sm_manager_->fhs_.at(tab_name).get();
  Page *page = buffer_pool_manager_->RecoverPage(page_id);

  // 1. Skip or not
  if (redo_skip(delete_log, page_id, page)) {
    return;
  }

  // 2. Redo the operation
  auto &tab = sm_manager_->db_.get_table(tab_name);
  int index_len = tab.indexes.size();
  // auto& rec = delete_log->delete_value_;
  RID rid = delete_log->rid_;
  // Delete from index
  if (index_len > 0) {
    tab_name_with_index_.emplace(tab_name);
  }
  // Delete from table
  fh->DeleteTuple(rid);

  // 3. Update the pageLSN
  page->SetLSN(delete_log->lsn_);
  buffer_pool_manager_->UnpinPage(page_id, true);
}

void RecoveryManager::redo_update(UpdateLogRecord *update_log) {
  std::string tab_name(update_log->table_name_, update_log->table_name_size_);
  int fd = disk_manager_->GetFileFd(tab_name);
  PageId page_id{fd, update_log->rid_.GetPageId()};

  auto fh = sm_manager_->fhs_.at(tab_name).get();
  Page *page = buffer_pool_manager_->RecoverPage(page_id);

  // 1. Skip or not
  if (redo_skip(update_log, page_id, page)) {
    return;
  }

  // 2. Redo the operation
  auto &tab = sm_manager_->db_.get_table(tab_name);
  int index_len = tab.indexes.size();
  // auto& old_rec = update_log->old_value_;
  auto &new_rec = update_log->new_value_;
  RID rid = update_log->rid_;
  // Update table
  fh->UpdateRecord(rid, new_rec.data);
  // Update index
  if (index_len > 0) {
    tab_name_with_index_.emplace(tab_name);
  }

  // 3. Update the pageLSN
  page->SetLSN(update_log->lsn_);
  buffer_pool_manager_->UnpinPage(page_id, true);
}

void RecoveryManager::redo_index() {
  for (auto &tab_name : tab_name_with_index_) {
    auto &tab = sm_manager_->db_.get_table(tab_name);
    for (auto &index : tab.indexes) {
      std::vector<std::string> col_names;
      for (auto col : index.cols) {
        col_names.emplace_back(col.name);
      }
      sm_manager_->DropIndex(tab_name, col_names, nullptr);
      sm_manager_->CreateIndex(tab_name, col_names, nullptr);
    }
  }
}

/**
 * @description: 回滚未完成的事务
 */
void RecoveryManager::undo() {
  LogRecord *log_record = new LogRecord();
  InsertLogRecord *insert_log = new InsertLogRecord();
  DeleteLogRecord *delete_log = new DeleteLogRecord();
  UpdateLogRecord *update_log = new UpdateLogRecord();

  AbortLogRecord *undo_log = new AbortLogRecord();

  // 1. Identify all transactions active at the time of crash (ATT)
  // use a priority queue to sort the transactions by lastLSN
  std::priority_queue<std::pair<lsn_t, txn_id_t>> lsn_pq;
  for (const auto &[txn_id, last_lsn] : att_) {
    lsn_pq.emplace(std::make_pair(last_lsn, txn_id));
  }

  while (!lsn_pq.empty()) {
    // 2.1 Find the transaction with the largest lastLSN
    auto [largest_lsn, txn_id] = lsn_pq.top();
    lsn_pq.pop();

    // 2.2 Undo the operations in reverse LSN order
    // Read the log record at the largest LSN
    int log_offset = lsn_mapping_[largest_lsn].first;
    int log_size = lsn_mapping_[largest_lsn].second;

    // // TODO 完成ReadLog
    // disk_manager_->read_log(buffer_.buffer_, log_size, log_offset);

    log_record->deserialize(buffer_.buffer_);

    // Log the undo like abort
    // check if it's an already aborted txn with a log
    // if not, add a new log to undo the txn
    if (aborted_txns_.find(txn_id) == aborted_txns_.end()) {
      // aborted_txns_[txn_id] = att_[txn_id];
      aborted_txns_.insert(txn_id);
      // new aborted, add a new log
      undo_log->log_tid_ = txn_id;
      undo_log->prev_lsn_ = att_[txn_id];
      lsn_t lsn = log_manager_->add_log_to_buffer(undo_log);
      att_[txn_id] = lsn;
    }
    // // Actually, we don't need this log
    // else {
    //     // already aborted, use the same lsn(prev in aborted, last_lsn in att)
    //     undo_log->prev_lsn_ = aborted_txns_[txn_id];
    //     undo_log->lsn_ = att_[txn_id];
    // }

    // Process the log record for undo
    switch (log_record->log_type_) {
      case LogType::INSERT:
        insert_log->deserialize(buffer_.buffer_);
        undo_insert(insert_log);
        break;
      case LogType::DELETE:
        delete_log->deserialize(buffer_.buffer_);
        undo_delete(delete_log);
        break;
      case LogType::UPDATE:
        update_log->deserialize(buffer_.buffer_);
        undo_update(update_log);
        break;
      default:
        break;
    }

    // If the transaction has been fully undone, remove it from the ATT
    if (log_record->prev_lsn_ == INVALID_LSN) {
      log_manager_->flush_log_to_disk();
      att_.erase(txn_id);
    } else {
      // Add the previous LSN to the priority queue for later processing
      lsn_pq.emplace(std::make_pair(log_record->prev_lsn_, txn_id));
    }
  }
  delete log_record;
  delete insert_log;
  delete delete_log;
  delete update_log;
  delete undo_log;

  clean_up();
}

void RecoveryManager::undo_insert(InsertLogRecord *insert_log) {
  // SmManager::rollback_insert
  std::string table_name(insert_log->table_name_, insert_log->table_name_size_);
  auto fh = sm_manager_->fhs_.at(table_name).get();
  auto &record = insert_log->insert_value_;
  RID rid = insert_log->rid_;
  // Delete from index
  for (auto &index : sm_manager_->db_.get_table(table_name).indexes) {
    auto index_name = sm_manager_->GetIxManager()->GetIndexName(table_name, index.cols);
    auto Iih = sm_manager_->ihs_.at(index_name).get();
    char *key = new char[index.col_tot_len];
    int offset = 0;
    for (int i = 0; i < index.col_num; ++i) {
      memcpy(key + offset, record.data + index.cols[i].offset, index.cols[i].len);
      offset += index.cols[i].len;
    }
    Iih->DeleteEntry(key);
    delete[] key;
  }
  // Delete from table
  fh->DeleteTuple(rid);

  // // TODO: DeleteLogRecord(CLR)
  // DeleteLogRecord del_log_rec(insert_log->log_tid_, record, rid, table_name);
  // del_log_rec.prev_lsn_ = att_[insert_log->log_tid_];  // lastLSN in recovery
  // lsn_t lsn = log_manager_->add_log_to_buffer(&del_log_rec);
  // att_[insert_log->log_tid_] = lsn;
  // // set lsn in page header
  // fh->set_page_lsn(rid.GetPageId(), lsn);

  // Set lsn(abort lsn) in page header(not CLR lsn)
  fh->SetPageLSN(rid.GetPageId(), att_[insert_log->log_tid_]);
}

void RecoveryManager::undo_delete(DeleteLogRecord *delete_log) {
  // SmManager::rollback_delete
  std::string table_name(delete_log->table_name_, delete_log->table_name_size_);
  auto fh = sm_manager_->fhs_.at(table_name).get();
  auto &record = delete_log->delete_value_;
  RID rid = delete_log->rid_;
  // Insert into table
  fh->InsertRecord(rid, record.data);
  // Insert into index
  for (auto &index : sm_manager_->db_.get_table(table_name).indexes) {
    auto index_name = sm_manager_->GetIxManager()->GetIndexName(table_name, index.cols);
    auto Iih = sm_manager_->ihs_.at(index_name).get();
    char *key = new char[index.col_tot_len];
    int offset = 0;
    for (int i = 0; i < index.col_num; ++i) {
      memcpy(key + offset, record.data + index.cols[i].offset, index.cols[i].len);
      offset += index.cols[i].len;
    }
    auto is_insert = Iih->InsertEntry(key, rid);
    if (is_insert == -1) {
      // should not happen because this is logged
      throw InternalError("RecoveryManager::undo_delete: index entry not found");
    }
    delete[] key;
  }

  // // TODO: InsertLogRecord(CLR)
  // InsertLogRecord insert_log_rec(delete_log->log_tid_, record, rid, table_name);
  // insert_log_rec.prev_lsn_ = att_[delete_log->log_tid_];  // lastLSN in recovery
  // lsn_t lsn = log_manager_->add_log_to_buffer(&insert_log_rec);
  // att_[delete_log->log_tid_] = lsn;
  // // set lsn in page header
  // fh->set_page_lsn(rid.GetPageId(), lsn);

  // Set lsn(abort lsn) in page header(not CLR lsn)
  fh->SetPageLSN(rid.GetPageId(), att_[delete_log->log_tid_]);
}

void RecoveryManager::undo_update(UpdateLogRecord *update_log) {
  // SmManager::rollback_update
  std::string table_name(update_log->table_name_, update_log->table_name_size_);
  auto fh = sm_manager_->fhs_.at(table_name).get();
  auto &old_record = update_log->old_value_;
  auto &new_record = update_log->new_value_;
  RID rid = update_log->rid_;

  // Update table
  fh->UpdateRecord(rid, old_record.data);
  // Update index
  for (auto &index : sm_manager_->db_.get_table(table_name).indexes) {
    auto index_name = sm_manager_->GetIxManager()->GetIndexName(table_name, index.cols);
    auto Iih = sm_manager_->ihs_.at(index_name).get();
    char *old_key = new char[index.col_tot_len];
    char *new_key = new char[index.col_tot_len];
    int offset = 0;
    for (int i = 0; i < index.col_num; ++i) {
      memcpy(old_key + offset, old_record.data + index.cols[i].offset, index.cols[i].len);
      memcpy(new_key + offset, new_record.data + index.cols[i].offset, index.cols[i].len);
      offset += index.cols[i].len;
    }
    // check if the key is the same as before
    if (memcmp(old_key, new_key, index.col_tot_len) == 0) {
      delete[] old_key;
      delete[] new_key;
      continue;
    }
    // check if the new key duplicated
    auto is_insert = Iih->InsertEntry(old_key, rid);
    if (is_insert == -1) {
      // should not happen because this is logged
      throw InternalError("RecoveryManager::undo_update: index entry not found");
    }
    Iih->DeleteEntry(new_key);
    delete[] old_key;
    delete[] new_key;
  }

  // // TODO: UpdateLogRecord(CLR)
  // // Note: log after the update, because old_record & new_record will not be changed
  // // after the update, which is different from rollback_update
  // UpdateLogRecord update_log_rec(update_log->log_tid_, new_record, old_record, rid, table_name);
  // update_log_rec.prev_lsn_ = att_[update_log->log_tid_];  // lastLSN in recovery
  // lsn_t lsn = log_manager_->add_log_to_buffer(&update_log_rec);
  // att_[update_log->log_tid_] = lsn;
  // // set lsn in page header
  // fh->set_page_lsn(rid.GetPageId(), lsn);

  // Set lsn(abort lsn) in page header(not CLR lsn)
  fh->SetPageLSN(rid.GetPageId(), att_[update_log->log_tid_]);
}

}  // namespace easydb
