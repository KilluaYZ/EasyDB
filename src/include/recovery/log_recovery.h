/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <map>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "common/config.h"
#include "log_manager.h"
#include "storage/disk/disk_manager.h"
#include "storage/page/page.h"
#include "system/sm_manager.h"
#include "transaction/transaction_manager.h"

namespace easydb {
class RedoLogsInPage {
 public:
  RedoLogsInPage() { table_file_ = nullptr; }
  RmFileHandle *table_file_;
  std::vector<lsn_t> redo_logs_;  // 在该page上需要redo的操作的lsn
};

class RecoveryManager {
 public:
  RecoveryManager(SmManager *sm_manager, LogManager *log_manager) {
    disk_manager_ = sm_manager->GetDiskManager();
    buffer_pool_manager_ = sm_manager->GetBpm();
    sm_manager_ = sm_manager;
    min_rec_lsn_ = INVALID_LSN;

    txn_manager_ = nullptr;
    log_manager_ = log_manager;
    last_txn_id_ = INVALID_TXN_ID;
    last_lsn_ = INVALID_LSN;
  }
  RecoveryManager(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, SmManager *sm_manager,
                  TransactionManager *txn_manager, LogManager *log_manager) {
    disk_manager_ = disk_manager;
    buffer_pool_manager_ = buffer_pool_manager;
    sm_manager_ = sm_manager;
    min_rec_lsn_ = INVALID_LSN;

    txn_manager_ = txn_manager;
    log_manager_ = log_manager;
    last_txn_id_ = INVALID_TXN_ID;
    last_lsn_ = INVALID_LSN;
  }

  void analyze();
  void redo();
  void undo();
  void analyze4chkpt(CheckpointLogRecord *checkpoint);

 private:
  LogBuffer buffer_;                        // 读入日志
  DiskManager *disk_manager_;               // 用来读写文件
  BufferPoolManager *buffer_pool_manager_;  // 对页面进行读写
  SmManager *sm_manager_;                   // 访问数据库元数据
  // store the running transactions, the mapping of running transactions to their lastest log records
  std::unordered_map<txn_id_t, lsn_t> att_;    // Active Transaction Table (ATT): txn_id -> last_lsn
  std::unordered_set<txn_id_t> aborted_txns_;  // Aborted Txn (set of aborted txn in ATT)
  std::unordered_map<PageId, lsn_t> dpt_;      // Dirty Page Table (DPT): page_id -> rec_lsn
  lsn_t min_rec_lsn_;
  std::unordered_map<lsn_t, std::pair<int, int>> lsn_mapping_;
  // better instead of must
  TransactionManager *txn_manager_;  // 事务管理器(置next_txn_id_/next_timestamp_)
  LogManager *log_manager_;          // 日志管理器(置global_lsn_/persist_lsn_)
  txn_id_t last_txn_id_;
  lsn_t last_lsn_;
  // for index
  std::unordered_set<std::string> tab_name_with_index_;

  int analyze_checkpoint();
  void analyze_process(LogRecord *log_record, PageId &page_id);
  void analyze_finish();

  void redo_insert(InsertLogRecord *insert_log);
  void redo_delete(DeleteLogRecord *delete_log);
  void redo_update(UpdateLogRecord *update_log);
  bool redo_skip(LogRecord *log_record, PageId &page_id, Page *page);
  void redo_index();

  void undo_insert(InsertLogRecord *insert_log);
  void undo_delete(DeleteLogRecord *delete_log);
  void undo_update(UpdateLogRecord *update_log);

  void format_print() {
    printf("+-------- RecoveryManager --------+\n");
    printf("| ATT: %lu\n", att_.size());
    for (auto &[txn_id, lsn] : att_) {
      printf("|   %d -> %d\n", txn_id, lsn);
    }
    printf("| Aborted Txn (set of aborted txn in ATT): %lu\n", aborted_txns_.size());
    for (auto &txn_id : aborted_txns_) {
      printf("|   txn_id: %d\n", txn_id);
    }
    printf("| DPT: %lu\n", dpt_.size());
    for (auto &[page_id, rec_lsn] : dpt_) {
      printf("|   %d,%d -> %d\n", page_id.fd, page_id.page_no, rec_lsn);
    }
    printf("| Min rec lsn: %d\n", min_rec_lsn_);
    printf("| Last txn id: %d\n", last_txn_id_);
    printf("| Last lsn: %d\n", last_lsn_);
    printf("+---------------------------------+\n");
  }

  void clean_up() {
    // clean up
    buffer_.offset_ = 0;
    buffer_.buffer_[0] = '\0';
    att_.clear();
    aborted_txns_.clear();
    dpt_.clear();
    lsn_mapping_.clear();
    tab_name_with_index_.clear();
  }
};
};  // namespace easydb