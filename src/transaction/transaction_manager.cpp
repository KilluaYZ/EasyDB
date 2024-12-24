/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "transaction/transaction_manager.h"

#include "common/context.h"
#include "record/rm_file_handle.h"
#include "recovery/log_recovery.h"
#include "system/sm_manager.h"
#include "transaction/txn_defs.h"

namespace easydb {

std::unordered_map<txn_id_t, Transaction *> TransactionManager::txn_map = {};

/**
 * @description: 事务的开始方法
 * @return {Transaction*} 开始事务的指针
 * @param {Transaction*} txn 事务指针，空指针代表需要创建新事务，否则开始已有事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
Transaction *TransactionManager::begin(Transaction *txn, LogManager *log_manager) {
  // Todo:
  // 1. 判断传入事务参数是否为空指针
  // 2. 如果为空指针，创建新事务
  // 3. 把开始事务加入到全局事务表中
  // 4. 返回当前事务指针

  std::scoped_lock lock(latch_);

  // 1. Check if txn is null
  if (txn == nullptr) {
    // 2. Create new transaction if txn is null
    txn = new Transaction(next_txn_id_.fetch_add(1));

    // Assign a start timestamp
    txn->set_start_ts(next_timestamp_.fetch_add(1));
    txn->set_state(TransactionState::DEFAULT);
  }

  // 3. Add transaction to global transaction map
  txn_map[txn->get_transaction_id()] = txn;

  // 4. Log the transaction begin
  BeginLogRecord begin_log_record(txn->get_transaction_id());
  lsn_t lsn = log_manager->add_log_to_buffer(&begin_log_record);
  txn->set_prev_lsn(lsn);

  return txn;
}

/**
 * @description: 事务的提交方法
 * @param {Transaction*} txn 需要提交的事务
 * @param {LogManager*} log_manager 日志管理器指针
 * @todo 提交写操作
 */
void TransactionManager::commit(Transaction *txn, LogManager *log_manager) {
  // Todo:
  // 1. 如果存在未提交的写操作，提交所有的写操作
  // 2. 释放所有锁
  // 3. 释放事务相关资源，eg.锁集
  // 4. 把事务日志刷入磁盘中
  // 5. 更新事务状态

  // std::scoped_lock lock(latch_);

  // Log the commit
  CommitLogRecord commit_log_record(txn->get_transaction_id(), txn->get_prev_lsn());
  lsn_t lsn = log_manager->add_log_to_buffer(&commit_log_record);
  txn->set_prev_lsn(lsn);

  // 1. Commit all uncommitted write operations
  for (auto write_record : *txn->get_write_set()) {
    // // TODO: Commit the write operation
    // std::cout << "Committing write operation: " << write_record->GetWriteType() << std::endl;
    delete write_record;
  }
  txn->get_write_set()->clear();

  // 2. Release all locks
  auto lock_set = *txn->get_lock_set();
  for (auto const &lock_data_id : lock_set) {
    lock_manager_->unlock(txn, lock_data_id);
  }

  // 3. Release transaction-related resources, e.g., lock set, index page sets
  // no need because we delete one by one in unlock()
  // txn->get_lock_set()->clear();
  txn->get_index_latch_page_set()->clear();
  txn->get_index_deleted_page_set()->clear();

  // 4. Flush the log to disk
  log_manager->flush_log_to_disk();

  // 5. Update transaction state
  txn->set_state(TransactionState::COMMITTED);
}

/**
 * @description: 事务的终止（回滚）方法
 * @param {Transaction *} txn 需要回滚的事务
 * @param {LogManager} *log_manager 日志管理器指针
 */
void TransactionManager::abort(Transaction *txn, LogManager *log_manager) {
  // Todo:
  // 1. 回滚所有写操作
  // 2. 释放所有锁
  // 3. 清空事务相关资源，eg.锁集
  // 4. 把事务日志刷入磁盘中
  // 5. 更新事务状态

  // std::scoped_lock lock(latch_);

  // Log the abort
  AbortLogRecord abort_log_record(txn->get_transaction_id(), txn->get_prev_lsn());
  lsn_t lsn = log_manager->add_log_to_buffer(&abort_log_record);
  txn->set_prev_lsn(lsn);

  // 1. Rollback all write operations
  Context *context = new Context(lock_manager_, log_manager, txn, nullptr, 0);
  // Backward scanning
  auto write_set = txn->get_write_set();
  while (!write_set->empty()) {
    auto write_record = write_set->back();
    sm_manager_->Rollback(write_record, context);
    write_set->pop_back();
    delete write_record;
  }
  delete context;

  // 2. Release all locks
  auto lock_set = *txn->get_lock_set();
  for (const auto &lock_data_id : lock_set) {
    lock_manager_->unlock(txn, lock_data_id);
  }

  // 3. Clear transaction-related resources
  // no need because we delete one by one in unlock()
  // txn->get_lock_set()->clear();
  txn->get_index_latch_page_set()->clear();
  txn->get_index_deleted_page_set()->clear();

  // 4. Flush the log to disk
  log_manager->flush_log_to_disk();

  // 5. Update transaction state
  txn->set_state(TransactionState::ABORTED);
}

void TransactionManager::create_static_checkpoint(Transaction *txn, LogManager *log_manager) {
  // std::cout << "Creating static checkpoint" << std::endl;

  std::scoped_lock lock(latch_);

  // 1. Flush log to disk
  log_manager->flush_log_to_disk();

  // 2. Create a checkpoint
  CheckpointLogRecord checkpoint(txn->get_transaction_id(), txn->get_prev_lsn());
  RecoveryManager *recovery = new RecoveryManager(sm_manager_, log_manager);
  recovery->analyze4chkpt(&checkpoint);
  delete recovery;

  // Flush the log to disk
  lsn_t checkpoint_lsn = log_manager->add_log_to_buffer(&checkpoint);
  txn->set_prev_lsn(checkpoint_lsn);
  log_manager->flush_log_to_disk();

  // 3. Flush all dirty pages to disk
  auto bpm = sm_manager_->GetBpm();
  bpm->FlushAllDirtyPages();

  // // 4. Write checkpoint to disk
  // auto disk_mgr = sm_manager_->GetDiskManager();
  // disk_mgr->write_checkpoint(checkpoint_lsn);
}

}  // namespace easydb
