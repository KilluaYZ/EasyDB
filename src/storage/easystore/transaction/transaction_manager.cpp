/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * transaction_manager.cpp
 *
 * Identification: src/storage/easystore/transaction/transaction_manager.cpp
 *
 *-------------------------------------------------------------------------
 */

/*
 * Original copyright:
 * Copyright (c) 2023 Renmin University of China
 */

#include "storage/easystore/transaction/transaction_manager.h"

#include "buffer/buffer_pool_manager.h"
#include "common/errors.h"
#include "storage/easystore/catalog/catalog_manager.h"
#include "storage/easystore/catalog/ddl_manager.h"
#include "storage/easystore/recovery/recovery_manager.h"
#include "storage/easystore/table/clustered_table_manager.h"
#include "storage/easystore/transaction/txn_defs.h"

namespace easydb {

/**
 * @description: 事务的开始方法
 * @return {Transaction*} 开始事务的指针
 * @param {Transaction*} txn
 * 事务指针，空指针代表需要创建新事务，否则开始已有事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
Transaction *TransactionManager::Begin(Transaction *txn,
                                       LogManager *log_manager) {
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
    txn->SetStartTs(next_timestamp_.fetch_add(1));
    txn->SetState(TransactionState::DEFAULT);
  }

  // 3. Add transaction to global transaction map
  txn_map[txn->GetTransactionId()] = txn;

  // 4. Log the transaction begin
  BeginLogRecord begin_log_record(txn->GetTransactionId());
  lsn_t lsn = log_manager->add_log_to_buffer(&begin_log_record);
  txn->SetPrevLsn(lsn);

  return txn;
}

/**
 * @description: 事务的提交方法
 * @param {Transaction*} txn 需要提交的事务
 * @param {LogManager*} log_manager 日志管理器指针
 * @todo 提交写操作
 */
void TransactionManager::Commit(Transaction *txn, LogManager *log_manager) {
  // Todo:
  // 1. 如果存在未提交的写操作，提交所有的写操作
  // 2. 释放所有锁
  // 3. 释放事务相关资源，eg.锁集
  // 4. 把事务日志刷入磁盘中
  // 5. 更新事务状态

  // std::scoped_lock lock(latch_);

  // Log the commit
  CommitLogRecord commit_log_record(txn->GetTransactionId(), txn->GetPrevLsn());
  lsn_t lsn = log_manager->add_log_to_buffer(&commit_log_record);
  txn->SetPrevLsn(lsn);

  // 1. Release all locks
  auto lock_set = *txn->GetLockSet();
  for (auto const &lock_data_id : lock_set) {
    lock_manager_->Unlock(txn, lock_data_id);
  }

  // 2. Release transaction-related resources, e.g., lock set, index page sets
  // no need because we delete one by one in unlock()
  // txn->GetLockSet()->clear();
  txn->GetIndexLatchPageSet()->clear();
  txn->GetIndexDeletedPageSet()->clear();

  // 3. Flush the log to disk
  log_manager->flush_log_to_disk();

  // 4. Update transaction state
  txn->SetState(TransactionState::COMMITTED);
}

/**
 * @description: 事务的终止（回滚）方法
 * @param {Transaction *} txn 需要回滚的事务
 * @param {LogManager} *log_manager 日志管理器指针
 */
void TransactionManager::Abort(Transaction *txn, LogManager *log_manager) {
  // Todo:
  // 1. 回滚所有写操作
  // 2. 释放所有锁
  // 3. 清空事务相关资源，eg.锁集
  // 4. 把事务日志刷入磁盘中
  // 5. 更新事务状态

  // std::scoped_lock lock(latch_);

  // Log the abort
  AbortLogRecord abort_log_record(txn->GetTransactionId(), txn->GetPrevLsn());
  lsn_t lsn = log_manager->add_log_to_buffer(&abort_log_record);
  txn->SetPrevLsn(lsn);

  // 1. Roll back from durable Undo Logs.
  if (clustered_table_manager_ == nullptr) {
    throw InternalError("clustered storage is required for Undo rollback");
  }
  RecoveryManager recovery(clustered_table_manager_, log_manager);
  recovery.UndoTransaction(txn->GetTransactionId());

  // 2. Release all locks
  auto lock_set = *txn->GetLockSet();
  for (const auto &lock_data_id : lock_set) {
    lock_manager_->Unlock(txn, lock_data_id);
  }

  // 3. Clear transaction-related resources
  // no need because we delete one by one in unlock()
  // txn->GetLockSet()->clear();
  txn->GetIndexLatchPageSet()->clear();
  txn->GetIndexDeletedPageSet()->clear();

  // 4. Flush the log to disk
  log_manager->flush_log_to_disk();

  // 5. Update transaction state
  txn->SetState(TransactionState::ABORTED);
}

void TransactionManager::CreateStaticCheckpoint(Transaction *txn,
                                                LogManager *log_manager) {
  // std::cout << "Creating static checkpoint" << std::endl;

  std::scoped_lock lock(latch_);

  // 1. Flush log to disk
  log_manager->flush_log_to_disk();

  // 2. Create a checkpoint
  CheckpointLogRecord checkpoint(txn->GetTransactionId(), txn->GetPrevLsn());
  // Flush the log to disk
  lsn_t checkpoint_lsn = log_manager->add_log_to_buffer(&checkpoint);
  txn->SetPrevLsn(checkpoint_lsn);
  log_manager->flush_log_to_disk();

  // 3. Flush all dirty pages to disk
  auto *bpm = buffer_pool_manager_;
  bpm->FlushAllDirtyPages();

  // // 4. Write checkpoint to disk
  // disk_mgr->write_checkpoint(checkpoint_lsn);
}

}  // namespace easydb
