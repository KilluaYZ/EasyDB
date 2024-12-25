/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * transaction.h
 *
 * Identification: src/include/transaction/transaction.h
 *
 *-------------------------------------------------------------------------
 */

/*
 * Original copyright:
 * Copyright (c) 2023 Renmin University of China
 */

#pragma once

#include <deque>
#include <memory>
#include <thread>
#include <unordered_set>

#include "common/config.h"
#include "transaction/txn_defs.h"
#include "storage/page/page.h"

namespace easydb {

class Transaction {
 public:
  explicit Transaction(txn_id_t txn_id, IsolationLevel isolation_level = IsolationLevel::SERIALIZABLE)
      : state_(TransactionState::DEFAULT), isolation_level_(isolation_level), txn_id_(txn_id) {
    write_set_ = std::make_shared<std::deque<WriteRecord *>>();
    lock_set_ = std::make_shared<std::unordered_set<LockDataId>>();
    index_latch_page_set_ = std::make_shared<std::deque<Page *>>();
    index_deleted_page_set_ = std::make_shared<std::deque<Page *>>();
    prev_lsn_ = INVALID_LSN;
    thread_id_ = std::this_thread::get_id();
  }

  ~Transaction() = default;

  inline txn_id_t GetTransactionId() { return txn_id_; }

  inline std::thread::id GetThreadId() { return thread_id_; }

  inline void SetTxnMode(bool txn_mode) { txn_mode_ = txn_mode; }
  inline bool GetTxnMode() { return txn_mode_; }

  inline void SetStartTs(timestamp_t start_ts) { start_ts_ = start_ts; }
  inline timestamp_t GetStartTs() { return start_ts_; }

  inline IsolationLevel GetIsolationLevel() { return isolation_level_; }

  inline TransactionState GetState() { return state_; }
  inline void SetState(TransactionState state) { state_ = state; }

  inline lsn_t GetPrevLsn() { return prev_lsn_; }
  inline void SetPrevLsn(lsn_t prev_lsn) { prev_lsn_ = prev_lsn; }

  inline std::shared_ptr<std::deque<WriteRecord *>> GetWriteSet() { return write_set_; }
  inline void AppendWriteRecord(WriteRecord *write_record) { write_set_->push_back(write_record); }

  inline std::shared_ptr<std::deque<Page *>> GetIndexDeletedPageSet() { return index_deleted_page_set_; }
  inline void AppendIndexDeletedPage(Page *page) { index_deleted_page_set_->push_back(page); }

  inline std::shared_ptr<std::deque<Page *>> GetIndexLatchPageSet() { return index_latch_page_set_; }
  inline void AppendIndexLatchPageSet(Page *page) { index_latch_page_set_->push_back(page); }

  inline std::shared_ptr<std::unordered_set<LockDataId>> GetLockSet() { return lock_set_; }

 private:
  bool txn_mode_;                   // 用于标识当前事务为显式事务还是单条SQL语句的隐式事务
  TransactionState state_;          // 事务状态
  IsolationLevel isolation_level_;  // 事务的隔离级别，默认隔离级别为可串行化
  std::thread::id thread_id_;       // 当前事务对应的线程id
  lsn_t prev_lsn_;                  // 当前事务执行的最后一条操作对应的lsn，用于系统故障恢复
  txn_id_t txn_id_;                 // 事务的ID，唯一标识符
  timestamp_t start_ts_;            // 事务的开始时间戳

  std::shared_ptr<std::deque<WriteRecord *>> write_set_;        // 事务包含的所有写操作
  std::shared_ptr<std::unordered_set<LockDataId>> lock_set_;    // 事务申请的所有锁
  std::shared_ptr<std::deque<Page *>> index_latch_page_set_;    // 维护事务执行过程中加锁的索引页面
  std::shared_ptr<std::deque<Page *>> index_deleted_page_set_;  // 维护事务执行过程中删除的索引页面
};

}  // namespace easydb
