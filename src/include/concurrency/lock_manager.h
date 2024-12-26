/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * lock_manager.h
 *
 * Identification: src/include/concurrency/lock_manager.h
 *
 *-------------------------------------------------------------------------
 */

/*
 * Original copyright:
 * Copyright (c) 2023 Renmin University of China
 */

#pragma once

#include <condition_variable>
#include <functional>
#include <list>
#include <mutex>
#include "transaction/transaction.h"

namespace easydb {

// static const std::string GroupLockModeStr[10] = {"NON_LOCK", "IS", "IX", "S", "X", "SIX"};

class LockManager {
  /* 加锁类型，包括共享锁、排他锁、意向共享锁、意向排他锁、SIX（意向排他锁+共享锁）、GAP（间隙锁） */
  enum class LockMode { SHARED, EXCLUSIVE, INTENTION_SHARED, INTENTION_EXCLUSIVE, S_IX, GAP };

  /* 用于标识加锁队列中排他性最强的锁类型，例如加锁队列中有SHARED和EXLUSIVE两个加锁操作，则该队列的锁模式为X */
  enum class GroupLockMode { NON_LOCK, IS, IX, S, X, SIX, GAP };

  /* 事务的加锁申请 */
  class LockRequest {
   public:
    LockRequest(txn_id_t txn_id, LockMode lock_mode) : txn_id_(txn_id), lock_mode_(lock_mode), granted_(false) {}

    txn_id_t txn_id_;     // 申请加锁的事务ID
    LockMode lock_mode_;  // 事务申请加锁的类型
    bool granted_;        // 该事务是否已经被赋予锁
  };

  /* 数据项上的加锁队列 */
  class LockRequestQueue {
   public:
    std::list<LockRequest> request_queue_;  // 加锁队列
    std::condition_variable cv_;  // 条件变量，用于唤醒正在等待加锁的申请，在no-wait策略下无需使用
    GroupLockMode group_lock_mode_ = GroupLockMode::NON_LOCK;  // 加锁队列的锁模式
    // TODO - OPT: 记录first_lock_pos(group_lock_mode_)，优化 wait-die 中的判断
  };

 public:
  // LockManager() {}

  ~LockManager() { lock_table_.clear(); }

  bool LockSharedOnRecord(Transaction *txn, const RID &rid, int tab_fd);

  bool LockGapOnIndex(Transaction *txn, const Iid &rid, int tab_fd);

  void HandleIndexGapWaitDie(Transaction *txn, const Iid &rid, int tab_fd);

  bool LockExclusiveOnRecord(Transaction *txn, const RID &rid, int tab_fd);

  bool LockSharedOnTable(Transaction *txn, int tab_fd);

  bool LockExclusiveOnTable(Transaction *txn, int tab_fd);

  bool LockISOnTable(Transaction *txn, int tab_fd);

  bool LockIXOnTable(Transaction *txn, int tab_fd);

  bool Unlock(Transaction *txn, LockDataId lock_data_id);

  bool CheckTxnStateLock(Transaction *txn);

  bool CheckTxnStateUnlock(Transaction *txn);

  inline void WaitDie(Transaction *txn, LockRequest &req_holder, LockRequestQueue &queue,
                      std::unique_lock<std::mutex> &lock, std::function<bool()> wake);

 private:
  std::mutex latch_;                                             // 用于锁表的并发
  std::unordered_map<LockDataId, LockRequestQueue> lock_table_;  // 全局锁表
};

}  // namespace easydb
