/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * transaction_manager.h
 *
 * Identification: src/include/transaction/transaction_manager.h
 *
 *-------------------------------------------------------------------------
 */

/*
 * Original copyright:
 * Copyright (c) 2023 Renmin University of China
 */

#pragma once

#include <atomic>
#include <unordered_map>

#include "concurrency/lock_manager.h"
#include "recovery/log_manager.h"
#include "system/sm_manager.h"
#include "transaction.h"

namespace easydb {

/* 系统采用的并发控制算法，当前题目中要求两阶段封锁并发控制算法 */
enum class ConcurrencyMode { TWO_PHASE_LOCKING = 0, BASIC_TO };

class TransactionManager {
  friend class RecoveryManager;

 public:
  explicit TransactionManager(LockManager *lock_manager, SmManager *sm_manager,
                              ConcurrencyMode concurrency_mode = ConcurrencyMode::TWO_PHASE_LOCKING) {
    sm_manager_ = sm_manager;
    lock_manager_ = lock_manager;
    concurrency_mode_ = concurrency_mode;
  }

  ~TransactionManager() = default;

  Transaction *Begin(Transaction *txn, LogManager *log_manager);

  void Commit(Transaction *txn, LogManager *log_manager);

  void Abort(Transaction *txn, LogManager *log_manager);

  void CreateStaticCheckpoint(Transaction *txn, LogManager *log_manager);

  ConcurrencyMode GetConcurrencyMode() { return concurrency_mode_; }

  void SetConcurrencyMode(ConcurrencyMode concurrency_mode) { concurrency_mode_ = concurrency_mode; }

  LockManager *GetLockManager() { return lock_manager_; }

  /**
   * @description: 获取事务ID为txn_id的事务对象
   * @return {Transaction*} 事务对象的指针
   * @param {txn_id_t} txn_id 事务ID
   */
  Transaction *GetTransaction(txn_id_t txn_id) {
    if (txn_id == INVALID_TXN_ID) return nullptr;

    std::unique_lock<std::mutex> lock(latch_);
    assert(TransactionManager::txn_map.find(txn_id) != TransactionManager::txn_map.end());
    auto *res = TransactionManager::txn_map[txn_id];
    lock.unlock();
    assert(res != nullptr);
    assert(res->GetThreadId() == std::this_thread::get_id());

    return res;
  }

  // release txn of the thread in the map
  void ReleaseTxnOfThread(std::thread::id thread_id) {
    std::unique_lock<std::mutex> lock(latch_);
    for (auto it = txn_map.begin(); it != txn_map.end();) {
      if (it->second->GetThreadId() == thread_id) {
        delete it->second;
        it = txn_map.erase(it);
      } else {
        ++it;
      }
    }
    lock.unlock();
  }

  static std::unordered_map<txn_id_t, Transaction *> txn_map;  // 全局事务表，存放事务ID与事务对象的映射关系

 private:
  ConcurrencyMode concurrency_mode_;            // 事务使用的并发控制算法，目前只需要考虑2PL
  std::atomic<txn_id_t> next_txn_id_{0};        // 用于分发事务ID
  std::atomic<timestamp_t> next_timestamp_{0};  // 用于分发事务时间戳
  std::mutex latch_;                            // 用于txn_map的并发
  SmManager *sm_manager_;
  LockManager *lock_manager_;
};

}  // namespace easydb
