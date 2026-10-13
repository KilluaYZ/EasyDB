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

#include <algorithm>
#include <atomic>
#include <unordered_map>

#include "storage/easystore/concurrency/lock_manager.h"
#include "storage/easystore/recovery/log_manager.h"
#include "transaction.h"

namespace easydb {

class BufferPoolManager;
class CatalogManager;
class DiskManager;
class DdlManager;
class ClusteredTableManager;

/**
 * @brief 系统采用的并发控制算法枚举
 * @note 当前题目中要求两阶段封锁并发控制算法
 */
enum class ConcurrencyMode {
  TWO_PHASE_LOCKING = 0, /**< 两阶段锁定（2PL）算法 */
  BASIC_TO               /**< 基础TO（时间戳排序）算法 */
};

/**
 * @brief 事务管理器类
 *
 * TransactionManager 负责管理事务的生命周期，包括：
 * - 事务的开始、提交和回滚
 * - 事务ID和时间戳的分配
 * - 与锁管理器和日志管理器的协调
 */
class TransactionManager {
 public:
  TransactionManager(
      LockManager *lock_manager, DiskManager *disk_manager,
      BufferPoolManager *buffer_pool_manager,
      CatalogManager *catalog_manager, DdlManager *ddl_manager,
      ClusteredTableManager *clustered_table_manager = nullptr,
      ConcurrencyMode concurrency_mode = ConcurrencyMode::TWO_PHASE_LOCKING) {
    lock_manager_ = lock_manager;
    disk_manager_ = disk_manager;
    buffer_pool_manager_ = buffer_pool_manager;
    catalog_manager_ = catalog_manager;
    ddl_manager_ = ddl_manager;
    clustered_table_manager_ = clustered_table_manager;
    concurrency_mode_ = concurrency_mode;
  }

  /**
   * @brief 析构函数
   */
  ~TransactionManager() = default;

  /**
   * @brief 开始一个新事务
   * @param txn 事务对象指针
   * @param log_manager 日志管理器指针
   * @return 事务对象指针
   */
  Transaction *Begin(Transaction *txn, LogManager *log_manager);

  /** Rebuild committed transaction state after opening a database. */
  void RecoverFromLogs(LogManager *log_manager);

  /**
   * @brief 提交事务
   * @param txn 要提交的事务对象指针
   * @param log_manager 日志管理器指针
   * @note 释放所有锁，写入提交日志，更新事务状态
   */
  void Commit(Transaction *txn, LogManager *log_manager);

  /**
   * @brief 回滚事务
   * @param txn 要回滚的事务对象指针
   * @param log_manager 日志管理器指针
   * @note 撤销所有写操作，释放所有锁，写入回滚日志，更新事务状态
   */
  void Abort(Transaction *txn, LogManager *log_manager);

  /**
   * @brief 创建静态检查点
   * @param txn 事务对象指针
   * @param log_manager 日志管理器指针
   * @note 用于恢复系统，记录当前所有活跃事务的状态
   */
  void CreateStaticCheckpoint(Transaction *txn, LogManager *log_manager);

  /**
   * @brief 获取并发控制模式
   * @return 当前的并发控制模式
   */
  ConcurrencyMode GetConcurrencyMode() { return concurrency_mode_; }

  /**
   * @brief 设置并发控制模式
   * @param concurrency_mode 新的并发控制模式
   */
  void SetConcurrencyMode(ConcurrencyMode concurrency_mode) {
    concurrency_mode_ = concurrency_mode;
  }

  /**
   * @brief 获取锁管理器
   * @return 锁管理器指针
   */
  LockManager *GetLockManager() { return lock_manager_; }

  /** Create or return the repeatable-read snapshot for a transaction. */
  std::shared_ptr<ReadView> GetReadView(Transaction *txn) {
    if (txn == nullptr) return nullptr;
    std::scoped_lock lock(latch_);
    const auto isolation_level = txn->GetIsolationLevel();
    if (txn->GetReadView() != nullptr &&
        isolation_level != IsolationLevel::READ_COMMITTED &&
        isolation_level != IsolationLevel::READ_UNCOMMITTED) {
      return txn->GetReadView();
    }

    std::unordered_set<txn_id_t> active_txns;
    for (const auto &[txn_id, other] : txn_map) {
      if (other != nullptr && other->GetState() != TransactionState::COMMITTED &&
          other->GetState() != TransactionState::ABORTED) {
        if (isolation_level != IsolationLevel::READ_UNCOMMITTED) {
          active_txns.insert(txn_id);
        }
      }
    }
    auto view = std::make_shared<ReadView>(
        next_timestamp_.load(), txn->GetTransactionId(),
        std::move(active_txns), committed_txns_);
    if (isolation_level != IsolationLevel::READ_COMMITTED &&
        isolation_level != IsolationLevel::READ_UNCOMMITTED) {
      txn->SetReadView(view);
    }
    return view;
  }

  /** Oldest snapshot that may still need a deleted version. */
  timestamp_t GetOldestReadViewTs() {
    std::scoped_lock lock(latch_);
    timestamp_t oldest = next_timestamp_.load();
    for (const auto &[txn_id, txn] : txn_map) {
      if (txn != nullptr && txn->GetReadView() != nullptr &&
          txn->GetState() != TransactionState::ABORTED &&
          txn->GetState() != TransactionState::COMMITTED) {
        oldest = std::min(oldest, txn->GetReadView()->GetSnapshotTs());
      }
    }
    return oldest;
  }

  /**
   * @brief 获取事务ID为txn_id的事务对象
   * @param txn_id 事务ID
   * @return 事务对象的指针，如果事务ID无效则返回nullptr
   * @note 确保返回的事务对象属于当前线程
   */
  Transaction *GetTransaction(txn_id_t txn_id) {
    if (txn_id == INVALID_TXN_ID) return nullptr;

    std::unique_lock<std::mutex> lock(latch_);
    assert(txn_map.find(txn_id) != txn_map.end());
    auto *res = txn_map[txn_id];
    lock.unlock();
    assert(res != nullptr);
    assert(res->GetThreadId() == std::this_thread::get_id());

    return res;
  }

  /**
   * @brief 释放线程在映射表中的所有事务
   * @param thread_id 线程ID
   * @note 删除并释放指定线程的所有事务对象
   */
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

  /**
   * @brief 全局事务表，存放事务ID与事务对象的映射关系
   * @note 每个数据库实例维护独立的事务表
   */
  std::unordered_map<txn_id_t, Transaction *> txn_map;

 private:
  /**
   * @brief 事务使用的并发控制算法
   * @note 目前只需要考虑2PL（两阶段锁定）
   */
  ConcurrencyMode concurrency_mode_;

  /**
   * @brief 用于分发事务ID的原子计数器
   * @note 每次分配新事务时递增，确保事务ID唯一
   */
  std::atomic<txn_id_t> next_txn_id_{0};

  /**
   * @brief 用于分发事务时间戳的原子计数器
   * @note 每次分配新事务时递增，用于MVCC和死锁预防
   */
  std::atomic<timestamp_t> next_timestamp_{0};

  std::unordered_map<txn_id_t, timestamp_t> committed_txns_;

  /**
   * @brief 保护txn_map的互斥锁
   * @note 确保多线程环境下事务表操作的线程安全性
   */
  std::mutex latch_;

  /** @brief 系统管理器指针 */
  DiskManager *disk_manager_ = nullptr;
  BufferPoolManager *buffer_pool_manager_ = nullptr;
  CatalogManager *catalog_manager_ = nullptr;
  DdlManager *ddl_manager_ = nullptr;
  ClusteredTableManager *clustered_table_manager_ = nullptr;

  /** @brief 锁管理器指针 */
  LockManager *lock_manager_;
};

}  // namespace easydb
