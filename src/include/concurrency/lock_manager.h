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

/**
 * @brief 锁管理器类，负责管理数据库中的锁
 * 
 * LockManager 实现了两阶段锁定（2PL）协议，支持多种锁类型和死锁预防策略。
 * 主要功能：
 * - 管理表级锁和行级锁
 * - 支持锁的升级和降级
 * - 实现wait-die死锁预防策略
 * - 维护全局锁表
 */
class LockManager {
  /**
   * @brief 锁模式枚举
   * @note 定义了系统中支持的所有锁类型
   */
  enum class LockMode {
    SHARED,              /**< 共享锁（S锁），允许多个事务同时读取 */
    EXCLUSIVE,           /**< 排他锁（X锁），独占访问，不允许其他事务读取或写入 */
    INTENTION_SHARED,    /**< 意向共享锁（IS锁），表示事务将在表的某些行上加S锁 */
    INTENTION_EXCLUSIVE,  /**< 意向排他锁（IX锁），表示事务将在表的某些行上加X锁 */
    S_IX,                /**< SIX锁（意向排他锁+共享锁），表级锁，允许在表上加S锁，在行上加X锁 */
    GAP                  /**< 间隙锁，用于索引，防止幻读 */
  };

  /**
   * @brief 组锁模式枚举
   * @note 用于标识加锁队列中排他性最强的锁类型
   *       例如：如果队列中有SHARED和EXCLUSIVE两个加锁操作，则该队列的锁模式为X
   */
  enum class GroupLockMode {
    NON_LOCK,  /**< 无锁 */
    IS,         /**< 意向共享锁 */
    IX,         /**< 意向排他锁 */
    S,          /**< 共享锁 */
    X,          /**< 排他锁 */
    SIX,        /**< SIX锁 */
    GAP         /**< 间隙锁 */
  };

  /**
   * @brief 事务的加锁申请类
   * 
   * LockRequest 表示一个事务对某个数据项的加锁请求。
   */
  class LockRequest {
   public:
    /**
     * @brief 构造函数
     * @param txn_id 申请加锁的事务ID
     * @param lock_mode 事务申请加锁的类型
     */
    LockRequest(txn_id_t txn_id, LockMode lock_mode) : txn_id_(txn_id), lock_mode_(lock_mode), granted_(false) {}

    /** @brief 申请加锁的事务ID */
    txn_id_t txn_id_;
    
    /** @brief 事务申请加锁的类型 */
    LockMode lock_mode_;
    
    /** @brief 该事务是否已经被赋予锁 */
    bool granted_;
  };

  /**
   * @brief 数据项上的加锁队列类
   * 
   * LockRequestQueue 管理对某个数据项（表或行）的所有加锁请求。
   */
  class LockRequestQueue {
   public:
    /** @brief 加锁请求队列，按FIFO顺序存储锁请求 */
    std::list<LockRequest> request_queue_;
    
    /**
     * @brief 条件变量，用于唤醒正在等待加锁的申请
     * @note 在no-wait策略下无需使用，在wait-die策略下用于等待和唤醒
     */
    std::condition_variable cv_;
    
    /**
     * @brief 加锁队列的锁模式（排他性最强的锁类型）
     * @note 用于快速判断队列中是否有冲突的锁请求
     */
    GroupLockMode group_lock_mode_ = GroupLockMode::NON_LOCK;
    
    // TODO - OPT: 记录first_lock_pos(group_lock_mode_)，优化 wait-die 中的判断
  };

 public:
  // LockManager() {}

  /**
   * @brief 析构函数，清空锁表
   */
  ~LockManager() { lock_table_.clear(); }

  /**
   * @brief 在记录上申请共享锁（行级读锁）
   * @param txn 要申请锁的事务对象指针
   * @param rid 加锁的目标记录ID
   * @param tab_fd 记录所在的表的文件描述符
   * @return true 如果加锁成功，false 否则
   */
  bool LockSharedOnRecord(Transaction *txn, const RID &rid, int tab_fd);

  /**
   * @brief 在索引间隙上申请间隙锁
   * @param txn 要申请锁的事务对象指针
   * @param iid 索引项ID
   * @param tab_fd 表文件描述符
   * @return true 如果加锁成功，false 否则
   */
  bool LockGapOnIndex(Transaction *txn, const Iid &rid, int tab_fd);

  /**
   * @brief 处理索引间隙锁的wait-die机制
   * @param txn 等待间隙锁的事务
   * @param iid 索引项ID
   * @param tab_fd 表文件描述符
   */
  void HandleIndexGapWaitDie(Transaction *txn, const Iid &rid, int tab_fd);

  /**
   * @brief 在记录上申请排他锁（行级写锁）
   * @param txn 要申请锁的事务对象指针
   * @param rid 加锁的目标记录ID
   * @param tab_fd 记录所在的表的文件描述符
   * @return true 如果加锁成功，false 否则
   */
  bool LockExclusiveOnRecord(Transaction *txn, const RID &rid, int tab_fd);

  /**
   * @brief 在表上申请共享锁（表级读锁）
   * @param txn 要申请锁的事务对象指针
   * @param tab_fd 目标表的文件描述符
   * @return true 如果加锁成功，false 否则
   */
  bool LockSharedOnTable(Transaction *txn, int tab_fd);

  /**
   * @brief 在表上申请排他锁（表级写锁）
   * @param txn 要申请锁的事务对象指针
   * @param tab_fd 目标表的文件描述符
   * @return true 如果加锁成功，false 否则
   */
  bool LockExclusiveOnTable(Transaction *txn, int tab_fd);

  /**
   * @brief 在表上申请意向共享锁（IS锁）
   * @param txn 要申请锁的事务对象指针
   * @param tab_fd 目标表的文件描述符
   * @return true 如果加锁成功，false 否则
   */
  bool LockISOnTable(Transaction *txn, int tab_fd);

  /**
   * @brief 在表上申请意向排他锁（IX锁）
   * @param txn 要申请锁的事务对象指针
   * @param tab_fd 目标表的文件描述符
   * @return true 如果加锁成功，false 否则
   */
  bool LockIXOnTable(Transaction *txn, int tab_fd);

  /**
   * @brief 释放锁
   * @param txn 要释放锁的事务对象指针
   * @param lock_data_id 要释放的锁ID
   * @return true 如果解锁成功，false 否则
   */
  bool Unlock(Transaction *txn, LockDataId lock_data_id);

  /**
   * @brief 检查事务状态，判断是否可以加锁
   * @param txn 事务对象指针
   * @return true 如果可以加锁，false 否则
   * @note 遵循SS2PL（严格两阶段锁定）协议
   */
  bool CheckTxnStateLock(Transaction *txn);

  /**
   * @brief 检查事务状态，判断是否可以解锁
   * @param txn 事务对象指针
   * @return true 如果可以解锁，false 否则
   * @note 遵循SS2PL（严格两阶段锁定）协议
   */
  bool CheckTxnStateUnlock(Transaction *txn);

  /**
   * @brief Wait-Die死锁预防机制
   * @param txn 请求锁的事务
   * @param req_holder 持有锁的请求
   * @param queue 锁请求队列
   * @param lock 锁管理器的互斥锁
   * @param wake 唤醒条件函数
   * @note 
   *   - 如果请求事务比持有事务更老，则等待
   *   - 如果请求事务比持有事务更年轻，则中止
   */
  inline void WaitDie(Transaction *txn, LockRequest &req_holder, LockRequestQueue &queue,
                      std::unique_lock<std::mutex> &lock, std::function<bool()> wake);

 private:
  /**
   * @brief 保护锁表的互斥锁，用于确保锁表操作的线程安全性
   */
  std::mutex latch_;
  
  /**
   * @brief 全局锁表，存储所有数据项的锁请求队列
   * @note Key: LockDataId（数据项标识），Value: LockRequestQueue（锁请求队列）
   */
  std::unordered_map<LockDataId, LockRequestQueue> lock_table_;
};

}  // namespace easydb
