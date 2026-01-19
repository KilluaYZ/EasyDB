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
#include "storage/page/page.h"
#include "transaction/txn_defs.h"

namespace easydb {

/**
 * @brief 事务类，表示数据库中的一个事务
 * 
 * Transaction 类封装了事务的所有状态和操作信息，包括：
 * - 事务ID和状态
 * - 隔离级别
 * - 写操作集合（用于回滚）
 * - 锁集合（用于释放锁）
 * - 索引页面集合（用于索引操作）
 */
class Transaction {
 public:
  /**
   * @brief 构造函数，创建新事务
   * @param txn_id 事务ID
   * @param isolation_level 隔离级别，默认为SERIALIZABLE（可串行化）
   */
  explicit Transaction(txn_id_t txn_id, IsolationLevel isolation_level = IsolationLevel::SERIALIZABLE)
      : state_(TransactionState::DEFAULT), isolation_level_(isolation_level), txn_id_(txn_id) {
    write_set_ = std::make_shared<std::deque<WriteRecord *>>();
    lock_set_ = std::make_shared<std::unordered_set<LockDataId>>();
    index_latch_page_set_ = std::make_shared<std::deque<Page *>>();
    index_deleted_page_set_ = std::make_shared<std::deque<Page *>>();
    prev_lsn_ = INVALID_LSN;
    thread_id_ = std::this_thread::get_id();
  }

  /**
   * @brief 析构函数
   */
  ~Transaction() = default;

  /**
   * @brief 获取事务ID
   * @return 事务的唯一标识符
   */
  inline txn_id_t GetTransactionId() { return txn_id_; }

  /**
   * @brief 获取线程ID
   * @return 执行此事务的线程ID
   */
  inline std::thread::id GetThreadId() { return thread_id_; }

  /**
   * @brief 设置事务模式
   * @param txn_mode true表示显式事务，false表示隐式事务（单条SQL语句）
   */
  inline void SetTxnMode(bool txn_mode) { txn_mode_ = txn_mode; }
  
  /**
   * @brief 获取事务模式
   * @return true表示显式事务，false表示隐式事务
   */
  inline bool GetTxnMode() { return txn_mode_; }

  /**
   * @brief 设置事务开始时间戳
   * @param start_ts 开始时间戳
   */
  inline void SetStartTs(timestamp_t start_ts) { start_ts_ = start_ts; }
  
  /**
   * @brief 获取事务开始时间戳
   * @return 开始时间戳
   */
  inline timestamp_t GetStartTs() { return start_ts_; }

  /**
   * @brief 获取隔离级别
   * @return 事务的隔离级别
   */
  inline IsolationLevel GetIsolationLevel() { return isolation_level_; }

  /**
   * @brief 获取事务状态
   * @return 当前事务状态
   */
  inline TransactionState GetState() { return state_; }
  
  /**
   * @brief 设置事务状态
   * @param state 新的事务状态
   */
  inline void SetState(TransactionState state) { state_ = state; }

  /**
   * @brief 获取前一个LSN（日志序列号）
   * @return 当前事务执行的最后一条操作对应的LSN
   */
  inline lsn_t GetPrevLsn() { return prev_lsn_; }
  
  /**
   * @brief 设置前一个LSN
   * @param prev_lsn 新的LSN值
   */
  inline void SetPrevLsn(lsn_t prev_lsn) { prev_lsn_ = prev_lsn; }

  /**
   * @brief 获取写操作集合
   * @return 事务包含的所有写操作的指针
   * @note 用于事务回滚时撤销所有写操作
   */
  inline std::shared_ptr<std::deque<WriteRecord *>> GetWriteSet() { return write_set_; }
  
  /**
   * @brief 添加写操作记录
   * @param write_record 写操作记录指针
   */
  inline void AppendWriteRecord(WriteRecord *write_record) { write_set_->push_back(write_record); }

  /**
   * @brief 获取索引删除页面集合
   * @return 事务执行过程中删除的索引页面的集合
   */
  inline std::shared_ptr<std::deque<Page *>> GetIndexDeletedPageSet() { return index_deleted_page_set_; }
  
  /**
   * @brief 添加索引删除页面
   * @param page 被删除的索引页面指针
   */
  inline void AppendIndexDeletedPage(Page *page) { index_deleted_page_set_->push_back(page); }

  /**
   * @brief 获取索引加锁页面集合
   * @return 事务执行过程中加锁的索引页面的集合
   */
  inline std::shared_ptr<std::deque<Page *>> GetIndexLatchPageSet() { return index_latch_page_set_; }
  
  /**
   * @brief 添加索引加锁页面
   * @param page 加锁的索引页面指针
   */
  inline void AppendIndexLatchPageSet(Page *page) { index_latch_page_set_->push_back(page); }

  /**
   * @brief 获取锁集合
   * @return 事务申请的所有锁的集合
   * @note 用于事务提交或回滚时释放所有锁
   */
  inline std::shared_ptr<std::unordered_set<LockDataId>> GetLockSet() { return lock_set_; }

 private:
  /**
   * @brief 事务模式标志
   * @note true表示显式事务（BEGIN...COMMIT），false表示隐式事务（单条SQL语句）
   */
  bool txn_mode_;
  
  /**
   * @brief 事务状态
   * @note 可能的状态：DEFAULT、GROWING、SHRINKING、COMMITTED、ABORTED
   */
  TransactionState state_;
  
  /**
   * @brief 事务的隔离级别
   * @note 默认隔离级别为SERIALIZABLE（可串行化），提供最高的事务隔离性
   */
  IsolationLevel isolation_level_;
  
  /**
   * @brief 当前事务对应的线程ID
   * @note 用于确保事务只能由创建它的线程访问
   */
  std::thread::id thread_id_;
  
  /**
   * @brief 当前事务执行的最后一条操作对应的LSN（日志序列号）
   * @note 用于系统故障恢复，记录事务的最后操作位置
   */
  lsn_t prev_lsn_;
  
  /**
   * @brief 事务的ID，唯一标识符
   */
  txn_id_t txn_id_;
  
  /**
   * @brief 事务的开始时间戳
   * @note 用于MVCC（多版本并发控制）和死锁预防
   */
  timestamp_t start_ts_;

  /**
   * @brief 事务包含的所有写操作
   * @note 用于事务回滚时撤销所有写操作
   */
  std::shared_ptr<std::deque<WriteRecord *>> write_set_;
  
  /**
   * @brief 事务申请的所有锁
   * @note 用于事务提交或回滚时释放所有锁
   */
  std::shared_ptr<std::unordered_set<LockDataId>> lock_set_;
  
  /**
   * @brief 维护事务执行过程中加锁的索引页面
   * @note 用于事务结束时释放索引页面的锁
   */
  std::shared_ptr<std::deque<Page *>> index_latch_page_set_;
  
  /**
   * @brief 维护事务执行过程中删除的索引页面
   * @note 用于事务回滚时恢复被删除的索引页面
   */
  std::shared_ptr<std::deque<Page *>> index_deleted_page_set_;
};

}  // namespace easydb
