/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * rwmutex.h
 *
 * Identification: src/include/common/rwlatch.h
 *
 *-------------------------------------------------------------------------
 */

/*
 * Original copyright:
 * Copyright (c) 2015-2019, Carnegie Mellon University Database Group
 */

#pragma once

#include <mutex>
#include <shared_mutex>

// #include "common/macros.h"

namespace easydb {

/**
 * @brief 读写锁（Reader-Writer Latch）类
 *
 * ReaderWriterLatch 提供了读写锁的功能，允许多个读者同时访问，但写者独占访问。
 * 该类基于 std::shared_mutex 实现，用于保护共享数据结构的并发访问。
 *
 * 使用场景：
 * - 保护数据库页面的并发访问
 * - 保护索引结构的并发操作
 * - 其他需要读写分离锁定的场景
 */
class ReaderWriterLatch {
 public:
  /**
   * @brief 获取写锁（独占锁）
   * @note
   *   - 写锁是独占的，获取写锁时会阻塞所有读者和写者
   *   - 用于修改操作，确保数据一致性
   */
  void WLock() { mutex_.lock(); }

  /**
   * @brief 释放写锁
   * @note 释放后，等待的读者或写者可以继续执行
   */
  void WUnlock() { mutex_.unlock(); }

  /**
   * @brief 获取读锁（共享锁）
   * @note
   *   - 读锁是共享的，多个读者可以同时持有读锁
   *   - 用于只读操作，提高并发性能
   *   - 如果当前有写锁持有，则阻塞等待
   */
  void RLock() { mutex_.lock_shared(); }

  /**
   * @brief 释放读锁
   * @note 释放后，如果这是最后一个读锁，等待的写者可以继续执行
   */
  void RUnlock() { mutex_.unlock_shared(); }

 private:
  /** @brief 底层的共享互斥锁，提供读写锁功能 */
  std::shared_mutex mutex_;
};

}  // namespace easydb
