/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * lock_manager.cpp
 *
 * Identification: src/concurrency/lock_manager.cpp
 *
 *-------------------------------------------------------------------------
 */

/*
 * Original copyright:
 * Copyright (c) 2023 Renmin University of China
 */

#include "concurrency/lock_manager.h"

#include "common/errors.h"
#include "transaction/txn_defs.h"

namespace easydb {

/**
 * @description: 申请行级共享锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID 记录所在的表的fd
 * @param {int} tab_fd
 */
bool LockManager::LockSharedOnRecord(Transaction *txn, const RID &rid, int tab_fd) {
  // 1. Check the txn state(SS2PL)
  if (!CheckTxnStateLock(txn)) {
    return false;
  }

  // 2. Check if the lock is already granted
  LockDataId lock_data_id(tab_fd, rid, LockDataType::RECORD);
  // Acquire lock on the global lock table for thread safety
  std::unique_lock<std::mutex> lock(latch_);
  // Find or create the LockRequestQueue
  LockRequestQueue &request_queue = lock_table_[lock_data_id];
  for (auto &req : request_queue.request_queue_) {
    if (req.txn_id_ == txn->GetTransactionId()) {
      // Lock is already granted, return true
      // Note: In the current design, there is only S or X lock on record
      return true;
    }
  }

  // // Another way to check if the lock is already granted(just for S)
  // if (txn->GetLockSet()->count(lock_data_id) > 0) {
  //     // Lock is already granted, return true
  //     // Note: In the current design, there is only S or X lock on record
  //     return true;
  // }

  // 3. Check the group lock mode
  // Create a new lock request for shared mode
  LockRequest lock_request(txn->GetTransactionId(), LockMode::SHARED);
  // 3.1 If there is a conflicting lock request
  if (request_queue.group_lock_mode_ == GroupLockMode::X) {
    // /* no-wait */
    // throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK_PREVENTION);
    /* wait-die */
    // the condition to wake
    auto wake = [&]() {
      if (request_queue.group_lock_mode_ == GroupLockMode::NON_LOCK ||
          request_queue.group_lock_mode_ == GroupLockMode::S) {
        return true;
      }
      return false;
    };
    // request_queue.cv_.wait(lock, wake);
    for (auto &req : request_queue.request_queue_) {
      if (req.lock_mode_ == LockMode::EXCLUSIVE) {
        WaitDie(txn, req, request_queue, lock, wake);
        break;
      }
    }
  }

  // 3.2 If no conflicts, grant the lock
  lock_request.granted_ = true;
  // Add the lock request to the queue
  request_queue.request_queue_.emplace_back(lock_request);
  // Update the group lock mode(change from NON_LOCK or S)
  request_queue.group_lock_mode_ = GroupLockMode::S;
  txn->GetLockSet()->emplace(lock_data_id);

  return true;
}

/**
 * @brief Locks a gap on an index for a transaction.
 *
 * This function is responsible for locking a gap on an index for a given transaction.
 * It follows the SS2PL (Strict Two-Phase Locking) protocol to ensure transaction isolation and consistency.
 *
 * @param txn Pointer to the transaction object.
 * @param iid The identifier of the index gap to be locked.
 * @param tab_fd The file descriptor of the table containing the index.
 * @return True if the lock is successfully granted, false otherwise.
 */
bool LockManager::LockGapOnIndex(Transaction *txn, const Iid &iid, int tab_fd) {
  // 1. Check the txn state(SS2PL)
  if (!CheckTxnStateLock(txn)) {
    return false;
  }

  // 2. Check if the lock is already granted
  LockDataId lock_data_id(tab_fd, iid, LockDataType::GAP);
  std::unique_lock<std::mutex> lock(latch_);
  LockRequestQueue &request_queue = lock_table_[lock_data_id];
  for (auto &req : request_queue.request_queue_) {
    if (req.txn_id_ == txn->GetTransactionId()) {
      return true;
    }
  }
  // Because LockDataId's type is GAP, group lock mode will always be NON_LOCK/GAP
  // We don't need to check group lock mode because there is no conflict lock request
  // 3. Grant the lock
  LockRequest lock_request(txn->GetTransactionId(), LockMode::GAP);
  lock_request.granted_ = true;
  request_queue.request_queue_.emplace_back(lock_request);
  // Update the group lock mode(change from NON_LOCK/GAP)
  request_queue.group_lock_mode_ = GroupLockMode::GAP;
  txn->GetLockSet()->insert(lock_data_id);

  return true;
}

/**
 * Handles the wait-die mechanism for transactions waiting on a gap lock in the lock manager.
 * This function is called when a transaction is waiting for a gap lock on a specific index.
 * It checks if there are any lock requests from other transactions and applies the wait-die mechanism if necessary.
 *
 * @param txn       The transaction waiting for the gap lock.
 * @param iid       The identifier of the index.
 * @param tab_fd    The file descriptor of the table.
 */
void LockManager::HandleIndexGapWaitDie(Transaction *txn, const Iid &iid, int tab_fd) {
  LockDataId lock_data_id(tab_fd, iid, LockDataType::GAP);
  std::unique_lock<std::mutex> lock(latch_);
  LockRequestQueue &request_queue = lock_table_[lock_data_id];
  // the condition to wake
  auto wake = [&]() {
    // If no other lock request or the lock request is from the same transaction, wake
    // Note that group_lock_mode_ will only be NON_LOCK or GAP
    if (request_queue.group_lock_mode_ == GroupLockMode::NON_LOCK) {
      return true;
    }
    for (auto &req : request_queue.request_queue_) {
      if (req.txn_id_ != txn->GetTransactionId()) {
        return false;
      }
    }
    return true;
  };
  // If there is a lock request from other transactions, wait
  for (auto &req : request_queue.request_queue_) {
    if (req.txn_id_ != txn->GetTransactionId()) {
      /* wait-die */
      WaitDie(txn, req, request_queue, lock, wake);
      break;
    }
  }
}

/**
 * @description: 申请行级排他锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID
 * @param {int} tab_fd 记录所在的表的fd
 */
bool LockManager::LockExclusiveOnRecord(Transaction *txn, const RID &rid, int tab_fd) {
  // 1. Check the txn state(SS2PL)
  if (!CheckTxnStateLock(txn)) {
    return false;
  }

  // 2. Check if the lock is already granted
  LockDataId lock_data_id(tab_fd, rid, LockDataType::RECORD);
  // Acquire lock on the global lock table for thread safety
  std::unique_lock<std::mutex> lock(latch_);
  // Find or create the LockRequestQueue
  LockRequestQueue &request_queue = lock_table_[lock_data_id];
  for (auto &req : request_queue.request_queue_) {
    if (req.txn_id_ == txn->GetTransactionId()) {
      if (req.lock_mode_ == LockMode::EXCLUSIVE) {
        return true;
      } else {
        // S lock, try to upgrade to X
        // Note: Now, lock_mode_ will at least be S
        auto upgrade = [&]() {
          if (request_queue.request_queue_.size() == 1) {
            return true;
          }
          return false;
        };
        // If there are other lock requests, it will conflict with them
        if (request_queue.request_queue_.size() > 1) {
          // /* no-wait */
          // throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK_PREVENTION);
          /* wait-die */
          for (auto &r : request_queue.request_queue_) {
            if (r.txn_id_ != txn->GetTransactionId()) {
              WaitDie(txn, r, request_queue, lock, upgrade);
              break;
            }
          }
        }
        // There is no other lock request, upgrade
        req.lock_mode_ = LockMode::EXCLUSIVE;
        request_queue.group_lock_mode_ = GroupLockMode::X;
        return true;
      }
    }
  }

  // 3. Check the group lock mode
  LockRequest lock_request(txn->GetTransactionId(), LockMode::EXCLUSIVE);
  // Note: There is only S or X or NON_LOCK lock on record
  // 3.1 If there is a conflicting lock request
  if (request_queue.group_lock_mode_ != GroupLockMode::NON_LOCK) {
    // /* no-wait */
    // throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK_PREVENTION);
    /* wait-die */
    auto wake = [&]() {
      if (request_queue.group_lock_mode_ == GroupLockMode::NON_LOCK) {
        return true;
      }
      return false;
    };
    for (auto &req : request_queue.request_queue_) {
      // // Don't need check actually
      // if (req.txn_id_ != txn->GetTransactionId()) {
      // }
      WaitDie(txn, req, request_queue, lock, wake);
      break;
    }
  }

  // 3.2 If no conflicts, grant the lock
  lock_request.granted_ = true;
  // Add the lock request to the queue
  request_queue.request_queue_.emplace_back(lock_request);
  // Update the group lock mode(change from NON_LOCK or S)
  request_queue.group_lock_mode_ = GroupLockMode::X;
  txn->GetLockSet()->insert(lock_data_id);

  return true;
}

/**
 * @description: 申请表级读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::LockSharedOnTable(Transaction *txn, int tab_fd) {
  // 1. Check the txn state(SS2PL)
  if (!CheckTxnStateLock(txn)) {
    return false;
  }

  // 2. Check if the lock is already granted
  LockDataId lock_data_id(tab_fd, LockDataType::TABLE);
  // Acquire lock on the global lock table for thread safety
  std::unique_lock<std::mutex> lock(latch_);
  // Find or create the LockRequestQueue
  LockRequestQueue &request_queue = lock_table_[lock_data_id];
  // Condition to wake
  auto wake = [&]() {
    if (request_queue.group_lock_mode_ == GroupLockMode::NON_LOCK ||
        request_queue.group_lock_mode_ == GroupLockMode::IS || request_queue.group_lock_mode_ == GroupLockMode::S) {
      return true;
    }
    return false;
  };
  for (auto &req : request_queue.request_queue_) {
    if (req.txn_id_ == txn->GetTransactionId()) {
      if (req.lock_mode_ == LockMode::SHARED || req.lock_mode_ == LockMode::S_IX ||
          req.lock_mode_ == LockMode::EXCLUSIVE) {
        return true;
      } else if (req.lock_mode_ == LockMode::INTENTION_SHARED) {
        // IS lock，try to upgrade to S
        if (request_queue.group_lock_mode_ == GroupLockMode::IS || request_queue.group_lock_mode_ == GroupLockMode::S) {
        } else {
          // /* no-wait */
          // throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK_PREVENTION);
          /* wait-die */
          for (auto &r : request_queue.request_queue_) {
            if (r.txn_id_ != txn->GetTransactionId() &&
                (r.lock_mode_ == LockMode::INTENTION_EXCLUSIVE || r.lock_mode_ == LockMode::S_IX ||
                 r.lock_mode_ == LockMode::EXCLUSIVE)) {
              WaitDie(txn, r, request_queue, lock, wake);
              break;
            }
          }
        }
        req.lock_mode_ = LockMode::SHARED;
        request_queue.group_lock_mode_ = GroupLockMode::S;
        return true;
      } else {
        // IX lock，try to upgrade to S_IX
        // Note: Now, lock_mode_ will must be IX
        auto upgrade = [&]() {
          for (auto &r : request_queue.request_queue_) {
            if (r.lock_mode_ == LockMode::INTENTION_EXCLUSIVE && r.txn_id_ != txn->GetTransactionId()) {
              return false;
            }
          }
          return true;
        };
        // If there are other IX lock requests, it will conflict with them
        for (auto &r : request_queue.request_queue_) {
          if (r.lock_mode_ == LockMode::INTENTION_EXCLUSIVE && r.txn_id_ != txn->GetTransactionId()) {
            /* wait-die */
            WaitDie(txn, r, request_queue, lock, upgrade);
            break;
          }
        }
        // There is no other IX lock request, upgrade
        req.lock_mode_ = LockMode::S_IX;
        request_queue.group_lock_mode_ = GroupLockMode::SIX;
        return true;
      }
    }
  }

  // 3. Check the group lock mode
  LockRequest lock_request(txn->GetTransactionId(), LockMode::SHARED);
  // 3.1 If there is a conflicting lock request
  if (request_queue.group_lock_mode_ == GroupLockMode::IX || request_queue.group_lock_mode_ == GroupLockMode::SIX ||
      request_queue.group_lock_mode_ == GroupLockMode::X) {
    // /* no-wait */
    // throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK_PREVENTION);
    /* wait-die */
    for (auto &req : request_queue.request_queue_) {
      if (req.lock_mode_ == LockMode::INTENTION_EXCLUSIVE || req.lock_mode_ == LockMode::S_IX ||
          req.lock_mode_ == LockMode::EXCLUSIVE) {
        WaitDie(txn, req, request_queue, lock, wake);
        break;
      }
    }
  }

  // 3.2 If no conflicts, grant the lock
  lock_request.granted_ = true;
  // Add the lock request to the queue
  request_queue.request_queue_.emplace_back(lock_request);
  // Update the group lock mode(change from NON_LOCK/IS/S)
  request_queue.group_lock_mode_ = GroupLockMode::S;
  txn->GetLockSet()->emplace(lock_data_id);

  return true;
}

/**
 * @description: 申请表级写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::LockExclusiveOnTable(Transaction *txn, int tab_fd) {
  // 1. Check the txn state(SS2PL)
  if (!CheckTxnStateLock(txn)) {
    return false;
  }

  // 2. Check if the lock is already granted
  LockDataId lock_data_id(tab_fd, LockDataType::TABLE);
  // Acquire lock on the global lock table for thread safety
  std::unique_lock<std::mutex> lock(latch_);
  // Find or create the LockRequestQueue
  LockRequestQueue &request_queue = lock_table_[lock_data_id];

  for (auto &req : request_queue.request_queue_) {
    if (req.txn_id_ == txn->GetTransactionId()) {
      if (req.lock_mode_ == LockMode::EXCLUSIVE) {
        return true;
      } else {
        // Try to upgrade to X
        auto upgrade2X = [&]() {
          if (request_queue.request_queue_.size() == 1) {
            return true;
          }
          return false;
        };
        // If there are other lock requests, it will conflict with them
        if (request_queue.request_queue_.size() != 1) {
          // /* no-wait */
          // throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK_PREVENTION);
          /* wait-die */
          for (auto &r : request_queue.request_queue_) {
            if (r.txn_id_ != txn->GetTransactionId()) {
              WaitDie(txn, r, request_queue, lock, upgrade2X);
              break;
            }
          }
        }
        // There is no other lock request, upgrade
        req.lock_mode_ = LockMode::EXCLUSIVE;
        request_queue.group_lock_mode_ = GroupLockMode::X;
        return true;
      }
    }
  }

  // 3. Check the group lock mode
  LockRequest lock_request(txn->GetTransactionId(), LockMode::EXCLUSIVE);
  // 3.1 If there is a conflicting lock request
  if (request_queue.group_lock_mode_ != GroupLockMode::NON_LOCK) {
    // /* no-wait */
    // throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK_PREVENTION);
    /* wait-die */
    auto wake = [&]() {
      if (request_queue.group_lock_mode_ == GroupLockMode::NON_LOCK) {
        return true;
      }
      return false;
    };
    for (auto &req : request_queue.request_queue_) {
      WaitDie(txn, req, request_queue, lock, wake);
      break;
    }
  }

  // 3.2 If no conflicts, grant the lock
  lock_request.granted_ = true;
  // Add the lock request to the queue
  request_queue.request_queue_.emplace_back(lock_request);
  // Update the group lock mode(change from NON_LOCK)
  request_queue.group_lock_mode_ = GroupLockMode::X;
  txn->GetLockSet()->insert(lock_data_id);

  return true;
}

/**
 * @description: 申请表级意向读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::LockISOnTable(Transaction *txn, int tab_fd) {
  // 1. Check the txn state(SS2PL)
  if (!CheckTxnStateLock(txn)) {
    return false;
  }

  // 2. Check if the lock is already granted
  LockDataId lock_data_id(tab_fd, LockDataType::TABLE);
  // Acquire lock on the global lock table for thread safety
  std::unique_lock<std::mutex> lock(latch_);
  // Find or create the LockRequestQueue
  LockRequestQueue &request_queue = lock_table_[lock_data_id];
  for (auto &req : request_queue.request_queue_) {
    if (req.txn_id_ == txn->GetTransactionId()) {
      return true;
    }
  }

  // 3. Check the group lock mode
  LockRequest lock_request(txn->GetTransactionId(), LockMode::INTENTION_SHARED);
  // 3.1 If there is a conflicting lock request
  if (request_queue.group_lock_mode_ == GroupLockMode::X) {
    // /* no-wait */
    // throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK_PREVENTION);
    /* wait-die */
    auto wake = [&]() {
      if (request_queue.group_lock_mode_ != GroupLockMode::X) {
        return true;
      }
      return false;
    };
    for (auto &req : request_queue.request_queue_) {
      if (req.lock_mode_ == LockMode::EXCLUSIVE) {
        WaitDie(txn, req, request_queue, lock, wake);
        break;
      }
    }
  }
  // 3.2 If no conflicts, grant the lock
  lock_request.granted_ = true;
  // Add the lock request to the queue
  request_queue.request_queue_.emplace_back(lock_request);
  // Update group_lock_mode_ iff there is no lock
  if (request_queue.group_lock_mode_ == GroupLockMode::NON_LOCK) {
    request_queue.group_lock_mode_ = GroupLockMode::IS;
  }
  txn->GetLockSet()->emplace(lock_data_id);

  return true;
}

/**
 * @description: 申请表级意向写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::LockIXOnTable(Transaction *txn, int tab_fd) {
  // 1. Check the txn state(SS2PL)
  if (!CheckTxnStateLock(txn)) {
    return false;
  }

  // 2. Check if the lock is already granted
  LockDataId lock_data_id(tab_fd, LockDataType::TABLE);
  // Acquire lock on the global lock table for thread safety
  std::unique_lock<std::mutex> lock(latch_);
  // Find or create the LockRequestQueue
  LockRequestQueue &request_queue = lock_table_[lock_data_id];
  auto wake = [&]() {
    if (request_queue.group_lock_mode_ != GroupLockMode::S && request_queue.group_lock_mode_ != GroupLockMode::SIX &&
        request_queue.group_lock_mode_ != GroupLockMode::X) {
      return true;
    }
    return false;
  };
  for (auto &req : request_queue.request_queue_) {
    if (req.txn_id_ == txn->GetTransactionId()) {
      if (req.lock_mode_ == LockMode::INTENTION_EXCLUSIVE || req.lock_mode_ == LockMode::S_IX ||
          req.lock_mode_ == LockMode::EXCLUSIVE) {
        return true;
      } else if (req.lock_mode_ == LockMode::INTENTION_SHARED) {
        // IS lock，try to upgrade to IX
        if (request_queue.group_lock_mode_ == GroupLockMode::IS ||
            request_queue.group_lock_mode_ == GroupLockMode::IX) {
        } else {
          // /* no-wait */
          // throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK_PREVENTION);
          /* wait-die */
          for (auto &r : request_queue.request_queue_) {
            if (r.txn_id_ != txn->GetTransactionId() && r.lock_mode_ != LockMode::INTENTION_SHARED &&
                r.lock_mode_ != LockMode::INTENTION_EXCLUSIVE) {
              WaitDie(txn, r, request_queue, lock, wake);
              break;
            }
          }
        }
        // There is no conflicting lock request, upgrade
        req.lock_mode_ = LockMode::INTENTION_EXCLUSIVE;
        request_queue.group_lock_mode_ = GroupLockMode::IX;
        return true;
      } else {
        // S lock, try to upgrade to S_IX
        // Note: Now, lock_mode_ will at least be S
        auto upgrade2SIX = [&]() {
          for (auto &r : request_queue.request_queue_) {
            if (r.lock_mode_ == LockMode::SHARED && r.txn_id_ != txn->GetTransactionId()) {
              return false;
            }
          }
          return true;
        };
        // If there are other S lock, upgrade
        for (auto &r : request_queue.request_queue_) {
          if (r.lock_mode_ == LockMode::SHARED && r.txn_id_ != txn->GetTransactionId()) {
            /* wait-die */
            WaitDie(txn, r, request_queue, lock, upgrade2SIX);
            break;
          }
        }
        // There is no other S lock request, upgrade
        req.lock_mode_ = LockMode::S_IX;
        request_queue.group_lock_mode_ = GroupLockMode::SIX;
        return true;
      }
    }
  }

  // 3. Check the group lock mode
  LockRequest lock_request(txn->GetTransactionId(), LockMode::INTENTION_EXCLUSIVE);
  // 3.1 If there is a conflicting lock request
  if (request_queue.group_lock_mode_ == GroupLockMode::S || request_queue.group_lock_mode_ == GroupLockMode::SIX ||
      request_queue.group_lock_mode_ == GroupLockMode::X) {
    // /* no-wait */
    // throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK_PREVENTION);
    /* wait-die */
    for (auto &req : request_queue.request_queue_) {
      if (req.lock_mode_ == LockMode::SHARED || req.lock_mode_ == LockMode::S_IX ||
          req.lock_mode_ == LockMode::EXCLUSIVE) {
        WaitDie(txn, req, request_queue, lock, wake);
        break;
      }
    }
  }
  // 3.2 If no conflicts, grant the lock
  lock_request.granted_ = true;
  // Add the lock request to the queue
  request_queue.request_queue_.emplace_back(lock_request);
  // Update group_lock_mode_(change from NON_LOCK/IS/IX)
  request_queue.group_lock_mode_ = GroupLockMode::IX;
  txn->GetLockSet()->insert(lock_data_id);

  return true;
}

/**
 * @description: 释放锁
 * @return {bool} 返回解锁是否成功
 * @param {Transaction*} txn 要释放锁的事务对象指针
 * @param {LockDataId} lock_data_id 要释放的锁ID
 */
bool LockManager::Unlock(Transaction *txn, LockDataId lock_data_id) {
  // 1. Check the txn state(SS2PL)
  if (!CheckTxnStateUnlock(txn)) {
    return false;
  }

  // 2. Relase the lock
  std::unique_lock<std::mutex> lock(latch_);
  if (lock_table_.count(lock_data_id) == 0) {
    return true;
  }
  LockRequestQueue &request_queue = lock_table_[lock_data_id];
  // Delete the lock request from the queue
  request_queue.request_queue_.remove_if(
      [&txn](const LockRequest &req) { return req.txn_id_ == txn->GetTransactionId(); });
  // Remove the lock from the txn's lock set
  txn->GetLockSet()->erase(lock_data_id);

  // 3. Update the group lock mode
  // Find the most strict lock mode in the queue
  // Note: There will only be compatible lock requests left in the queue
  std::array<int, 6> lock_mode_count = {0};
  for (auto &req : request_queue.request_queue_) {
    lock_mode_count[static_cast<int>(req.lock_mode_)]++;
  }
  if (lock_mode_count[static_cast<int>(LockMode::EXCLUSIVE)] > 0) {
    request_queue.group_lock_mode_ = GroupLockMode::X;
  } else if (lock_mode_count[static_cast<int>(LockMode::S_IX)] > 0) {
    request_queue.group_lock_mode_ = GroupLockMode::SIX;
  } else if (lock_mode_count[static_cast<int>(LockMode::INTENTION_EXCLUSIVE)] > 0) {
    request_queue.group_lock_mode_ = GroupLockMode::IX;
  } else if (lock_mode_count[static_cast<int>(LockMode::SHARED)] > 0) {
    request_queue.group_lock_mode_ = GroupLockMode::S;
  } else if (lock_mode_count[static_cast<int>(LockMode::INTENTION_SHARED)] > 0) {
    request_queue.group_lock_mode_ = GroupLockMode::IS;
  } else if (lock_mode_count[static_cast<int>(LockMode::GAP)] > 0) {
    request_queue.group_lock_mode_ = GroupLockMode::GAP;
  } else {
    request_queue.group_lock_mode_ = GroupLockMode::NON_LOCK;
  }

  // Notify all waiting transactions
  request_queue.cv_.notify_all();

  return true;
}

/**
 * Checks the state of a transaction and determines if it can acquire a lock.
 *
 * @param txn A pointer to the transaction object.
 * @return True if the transaction can acquire a lock, false otherwise.
 * @throws TransactionAbortException If the transaction is in the shrinking state.
 * @throws InternalError If the transaction state is invalid.
 */
bool LockManager::CheckTxnStateLock(Transaction *txn) {
  auto state = txn->GetState();
  switch (state) {
    case TransactionState::COMMITTED:
    case TransactionState::ABORTED:
      // Transaction has been committed or aborted, cannot acquire lock
      return false;
    case TransactionState::DEFAULT:
      // Transaction is in default state, set it to growing state
      txn->SetState(TransactionState::GROWING);
    case TransactionState::GROWING:
      // Transaction is in growing state, break
      return true;
    case TransactionState::SHRINKING:
      // Transaction is in shrinking state, throw exception
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHIRINKING);
    default:
      // Transaction state is invalid, throw exception
      throw InternalError("LockManager::check_txn_state: Invalid transaction state");
  }
}

/**
 * Checks the state of a transaction and determines if it can be unlocked.
 *
 * @param txn A pointer to the transaction to check.
 * @return True if the transaction can be unlocked, false otherwise.
 * @throws InternalError if the transaction state is invalid.
 */
bool LockManager::CheckTxnStateUnlock(Transaction *txn) {
  auto state = txn->GetState();
  switch (state) {
    case TransactionState::COMMITTED:
    case TransactionState::ABORTED:
      // Transaction has been committed or aborted, cannot release lock
      return false;
    case TransactionState::DEFAULT:
    case TransactionState::GROWING:
      // Transaction is in default or growing state, set it to shrinking state
      txn->SetState(TransactionState::SHRINKING);
    case TransactionState::SHRINKING:
      // Transaction is in shrinking state, break
      return true;
    default:
      // Transaction state is invalid, throw exception
      throw InternalError("LockManager::check_txn_state_unlock: Invalid transaction state");
  }
}

/**
 * Waits or aborts the transaction based on the wait-die protocol.
 * If the transaction is older than the requesting transaction, it waits.
 * If the transaction is younger than the requesting transaction, it aborts.
 *
 * @param txn The transaction that is requesting the lock.
 * @param req_holder The lock request holder containing the requesting transaction's information.
 * @param queue The lock request queue.
 * @param lock The unique lock on the lock manager's mutex.
 * @param wake The wake condition for waiting on the lock request queue.
 * @throws TransactionAbortException If the transaction is younger than the requesting transaction.
 */
inline void LockManager::WaitDie(Transaction *txn, LockRequest &req_holder, LockRequestQueue &queue,
                                 std::unique_lock<std::mutex> &lock, std::function<bool()> wake) {
  // Note: We use id instead of start_ts because we cannot get the req.start_ts,
  // but the id increments with the start_ts, which means it's ok to use id.
  if (txn->GetTransactionId() < req_holder.txn_id_) {
    // Older transaction, wait
    queue.cv_.wait(lock, wake);
  } else {
    // Younger transaction, abort
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK_PREVENTION);
  }
}

}  // namespace easydb
