/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * context.h
 *
 * Identification: src/include/common/context.h
 *
 *-------------------------------------------------------------------------
 */

/*
 * Original copyright:
 * Copyright (c) 2023 Renmin University of China
 */

#pragma once

#include "concurrency/lock_manager.h"
#include "recovery/log_manager.h"
#include "transaction/transaction.h"

namespace easydb {
// class TransactionManager;

// used for data_send
static int const_offset = -1;
/*
系统所需的上下文信息（包括事务对象指针、锁管理器指针、日志管理器指针、存放结果的buffer、记录结果长度的变量）
*/
class Context {
 public:
  Context(LockManager *lock_mgr, LogManager *log_mgr, Transaction *txn, char *data_send = nullptr,
          int *offset = &const_offset)
      : lock_mgr_(lock_mgr), log_mgr_(log_mgr), txn_(txn), data_send_(data_send), offset_(offset) {
    ellipsis_ = false;
  }

  // TransactionManager *txn_mgr_;
  LockManager *lock_mgr_;
  LogManager *log_mgr_;
  Transaction *txn_;
  char *data_send_;
  int *offset_;
  bool ellipsis_;
};

}  // namespace easydb