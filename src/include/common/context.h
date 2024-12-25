/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <cstdlib>
#include <nlohmann/json.hpp>
#include <vector>
#include "concurrency/lock_manager.h"
#include "recovery/log_manager.h"
#include "transaction/transaction.h"

using json = nlohmann::json;

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
  json result_json;

  void InitJson() {
    InitJsonData();
    SetJsonMsg("");
  }

  void InitJsonData() { result_json["data"] = json::array(); }
  void AddJsonData(const std::vector<std::string> &row) { result_json["data"].push_back(row); }
  void SetJsonMsg(const std::string &msg) { result_json["msg"] = msg; }

  void PrintJsonMsg() { std::cout << result_json["msg"] << std::endl; }
  void PrintJson() { std::cout << result_json.dump(4) << std::endl; }
  int SerializeTo(std::vector<char> &buf) {
    int len = result_json.dump(4).length();
    buf.resize(len + 1);
    memcpy(buf.data(), result_json.dump(4).c_str(), len);
    buf[len] = '\0';
    return len;
  }
};

}  // namespace easydb