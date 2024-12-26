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

#include <cstddef>
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
    SetJsonMsg("");
    InitJsonData();
  }

  void SetJsonMsg(const std::string &msg) { result_json["msg"] = msg; }
  void InitJsonData() {
    result_json["data"] = json::array();
    result_json["total"] = 0;
  }
  void AddJsonData(const std::vector<std::string> &row) {
    result_json["data"].push_back(row);
    // Update the total number of rows without the header row
    result_json["total"] = result_json["data"].size() - 1;
  }

  void PrintJsonMsg() { std::cout << result_json["msg"] << std::endl; }
  void PrintJson() { std::cout << result_json.dump(4) << std::endl; }

  int SerializeJsonTo(json &json, std::vector<char> &buf) {
    std::string json_str = json.dump(4);
    int len = json_str.length();
    buf.resize(len + 1);
    memcpy(buf.data(), json_str.c_str(), len);
    buf[len] = '\0';
    return len;
  }
  int SerializeTo(std::vector<char> &buf) { return SerializeJsonTo(result_json, buf); }

  int SerializeToWithLimit(std::vector<char> &buf, size_t max_size = 100) {
    // Create a copy of the original JSON object to modify
    auto limited_json = result_json;

    auto &data_array = limited_json["data"];
    // If the size of the "data" array(excluding the header row) is greater than max_size, slice it
    if (data_array.size() > max_size + 1) {
      // Create a new json array with the first max_size elements
      auto sliced_data = json::array();
      for (size_t i = 0; i < max_size + 1; ++i) {
        sliced_data.push_back(data_array[i]);
      }
      limited_json["data"] = sliced_data;
    }

    return SerializeJsonTo(limited_json, buf);
  }
};

}  // namespace easydb