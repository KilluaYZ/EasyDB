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

#include <iostream>
#include <mutex>
#include <unordered_set>
#include <vector>

#include "common/config.h"
#include "common/rid.h"
#include "log_defs.h"
#include "record/rm_defs.h"
#include "storage/page/page.h"

namespace easydb {

/**
 * @brief 日志记录对应操作的类型枚举
 * @note 定义了系统中所有类型的日志记录
 */
enum LogType : int {
  UPDATE = 0,     /**< 更新操作日志 */
  INSERT,         /**< 插入操作日志 */
  DELETE,         /**< 删除操作日志 */
  BEGIN,          /**< 事务开始日志 */
  COMMIT,         /**< 事务提交日志 */
  ABORT,          /**< 事务中止日志 */
  CHECKPOINT      /**< 检查点日志 */
};

/** @brief 日志类型到字符串的映射数组，用于调试输出 */
static std::string LogTypeStr[] = {"UPDATE", "INSERT", "DELETE", "BEGIN", "COMMIT", "ABORT", "CHECKPOINT"};

// 注意：我们在撤销时不写CLR（补偿日志记录），所以在重启期间无法在故障中存活。
// 回滚时通常也是如此。但现在我们在回滚时写"Update"日志，然后在完成时写ABORT日志，
// 所以我们可以将其视为COMMIT。
// 要支持CLR，我们还需要支持TXN-END。

/**
 * @brief 日志记录基类
 * 
 * LogRecord 是所有日志记录类型的基类，定义了日志记录的通用格式和操作。
 * 所有具体的日志记录类型（如InsertLogRecord、UpdateLogRecord等）都继承此类。
 */
class LogRecord {
 public:
  /** @brief 日志对应操作的类型 */
  LogType log_type_;
  
  /** @brief 当前日志的LSN（日志序列号） */
  lsn_t lsn_;
  
  /** @brief 整个日志记录的长度（字节数） */
  uint32_t log_tot_len_;
  
  /** @brief 创建当前日志的事务ID */
  txn_id_t log_tid_;
  
  /**
   * @brief 事务创建的前一条日志记录的LSN
   * @note 用于undo操作，通过prev_lsn_链可以回溯事务的所有操作
   */
  lsn_t prev_lsn_;

  /**
   * @brief 把日志记录序列化到dest中
   * @param dest 目标缓冲区指针
   * @note 将日志记录的各个字段按顺序写入缓冲区
   */
  virtual void serialize(char *dest) const {
    memcpy(dest + OFFSET_LOG_TYPE, &log_type_, sizeof(LogType));
    memcpy(dest + OFFSET_LSN, &lsn_, sizeof(lsn_t));
    memcpy(dest + OFFSET_LOG_TOT_LEN, &log_tot_len_, sizeof(uint32_t));
    memcpy(dest + OFFSET_LOG_TID, &log_tid_, sizeof(txn_id_t));
    memcpy(dest + OFFSET_PREV_LSN, &prev_lsn_, sizeof(lsn_t));
  }
  /**
   * @brief 从src中反序列化出一条日志记录
   * @param src 源缓冲区指针
   * @note 从缓冲区读取日志记录的各个字段
   */
  virtual void deserialize(const char *src) {
    log_type_ = *reinterpret_cast<const LogType *>(src);
    lsn_ = *reinterpret_cast<const lsn_t *>(src + OFFSET_LSN);
    log_tot_len_ = *reinterpret_cast<const uint32_t *>(src + OFFSET_LOG_TOT_LEN);
    log_tid_ = *reinterpret_cast<const txn_id_t *>(src + OFFSET_LOG_TID);
    prev_lsn_ = *reinterpret_cast<const lsn_t *>(src + OFFSET_PREV_LSN);
  }
  /**
   * @brief 格式化打印日志记录（用于调试）
   * @note 输出日志记录的所有字段信息
   */
  virtual void format_print() {
    std::cout << "log type in father_function: " << LogTypeStr[log_type_] << "\n";
    printf("Print Log Record:\n");
    printf("log_type_: %s\n", LogTypeStr[log_type_].c_str());
    printf("lsn: %d\n", lsn_);
    printf("log_tot_len: %d\n", log_tot_len_);
    printf("log_tid: %ld\n", log_tid_);
    printf("prev_lsn: %d\n", prev_lsn_);
  }
  virtual ~LogRecord() {}
};

/**
 * @brief 事务开始日志记录类
 * 
 * BeginLogRecord 记录事务的开始，包含事务ID信息。
 */
class BeginLogRecord : public LogRecord {
 public:
  /**
   * @brief 默认构造函数
   * @note 初始化所有字段为默认值
   */
  BeginLogRecord() {
    log_type_ = LogType::BEGIN;
    lsn_ = INVALID_LSN;
    log_tot_len_ = LOG_HEADER_SIZE;
    log_tid_ = INVALID_TXN_ID;
    prev_lsn_ = INVALID_LSN;
  }
  
  /**
   * @brief 根据事务ID构造Begin日志记录
   * @param txn_id 事务ID
   */
  BeginLogRecord(txn_id_t txn_id) : BeginLogRecord() { log_tid_ = txn_id; }
  
  /**
   * @brief 序列化Begin日志记录到dest中
   * @param dest 目标缓冲区指针
   */
  void serialize(char *dest) const override { LogRecord::serialize(dest); }
  
  /**
   * @brief 从src中反序列化出一条Begin日志记录
   * @param src 源缓冲区指针
   */
  void deserialize(const char *src) override { LogRecord::deserialize(src); }
  virtual void format_print() override {
    std::cout << "log type in son_function: " << LogTypeStr[log_type_] << "\n";
    LogRecord::format_print();
  }
};

/**
 * @brief 事务提交日志记录类
 * 
 * CommitLogRecord 记录事务的提交，表示事务成功完成。
 */
class CommitLogRecord : public LogRecord {
 public:
  /**
   * @brief 默认构造函数
   */
  CommitLogRecord() {
    log_type_ = LogType::COMMIT;
    lsn_ = INVALID_LSN;
    log_tot_len_ = LOG_HEADER_SIZE;
    log_tid_ = INVALID_TXN_ID;
    prev_lsn_ = INVALID_LSN;
  }
  
  /**
   * @brief 根据事务ID和前一个LSN构造Commit日志记录
   * @param txn_id 事务ID
   * @param prev_lsn 前一个LSN
   */
  CommitLogRecord(txn_id_t txn_id, lsn_t prev_lsn) : CommitLogRecord() {
    log_tid_ = txn_id;
    prev_lsn_ = prev_lsn;
  }
  
  /**
   * @brief 序列化commit日志字段到dest中
   * @param dest 目标缓冲区指针
   */
  void serialize(char *dest) const override { LogRecord::serialize(dest); }
  
  /**
   * @brief 从src中反序列化commit日志字段
   * @param src 源缓冲区指针
   */
  void deserialize(const char *src) override { LogRecord::deserialize(src); }
  void format_print() override {
    std::cout << "log type in son_function: " << LogTypeStr[log_type_] << "\n";
    LogRecord::format_print();
  }
};

/**
 * @brief 事务中止日志记录类
 * 
 * AbortLogRecord 记录事务的中止，表示事务因错误或冲突而被回滚。
 */
class AbortLogRecord : public LogRecord {
 public:
  /**
   * @brief 默认构造函数
   */
  AbortLogRecord() {
    log_type_ = LogType::ABORT;
    lsn_ = INVALID_LSN;
    log_tot_len_ = LOG_HEADER_SIZE;
    log_tid_ = INVALID_TXN_ID;
    prev_lsn_ = INVALID_LSN;
  }
  
  /**
   * @brief 根据事务ID和前一个LSN构造Abort日志记录
   * @param txn_id 事务ID
   * @param prev_lsn 前一个LSN
   */
  AbortLogRecord(txn_id_t txn_id, lsn_t prev_lsn) : AbortLogRecord() {
    log_tid_ = txn_id;
    prev_lsn_ = prev_lsn;
  }
  
  /**
   * @brief 序列化abort日志字段到dest中
   * @param dest 目标缓冲区指针
   */
  void serialize(char *dest) const override { LogRecord::serialize(dest); }
  
  /**
   * @brief 从src中反序列化abort日志字段
   * @param src 源缓冲区指针
   */
  void deserialize(const char *src) override { LogRecord::deserialize(src); }
  void format_print() override {
    std::cout << "log type in son_function: " << LogTypeStr[log_type_] << "\n";
    LogRecord::format_print();
  }
};

/**
 * @brief 插入操作日志记录类
 * 
 * InsertLogRecord 记录插入操作，包含插入的记录值、记录ID和表名。
 * 用于事务回滚时删除插入的记录。
 */
class InsertLogRecord : public LogRecord {
 public:
  /**
   * @brief 默认构造函数
   */
  InsertLogRecord() {
    log_type_ = LogType::INSERT;
    lsn_ = INVALID_LSN;
    log_tot_len_ = LOG_HEADER_SIZE;
    log_tid_ = INVALID_TXN_ID;
    prev_lsn_ = INVALID_LSN;
    table_name_ = nullptr;
  }
  
  /**
   * @brief 根据插入信息构造Insert日志记录
   * @param txn_id 事务ID
   * @param insert_value 插入的记录值
   * @param rid 记录ID
   * @param table_name 表名
   */
  InsertLogRecord(txn_id_t txn_id, RmRecord &insert_value, RID &rid, std::string table_name) : InsertLogRecord() {
    log_tid_ = txn_id;
    insert_value_ = insert_value;
    rid_ = rid;
    log_tot_len_ += sizeof(int);
    log_tot_len_ += insert_value_.size;
    log_tot_len_ += sizeof(RID);
    table_name_size_ = table_name.length();
    table_name_ = new char[table_name_size_];
    memcpy(table_name_, table_name.c_str(), table_name_size_);
    log_tot_len_ += sizeof(size_t) + table_name_size_;
  }

  /**
   * @brief 把insert日志记录序列化到dest中
   * @param dest 目标缓冲区指针
   * @note 序列化格式：日志头部 + 记录大小 + 记录数据 + RID + 表名大小 + 表名
   */
  void serialize(char *dest) const override {
    LogRecord::serialize(dest);
    int offset = OFFSET_LOG_DATA;
    memcpy(dest + offset, &insert_value_.size, sizeof(int));
    offset += sizeof(int);
    memcpy(dest + offset, insert_value_.data, insert_value_.size);
    offset += insert_value_.size;
    memcpy(dest + offset, &rid_, sizeof(RID));
    offset += sizeof(RID);
    memcpy(dest + offset, &table_name_size_, sizeof(size_t));
    offset += sizeof(size_t);
    memcpy(dest + offset, table_name_, table_name_size_);
  }
  /**
   * @brief 从src中反序列化出一条Insert日志记录
   * @param src 源缓冲区指针
   */
  void deserialize(const char *src) override {
    LogRecord::deserialize(src);
    insert_value_.Deserialize(src + OFFSET_LOG_DATA);
    int offset = OFFSET_LOG_DATA + insert_value_.size + sizeof(int);
    rid_ = *reinterpret_cast<const RID *>(src + offset);
    offset += sizeof(RID);
    table_name_size_ = *reinterpret_cast<const size_t *>(src + offset);
    offset += sizeof(size_t);
    table_name_ = new char[table_name_size_];
    memcpy(table_name_, src + offset, table_name_size_);
  }
  void format_print() override {
    printf("insert record\n");
    LogRecord::format_print();
    printf("insert_value: %s\n", insert_value_.data);
    printf("insert rid: %d, %d\n", rid_.GetPageId(), rid_.GetSlotNum());
    printf("table name: %s\n", table_name_);
  }
  /**
   * @brief 析构函数
   * @note 释放表名字符数组的内存
   */
  ~InsertLogRecord() override {
    delete[] table_name_;
    table_name_ = nullptr;
  }

  /** @brief 插入的记录 */
  RmRecord insert_value_;
  
  /** @brief 记录插入的位置 */
  RID rid_;
  
  /** @brief 插入记录的表名称 */
  char *table_name_;
  
  /** @brief 表名称的大小（字节数） */
  size_t table_name_size_;
};

/**
 * @brief 删除操作日志记录类
 * 
 * DeleteLogRecord 记录删除操作，包含被删除的记录值、记录ID和表名。
 * 用于事务回滚时恢复被删除的记录。
 */
class DeleteLogRecord : public LogRecord {
 public:
  DeleteLogRecord() {
    log_type_ = LogType::DELETE;
    lsn_ = INVALID_LSN;
    log_tot_len_ = LOG_HEADER_SIZE;
    log_tid_ = INVALID_TXN_ID;
    prev_lsn_ = INVALID_LSN;
    table_name_ = nullptr;
  }
  DeleteLogRecord(txn_id_t txn_id, RmRecord &delete_value, RID &rid, std::string table_name) : DeleteLogRecord() {
    log_tid_ = txn_id;
    delete_value_ = delete_value;
    rid_ = rid;
    log_tot_len_ += sizeof(int);
    log_tot_len_ += delete_value_.size;
    log_tot_len_ += sizeof(RID);
    table_name_size_ = table_name.length();
    table_name_ = new char[table_name_size_];
    memcpy(table_name_, table_name.c_str(), table_name_size_);
    log_tot_len_ += sizeof(size_t) + table_name_size_;
  }

  /**
   * @brief 序列化delete日志字段到dest中
   * @param dest 目标缓冲区指针
   */
  void serialize(char *dest) const override {
    LogRecord::serialize(dest);
    int offset = OFFSET_LOG_DATA;
    memcpy(dest + offset, &delete_value_.size, sizeof(int));
    offset += sizeof(int);
    memcpy(dest + offset, delete_value_.data, delete_value_.size);
    offset += delete_value_.size;
    memcpy(dest + offset, &rid_, sizeof(RID));
    offset += sizeof(RID);
    memcpy(dest + offset, &table_name_size_, sizeof(size_t));
    offset += sizeof(size_t);
    memcpy(dest + offset, table_name_, table_name_size_);
  }

  /**
   * @brief 从src中反序列化delete日志字段
   * @param src 源缓冲区指针
   */
  void deserialize(const char *src) override {
    LogRecord::deserialize(src);
    delete_value_.Deserialize(src + OFFSET_LOG_DATA);
    int offset = OFFSET_LOG_DATA + delete_value_.size + sizeof(int);
    rid_ = *reinterpret_cast<const RID *>(src + offset);
    offset += sizeof(RID);
    table_name_size_ = *reinterpret_cast<const size_t *>(src + offset);
    offset += sizeof(size_t);
    table_name_ = new char[table_name_size_];
    memcpy(table_name_, src + offset, table_name_size_);
  }

  void format_print() override {
    printf("delete record\n");
    LogRecord::format_print();
    printf("delete_value: %s\n", delete_value_.data);
    printf("delete rid: %d, %d\n", rid_.GetPageId(), rid_.GetSlotNum());
    printf("table name: %s\n", table_name_);
  }

  /**
   * @brief 析构函数
   * @note 释放表名字符数组的内存
   */
  ~DeleteLogRecord() override {
    delete[] table_name_;
    table_name_ = nullptr;
  }

  /** @brief 被删除的记录 */
  RmRecord delete_value_;
  
  /** @brief 记录位置 */
  RID rid_;
  
  /** @brief 表名 */
  char *table_name_;
  
  /** @brief 表名大小（字节数） */
  size_t table_name_size_;
};

/**
 * @brief 更新操作日志记录类
 * 
 * UpdateLogRecord 记录更新操作，包含旧值、新值、记录ID和表名。
 * 用于事务回滚时将记录恢复为旧值。
 */
class UpdateLogRecord : public LogRecord {
 public:
  UpdateLogRecord() {
    log_type_ = LogType::UPDATE;
    lsn_ = INVALID_LSN;
    log_tot_len_ = LOG_HEADER_SIZE;
    log_tid_ = INVALID_TXN_ID;
    prev_lsn_ = INVALID_LSN;
    table_name_ = nullptr;
  }
  UpdateLogRecord(txn_id_t txn_id, RmRecord &old_value, RmRecord &new_value, RID &rid, std::string table_name)
      : UpdateLogRecord() {
    log_tid_ = txn_id;
    old_value_ = old_value;
    new_value_ = new_value;
    rid_ = rid;
    log_tot_len_ += 2 * sizeof(int);
    log_tot_len_ += old_value_.size + new_value_.size;
    log_tot_len_ += sizeof(RID);
    table_name_size_ = table_name.length();
    table_name_ = new char[table_name_size_];
    memcpy(table_name_, table_name.c_str(), table_name_size_);
    log_tot_len_ += sizeof(size_t) + table_name_size_;
  }

  /**
   * @brief 序列化update日志字段到dest中
   * @param dest 目标缓冲区指针
   * @note 序列化格式：日志头部 + 旧值大小 + 旧值数据 + 新值大小 + 新值数据 + RID + 表名大小 + 表名
   */
  void serialize(char *dest) const override {
    LogRecord::serialize(dest);
    int offset = OFFSET_LOG_DATA;
    memcpy(dest + offset, &old_value_.size, sizeof(int));
    offset += sizeof(int);
    memcpy(dest + offset, old_value_.data, old_value_.size);
    offset += old_value_.size;
    memcpy(dest + offset, &new_value_.size, sizeof(int));
    offset += sizeof(int);
    memcpy(dest + offset, new_value_.data, new_value_.size);
    offset += new_value_.size;
    memcpy(dest + offset, &rid_, sizeof(RID));
    offset += sizeof(RID);
    memcpy(dest + offset, &table_name_size_, sizeof(size_t));
    offset += sizeof(size_t);
    memcpy(dest + offset, table_name_, table_name_size_);
  }

  /**
   * @brief 从src中反序列化update日志字段
   * @param src 源缓冲区指针
   */
  void deserialize(const char *src) override {
    LogRecord::deserialize(src);
    old_value_.Deserialize(src + OFFSET_LOG_DATA);
    int offset = OFFSET_LOG_DATA + old_value_.size + sizeof(int);
    new_value_.Deserialize(src + offset);
    offset += new_value_.size + sizeof(int);
    rid_ = *reinterpret_cast<const RID *>(src + offset);
    offset += sizeof(RID);
    table_name_size_ = *reinterpret_cast<const size_t *>(src + offset);
    offset += sizeof(size_t);
    table_name_ = new char[table_name_size_];
    memcpy(table_name_, src + offset, table_name_size_);
  }

  void format_print() override {
    printf("update record\n");
    LogRecord::format_print();
    printf("old_value: %s\n", old_value_.data);
    printf("new_value: %s\n", new_value_.data);
    printf("update rid: %d, %d\n", rid_.GetPageId(), rid_.GetSlotNum());
    printf("table name: %s\n", table_name_);
  }

  /**
   * @brief 析构函数
   * @note 释放表名字符数组的内存
   */
  ~UpdateLogRecord() override {
    delete[] table_name_;
    table_name_ = nullptr;
  }

  /** @brief 旧记录值 */
  RmRecord old_value_;
  
  /** @brief 新记录值 */
  RmRecord new_value_;
  
  /** @brief 记录位置 */
  RID rid_;
  
  /** @brief 表名 */
  char *table_name_;
  
  /** @brief 表名大小（字节数） */
  size_t table_name_size_;
};

/**
 * @brief 检查点操作的日志记录类
 * 
 * CheckpointLogRecord 记录检查点信息，包含：
 * - 活跃事务表（ATT）：所有活跃事务及其最后LSN
 * - 已中止事务列表：所有已中止的事务ID
 * - 脏页表（DPT）：所有脏页及其LSN
 * - 最小恢复LSN：用于恢复的起始位置
 * 
 * @note 日志不再改变时调用add_log_to_buffer
 */
class CheckpointLogRecord : public LogRecord {
 public:
  CheckpointLogRecord() {
    log_type_ = LogType::CHECKPOINT;
    lsn_ = INVALID_LSN;
    log_tot_len_ = LOG_HEADER_SIZE;
    log_tid_ = INVALID_TXN_ID;
    prev_lsn_ = INVALID_LSN;
  }
  CheckpointLogRecord(txn_id_t txn_id, lsn_t prev_lsn) : CheckpointLogRecord() {
    log_tid_ = txn_id;
    prev_lsn_ = prev_lsn;
    // add checkpoint log fields here
    att_size_ = 0;
    att_vec_ = std::vector<std::pair<txn_id_t, lsn_t>>();
    aborted_txns_size_ = 0;
    aborted_txns_vec_ = std::vector<txn_id_t>();
    dpt_size_ = 0;
    dpt_vec_ = std::vector<std::pair<page_id_t, lsn_t>>();
    min_rec_lsn_ = INVALID_LSN;
    tab_name_offset_size_ = 1;
    tab_name_offset_vec_ = std::vector<size_t>(1, 0);
    tab_name_str_size_ = 0;
    tab_name_str_ = "";
    log_tot_len_ += sizeof(lsn_t) + sizeof(size_t) * 5 + tab_name_offset_size_ * sizeof(size_t);
  }

  /**
   * @brief 序列化checkpoint日志字段到dest中
   * @param dest 目标缓冲区指针
   * @note 序列化格式：日志头部 + ATT + 已中止事务列表 + DPT + 最小恢复LSN + 表名信息
   */
  void serialize(char *dest) const override {
    LogRecord::serialize(dest);
    int offset = OFFSET_LOG_DATA;
    // att
    // size_t att_size = att_vec_.size();
    memcpy(dest + offset, &att_size_, sizeof(size_t));
    offset += sizeof(size_t);
    memcpy(dest + offset, att_vec_.data(), att_size_ * sizeof(std::pair<txn_id_t, lsn_t>));
    offset += att_size_ * sizeof(std::pair<txn_id_t, lsn_t>);
    // size_t aborted_txns_size = aborted_txns_vec_.size();
    memcpy(dest + offset, &aborted_txns_size_, sizeof(size_t));
    offset += sizeof(size_t);
    memcpy(dest + offset, aborted_txns_vec_.data(), aborted_txns_size_ * sizeof(txn_id_t));
    offset += aborted_txns_size_ * sizeof(txn_id_t);
    // dpt
    // size_t dpt_size = dpt_vec_.size();
    memcpy(dest + offset, &dpt_size_, sizeof(size_t));
    offset += sizeof(size_t);
    memcpy(dest + offset, dpt_vec_.data(), dpt_size_ * sizeof(std::pair<page_id_t, lsn_t>));
    offset += dpt_size_ * sizeof(std::pair<page_id_t, lsn_t>);
    memcpy(dest + offset, &min_rec_lsn_, sizeof(lsn_t));
    offset += sizeof(lsn_t);
    // dpt-tab_name
    // size_t tab_name_offset_size = tab_name_offset_vec_.size();
    // assert(tab_name_offset_size == dpt_size_ + 1);
    memcpy(dest + offset, &tab_name_offset_size_, sizeof(size_t));
    offset += sizeof(size_t);
    memcpy(dest + offset, tab_name_offset_vec_.data(), tab_name_offset_size_ * sizeof(size_t));
    offset += tab_name_offset_size_ * sizeof(size_t);
    // size_t tab_name_str_size = tab_name_str_.length();
    memcpy(dest + offset, &tab_name_str_size_, sizeof(size_t));
    offset += sizeof(size_t);
    memcpy(dest + offset, tab_name_str_.c_str(), tab_name_str_.length());
    offset += tab_name_str_.length();
  }

  /**
   * @brief 从src中反序列化checkpoint日志字段
   * @param src 源缓冲区指针
   */
  void deserialize(const char *src) override {
    LogRecord::deserialize(src);
    int offset = OFFSET_LOG_DATA;
    // att
    att_size_ = *reinterpret_cast<const size_t *>(src + offset);
    offset += sizeof(size_t);
    att_vec_.resize(att_size_);
    memcpy(att_vec_.data(), src + offset, att_size_ * sizeof(std::pair<txn_id_t, lsn_t>));
    offset += att_size_ * sizeof(std::pair<txn_id_t, lsn_t>);
    // aborted_txns
    aborted_txns_size_ = *reinterpret_cast<const size_t *>(src + offset);
    offset += sizeof(size_t);
    aborted_txns_vec_.resize(aborted_txns_size_);
    memcpy(aborted_txns_vec_.data(), src + offset, aborted_txns_size_ * sizeof(txn_id_t));
    offset += aborted_txns_size_ * sizeof(txn_id_t);
    // dpt
    dpt_size_ = *reinterpret_cast<const size_t *>(src + offset);
    offset += sizeof(size_t);
    dpt_vec_.resize(dpt_size_);
    memcpy(dpt_vec_.data(), src + offset, dpt_size_ * sizeof(std::pair<page_id_t, lsn_t>));
    offset += dpt_size_ * sizeof(std::pair<page_id_t, lsn_t>);
    min_rec_lsn_ = *reinterpret_cast<const lsn_t *>(src + offset);
    offset += sizeof(lsn_t);
    // dpt-tab_name
    tab_name_offset_size_ = *reinterpret_cast<const size_t *>(src + offset);
    offset += sizeof(size_t);
    tab_name_offset_vec_.resize(tab_name_offset_size_);
    memcpy(tab_name_offset_vec_.data(), src + offset, tab_name_offset_size_ * sizeof(size_t));
    offset += tab_name_offset_size_ * sizeof(size_t);
    // tab_name_str
    tab_name_str_size_ = *reinterpret_cast<const size_t *>(src + offset);
    offset += sizeof(size_t);
    tab_name_str_ = std::string(src + offset, tab_name_str_size_);
    offset += tab_name_str_size_;
  }

  void format_print() override {
    printf("\n+-------- Checkpoint Log Record --------+\n");
    LogRecord::format_print();
    printf("att_vec: %lu\n", att_vec_.size());
    for (auto &att : att_vec_) {
      printf(" txn_id: %ld, lsn: %d\n", att.first, att.second);
    }
    printf("aborted_txns_vec: %lu\n", aborted_txns_vec_.size());
    for (auto &aborted_txn : aborted_txns_vec_) {
      printf(" txn_id: %ld\n", aborted_txn);
    }
    printf("dpt_vec: %lu\n", dpt_vec_.size());
    for (auto &dpt : dpt_vec_) {
      printf(" page_id: %d, lsn: %d\n", dpt.first, dpt.second);
    }
    printf("min_rec_lsn: %d\n", min_rec_lsn_);
    printf("tab_name_offset_vec: %lu\n", tab_name_offset_vec_.size());
    for (auto &tab_name_offset : tab_name_offset_vec_) {
      printf(" tab_name_offset: %lu\n", tab_name_offset);
    }
    printf("tab_name_str: %s\n", tab_name_str_.c_str());
    printf("+---------------------------------+\n");
  }

  void set_att(std::unordered_map<txn_id_t, lsn_t> &att) {
    for (auto &att_pair : att) {
      add_att(att_pair.first, att_pair.second);
    }
  }

  void set_aborted_txns(std::unordered_set<txn_id_t> &aborted_txns) {
    for (auto &aborted_txn : aborted_txns) {
      add_aborted_txn(aborted_txn);
    }
  }

  void set_dpt_with_tab_name(std::unordered_map<PageId, lsn_t> &dpt,
                             std::unordered_map<PageId, std::string> &page2tab_name) {
    for (auto &dpt_pair : dpt) {
      add_dpt(dpt_pair.first.page_no, dpt_pair.second);
      append_tab_name(page2tab_name[dpt_pair.first]);
    }
  }

  void set_min_rec_lsn(lsn_t min_rec_lsn) { min_rec_lsn_ = min_rec_lsn; }

  void add_att(txn_id_t txn_id, lsn_t lsn) {
    att_vec_.emplace_back(txn_id, lsn);
    att_size_++;
    log_tot_len_ += sizeof(txn_id_t) + sizeof(lsn_t);
  }

  void add_aborted_txn(txn_id_t txn_id) {
    aborted_txns_vec_.emplace_back(txn_id);
    aborted_txns_size_++;
    log_tot_len_ += sizeof(txn_id_t);
  }

  void add_dpt(page_id_t page_no, lsn_t rec_lsn) {
    dpt_vec_.emplace_back(page_no, rec_lsn);
    dpt_size_++;
    log_tot_len_ += sizeof(page_id_t) + sizeof(lsn_t);
  }

  void update_min_rec_lsn(lsn_t rec_lsn) {
    if (min_rec_lsn_ == INVALID_LSN) min_rec_lsn_ = rec_lsn;
  }

  void append_tab_name(std::string tab_name) {
    tab_name_str_ += tab_name;
    tab_name_offset_vec_.push_back(tab_name_str_.length());
    tab_name_offset_size_++;
    tab_name_str_size_ = tab_name_str_.length();

    log_tot_len_ += sizeof(size_t) + tab_name.length();
  }

  /**
   * @brief 活跃事务表（ATT）：事务ID到最后一个LSN的映射
   * @note att_size_: ATT中事务的数量
   *       att_vec_: ATT的向量表示，每个元素是(txn_id, last_lsn)对
   */
  size_t att_size_;
  std::vector<std::pair<txn_id_t, lsn_t>> att_vec_;
  
  /**
   * @brief 已中止事务列表
   * @note aborted_txns_size_: 已中止事务的数量
   *       aborted_txns_vec_: 已中止事务ID的向量
   */
  size_t aborted_txns_size_;
  std::vector<txn_id_t> aborted_txns_vec_;
  
  /**
   * @brief 脏页表（DPT）：页面ID到恢复LSN的映射
   * @note dpt_size_: DPT中页面的数量
   *       dpt_vec_: DPT的向量表示，每个元素是(page_no, rec_lsn)对
   *       表名索引从0到dpt_size_-1
   */
  size_t dpt_size_;
  std::vector<std::pair<page_id_t, lsn_t>> dpt_vec_;
  
  /**
   * @brief 最小恢复LSN
   * @note 用于确定恢复的起始位置，是所有脏页LSN的最小值
   */
  lsn_t min_rec_lsn_;
  
  /**
   * @brief 表名偏移量信息
   * @note tab_name_offset_size_: 偏移量数组的大小（等于dpt_size_ + 1）
   *       tab_name_offset_vec_: 表名字符串中每个表名的偏移量（初始化为{0}）
   *       tab_name_str_size_: 表名字符串的总长度
   *       tab_name_str_: 所有表名连接成的字符串
   */
  size_t tab_name_offset_size_;
  std::vector<size_t> tab_name_offset_vec_;  // Note: initialized to {0}
  size_t tab_name_str_size_;
  std::string tab_name_str_;
};

/**
 * @brief 日志缓冲区类
 * 
 * LogBuffer 用于暂存日志记录，减少磁盘I/O次数。
 * 由于只有一个缓冲区，因此需要阻塞地将日志写入缓冲区中。
 */
class LogBuffer {
 public:
  /**
   * @brief 构造函数，初始化缓冲区
   * @note 将offset_设置为0，缓冲区清零
   */
  LogBuffer() {
    offset_ = 0;
    memset(buffer_, 0, sizeof(buffer_));
  }

  /**
   * @brief 检查缓冲区是否已满（无法容纳指定大小的日志）
   * @param append_size 要追加的日志大小（字节数）
   * @return true 如果缓冲区已满，false 否则
   */
  bool is_full(int append_size) {
    if (offset_ + append_size > LOG_BUFFER_SIZE) return true;
    return false;
  }

  /**
   * @brief 日志缓冲区数组
   * @note 大小为LOG_BUFFER_SIZE + 1，多出的1字节用于安全边界
   */
  char buffer_[LOG_BUFFER_SIZE + 1];
  
  /**
   * @brief 写入日志的偏移量
   * @note 表示缓冲区中下一个日志记录的写入位置
   */
  int offset_;
};

/**
 * @brief 日志管理器类
 * 
 * LogManager 负责：
 * - 将日志写入日志缓冲区
 * - 将日志缓冲区中的内容写入磁盘
 * - 管理全局LSN的分配
 */
class LogManager {
  friend class RecoveryManager;

 public:
  /**
   * @brief 构造函数
   * @param disk_manager 磁盘管理器指针
   */
  LogManager(DiskManager *disk_manager) { disk_manager_ = disk_manager; }

  /**
   * @brief 将日志记录添加到缓冲区
   * @param log_record 日志记录指针
   * @return 分配给日志记录的LSN
   * @note 
   *   - 如果缓冲区已满，先刷新到磁盘
   *   - 分配新的LSN并设置到日志记录
   *   - 将日志记录序列化到缓冲区
   */
  lsn_t add_log_to_buffer(LogRecord *log_record);
  
  /**
   * @brief 将日志缓冲区刷新到磁盘
   * @note 将缓冲区中的所有日志写入磁盘文件，并更新persist_lsn_
   */
  void flush_log_to_disk();

  /**
   * @brief 获取日志缓冲区
   * @return 日志缓冲区的指针
   */
  LogBuffer *get_log_buffer() { return &log_buffer_; }

 private:
  /**
   * @brief 全局LSN（日志序列号）
   * @note 原子变量，递增，用于为每条日志记录分发唯一的LSN
   */
  std::atomic<lsn_t> global_lsn_{0};
  
  /**
   * @brief 保护log_buffer_的互斥锁
   * @note 用于确保多线程环境下日志缓冲区操作的线程安全性
   */
  std::mutex latch_;
  
  /**
   * @brief 日志缓冲区
   * @note 用于暂存日志记录，减少磁盘I/O次数
   */
  LogBuffer log_buffer_;
  
  /**
   * @brief 已持久化到磁盘的最后一条日志的LSN
   * @note 用于跟踪哪些日志已经安全地写入磁盘
   */
  lsn_t persist_lsn_;
  
  /**
   * @brief 磁盘管理器指针
   * @note 用于执行日志文件的磁盘I/O操作
   */
  DiskManager *disk_manager_;
};

}  // namespace easydb
