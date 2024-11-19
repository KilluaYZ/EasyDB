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
#include "log_defs.h"
#include "record/rm_defs.h"

namespace easydb {
/* 日志记录对应操作的类型 */
enum LogType : int { UPDATE = 0, INSERT, DELETE, BEGIN, COMMIT, ABORT, CHECKPOINT };
static std::string LogTypeStr[] = {"UPDATE", "INSERT", "DELETE", "BEGIN", "COMMIT", "ABORT", "CHECKPOINT"};
// Note that we don't write CLRs when undoing, so we can't survive failures during restarts.
// When rolling back, it's usually the same. But now we write "Update" logs when rolling back
// and *then* write ABORT log when finishing it, so we can treat it as COMMIT.
// To support CLRs, we also need to support TXN-END.

class LogRecord {
 public:
  LogType log_type_;     /* 日志对应操作的类型 */
  lsn_t lsn_;            /* 当前日志的lsn */
  uint32_t log_tot_len_; /* 整个日志记录的长度 */
  txn_id_t log_tid_;     /* 创建当前日志的事务ID */
  lsn_t prev_lsn_;       /* 事务创建的前一条日志记录的lsn，用于undo */

  // 把日志记录序列化到dest中
  virtual void serialize(char *dest) const {
    memcpy(dest + OFFSET_LOG_TYPE, &log_type_, sizeof(LogType));
    memcpy(dest + OFFSET_LSN, &lsn_, sizeof(lsn_t));
    memcpy(dest + OFFSET_LOG_TOT_LEN, &log_tot_len_, sizeof(uint32_t));
    memcpy(dest + OFFSET_LOG_TID, &log_tid_, sizeof(txn_id_t));
    memcpy(dest + OFFSET_PREV_LSN, &prev_lsn_, sizeof(lsn_t));
  }
  // 从src中反序列化出一条日志记录
  virtual void deserialize(const char *src) {
    log_type_ = *reinterpret_cast<const LogType *>(src);
    lsn_ = *reinterpret_cast<const lsn_t *>(src + OFFSET_LSN);
    log_tot_len_ = *reinterpret_cast<const uint32_t *>(src + OFFSET_LOG_TOT_LEN);
    log_tid_ = *reinterpret_cast<const txn_id_t *>(src + OFFSET_LOG_TID);
    prev_lsn_ = *reinterpret_cast<const lsn_t *>(src + OFFSET_PREV_LSN);
  }
  // used for debug
  virtual void format_print() {
    std::cout << "log type in father_function: " << LogTypeStr[log_type_] << "\n";
    printf("Print Log Record:\n");
    printf("log_type_: %s\n", LogTypeStr[log_type_].c_str());
    printf("lsn: %d\n", lsn_);
    printf("log_tot_len: %d\n", log_tot_len_);
    printf("log_tid: %d\n", log_tid_);
    printf("prev_lsn: %d\n", prev_lsn_);
  }
  virtual ~LogRecord() {}
};

class BeginLogRecord : public LogRecord {
 public:
  BeginLogRecord() {
    log_type_ = LogType::BEGIN;
    lsn_ = INVALID_LSN;
    log_tot_len_ = LOG_HEADER_SIZE;
    log_tid_ = INVALID_TXN_ID;
    prev_lsn_ = INVALID_LSN;
  }
  BeginLogRecord(txn_id_t txn_id) : BeginLogRecord() { log_tid_ = txn_id; }
  // 序列化Begin日志记录到dest中
  void serialize(char *dest) const override { LogRecord::serialize(dest); }
  // 从src中反序列化出一条Begin日志记录
  void deserialize(const char *src) override { LogRecord::deserialize(src); }
  virtual void format_print() override {
    std::cout << "log type in son_function: " << LogTypeStr[log_type_] << "\n";
    LogRecord::format_print();
  }
};

/**
 * TODO: commit操作的日志记录
 */
class CommitLogRecord : public LogRecord {
 public:
  CommitLogRecord() {
    log_type_ = LogType::COMMIT;
    lsn_ = INVALID_LSN;
    log_tot_len_ = LOG_HEADER_SIZE;
    log_tid_ = INVALID_TXN_ID;
    prev_lsn_ = INVALID_LSN;
  }
  CommitLogRecord(txn_id_t txn_id, lsn_t prev_lsn) : CommitLogRecord() {
    log_tid_ = txn_id;
    prev_lsn_ = prev_lsn;
  }
  // Serialize commit log fields to dest
  void serialize(char *dest) const override { LogRecord::serialize(dest); }
  // Deserialize commit log fields from src
  void deserialize(const char *src) override { LogRecord::deserialize(src); }
  void format_print() override {
    std::cout << "log type in son_function: " << LogTypeStr[log_type_] << "\n";
    LogRecord::format_print();
  }
};

/**
 * TODO: abort操作的日志记录
 */
class AbortLogRecord : public LogRecord {
 public:
  AbortLogRecord() {
    log_type_ = LogType::ABORT;
    lsn_ = INVALID_LSN;
    log_tot_len_ = LOG_HEADER_SIZE;
    log_tid_ = INVALID_TXN_ID;
    prev_lsn_ = INVALID_LSN;
  }
  AbortLogRecord(txn_id_t txn_id, lsn_t prev_lsn) : AbortLogRecord() {
    log_tid_ = txn_id;
    prev_lsn_ = prev_lsn;
  }
  // Serialize abort log fields to dest
  void serialize(char *dest) const override { LogRecord::serialize(dest); }
  // Deserialize abort log fields from src
  void deserialize(const char *src) override { LogRecord::deserialize(src); }
  void format_print() override {
    std::cout << "log type in son_function: " << LogTypeStr[log_type_] << "\n";
    LogRecord::format_print();
  }
};

class InsertLogRecord : public LogRecord {
 public:
  InsertLogRecord() {
    log_type_ = LogType::INSERT;
    lsn_ = INVALID_LSN;
    log_tot_len_ = LOG_HEADER_SIZE;
    log_tid_ = INVALID_TXN_ID;
    prev_lsn_ = INVALID_LSN;
    table_name_ = nullptr;
  }
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

  // 把insert日志记录序列化到dest中
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
  // 从src中反序列化出一条Insert日志记录
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
  // destructor
  ~InsertLogRecord() override {
    delete[] table_name_;
    table_name_ = nullptr;
  }

  RmRecord insert_value_;   // 插入的记录
  RID rid_;                 // 记录插入的位置
  char *table_name_;        // 插入记录的表名称
  size_t table_name_size_;  // 表名称的大小
};

/**
 * TODO: delete操作的日志记录
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

  // Serialize delete log fields to dest
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

  // Deserialize delete log fields from src
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

  // destructor
  ~DeleteLogRecord() override {
    delete[] table_name_;
    table_name_ = nullptr;
  }

  RmRecord delete_value_;   // Deleted record
  RID rid_;                 // Record location
  char *table_name_;        // Table name
  size_t table_name_size_;  // Table name size
};

/**
 * TODO: update操作的日志记录
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

  // Serialize update log fields to dest
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

  // Deserialize update log fields from src
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

  // destructor
  ~UpdateLogRecord() override {
    delete[] table_name_;
    table_name_ = nullptr;
  }

  RmRecord old_value_;      // Old record value
  RmRecord new_value_;      // New record value
  RID rid_;                 // Record location
  char *table_name_;        // Table name
  size_t table_name_size_;  // Table name size
};

/**
 * checkpoint操作的日志记录
 * @note: log 不再改变时 add_log_to_buffer
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

  // Serialize checkpoint log fields to dest
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

  // Deserialize checkpoint log fields from src
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
      printf(" txn_id: %d, lsn: %d\n", att.first, att.second);
    }
    printf("aborted_txns_vec: %lu\n", aborted_txns_vec_.size());
    for (auto &aborted_txn : aborted_txns_vec_) {
      printf(" txn_id: %d\n", aborted_txn);
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

  // att: txn_id -> last_lsn
  size_t att_size_;
  std::vector<std::pair<txn_id_t, lsn_t>> att_vec_;
  size_t aborted_txns_size_;
  std::vector<txn_id_t> aborted_txns_vec_;
  // dpt: (tab_name_idx from 0..dpt_size_-1, page_no) -> rec_lsn
  size_t dpt_size_;
  std::vector<std::pair<page_id_t, lsn_t>> dpt_vec_;
  lsn_t min_rec_lsn_;
  size_t tab_name_offset_size_;
  std::vector<size_t> tab_name_offset_vec_;  // Note: initialized to {0}
  size_t tab_name_str_size_;
  std::string tab_name_str_;
};

/* 日志缓冲区，只有一个buffer，因此需要阻塞地去把日志写入缓冲区中 */

class LogBuffer {
 public:
  LogBuffer() {
    offset_ = 0;
    memset(buffer_, 0, sizeof(buffer_));
  }

  bool is_full(int append_size) {
    if (offset_ + append_size > LOG_BUFFER_SIZE) return true;
    return false;
  }

  char buffer_[LOG_BUFFER_SIZE + 1];
  int offset_;  // 写入log的offset
};

/* 日志管理器，负责把日志写入日志缓冲区，以及把日志缓冲区中的内容写入磁盘中 */
class LogManager {
  friend class RecoveryManager;

 public:
  LogManager(DiskManager *disk_manager) { disk_manager_ = disk_manager; }

  lsn_t add_log_to_buffer(LogRecord *log_record);
  void flush_log_to_disk();

  LogBuffer *get_log_buffer() { return &log_buffer_; }

 private:
  std::atomic<lsn_t> global_lsn_{0};  // 全局lsn，递增，用于为每条记录分发lsn
  std::mutex latch_;                  // 用于对log_buffer_的互斥访问
  LogBuffer log_buffer_;              // 日志缓冲区
  lsn_t persist_lsn_;                 // 记录已经持久化到磁盘中的最后一条日志的日志号
  DiskManager *disk_manager_;
};
};  // namespace easydb