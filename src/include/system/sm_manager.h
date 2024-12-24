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

#include "common/context.h"
#include "record/rm_file_handle.h"
#include "record/rm_manager.h"
#include "sm_defs.h"
#include "sm_meta.h"
#include "storage/index/ix_defs.h"
#include "storage/index/ix_manager.h"
#include "transaction/txn_defs.h"

namespace easydb {

class Context;

struct ColDef {
  std::string name;  // Column name
  ColType type;      // Type of column
  int len;           // Length of column
};

/* 系统管理器，负责元数据管理和DDL语句的执行 */
class SmManager {
 public:
  DbMeta db_;  // 当前打开的数据库的元数据
  std::unordered_map<std::string, std::unique_ptr<RmFileHandle>>
      fhs_;  // file name -> record file handle, 当前数据库中每张表的数据文件
  std::unordered_map<std::string, std::unique_ptr<IxIndexHandle>>
      ihs_;  // file name -> index file handle, 当前数据库中每个索引的文件
 private:
  DiskManager *disk_manager_;
  BufferPoolManager *buffer_pool_manager_;
  RmManager *rm_manager_;
  IxManager *ix_manager_;
  bool enable_output_;
  // map from table name to statistics
  std::unordered_map<std::string, int> table_count_;
  std::unordered_map<std::string, std::unordered_map<std::string, float>> table_attr_max_;
  std::unordered_map<std::string, std::unordered_map<std::string, float>> table_attr_min_;
  std::unordered_map<std::string, std::unordered_map<std::string, float>> table_attr_sum_;
  std::unordered_map<std::string, std::unordered_map<std::string, int>> table_attr_distinct_;
  // -1 for not load, 0 for loading, 1 for loaded
  int load_ = -1;
  std::vector<std::future<void>> futures_;

 public:
  SmManager(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, RmManager *rm_manager,
            IxManager *ix_manager, bool enable_output = true)
      : disk_manager_(disk_manager),
        buffer_pool_manager_(buffer_pool_manager),
        rm_manager_(rm_manager),
        ix_manager_(ix_manager),
        enable_output_(enable_output) {}

  ~SmManager() {}

  DiskManager *GetDiskManager() { return disk_manager_; }

  BufferPoolManager *GetBpm() { return buffer_pool_manager_; }

  RmManager *GetRmManager() { return rm_manager_; }

  IxManager *GetIxManager() { return ix_manager_; }

  bool IsDir(const std::string &db_name);

  void CreateDB(const std::string &db_name);

  void DropDB(const std::string &db_name);

  void OpenDB(const std::string &db_name);

  void CloseDB();

  void FlushMeta();

  void ShowTables(Context *context);

  void DescTable(const std::string &tab_name, Context *context);

  void CreateTable(const std::string &tab_name, const std::vector<ColDef> &col_defs, Context *context);

  void DropTable(const std::string &tab_name, Context *context);

  void ShowIndex(const std::string &tab_name, Context *context);

  void CreateIndex(const std::string &tab_name, const std::vector<std::string> &col_names, Context *context);

  void DropIndex(const std::string &tab_name, const std::vector<std::string> &col_names, Context *context);

  void DropIndex(const std::string &tab_name, const std::vector<ColMeta> &col_names, Context *context);

  // rollback for transaction
  void Rollback(WriteRecord *record, Context *context);

  void RollbackInsert(const std::string &table_name, RID &rid, Context *context);

  void RollbackDelete(const std::string &table_name, RID &rid, RmRecord &record, Context *context);

  void RollbackUpdate(const std::string &table_name, RID &rid, RmRecord &record, Context *context);

  // split string by delimiter
  void Split(const std::string &s, char delimiter, std::vector<std::string> &tokens);

  void Split(const char *start, size_t length, char delimiter, std::vector<std::string> &tokens);

  // load data from file
  void LoadData(const std::string &file_name, const std::string &table_name, Context *context);

  void AsyncLoadData(const std::string &file_name, const std::string &tab_name, Context *context);

  void AsyncLoadDataFinish();

  int GetLoadStatus() { return load_; }

  // output control
  void SetEnableOutput(bool set_val) { enable_output_ = set_val; }

  bool IsEnableOutput() { return enable_output_; }

  // table statistics
  void SetTableCount(const std::string &table_name, int count) {
    if (table_count_.find(table_name) == table_count_.end())
      table_count_.emplace(table_name, count);
    else
      (table_count_[table_name] += count);
  }

  // -1 if table not found
  int GetTableCount(const std::string &table_name) {
    if (table_count_.find(table_name) == table_count_.end()) return -1;
    return table_count_[table_name];
  }

  // update if table found
  void UpdateTableCount(const std::string &table_name, int count) {
    if (table_count_.find(table_name) == table_count_.end()) return;
    table_count_[table_name] += count;
  }

  // table statistics
  void SetTableAttrMax(const std::string &table_name, const std::string &attr_name, float count) {
    if (table_attr_max_.find(table_name) == table_attr_max_.end()) {
      std::unordered_map<std::string, float> map_tp;
      map_tp.emplace(attr_name, count);
      table_attr_max_.emplace(table_name, map_tp);
    } else if (table_attr_max_[table_name].find(attr_name) == table_attr_max_[table_name].end()) {
      table_attr_max_[table_name].emplace(attr_name, count);
    } else {
      table_attr_max_[table_name][attr_name] = count;
    }
  }

  // -1 if table or attr not found
  float GetTableAttrMax(const std::string &table_name, const std::string &attr_name) {
    if (table_attr_max_.find(table_name) == table_attr_max_.end())
      return -1;
    else if (table_attr_max_[table_name].find(attr_name) == table_attr_max_[table_name].end())
      return -1;
    return table_attr_max_[table_name][attr_name];
  }

  // table statistics
  void SetTableAttrMin(const std::string &table_name, const std::string &attr_name, float count) {
    if (table_attr_min_.find(table_name) == table_attr_min_.end()) {
      std::unordered_map<std::string, float> map_tp;
      map_tp.emplace(attr_name, count);
      table_attr_min_.emplace(table_name, map_tp);
    } else if (table_attr_min_[table_name].find(attr_name) == table_attr_min_[table_name].end()) {
      table_attr_min_[table_name].emplace(attr_name, count);
    } else {
      table_attr_min_[table_name][attr_name] = count;
    }
  }

  // -1 if table or attr not found
  float GetTableAttrMin(const std::string &table_name, const std::string &attr_name) {
    if (table_attr_min_.find(table_name) == table_attr_min_.end())
      return -1;
    else if (table_attr_min_[table_name].find(attr_name) == table_attr_min_[table_name].end())
      return -1;
    return table_attr_min_[table_name][attr_name];
  }

  // table statistics
  void SetTableAttrDistinct(const std::string &table_name, const std::string &attr_name, int count) {
    if (table_attr_distinct_.find(table_name) == table_attr_distinct_.end()) {
      std::unordered_map<std::string, int> map_tp;
      map_tp.emplace(attr_name, count);
      table_attr_distinct_.emplace(table_name, map_tp);
    } else if (table_attr_distinct_[table_name].find(attr_name) == table_attr_distinct_[table_name].end()) {
      table_attr_distinct_[table_name].emplace(attr_name, count);
    } else {
      table_attr_distinct_[table_name][attr_name] = count;
    }
  }

  // -1 if table or attr not found
  int GetTableAttrDistinct(const std::string &table_name, const std::string &attr_name) {
    if (table_attr_distinct_.find(table_name) == table_attr_distinct_.end())
      return -1;
    else if (table_attr_distinct_[table_name].find(attr_name) == table_attr_distinct_[table_name].end())
      return -1;
    return table_attr_distinct_[table_name][attr_name];
  }

  // table statistics
  void SetTableAttrSum(const std::string &table_name, const std::string &attr_name, float count) {
    if (table_attr_sum_.find(table_name) == table_attr_sum_.end()) {
      std::unordered_map<std::string, float> map_tp;
      map_tp.emplace(attr_name, count);
      table_attr_sum_.emplace(table_name, map_tp);
    } else if (table_attr_sum_[table_name].find(attr_name) == table_attr_sum_[table_name].end()) {
      table_attr_sum_[table_name].emplace(attr_name, count);
    } else {
      table_attr_sum_[table_name][attr_name] = count;
    }
  }

  // -1 if table or attr not found
  float GetTableAttrSum(const std::string &table_name, const std::string &attr_name) {
    if (table_attr_sum_.find(table_name) == table_attr_sum_.end())
      return -1;
    else if (table_attr_sum_[table_name].find(attr_name) == table_attr_sum_[table_name].end())
      return -1;
    return table_attr_sum_[table_name][attr_name];
  }

};

}  // namespace easydb
