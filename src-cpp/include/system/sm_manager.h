/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL
v2. You may obtain a copy of Mulan PSL v2 at:
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
#include "storage/table/tuple.h"
#include "transaction/txn_defs.h"

namespace easydb {

class Context;

/**
 * @brief 列定义结构体
 * @note 用于CREATE TABLE语句中定义列
 */
struct ColDef {
  /** @brief 列名 */
  std::string name;

  /** @brief 列的类型 */
  ColType type;

  /** @brief 列的长度（对于变长类型） */
  int len;
};

/**
 * @brief 系统管理器类，负责元数据管理和DDL语句的执行
 *
 * SmManager 是数据库系统的核心管理器，负责：
 * - 数据库的创建、删除、打开、关闭
 * - 表的创建、删除、描述
 * - 索引的创建、删除
 * - 元数据的管理和维护
 * - 表统计信息的维护
 */
class SmManager {
 public:
  /**
   * @brief 当前打开的数据库的元数据
   * @note 包含数据库中的所有表、索引等元信息
   */
  DbMeta db_;

  /**
   * @brief 文件名到记录文件句柄的映射
   * @note 存储当前数据库中每张表的数据文件句柄
   *       Key: 文件名，Value: 记录文件句柄的智能指针
   */
  std::unordered_map<std::string, std::unique_ptr<RmFileHandle>> fhs_;

  /**
   * @brief 文件名到索引文件句柄的映射
   * @note 存储当前数据库中每个索引的文件句柄
   *       Key: 文件名，Value: 索引文件句柄的智能指针
   */
  std::unordered_map<std::string, std::unique_ptr<IxIndexHandle>> ihs_;

 private:
  /** @brief 磁盘管理器指针 */
  DiskManager *disk_manager_;

  /** @brief 缓冲池管理器指针 */
  BufferPoolManager *buffer_pool_manager_;

  /** @brief 记录管理器指针 */
  RmManager *rm_manager_;

  /** @brief 索引管理器指针 */
  IxManager *ix_manager_;

  /** @brief 是否启用输出标志 */
  bool enable_output_;

  // ==================== 表统计信息 ====================

  /**
   * @brief 表名到记录数量的映射
   * @note 用于统计每张表的记录数量
   */
  std::unordered_map<std::string, int> table_count_;

  /**
   * @brief 表名和属性名到最大值的映射
   * @note 用于统计每张表每个属性的最大值
   */
  std::unordered_map<std::string, std::unordered_map<std::string, float>>
      table_attr_max_;

  /**
   * @brief 表名和属性名到最小值的映射
   * @note 用于统计每张表每个属性的最小值
   */
  std::unordered_map<std::string, std::unordered_map<std::string, float>>
      table_attr_min_;

  /**
   * @brief 表名和属性名到总和的映射
   * @note 用于统计每张表每个属性的总和（用于计算平均值）
   */
  std::unordered_map<std::string, std::unordered_map<std::string, float>>
      table_attr_sum_;

  /**
   * @brief 表名和属性名到不同值数量的映射
   * @note 用于统计每张表每个属性的不同值数量
   */
  std::unordered_map<std::string, std::unordered_map<std::string, int>>
      table_attr_distinct_;

  /**
   * @brief 数据加载状态
   * @note -1表示未加载，0表示正在加载，1表示已加载
   */
  int load_ = -1;

  /**
   * @brief 异步加载数据的future向量
   * @note 用于跟踪异步数据加载任务
   */
  std::vector<std::future<void>> futures_;

 public:
  /**
   * @brief 构造函数
   * @param disk_manager 磁盘管理器指针
   * @param buffer_pool_manager 缓冲池管理器指针
   * @param rm_manager 记录管理器指针
   * @param ix_manager 索引管理器指针
   * @param enable_output 是否启用输出（默认true）
   */
  SmManager(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager,
            RmManager *rm_manager, IxManager *ix_manager,
            bool enable_output = true)
      : disk_manager_(disk_manager),
        buffer_pool_manager_(buffer_pool_manager),
        rm_manager_(rm_manager),
        ix_manager_(ix_manager),
        enable_output_(enable_output) {}

  /**
   * @brief 析构函数
   */
  ~SmManager() {}

  /**
   * @brief 获取磁盘管理器
   * @return 磁盘管理器指针
   */
  DiskManager *GetDiskManager() { return disk_manager_; }

  /**
   * @brief 获取缓冲池管理器
   * @return 缓冲池管理器指针
   */
  BufferPoolManager *GetBpm() { return buffer_pool_manager_; }

  /**
   * @brief 获取记录管理器
   * @return 记录管理器指针
   */
  RmManager *GetRmManager() { return rm_manager_; }

  /**
   * @brief 获取索引管理器
   * @return 索引管理器指针
   */
  IxManager *GetIxManager() { return ix_manager_; }

  /**
   * @brief 检查指定名称是否为目录
   * @param db_name 数据库名称
   * @return true 如果是目录，false 否则
   */
  bool IsDir(const std::string &db_name);

  /**
   * @brief 创建数据库
   * @param db_name 数据库名称
   * @throws DatabaseExistsError 如果数据库已存在
   */
  void CreateDB(const std::string &db_name);

  /**
   * @brief 删除数据库
   * @param db_name 数据库名称
   * @throws DatabaseNotFoundError 如果数据库不存在
   */
  void DropDB(const std::string &db_name);

  /**
   * @brief 打开数据库
   * @param db_name 数据库名称
   * @throws DatabaseNotFoundError 如果数据库不存在
   * @note 加载数据库元数据，打开所有表和索引文件
   */
  void OpenDB(const std::string &db_name);

  /**
   * @brief 关闭数据库
   * @note 刷新元数据到磁盘，关闭所有文件和索引
   */
  void CloseDB();

  /**
   * @brief 刷新元数据到磁盘
   * @note 将数据库元数据写回磁盘文件
   */
  void FlushMeta();

  /**
   * @brief 显示所有表
   * @param context 上下文对象指针
   */
  void ShowTables(Context *context);

  /**
   * @brief 描述表结构
   * @param tab_name 表名
   * @param context 上下文对象指针
   * @throws TableNotFoundError 如果表不存在
   */
  void DescTable(const std::string &tab_name, Context *context);

  /**
   * @brief 创建表
   * @param tab_name 表名
   * @param col_defs 列定义向量
   * @param context 上下文对象指针
   * @throws TableExistsError 如果表已存在
   */
  void CreateTable(const std::string &tab_name,
                   const std::vector<ColDef> &col_defs, Context *context);

  /**
   * @brief 删除表
   * @param tab_name 表名
   * @param context 上下文对象指针
   * @throws TableNotFoundError 如果表不存在
   * @note 同时删除表的所有索引
   */
  void DropTable(const std::string &tab_name, Context *context);

  /**
   * @brief 显示表的所有索引
   * @param tab_name 表名
   * @param context 上下文对象指针
   */
  void ShowIndex(const std::string &tab_name, Context *context);

  /**
   * @brief 创建索引
   * @param tab_name 表名
   * @param col_names 列名向量
   * @param context 上下文对象指针
   * @throws TableNotFoundError 如果表不存在
   * @throws IndexExistsError 如果索引已存在
   */
  void CreateIndex(const std::string &tab_name,
                   const std::vector<std::string> &col_names, Context *context);

  /**
   * @brief 删除索引（根据列名）
   * @param tab_name 表名
   * @param col_names 列名向量
   * @param context 上下文对象指针
   * @throws IndexNotFoundError 如果索引不存在
   */
  void DropIndex(const std::string &tab_name,
                 const std::vector<std::string> &col_names, Context *context);

  /**
   * @brief 删除索引（根据列元数据）
   * @param tab_name 表名
   * @param col_names 列元数据向量
   * @param context 上下文对象指针
   * @throws IndexNotFoundError 如果索引不存在
   */
  void DropIndex(const std::string &tab_name,
                 const std::vector<ColMeta> &col_names, Context *context);

  // ==================== 事务回滚相关 ====================

  /**
   * @brief 回滚写操作（用于事务回滚）
   * @param record 写操作记录指针
   * @param context 上下文对象指针
   * @note 根据写操作类型调用相应的回滚方法
   */
  void Rollback(WriteRecord *record, Context *context);

  /**
   * @brief 回滚插入操作
   * @param table_name 表名
   * @param rid 记录ID
   * @param context 上下文对象指针
   */
  void RollbackInsert(const std::string &table_name, RID &rid,
                      Context *context);

  /**
   * @brief 回滚删除操作
   * @param table_name 表名
   * @param rid 记录ID
   * @param record 被删除的记录
   * @param context 上下文对象指针
   */
  void RollbackDelete(const std::string &table_name, RID &rid, Tuple &record,
                      Context *context);

  /**
   * @brief 回滚更新操作
   * @param table_name 表名
   * @param rid 记录ID
   * @param record 旧记录值
   * @param context 上下文对象指针
   */
  void RollbackUpdate(const std::string &table_name, RID &rid, Tuple &record,
                      Context *context);

  // ==================== 工具函数 ====================

  /**
   * @brief 按分隔符分割字符串
   * @param s 要分割的字符串
   * @param delimiter 分隔符
   * @param[out] tokens 输出参数，存储分割后的字符串向量
   */
  void Split(const std::string &s, char delimiter,
             std::vector<std::string> &tokens);

  /**
   * @brief 按分隔符分割字符数组
   * @param start 字符数组起始指针
   * @param length 字符数组长度
   * @param delimiter 分隔符
   * @param[out] tokens 输出参数，存储分割后的字符串向量
   */
  void Split(const char *start, size_t length, char delimiter,
             std::vector<std::string> &tokens);

  // ==================== 数据加载 ====================

  /**
   * @brief 从文件加载数据到表
   * @param file_name 数据文件名
   * @param table_name 表名
   * @param context 上下文对象指针
   * @note 同步加载，会阻塞直到加载完成
   */
  void LoadData(const std::string &file_name, const std::string &table_name,
                Context *context);

  /**
   * @brief 异步从文件加载数据到表
   * @param file_name 数据文件名
   * @param tab_name 表名
   * @param context 上下文对象指针
   * @note 异步加载，不阻塞调用线程
   */
  void AsyncLoadData(const std::string &file_name, const std::string &tab_name,
                     Context *context);

  /**
   * @brief 完成异步数据加载
   * @note 等待所有异步加载任务完成
   */
  void AsyncLoadDataFinish();

  /**
   * @brief 获取数据加载状态
   * @return -1表示未加载，0表示正在加载，1表示已加载
   */
  int GetLoadStatus() { return load_; }

  // ==================== 输出控制 ====================

  /**
   * @brief 设置是否启用输出
   * @param set_val 新的输出标志值
   */
  void SetEnableOutput(bool set_val) { enable_output_ = set_val; }

  /**
   * @brief 获取是否启用输出
   * @return true 如果启用输出，false 否则
   */
  bool IsEnableOutput() { return enable_output_; }

  // ==================== 表统计信息 ====================

  /**
   * @brief 设置表的记录数量
   * @param table_name 表名
   * @param count 记录数量
   * @note 如果表不存在，创建新条目；如果存在，累加数量
   */
  void SetTableCount(const std::string &table_name, int count) {
    if (table_count_.find(table_name) == table_count_.end())
      table_count_.emplace(table_name, count);
    else
      (table_count_[table_name] += count);
  }

  /**
   * @brief 获取表的记录数量
   * @param table_name 表名
   * @return 记录数量，如果表不存在返回-1
   */
  int GetTableCount(const std::string &table_name) {
    if (table_count_.find(table_name) == table_count_.end()) return -1;
    return table_count_[table_name];
  }

  /**
   * @brief 更新表的记录数量（如果表存在）
   * @param table_name 表名
   * @param count 要增加的数量
   * @note 如果表不存在，不做任何操作
   */
  void UpdateTableCount(const std::string &table_name, int count) {
    if (table_count_.find(table_name) == table_count_.end()) return;
    table_count_[table_name] += count;
  }

  /**
   * @brief 设置表属性的最大值
   * @param table_name 表名
   * @param attr_name 属性名
   * @param count 最大值
   */
  void SetTableAttrMax(const std::string &table_name,
                       const std::string &attr_name, float count) {
    if (table_attr_max_.find(table_name) == table_attr_max_.end()) {
      std::unordered_map<std::string, float> map_tp;
      map_tp.emplace(attr_name, count);
      table_attr_max_.emplace(table_name, map_tp);
    } else if (table_attr_max_[table_name].find(attr_name) ==
               table_attr_max_[table_name].end()) {
      table_attr_max_[table_name].emplace(attr_name, count);
    } else {
      table_attr_max_[table_name][attr_name] = count;
    }
  }

  /**
   * @brief 获取表属性的最大值
   * @param table_name 表名
   * @param attr_name 属性名
   * @return 最大值，如果表或属性不存在返回-1
   */
  float GetTableAttrMax(const std::string &table_name,
                        const std::string &attr_name) {
    if (table_attr_max_.find(table_name) == table_attr_max_.end())
      return -1;
    else if (table_attr_max_[table_name].find(attr_name) ==
             table_attr_max_[table_name].end())
      return -1;
    return table_attr_max_[table_name][attr_name];
  }

  /**
   * @brief 设置表属性的最小值
   * @param table_name 表名
   * @param attr_name 属性名
   * @param count 最小值
   */
  void SetTableAttrMin(const std::string &table_name,
                       const std::string &attr_name, float count) {
    if (table_attr_min_.find(table_name) == table_attr_min_.end()) {
      std::unordered_map<std::string, float> map_tp;
      map_tp.emplace(attr_name, count);
      table_attr_min_.emplace(table_name, map_tp);
    } else if (table_attr_min_[table_name].find(attr_name) ==
               table_attr_min_[table_name].end()) {
      table_attr_min_[table_name].emplace(attr_name, count);
    } else {
      table_attr_min_[table_name][attr_name] = count;
    }
  }

  /**
   * @brief 获取表属性的最小值
   * @param table_name 表名
   * @param attr_name 属性名
   * @return 最小值，如果表或属性不存在返回-1
   */
  float GetTableAttrMin(const std::string &table_name,
                        const std::string &attr_name) {
    if (table_attr_min_.find(table_name) == table_attr_min_.end())
      return -1;
    else if (table_attr_min_[table_name].find(attr_name) ==
             table_attr_min_[table_name].end())
      return -1;
    return table_attr_min_[table_name][attr_name];
  }

  /**
   * @brief 设置表属性的不同值数量
   * @param table_name 表名
   * @param attr_name 属性名
   * @param count 不同值数量
   */
  void SetTableAttrDistinct(const std::string &table_name,
                            const std::string &attr_name, int count) {
    if (table_attr_distinct_.find(table_name) == table_attr_distinct_.end()) {
      std::unordered_map<std::string, int> map_tp;
      map_tp.emplace(attr_name, count);
      table_attr_distinct_.emplace(table_name, map_tp);
    } else if (table_attr_distinct_[table_name].find(attr_name) ==
               table_attr_distinct_[table_name].end()) {
      table_attr_distinct_[table_name].emplace(attr_name, count);
    } else {
      table_attr_distinct_[table_name][attr_name] = count;
    }
  }

  /**
   * @brief 获取表属性的不同值数量
   * @param table_name 表名
   * @param attr_name 属性名
   * @return 不同值数量，如果表或属性不存在返回-1
   */
  int GetTableAttrDistinct(const std::string &table_name,
                           const std::string &attr_name) {
    if (table_attr_distinct_.find(table_name) == table_attr_distinct_.end())
      return -1;
    else if (table_attr_distinct_[table_name].find(attr_name) ==
             table_attr_distinct_[table_name].end())
      return -1;
    return table_attr_distinct_[table_name][attr_name];
  }

  /**
   * @brief 设置表属性的总和
   * @param table_name 表名
   * @param attr_name 属性名
   * @param count 总和值
   * @note 用于计算平均值（平均值 = 总和 / 记录数）
   */
  void SetTableAttrSum(const std::string &table_name,
                       const std::string &attr_name, float count) {
    if (table_attr_sum_.find(table_name) == table_attr_sum_.end()) {
      std::unordered_map<std::string, float> map_tp;
      map_tp.emplace(attr_name, count);
      table_attr_sum_.emplace(table_name, map_tp);
    } else if (table_attr_sum_[table_name].find(attr_name) ==
               table_attr_sum_[table_name].end()) {
      table_attr_sum_[table_name].emplace(attr_name, count);
    } else {
      table_attr_sum_[table_name][attr_name] = count;
    }
  }

  /**
   * @brief 获取表属性的总和
   * @param table_name 表名
   * @param attr_name 属性名
   * @return 总和值，如果表或属性不存在返回-1
   */
  float GetTableAttrSum(const std::string &table_name,
                        const std::string &attr_name) {
    if (table_attr_sum_.find(table_name) == table_attr_sum_.end())
      return -1;
    else if (table_attr_sum_[table_name].find(attr_name) ==
             table_attr_sum_[table_name].end())
      return -1;
    return table_attr_sum_[table_name][attr_name];
  }
};

}  // namespace easydb
