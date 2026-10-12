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

#include "common/config.h"
#include "storage/easystore/table/tuple.h"

namespace easydb {

/**
 * @brief 事务状态枚举
 * @note 标识事务在其生命周期中的不同阶段
 */
enum class TransactionState {
  DEFAULT,   /**< 默认状态，事务刚创建时的初始状态 */
  GROWING,   /**< 增长阶段，事务正在获取锁（两阶段锁定的第一阶段） */
  SHRINKING, /**< 收缩阶段，事务正在释放锁（两阶段锁定的第二阶段） */
  COMMITTED, /**< 已提交，事务成功完成 */
  ABORTED    /**< 已中止，事务因错误或冲突而被回滚 */
};

/**
 * @brief 系统的隔离级别枚举
 * @note 定义了不同的事务隔离级别，当前赛题中为可串行化隔离级别
 */
enum class IsolationLevel {
  READ_UNCOMMITTED, /**< 读未提交，最低隔离级别，允许脏读 */
  READ_COMMITTED,   /**< 读已提交，防止脏读 */
  REPEATABLE_READ,  /**< 可重复读，防止脏读和不可重复读 */
  SERIALIZABLE      /**< 可串行化，最高隔离级别，完全隔离，防止所有并发问题 */
};

/**
 * @brief 事务写操作类型枚举
 * @note 包括插入、删除、更新三种操作
 */
enum class WType {
  INSERT_TUPLE = 0, /**< 插入元组操作 */
  DELETE_TUPLE,     /**< 删除元组操作 */
  UPDATE_TUPLE      /**< 更新元组操作 */
};

/**
 * @brief 事务的写操作记录类，用于事务的回滚
 *
 * WriteRecord 记录事务执行的所有写操作，以便在回滚时撤销这些操作。
 *
 * INSERT操作的记录格式：
 * --------------------------------
 * | wtype | tab_name | tuple_rid |
 * --------------------------------
 *
 * DELETE / UPDATE操作的记录格式：
 * ----------------------------------------------
 * | wtype | tab_name | tuple_rid | tuple_value |
 * ----------------------------------------------
 */
class WriteRecord {
 public:
  /**
   * @brief 默认构造函数
   */
  WriteRecord() = default;

  /**
   * @brief INSERT操作的构造函数
   * @param wtype 写操作类型（INSERT_TUPLE）
   * @param tab_name 表名
   * @param rid 插入的元组的记录ID
   */
  WriteRecord(WType wtype, const std::string &tab_name, const Tuple &key)
      : wtype_(wtype), tab_name_(tab_name), key_(key) {}

  /**
   * @brief DELETE和UPDATE操作的构造函数
   * @param wtype 写操作类型（DELETE_TUPLE或UPDATE_TUPLE）
   * @param tab_name 表名
   * @param rid 操作的元组的记录ID
   * @param tuple 元组值（DELETE时存储旧值，UPDATE时存储旧值用于回滚）
   */
  WriteRecord(WType wtype, const std::string &tab_name, const Tuple &key,
              const Tuple &tuple)
      : wtype_(wtype), tab_name_(tab_name), key_(key), tuple_(tuple) {}

  /**
   * @brief 析构函数
   */
  ~WriteRecord() = default;

  /**
   * @brief 获取元组值
   * @return 元组值的引用
   */
  inline Tuple &GetTuple() { return tuple_; }

  /**
   * @brief 获取记录ID
   * @return 记录ID的引用
   */
  inline Tuple &GetKey() { return key_; }

  /**
   * @brief 获取写操作类型
   * @return 写操作类型的引用
   */
  inline WType &GetWriteType() { return wtype_; }

  /**
   * @brief 获取表名
   * @return 表名字符串的引用
   */
  inline std::string &GetTableName() { return tab_name_; }

 private:
  /** @brief 写操作类型 */
  WType wtype_;

  /** @brief 表名 */
  std::string tab_name_;

  /** @brief 聚簇主键 tuple */
  Tuple key_;

  /** @brief 元组值（用于DELETE和UPDATE操作的回滚） */
  Tuple tuple_;
};

/**
 * @brief 加锁对象类型。
 *
 * 聚簇表的记录由主键定位，不再使用 RID 作为运行时存储标识。
 * 当前锁管理器只保留表级锁；记录级锁将在基于主键的锁键设计完成后再引入。
 */
enum class LockDataType { TABLE = 0 };

/**
 * @brief 加锁对象的唯一标识类
 *
 * LockDataId 用于唯一标识一个表级加锁对象。
 * 用于在锁表中查找和管理锁请求。
 */
class LockDataId {
 public:
  /**
   * @brief 表级锁的构造函数
   * @param fd 文件描述符
   * @param type 锁数据类型（必须是LockDataType::TABLE）
   */
  LockDataId(int fd, LockDataType type) {
    assert(type == LockDataType::TABLE);
    fd_ = fd;
    type_ = type;
  }

  /**
   * @brief 将LockDataId编码为64位整数
   * @return 64位整数表示
   */
  inline int64_t Get() const {
    return static_cast<int64_t>(fd_);
  }

  /**
   * @brief 相等运算符重载
   * @param other 另一个LockDataId对象
   * @return true 如果两个LockDataId的所有字段都相等
   */
  bool operator==(const LockDataId &other) const {
    if (type_ != other.type_) return false;
    return fd_ == other.fd_;
  }

  /** @brief 文件描述符 */
  int fd_;

  /** @brief 锁数据类型 */
  LockDataType type_;
};

/**
 * @brief 事务回滚原因枚举
 * @note 标识事务被中止的不同原因
 */
enum class AbortReason {
  LOCK_ON_SHIRINKING = 0, /**< 在收缩阶段请求锁（违反两阶段锁定协议） */
  UPGRADE_CONFLICT,       /**< 锁升级冲突（有其他事务等待升级） */
  DEADLOCK_PREVENTION     /**< 死锁预防（wait-die策略中，年轻事务被中止） */
};

/**
 * @brief 事务回滚异常类
 *
 * TransactionAbortException 在事务被中止时抛出，包含事务ID和回滚原因。
 * 在rmdb.cpp中进行处理。
 */
class TransactionAbortException : public std::exception {
  /** @brief 被中止的事务ID */
  txn_id_t txn_id_;

  /** @brief 回滚原因 */
  AbortReason abort_reason_;

 public:
  /**
   * @brief 构造函数
   * @param txn_id 被中止的事务ID
   * @param abort_reason 回滚原因
   */
  explicit TransactionAbortException(txn_id_t txn_id, AbortReason abort_reason)
      : txn_id_(txn_id), abort_reason_(abort_reason) {}

  /**
   * @brief 获取事务ID
   * @return 被中止的事务ID
   */
  txn_id_t GetTransactionId() { return txn_id_; }

  /**
   * @brief 获取回滚原因
   * @return 回滚原因枚举值
   */
  AbortReason GetAbortReason() { return abort_reason_; }

  /**
   * @brief 获取异常信息字符串
   * @return 描述异常原因的信息字符串
   */
  std::string GetInfo() {
    switch (abort_reason_) {
      case AbortReason::LOCK_ON_SHIRINKING: {
        return "Transaction " + std::to_string(txn_id_) +
               " aborted because it cannot request locks on SHRINKING phase\n";
      } break;

      case AbortReason::UPGRADE_CONFLICT: {
        return "Transaction " + std::to_string(txn_id_) +
               " aborted because another transaction is waiting for "
               "upgrading\n";
      } break;

      case AbortReason::DEADLOCK_PREVENTION: {
        return "Transaction " + std::to_string(txn_id_) +
               " aborted for deadlock prevention\n";
      } break;

      default: {
        return "Transaction aborted\n";
      } break;
    }
  }
};

};  // namespace easydb

namespace std {

template <>
struct std::hash<easydb::LockDataId> {
  size_t operator()(const easydb::LockDataId &obj) const {
    return std::hash<int64_t>()(obj.Get());
  }
};

}  // namespace std
