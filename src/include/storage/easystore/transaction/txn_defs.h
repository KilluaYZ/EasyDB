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

#include <functional>
#include <utility>
#include <vector>

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
 * @brief 加锁对象类型。
 *
 * 聚簇表的记录由主键定位。
 * 锁对象按聚簇索引组织。表锁使用 fd 标识；记录锁使用主键字节串标识；
 * 间隙锁和 Next-Key 锁使用 [lower_key, upper_key) 表示索引区间。
 */
enum class LockDataType {
  TABLE = 0,
  RECORD,
  GAP,
  NEXT_KEY,
  INSERT_INTENTION,
};

/**
 * @brief 加锁对象的唯一标识类
 *
 * LockDataId 用于唯一标识一个表、记录或索引区间加锁对象。
 * 用于在锁表中查找和管理锁请求。
 */
class LockDataId {
 public:
  /**
   * @brief 表级锁的构造函数
   */
  LockDataId(int fd, LockDataType type) {
    assert(type == LockDataType::TABLE);
    fd_ = fd;
    type_ = type;
  }

  /** @brief 记录锁或插入意向锁的构造函数。 */
  LockDataId(int fd, LockDataType type, std::vector<char> key)
      : fd_(fd), type_(type), lower_key_(std::move(key)) {
    assert(type == LockDataType::RECORD ||
           type == LockDataType::INSERT_INTENTION);
  }

  /** @brief 间隙锁或 Next-Key 锁的构造函数。 */
  LockDataId(int fd, LockDataType type, std::vector<char> lower_key,
             std::vector<char> upper_key)
      : fd_(fd),
        type_(type),
        lower_key_(std::move(lower_key)),
        upper_key_(std::move(upper_key)) {
    assert(type == LockDataType::GAP || type == LockDataType::NEXT_KEY ||
           type == LockDataType::INSERT_INTENTION);
  }

  /**
   * @brief 将LockDataId编码为64位整数
   * @return 64位整数表示
   */
  inline int64_t Get() const {
    size_t hash = std::hash<int>{}(fd_);
    hash ^= std::hash<int>{}(static_cast<int>(type_)) + 0x9e3779b9 +
            (hash << 6) + (hash >> 2);
    for (char byte : lower_key_) {
      hash ^= std::hash<unsigned char>{}(static_cast<unsigned char>(byte)) +
              0x9e3779b9 + (hash << 6) + (hash >> 2);
    }
    hash ^= 0x9e3779b9 + (hash << 6) + (hash >> 2);
    for (char byte : upper_key_) {
      hash ^= std::hash<unsigned char>{}(static_cast<unsigned char>(byte)) +
              0x9e3779b9 + (hash << 6) + (hash >> 2);
    }
    return static_cast<int64_t>(hash);
  }

  /**
   * @brief 相等运算符重载
   * @param other 另一个LockDataId对象
   * @return true 如果两个LockDataId的所有字段都相等
   */
  bool operator==(const LockDataId &other) const {
    if (type_ != other.type_) return false;
    return fd_ == other.fd_ && lower_key_ == other.lower_key_ &&
           upper_key_ == other.upper_key_;
  }

  LockDataType GetType() const { return type_; }
  const std::vector<char> &GetLowerKey() const { return lower_key_; }
  const std::vector<char> &GetUpperKey() const { return upper_key_; }

  /** @brief 文件描述符 */
  int fd_;

  /** @brief 锁数据类型 */
  LockDataType type_;

  /** @brief 记录主键，或区间下界 */
  std::vector<char> lower_key_;

  /** @brief 区间上界；记录锁和插入意向锁为空 */
  std::vector<char> upper_key_;
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
