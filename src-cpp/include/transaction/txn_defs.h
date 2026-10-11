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
#include "storage/index/ix_defs.h"
#include "storage/table/tuple.h"

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
  WriteRecord(WType wtype, const std::string &tab_name, const RID &rid)
      : wtype_(wtype), tab_name_(tab_name), rid_(rid) {}

  /**
   * @brief DELETE和UPDATE操作的构造函数
   * @param wtype 写操作类型（DELETE_TUPLE或UPDATE_TUPLE）
   * @param tab_name 表名
   * @param rid 操作的元组的记录ID
   * @param tuple 元组值（DELETE时存储旧值，UPDATE时存储旧值用于回滚）
   */
  WriteRecord(WType wtype, const std::string &tab_name, const RID &rid,
              const Tuple &tuple)
      : wtype_(wtype), tab_name_(tab_name), rid_(rid), tuple_(tuple) {}

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
  inline RID &GetRid() { return rid_; }

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

  /** @brief 记录ID */
  RID rid_;

  /** @brief 元组值（用于DELETE和UPDATE操作的回滚） */
  Tuple tuple_;
};

/**
 * @brief 多粒度锁的加锁对象类型枚举
 * @note 包括表、记录和间隙三种类型
 */
enum class LockDataType {
  TABLE = 0,  /**< 表级锁 */
  RECORD = 1, /**< 行级锁（记录锁） */
  GAP = 2     /**< 间隙锁（用于索引，防止幻读） */
};

/**
 * @brief 加锁对象的唯一标识类
 *
 * LockDataId 用于唯一标识一个加锁对象，可以是表、记录或索引间隙。
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
    // rid_.page_no = -1;
    // rid_.slot_no = -1;
    rid_.Set(0, 0);
  }

  /**
   * @brief 行级锁的构造函数
   * @param fd 文件描述符
   * @param rid 记录ID
   * @param type 锁数据类型（必须是LockDataType::RECORD）
   */
  LockDataId(int fd, const RID &rid, LockDataType type) {
    assert(type == LockDataType::RECORD);
    fd_ = fd;
    rid_ = rid;
    type_ = type;
  }

  /**
   * @brief 间隙锁的构造函数
   * @param fd 文件描述符
   * @param iid 索引项ID
   * @param type 锁数据类型（必须是LockDataType::GAP）
   */
  LockDataId(int fd, const Iid &iid, LockDataType type) {
    assert(type == LockDataType::GAP);
    fd_ = fd;
    rid_ = {iid.page_id_, iid.slot_num_};
    type_ = type;
  }

  /**
   * @brief 将LockDataId编码为64位整数
   * @return 64位整数表示
   * @note
   *   - 如果type_是GAP，由于2的二进制是10，移位会导致溢出
   *   - 但由于RECORD类型的二进制表示是1，且fd_不同，仍然可以区分
   *   - 避免与TABLE类型的二进制表示冲突
   */
  inline int64_t Get() const {
    if (type_ == LockDataType::TABLE) {
      // fd_
      return static_cast<int64_t>(fd_);
    } else {
      // fd_, rid_.page_no, rid.slot_no
      return ((static_cast<int64_t>(type_)) << 63) |
             ((static_cast<int64_t>(fd_)) << 31) |
             ((static_cast<int64_t>(rid_.GetPageId())) << 16) |
             rid_.GetSlotNum();
    }
  }

  /**
   * @brief 相等运算符重载
   * @param other 另一个LockDataId对象
   * @return true 如果两个LockDataId的所有字段都相等
   */
  bool operator==(const LockDataId &other) const {
    if (type_ != other.type_) return false;
    if (fd_ != other.fd_) return false;
    return rid_ == other.rid_;
  }

  /** @brief 文件描述符 */
  int fd_;

  /** @brief 记录ID（对于表级锁，此字段未使用） */
  RID rid_;

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
