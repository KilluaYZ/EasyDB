#pragma once

#include "common/common.h"
#include "type/value.h"

namespace easydb {

/**
 * @brief 比较操作符枚举
 * @note 用于WHERE子句中的条件表达式
 */
enum CompOp {
  OP_EQ, /**< 等于 (==) */
  OP_NE, /**< 不等于 (!=) */
  OP_LT, /**< 小于 (<) */
  OP_GT, /**< 大于 (>) */
  OP_LE, /**< 小于等于 (<=) */
  OP_GE, /**< 大于等于 (>=) */
  OP_IN  /**< IN操作符，检查值是否在集合中 */
};

/**
 * @brief 条件结构体，表示WHERE子句中的一个条件表达式
 *
 * Condition 结构体用于表示SQL查询中的条件，支持以下形式：
 * - 列与值的比较：column = value
 * - 列与列的比较：column1 = column2
 * - 列与子查询的比较：column IN (SELECT ...)
 * - IN操作：column IN (value1, value2, ...)
 */
struct Condition {
  /** @brief 左侧列（左操作数），表示条件表达式的左边 */
  TabCol lhs_col;

  /** @brief 比较操作符 */
  CompOp op;

  /** @brief 如果右侧是值（而非列）则为true */
  bool is_rhs_val;

  /** @brief 如果右侧是子查询则为true */
  bool is_rhs_stmt;

  /** @brief 右侧子查询是否已被执行处理 */
  bool is_rhs_exe_processed;

  /** @brief 右侧列（右操作数），当右侧是列时使用 */
  TabCol rhs_col;

  /** @brief 右侧值，当右侧是常量值时使用 */
  Value rhs_val;

  /** @brief 右侧子查询语句的指针，当右侧是子查询时使用 */
  std::shared_ptr<void> rhs_stmt;

  /** @brief 右侧子查询执行器的指针 */
  std::shared_ptr<void> rhs_stmt_exe;

  /** @brief IN操作符右侧的值列表 */
  std::vector<Value> rhs_in_col;

  /**
   * @brief 检查左侧值是否在IN列表中存在
   * @param lhs_v 左侧的值
   * @return true 如果lhs_v在rhs_in_col中，false 否则
   * @note 用于OP_IN操作符的求值
   */
  bool satisfy_in(Value lhs_v) {
    for (auto it : rhs_in_col) {
      if (lhs_v == it) {
        return true;
      }
    }
    return false;
  }

  /**
   * @brief 根据操作符计算条件是否满足
   * @param lhs_v 左侧的值
   * @param rhs_v 右侧的值
   * @return true 如果条件满足，false 否则
   * @note
   *   - 对于OP_IN操作符，使用satisfy_in方法
   *   - 对于其他操作符，直接比较lhs_v和rhs_v
   *   - 如果操作符不支持，抛出InternalError异常
   */
  bool satisfy(Value lhs_v, Value rhs_v) {
    switch (op) {
      case OP_EQ:
        /* code */
        return lhs_v == rhs_v;
      case OP_NE:
        return lhs_v != rhs_v;
      case OP_LT:
        return lhs_v < rhs_v;
      case OP_GT:
        return lhs_v > rhs_v;
      case OP_LE:
        return lhs_v <= rhs_v;
      case OP_GE:
        return lhs_v >= rhs_v;
      case OP_IN:
        return satisfy_in(lhs_v);
      default:
        throw InternalError("unsupported operator.");
    }
  }
};

};  // namespace easydb