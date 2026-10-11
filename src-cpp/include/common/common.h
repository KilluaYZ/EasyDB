/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <cassert>
#include <cstring>
#include <memory>
#include <string>
#include <vector>
#include "defs.h"
#include "record/rm_defs.h"
#include "storage/table/tuple.h"
#include "system/sm_meta.h"
#include "type/value.h"

namespace easydb {

/**
 * @brief 表列结构体，用于标识数据库中的一个列
 *
 * TabCol 结构体包含列所属的表名、列名、聚合类型等信息。
 * 用于查询计划中引用和操作列。
 */
struct TabCol {
  /** @brief 表名称 */
  std::string tab_name;

  /** @brief 列名称 */
  std::string col_name;

  /** @brief 聚合类型（如SUM、COUNT、AVG等），如果不是聚合列则为NO_AGG */
  AggregationType aggregation_type;

  /** @brief 聚合列的新列名（用于SELECT中的别名） */
  std::string new_col_name;

  /**
   * @brief 小于运算符重载，用于排序和比较
   * @param x 左侧TabCol对象
   * @param y 右侧TabCol对象
   * @return true 如果x < y（按表名和列名字典序比较）
   */
  friend bool operator<(const TabCol &x, const TabCol &y) {
    return std::make_pair(x.tab_name, x.col_name) <
           std::make_pair(y.tab_name, y.col_name);
  }
};

/**
 * @brief 算术操作符枚举
 * @note 用于UPDATE语句中的SET子句，支持对列值进行算术运算
 */
enum ArithOp {
  OP_PLUS,  /**< 加法 (+) */
  OP_MINUS, /**< 减法 (-) */
  OP_MULTI, /**< 乘法 (*) */
  OP_DIV    /**< 除法 (/) */
};

/**
 * @brief SET子句结构体，用于UPDATE语句中的SET操作
 *
 * SetClause 表示UPDATE语句中的一个赋值操作，支持：
 * - 直接赋值：column = value
 * - 算术运算：column = column + value
 */
struct SetClause {
  /** @brief 左侧列（要更新的列） */
  TabCol lhs;

  /** @brief 右侧值（常量值） */
  Value rhs;

  /** @brief 算术操作符 */
  ArithOp op;

  /** @brief 如果右侧是表达式（值）则为true，如果右侧是列则为false */
  bool is_rhs_exp = false;

  /** @brief 右侧列（当右侧是列时使用） */
  TabCol rhs_col;

  /**
   * @brief 计算表达式的值
   * @param rhs_v 右侧列的值
   * @return 计算结果的值
   * @note
   *   - 要求is_rhs_exp为true（右侧必须是值）
   *   - 根据op执行相应的算术运算：rhs_v op rhs
   */
  Value cal_val(Value rhs_v) {
    assert(is_rhs_exp);
    switch (op) {
      case OP_PLUS:
        /* code */
        return rhs_v + rhs;
      case OP_MINUS:
        return rhs_v - rhs;
      case OP_MULTI:
        return rhs_v * rhs;
      case OP_DIV:
        return rhs_v / rhs;
      default:
        throw InternalError("unsupported operator.");
    }
  }
};

// struct cmpRecord {
//   cmpRecord(bool asce, ColMeta col) : asce_(asce), col_(col) {}
//   bool operator()(const RmRecord &pl, const RmRecord &pr) const {
//     Value leftVal, rightVal;
//     // leftVal.get_value_from_record(pl, col_);
//     // rightVal.get_value_from_record(pr, col_);
//     return !asce_ ? leftVal < rightVal : leftVal > rightVal;
//   }
//  private:
//   bool asce_;
//   ColMeta col_;
// };

/**
 * @brief 元组比较器结构体，用于排序操作
 *
 * cmpTuple 实现了元组的比较逻辑，根据指定的列对元组进行排序。
 * 可以用作std::sort等算法的比较函数对象。
 */
struct cmpTuple {
  /**
   * @brief 构造函数
   * @param asce 如果为true表示升序，false表示降序
   * @param col 用于比较的列
   */
  cmpTuple(bool asce, Column col) : asce_(asce), col_(col) {}

  /**
   * @brief 函数调用运算符，比较两个元组
   * @param pl 左侧元组
   * @param pr 右侧元组
   * @return
   *   - 如果asce_为true（升序）：返回leftVal < rightVal
   *   - 如果asce_为false（降序）：返回leftVal > rightVal
   * @note 比较基于col_列的值
   */
  bool operator()(const Tuple &pl, const Tuple &pr) const {
    Value leftVal, rightVal;
    leftVal = pl.GetValue(col_);
    rightVal = pr.GetValue(col_);
    // leftVal.get_value_from_record(pl, col_);
    // rightVal.get_value_from_record(pr, col_);
    return !asce_ ? leftVal < rightVal : leftVal > rightVal;
  }

 private:
  /** @brief 排序方向：true表示升序，false表示降序 */
  bool asce_;

  /** @brief 用于比较的列 */
  Column col_;
};

}  // namespace easydb