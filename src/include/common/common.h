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
#include "system/sm_meta.h"
#include "type/value.h"

namespace easydb {
struct TabCol {
  std::string tab_name;
  std::string col_name;
  AggregationType aggregation_type;
  std::string new_col_name;  // new col name of aggregation col

  friend bool operator<(const TabCol &x, const TabCol &y) {
    return std::make_pair(x.tab_name, x.col_name) < std::make_pair(y.tab_name, y.col_name);
  }
};

enum CompOp { OP_EQ, OP_NE, OP_LT, OP_GT, OP_LE, OP_GE };
enum ArithOp { OP_PLUS, OP_MINUS, OP_MULTI, OP_DIV };

struct Condition {
  TabCol lhs_col;                  // left-hand side column
  CompOp op;                       // comparison operator
  bool is_rhs_val;                 // true if right-hand side is a value (not a column)
  TabCol rhs_col;                  // right-hand side column
  Value rhs_val;                   // right-hand side value
  std::shared_ptr<void> rhs_stmt;  // right-hand side subquery stmt
  std::shared_ptr<void> rhs_stmt_exe;
  std::vector<Value> rhs_in_col;

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
      default:
        throw InternalError("unsupported operator.");
    }
  }
};

struct SetClause {
  TabCol lhs;
  Value rhs;
  ArithOp op;               // comparison operator
  bool is_rhs_exp = false;  // true if right-hand side is a value (not a column)
  TabCol rhs_col;           // right-hand side column

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

struct cmpRecord {
  cmpRecord(bool asce, ColMeta col) : asce_(asce), col_(col) {}

  bool operator()(const RmRecord &pl, const RmRecord &pr) const {
    Value leftVal, rightVal;
    leftVal.get_value_from_record(pl, col_);
    rightVal.get_value_from_record(pr, col_);
    return !asce_ ? leftVal < rightVal : leftVal > rightVal;
  }

 private:
  bool asce_;
  ColMeta col_;
};

}  // namespace easydb