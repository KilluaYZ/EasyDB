#pragma once

#include "common/common.h"
#include "type/value.h"

namespace easydb {
enum CompOp { OP_EQ, OP_NE, OP_LT, OP_GT, OP_LE, OP_GE, OP_IN };
enum ArithOp { OP_PLUS, OP_MINUS, OP_MULTI, OP_DIV };

struct Condition {
  TabCol lhs_col;    // left-hand side column
  CompOp op;         // comparison operator
  bool is_rhs_val;   // true if right-hand side is a value (not a column)
  bool is_rhs_stmt;  // true if right-hand side is a subquery
  bool is_rhs_exe_processed;
  TabCol rhs_col;                  // right-hand side column
  Value rhs_val;                   // right-hand side value
  std::shared_ptr<void> rhs_stmt;  // right-hand side subquery stmt
  std::shared_ptr<void> rhs_stmt_exe;
  std::vector<Value> rhs_in_col;

  bool satisfy_in(Value lhs_v) {
    for (auto it : rhs_in_col) {
      if (lhs_v == it) {
        return true;
      }
    }
    return false;
  }

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
    // leftVal.get_value_from_record(pl, col_);
    // rightVal.get_value_from_record(pr, col_);
    return !asce_ ? leftVal < rightVal : leftVal > rightVal;
  }

 private:
  bool asce_;
  ColMeta col_;
};
};  // namespace easydb