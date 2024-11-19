#pragma once

#include "common/common.h"
#include "type/value.h"

namespace easydb {
enum CompOp { OP_EQ, OP_NE, OP_LT, OP_GT, OP_LE, OP_GE, OP_IN };

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

};  // namespace easydb