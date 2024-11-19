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

enum ArithOp { OP_PLUS, OP_MINUS, OP_MULTI, OP_DIV };

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


struct cmpTuple {
  cmpTuple(bool asce, Column col) : asce_(asce), col_(col) {}

  bool operator()(const Tuple &pl, const Tuple &pr) const {
    Value leftVal, rightVal;
    leftVal = pl.GetValue(col_);
    rightVal = pr.GetValue(col_);
    // leftVal.get_value_from_record(pl, col_);
    // rightVal.get_value_from_record(pr, col_);
    return !asce_ ? leftVal < rightVal : leftVal > rightVal;
  }

 private:
  bool asce_;
  Column col_;
};

}  // namespace easydb