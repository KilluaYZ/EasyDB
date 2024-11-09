/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <cassert>
#include <cstring>
#include <memory>
#include <string>
#include <vector>
#include "defs.h"
#include "record/rm_defs.h"
#include "system/sm_meta.h"

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

// struct Value {
//   ColType type;  // type of value
//   union {
//     int int_val;  // int value
//     long long long_val;
//     float float_val;  // float value
//     double double_val;
//   };
//   std::string str_val;  // string value

//   std::shared_ptr<RmRecord> raw = nullptr;  // raw record buffer

//   void set_int(int int_val_) {
//     type = TYPE_INT;
//     int_val = int_val_;
//   }

//   void set_long(long long long_val_) {
//     type = TYPE_LONG;
//     long_val = long_val_;
//   }

//   void set_float(float float_val_) {
//     type = TYPE_FLOAT;
//     float_val = float_val_;
//   }

//   void set_double(double double_val_) {
//     type = TYPE_DOUBLE;
//     double_val = double_val_;
//   }

//   void set_char(std::string str_val_) {
//     type = TYPE_CHAR;
//     str_val = std::move(str_val_);
//   }

//   void set_varchar(std::string str_val_) {
//     type = TYPE_VARCHAR;
//     str_val = std::move(str_val_);
//   }

//   void init_raw(int len) {
//     assert(raw == nullptr);
//     raw = std::make_shared<RmRecord>(len);
//     if (type == TYPE_INT) {
//       assert(len == sizeof(int));
//       *(int *)(raw->data) = int_val;
//     } else if (type == TYPE_LONG) {
//       assert(len == sizeof(long long));
//       *(long long *)(raw->data) = long_val;
//     } else if (type == TYPE_FLOAT) {
//       assert(len == sizeof(float));
//       *(float *)(raw->data) = float_val;
//     } else if (type == TYPE_DOUBLE) {
//       assert(len == sizeof(double));
//       *(double *)(raw->data) = double_val;
//     } else if (type == TYPE_CHAR || type == TYPE_VARCHAR) {
//       if (len < (int)str_val.size()) {
//         throw StringOverflowError();
//       }
//       memset(raw->data, 0, len);
//       memcpy(raw->data, str_val.c_str(), str_val.size());
//     }
//   }

//   Value *get_value_from_record(RmRecord record, std::vector<ColMeta> cols, std::string col_name) {
//     std::string str;
//     int str_size = 0;
//     for (auto &col : cols) {
//       if (col.name == col_name) {
//         char *data = record.data;
//         switch (col.type) {
//           case TYPE_INT:
//             this->set_int(*(int *)(data + col.offset));
//             break;
//           case TYPE_LONG:
//             this->set_long(*(long long *)(data + col.offset));
//             break;
//           case TYPE_FLOAT:
//             this->set_float(*(float *)(data + col.offset));
//             break;
//           case TYPE_DOUBLE:
//             this->set_double(*(double *)(data + col.offset));
//             break;
//           case TYPE_CHAR:
//             str_size = col.len < (int)strlen(data + col.offset) ? col.len : strlen(data + col.offset);
//             str.assign(data + col.offset, str_size);
//             str[str_size] = '\0';
//             this->set_char(str);
//             break;
//           case TYPE_VARCHAR:
//             str_size = col.len < (int)strlen(data + col.offset) ? col.len : strlen(data + col.offset);
//             str.assign(data + col.offset, str_size);
//             str[str_size] = '\0';
//             this->set_varchar(str);
//             break;
//           default:
//             throw InternalError("unsupported data type.");
//         }
//         return this;
//       }
//     }
//     return nullptr;
//   }

//   //   bool operator<(const Value &rfh) {
//   //     if (type == rfh.type) {
//   //       switch (type) {
//   //         case TYPE_INT:
//   //           return int_val < rfh.int_val;
//   //         case TYPE_FLOAT:
//   //           return float_val < rfh.float_val;
//   //         case TYPE_STRING:
//   //           return str_val < rfh.str_val;
//   //         default:
//   //           throw InternalError("unsupported data type.");
//   //       }
//   //     } else if (type == TYPE_STRING || rfh.type == TYPE_STRING) {
//   //       throw IncompatibleTypeError(coltype2str(type), coltype2str(rfh.type));
//   //     } else if (type == TYPE_INT) {
//   //       return int_val < rfh.float_val;
//   //     } else {
//   //       return float_val < rfh.int_val;
//   //     }
//   //   }

//   //   bool operator>(const Value &rfh) {
//   //     if (type == TYPE_EMPTY || rfh.type == TYPE_EMPTY) {
//   //       return false;
//   //     }
//   //     if (type == rfh.type) {
//   //       switch (type) {
//   //         case TYPE_INT:
//   //           return int_val > rfh.int_val;
//   //         case TYPE_FLOAT:
//   //           return float_val > rfh.float_val;
//   //         case TYPE_STRING:
//   //           return str_val > rfh.str_val;
//   //         default:
//   //           throw InternalError("unsupported data type.");
//   //       }
//   //     } else if (type == TYPE_STRING || rfh.type == TYPE_STRING) {
//   //       throw IncompatibleTypeError(coltype2str(type), coltype2str(rfh.type));
//   //     } else if (type == TYPE_INT) {
//   //       return int_val > rfh.float_val;
//   //     } else {
//   //       return float_val > rfh.int_val;
//   //     }
//   //   }

//   //   bool operator<=(const Value &rfh) {
//   //     if (type == TYPE_EMPTY || rfh.type == TYPE_EMPTY) {
//   //       return false;
//   //     }
//   //     if (type == rfh.type) {
//   //       switch (type) {
//   //         case TYPE_INT:
//   //           return int_val <= rfh.int_val;
//   //         case TYPE_FLOAT:
//   //           return float_val <= rfh.float_val;
//   //         case TYPE_STRING:
//   //           return str_val <= rfh.str_val;
//   //         default:
//   //           throw InternalError("unsupported data type.");
//   //       }
//   //     } else if (type == TYPE_STRING || rfh.type == TYPE_STRING) {
//   //       throw IncompatibleTypeError(coltype2str(type), coltype2str(rfh.type));
//   //     } else if (type == TYPE_INT) {
//   //       return int_val <= rfh.float_val;
//   //     } else {
//   //       return float_val <= rfh.int_val;
//   //     }
//   //   }

//   //   bool operator==(const Value &rfh) {
//   //     if (type == TYPE_EMPTY || rfh.type == TYPE_EMPTY) {
//   //       return false;
//   //     }
//   //     if (type == rfh.type) {
//   //       switch (type) {
//   //         case TYPE_INT:
//   //           return int_val == rfh.int_val;
//   //         case TYPE_FLOAT:
//   //           return float_val == rfh.float_val;
//   //         case TYPE_STRING:
//   //           return str_val == rfh.str_val;
//   //         default:
//   //           throw InternalError("unsupported data type.");
//   //       }
//   //     } else if (type == TYPE_STRING || rfh.type == TYPE_STRING) {
//   //       throw IncompatibleTypeError(coltype2str(type), coltype2str(rfh.type));
//   //     } else if (type == TYPE_INT) {
//   //       return int_val == rfh.float_val;
//   //     } else {
//   //       return float_val == rfh.int_val;
//   //     }
//   //   }

//   //   bool operator>=(const Value &rfh) {
//   //     if (type == TYPE_EMPTY || rfh.type == TYPE_EMPTY) {
//   //       return false;
//   //     }
//   //     if (type == rfh.type) {
//   //       switch (type) {
//   //         case TYPE_INT:
//   //           return int_val >= rfh.int_val;
//   //         case TYPE_FLOAT:
//   //           return float_val >= rfh.float_val;
//   //         case TYPE_STRING:
//   //           return str_val >= rfh.str_val;
//   //         default:
//   //           throw InternalError("unsupported data type.");
//   //       }
//   //     } else if (type == TYPE_STRING || rfh.type == TYPE_STRING) {
//   //       throw IncompatibleTypeError(coltype2str(type), coltype2str(rfh.type));
//   //     } else if (type == TYPE_INT) {
//   //       return int_val >= rfh.float_val;
//   //     } else {
//   //       return float_val >= rfh.int_val;
//   //     }
//   //   }

//   //   bool operator!=(const Value &rfh) {
//   //     if (type == TYPE_EMPTY || rfh.type == TYPE_EMPTY) {
//   //       return true;
//   //     }
//   //     if (type == rfh.type) {
//   //       switch (type) {
//   //         case TYPE_INT:
//   //           return int_val != rfh.int_val;
//   //         case TYPE_FLOAT:
//   //           return float_val != rfh.float_val;
//   //         case TYPE_STRING:
//   //           return str_val != rfh.str_val;
//   //         default:
//   //           throw InternalError("unsupported data type.");
//   //       }
//   //     } else if (type == TYPE_STRING || rfh.type == TYPE_STRING) {
//   //       throw IncompatibleTypeError(coltype2str(type), coltype2str(rfh.type));
//   //     } else if (type == TYPE_INT) {
//   //       return int_val != rfh.float_val;
//   //     } else {
//   //       return float_val != rfh.int_val;
//   //     }
//   //   }

//   //   Value operator+(const Value &rfh) {
//   //     Value res;
//   //     if (type == TYPE_STRING || rfh.type == TYPE_STRING) {
//   //       throw InternalError("TYPE_STRING don't support operator +.");
//   //     } else {
//   //       if (type == TYPE_FLOAT && rfh.type == TYPE_FLOAT) {
//   //         res.set_float(float_val + rfh.float_val);
//   //       } else if (type == TYPE_FLOAT && rfh.type == TYPE_INT) {
//   //         res.set_float(float_val + rfh.int_val);
//   //       } else if (type == TYPE_INT && rfh.type == TYPE_FLOAT) {
//   //         res.set_float(int_val + rfh.float_val);
//   //       } else {
//   //         res.set_int(int_val + rfh.int_val);
//   //       }
//   //     }
//   //     return res;
//   //   }
// };

// enum CompOp { OP_EQ, OP_NE, OP_LT, OP_GT, OP_LE, OP_GE };

// struct Condition {
//   TabCol lhs_col;   // left-hand side column
//   CompOp op;        // comparison operator
//   bool is_rhs_val;  // true if right-hand side is a value (not a column)
//   TabCol rhs_col;   // right-hand side column
//   Value rhs_val;    // right-hand side value

//   bool satisfy(Value lhs_v, Value rhs_v) {
//     switch (op) {
//       case OP_EQ:
//         /* code */
//         return lhs_v == rhs_v;
//       case OP_NE:
//         return lhs_v != rhs_v;
//       case OP_LT:
//         return lhs_v < rhs_v;
//       case OP_GT:
//         return lhs_v > rhs_v;
//       case OP_LE:
//         return lhs_v <= rhs_v;
//       case OP_GE:
//         return lhs_v >= rhs_v;
//       default:
//         throw InternalError("unsupported operator.");
//     }
//   }
// };

// struct SetClause {
//   TabCol lhs;
//   Value rhs;
// };
}  // namespace easydb