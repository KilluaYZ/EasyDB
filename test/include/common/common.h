//
// Created by ziyang on 24-10-24.
//

#ifndef COMMON_H
#define COMMON_H

#include <cassert>
#include <cstring>
#include <memory>
#include <string>
#include <vector>
#include "defs.h"
#include "error.h"
#include "errors.h"
#include "record.h"

namespace easydb {
struct TabCol {
  std::string tab_name;
  std::string col_name;
  std::string new_col_name;  // new col name of aggregation col

  friend bool operator<(const TabCol &x, const TabCol &y) {
    return std::make_pair(x.tab_name, x.col_name) < std::make_pair(y.tab_name, y.col_name);
  }
};

struct Value {
  ColType type;  // type of value
  union {
    int int_val;  // int value
    long long long_val;
    float float_val;  // float value
    double double_val;
  };
  std::string str_val;  // string value

  std::shared_ptr<EasyRecord> raw;  // raw record buffer

  void set_int(int int_val_) {
    type = TYPE_INT;
    int_val = int_val_;
  }

  void set_long(long long long_val_) {
    type = TYPE_LONG;
    long_val = long_val_;
  }

  void set_float(float float_val_) {
    type = TYPE_FLOAT;
    float_val = float_val_;
  }

  void set_double(double double_val_) {
    type = TYPE_DOUBLE;
    double_val = double_val_;
  }

  void set_char(std::string str_val_) {
    type = TYPE_CHAR;
    str_val = std::move(str_val_);
  }

  void set_varchar(std::string str_val_) {
    type = TYPE_VARCHAR;
    str_val = std::move(str_val_);
  }

  void init_raw(int len) {
    assert(raw == nullptr);
    raw = std::make_shared<EasyRecord>(len);
    if (type == TYPE_INT) {
      assert(len == sizeof(int));
      *(int *)(raw->data) = int_val;
    } else if (type == TYPE_LONG) {
      assert(len == sizeof(long long));
      *(long long *)(raw->data) = long_val;
    } else if (type == TYPE_FLOAT) {
      assert(len == sizeof(float));
      *(float *)(raw->data) = float_val;
    } else if (type == TYPE_DOUBLE) {
      assert(len == sizeof(double));
      *(double *)(raw->data) = double_val;
    } else if (type == TYPE_CHAR || type == TYPE_VARCHAR) {
      if (len < (int)str_val.size()) {
        throw StringOverflowError();
      }
      memset(raw->data, 0, len);
      memcpy(raw->data, str_val.c_str(), str_val.size());
    }
  }

  Value *get_value_from_record(EasyRecord record, std::vector<ColMeta> cols, std::string col_name) {
    std::string str;
    int str_size = 0;
    for (auto &col : cols) {
      if (col.name == col_name) {
        char *data = record.data;
        switch (col.type) {
          case TYPE_INT:
            this->set_int(*(int *)(data + col.offset));
            break;
          case TYPE_LONG:
            this->set_long(*(long long *)(data + col.offset));
            break;
          case TYPE_FLOAT:
            this->set_float(*(float *)(data + col.offset));
            break;
          case TYPE_DOUBLE:
            this->set_double(*(double *)(data + col.offset));
          break;
          case TYPE_CHAR:
            str_size = col.len < strlen(data + col.offset) ? col.len : strlen(data + col.offset);
            str.assign(data + col.offset, str_size);
            str[str_size] = '\0';
            this->set_char(str);
            break;
          case TYPE_VARCHAR:
            str_size = col.len < strlen(data + col.offset) ? col.len : strlen(data + col.offset);
            str.assign(data + col.offset, str_size);
            str[str_size] = '\0';
            this->set_varchar(str);
            break;
          default:
            throw InternalError("unsupported data type.");
        }
        return this;
      }
    }
    return nullptr;
  }
};

}  // namespace easydb

#endif  // COMMON_H
