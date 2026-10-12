#pragma once

#include "common/rid.h"

namespace easydb {

class TableScan {
 public:
  virtual ~TableScan() = default;
  virtual void Next() = 0;
  virtual bool IsEnd() const = 0;
  virtual RID GetRid() const = 0;
};

class IndexScan {
 public:
  virtual ~IndexScan() = default;
  virtual void Next() = 0;
  virtual bool IsEnd() const = 0;
  virtual RID GetRid() const = 0;
};

}  // namespace easydb
