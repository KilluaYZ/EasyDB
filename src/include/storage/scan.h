#pragma once

#include <memory>

#include "storage/easystore/table/tuple.h"

namespace easydb {

class TableScan {
 public:
  virtual ~TableScan() = default;
  virtual void Next() = 0;
  virtual bool IsEnd() const = 0;
  virtual std::unique_ptr<Tuple> GetTuple() const = 0;
};

class IndexScan {
 public:
  virtual ~IndexScan() = default;
  virtual void Next() = 0;
  virtual bool IsEnd() const = 0;
  virtual std::unique_ptr<Tuple> GetTuple() const = 0;
};

}  // namespace easydb
