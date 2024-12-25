#pragma once

#include <memory>
#include <unordered_map>
#include <vector>
#include "common/common.h"
#include "common/condition.h"
#include "common/errors.h"
#include "common/hashutil.h"
#include "defs.h"
#include "executor_abstract.h"
#include "planner/plan.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace easydb {
class EmptyExecutor : public AbstractExecutor {
 public:
  EmptyExecutor(Context *ctx) : context_(ctx), end_(false) {}

  void beginTuple() { end_ = true; }  // 一开始就设为true，表示无数据行
  bool IsEnd() { return end_; }
  RID &rid() { return rid(); }
  std::unique_ptr<Tuple> Next() { return nullptr; }
  void nextTuple() override {}
  TupleMeta tuple_meta() { return TupleMeta(); }
  Tuple tuple() { return Tuple(); }
  Schema GetSchema() {
    // 空schema，没有列
    return Schema();
  }

 private:
  Context *context_;
  bool end_;
};

}  // namespace easydb