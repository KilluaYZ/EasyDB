/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 *
 *-------------------------------------------------------------------------
 */

#pragma once
#include "common/errors.h"
#include "defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "storage/index/ix_manager.h"
#include "storage/index/ix_scan.h"
#include "system/sm_defs.h"
#include "system/sm_meta.h"
#include <memory>

namespace easydb {

class SortExecutor : public AbstractExecutor {
 private:
  std::unique_ptr<AbstractExecutor> prev_;
  ColMeta cols_;  // 框架中只支持一个键排序，需要自行修改数据结构支持多个键排序
  Schema schema_;                       // scan后生成的记录的字段
  size_t tuple_num;
  size_t len_;
  bool is_desc_;
  bool isend_ = false;
  std::vector<size_t> used_tuple;
  std::unique_ptr<RmRecord> current_tuple;

  std::unique_ptr<MergeSorter> sorter;

 public:
  SortExecutor(std::unique_ptr<AbstractExecutor> prev, TabCol sel_cols, bool is_desc);

  void beginTuple() override;

  void nextTuple() override;

  std::unique_ptr<RmRecord> Next() override { return std::move(current_tuple); }

  RID &rid() override { return _abstract_rid; }

  const std::vector<ColMeta> &cols() const override { return prev_->cols(); };

  std::string getType() override { return "SortExecutor"; };

  bool IsEnd() const override {
    // return sorter->IsEnd();
    return isend_;
  };

  size_t tupleLen() const override { return len_; };

  void printRecord(RmRecord record, std::vector<ColMeta> cols);

  void printRecord(char *data, std::vector<ColMeta> cols);
};

}  // namespace easydb
