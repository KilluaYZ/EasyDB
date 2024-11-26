/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 *
 *-------------------------------------------------------------------------
 */

#pragma once
#include <memory>
#include "common/common.h"
#include "common/errors.h"
#include "common/mergeSorter.h"
#include "defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "storage/index/ix_manager.h"
#include "storage/index/ix_scan.h"
#include "storage/table/tuple.h"
#include "system/sm_defs.h"
#include "system/sm_meta.h"

namespace easydb {

class NestedLoopJoinExecutor : public AbstractExecutor {
 private:
  std::unique_ptr<AbstractExecutor> left_;   // 左儿子节点（需要join的表）
  std::unique_ptr<AbstractExecutor> right_;  // 右儿子节点（需要join的表）
  std::string left_tab_name_;                // 表名称
  std::string right_tab_name_;               // 表名称
  size_t len_;                               // join后获得的每条记录的长度
  // std::vector<ColMeta> cols_;                // join后获得的记录的字段
  Schema schema_;  // scan后生成的记录的字段

  std::vector<Condition> fed_conds_;  // join条件
  bool isend;

  // RmRecord joined_records_;
  Tuple joined_records_;

  // std::vector<RmRecord> left_buffer_;
  // std::vector<RmRecord> right_buffer_;
  // ColMeta left_sel_col_;
  // ColMeta right_sel_col_;

  std::vector<Tuple> left_buffer_;
  std::vector<Tuple> right_buffer_;
  Column left_sel_colu_;
  Column right_sel_colu_;
  std::unique_ptr<MergeSorter> leftSorter_;
  int left_idx_;
  int right_idx_;
  int left_len_;
  int right_len_;

  int block_size = 4096;  // 4kb

  int buffer_record_count = 10;

  bool need_sort_ = false;

 public:
  NestedLoopJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right,
                         std::vector<Condition> conds);

  ColMeta get_col_offset(std::vector<ColMeta> cols, const TabCol &target) {
    for (auto &col : cols) {
      if (target.col_name == col.name && target.tab_name == col.tab_name) {
        return col;
      }
    }
    throw ColumnNotFoundError(target.col_name);
  }

  void beginTuple() override;

  // void printRecord(RmRecord record, std::vector<ColMeta> cols);

  // void printRecord(char *data, std::vector<ColMeta> cols);

  // void printRecord(std::unique_ptr<RmRecord> &Tuple, const std::vector<ColMeta> &cols);

  void nextTuple() override;

  std::unique_ptr<Tuple> Next() override { return std::make_unique<Tuple>(std::move(joined_records_)); }

  RID &rid() override { return _abstract_rid; }

  size_t tupleLen() const override { return len_; };

  // const std::vector<ColMeta> &cols() const override { return cols_; };

  const Schema &schema() const override { return schema_; };

  bool IsEnd() const override { return isend; };

  // ColMeta get_col_offset(const TabCol &target) override {
  //   for (auto &col : cols_) {
  //     if (target.col_name == col.name && target.tab_name == col.tab_name) {
  //       return col;
  //     }
  //   }
  //   throw ColumnNotFoundError(target.col_name);
  // }

  Column get_col_offset(Schema sche, const TabCol &target) {
    auto cols = sche.GetColumns();
    for (auto &col : cols) {
      if (target.col_name == col.GetName()) {
        return col;
      }
    }
    throw ColumnNotFoundError(target.col_name);
  }

 private:
  bool predicate(const Tuple &left_tuple, const Tuple &right_tuple);

  void sorted_iterate_helper();

  void iterate_helper();

  void iterate_next();

  // RmRecord concat_records();
  Tuple concat_records();
};
}  // namespace easydb
