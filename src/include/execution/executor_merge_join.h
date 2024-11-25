/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 *
 *-------------------------------------------------------------------------
 */

#pragma once
#include <cstdio>
#include <memory>
#include "catalog/column.h"
#include "common/common.h"
#include "common/errors.h"
#include "common/mergeSorter.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "record/rm_defs.h"
#include "storage/index/ix_manager.h"
#include "storage/index/ix_scan.h"
#include "system/sm_defs.h"
#include "system/sm_meta.h"
namespace easydb {

class MergeJoinExecutor : public AbstractExecutor {
 private:
  std::unique_ptr<AbstractExecutor> left_;   // 左儿子节点（需要join的表）
  std::unique_ptr<AbstractExecutor> right_;  // 右儿子节点（需要join的表）
  size_t len_;                               // join后获得的每条记录的长度
  Schema schema_;                            // scan后生成的记录的字段

  std::vector<Condition> fed_conds_;  // join条件
  bool isend;
  Tuple joined_records_;
  // RmRecord joined_records_;
  bool use_index_;

  // ColMeta left_sel_col_;
  // ColMeta right_sel_col_;

  Column left_sel_colu_;
  Column right_sel_colu_;

  std::unique_ptr<MergeSorter> leftSorter_;
  std::unique_ptr<MergeSorter> rightSorter_;

  // RmRecord current_left_rec_;
  // RmRecord current_right_rec_;

  Tuple current_left_tup_;
  Tuple current_right_tup_;

  const char *current_left_data_;
  const char *current_right_data_;

  // std::vector<RmRecord> left_buffer_;
  // std::vector<RmRecord> right_buffer_;

  std::vector<Tuple> left_buffer_;
  std::vector<Tuple> right_buffer_;

  int left_idx_;
  int right_idx_;

  std::fstream fd_left;
  std::fstream fd_right;

 public:
  MergeJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right,
                    std::vector<Condition> conds, bool use_index);

  void beginTuple() override;

  void nextTuple() override;

  std::unique_ptr<Tuple> Next() override { return std::make_unique<Tuple>(joined_records_); }

  RID &rid() override { return _abstract_rid; }

  size_t tupleLen() const override { return len_; };

  const Schema &schema() const override { return schema_; };

  bool IsEnd() const override { return isend; };

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
  // attention : statement with additional conds is not supported yet.
  bool predicate();

  void iterate_helper();

  void index_iterate_helper();

  Tuple concat_records();

};
}  // namespace easydb
