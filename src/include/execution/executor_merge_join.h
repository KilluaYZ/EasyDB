/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 *
 *-------------------------------------------------------------------------
 */

#pragma once
#include <cstdio>
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
  std::vector<ColMeta> cols_;                // join后获得的记录的字段

  std::vector<Condition> fed_conds_;  // join条件
  bool isend;
  RmRecord joined_records_;
  bool use_index_;

  ColMeta left_sel_col_;
  ColMeta right_sel_col_;

  std::unique_ptr<MergeSorter> leftSorter_;
  std::unique_ptr<MergeSorter> rightSorter_;

  RmRecord current_left_rec_;
  RmRecord current_right_rec_;
  char *current_left_data_;
  char *current_right_data_;

  std::vector<RmRecord> left_buffer_;
  std::vector<RmRecord> right_buffer_;

  int left_idx_;
  int right_idx_;

  std::fstream fd_left;
  std::fstream fd_right;

 public:
  MergeJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right,
                    std::vector<Condition> conds, bool use_index);

  void nextTuple() override;

  std::unique_ptr<RmRecord> Next() override { return std::make_unique<RmRecord>(joined_records_); }

  RID &rid() override { return _abstract_rid; }

  size_t tupleLen() const override { return len_; };

  const std::vector<ColMeta> &cols() const override { return cols_; };

  bool is_end() const override { return isend; };

  ColMeta get_col_offset(const TabCol &target) override {
    for (auto &col : cols_) {
      if (target.col_name == col.name && target.tab_name == col.tab_name) {
        return col;
      }
    }
    throw ColumnNotFoundError(target.col_name);
  }

  ColMeta get_col_offset(std::vector<ColMeta> cols, const TabCol &target) {
    for (auto &col : cols) {
      if (target.col_name == col.name && target.tab_name == col.tab_name) {
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

  RmRecord concat_records();

  void writeRecord(std::fstream &fd, char *data, std::vector<ColMeta> cols);

  void writeRecord(std::fstream &fd, std::unique_ptr<RmRecord> &Tuple, const std::vector<ColMeta> &cols);

  void writeRecord(std::fstream &fd, RmRecord &Tuple, const std::vector<ColMeta> &cols);

  void completeWriting();

  void writeHeader(std::fstream &fd, std::vector<ColMeta> all_cols) {
    fd << "|";
    for (auto &col : all_cols) {
      fd << " " << col.name << " |";
    }
    fd << "\n";
  }
};
}  // namespace easydb
