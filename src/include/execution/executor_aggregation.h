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

/**
 * e.g.
 *
 * select SUM(score) as sum_score from grade where id = 1;
 *
 * select course ,COUNT(*) as row_num , COUNT(id) as student_num
 * from grade
 * group by course;
 *
 * select id,MAX(score) as max_score,MIN(score) as min_score
 * from grade
 * group by id
 * having COUNT(*) > 1 and MIN(score) > 88;
 *
 * TYPE: COUNT MIN MAX SUM
 *
 * enum AggregationType {
 *  MAX_AGG, MIN_AGG, COUNT_AGG, SUM_AGG, NO_AGG
 *  };
 */

namespace easydb {

class AggregationExecutor : public AbstractExecutor {
 private:
  std::unique_ptr<AbstractExecutor> prev_;  // 聚合操作的儿子节点
  size_t len_;                              // 聚合计算后得到的记录的长度

  // std::vector<ColMeta> cols_;               // 聚合计算后得到的字段
  // std::vector<ColMeta> prev_cols_;          // 原始字段
  // std::vector<ColMeta> sel_cols_;           // 聚合计算选择的字段
  // std::vector<ColMeta> group_cols_;         // groupby选择的字段

  std::string tab_name_;           // 表名称
  Schema schema_;                  // 聚合计算后得到的字段
  Schema prev_schema_;             // 原始字段
  std::vector<Column> sel_colus_;  // 聚合计算选择的字段
  // std::vector<Column> group_colus_;  // groupby选择的字段

  // std::vector<Condition> having_conds_;  // having算子的条件

  // std::vector<Tuple> all_records_;                             // 所有records
  // std::map<std::string, std::vector<Tuple>> key_records_map_;  // 根据groupby条件分组的records <key,records>哈希表

  std::unique_ptr<Tuple> result_;  // 用于返回的聚合结果记录
  // bool isend_;
  int key_length;
  std::map<std::string, std::vector<Tuple>>::iterator it;
  bool isend_;
  // int traverse_idx;
  SmManager *sm_manager_;

 public:
  AggregationExecutor(SmManager *sm_manager, std::unique_ptr<AbstractExecutor> prev, std::vector<TabCol> sel_col_);
  // std::vector<TabCol> group_cols, std::vector<Condition> having_conds);

  void beginTuple() override;

  void nextTuple() override;

  std::unique_ptr<Tuple> Next() override;

  RID &rid() override { return _abstract_rid; }

  size_t tupleLen() const override { return len_; };

  // const std::vector<ColMeta> &cols() const override { return cols_; };
  const Schema &schema() const override { return schema_; };

  auto GetTabName() const -> std::string { return tab_name_; }

  bool IsEnd() const override { return isend_; };
  // bool IsEnd() const override { return it == key_records_map_.end(); };

 private:
  // bool in_groupby_cols(Column target);

  // bool predicate(std::vector<Tuple> records);

  // Value aggregation_to_value(std::vector<Tuple> records, Column target_colu);
  std::string generate_new_name(TabCol col);

  Value aggregation_to_value(Column target_colu);
};

}  // namespace easydb
