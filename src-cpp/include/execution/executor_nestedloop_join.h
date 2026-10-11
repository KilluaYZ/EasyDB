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

/**
 * @brief 嵌套循环连接执行器类
 *
 * NestedLoopJoinExecutor 实现嵌套循环连接操作，对两个表的元组进行笛卡尔积，
 * 然后根据连接条件过滤结果。支持排序优化以提高性能。
 */
class NestedLoopJoinExecutor : public AbstractExecutor {
 private:
  /**
   * @brief 左子节点执行器（需要join的表）
   */
  std::unique_ptr<AbstractExecutor> left_;

  /**
   * @brief 右子节点执行器（需要join的表）
   */
  std::unique_ptr<AbstractExecutor> right_;

  /**
   * @brief 左表名称
   */
  std::string left_tab_name_;

  /**
   * @brief 右表名称
   */
  std::string right_tab_name_;

  /**
   * @brief join后的表名称
   */
  std::string join_tab_name_;

  /**
   * @brief join后获得的每条记录的长度（字节数）
   */
  size_t len_;
  // std::vector<ColMeta> cols_;                // join后获得的记录的字段

  /**
   * @brief join后生成的记录的字段模式
   */
  Schema schema_;

  /**
   * @brief join条件列表
   */
  std::vector<Condition> fed_conds_;

  /**
   * @brief 是否到达末尾标志
   */
  bool isend;

  // RmRecord joined_records_;

  /**
   * @brief 连接后的记录
   */
  Tuple joined_records_;

  // std::vector<RmRecord> left_buffer_;
  // std::vector<RmRecord> right_buffer_;
  // ColMeta left_sel_col_;
  // ColMeta right_sel_col_;

  /**
   * @brief 左表元组缓冲区
   */
  std::vector<Tuple> left_buffer_;

  /**
   * @brief 右表元组缓冲区
   */
  std::vector<Tuple> right_buffer_;

  /**
   * @brief 左表连接列
   */
  Column left_sel_colu_;

  /**
   * @brief 右表连接列
   */
  Column right_sel_colu_;

  /**
   * @brief 左表排序器（用于排序优化）
   */
  std::unique_ptr<MergeSorter> leftSorter_;

  /**
   * @brief 左表缓冲区索引
   */
  int left_idx_;

  /**
   * @brief 右表缓冲区索引
   */
  int right_idx_;

  /**
   * @brief 左表缓冲区长度
   */
  int left_len_;

  /**
   * @brief 右表缓冲区长度
   */
  int right_len_;

  /**
   * @brief 块大小（4KB）
   * @note 用于分块处理大数据集
   */
  int block_size = 4096;  // 4kb

  /**
   * @brief 缓冲区记录数量
   */
  int buffer_record_count = 10;

  /**
   * @brief 是否需要排序标志
   * @note 如果连接列已排序，可以使用更高效的算法
   */
  bool need_sort_ = false;

 public:
  NestedLoopJoinExecutor(std::unique_ptr<AbstractExecutor> left,
                         std::unique_ptr<AbstractExecutor> right,
                         std::vector<Condition> conds);

  std::string getTabName() const override { return join_tab_name_; }

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

  // void printRecord(std::unique_ptr<RmRecord> &Tuple, const
  // std::vector<ColMeta> &cols);

  void nextTuple() override;

  std::unique_ptr<Tuple> Next() override {
    return std::make_unique<Tuple>(std::move(joined_records_));
  }

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
