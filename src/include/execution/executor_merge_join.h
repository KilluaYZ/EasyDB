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

/**
 * @brief 归并连接执行器类
 * 
 * MergeJoinExecutor 实现归并连接操作，要求两个输入表在连接列上已排序。
 * 使用归并算法高效地连接两个有序表，时间复杂度为O(n+m)。
 * 支持使用索引进行连接优化。
 */
class MergeJoinExecutor : public AbstractExecutor {
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
  
  /**
   * @brief 连接后的记录
   */
  Tuple joined_records_;
  // RmRecord joined_records_;
  
  /**
   * @brief 是否使用索引标志
   * @note true表示使用索引进行连接，false表示使用排序归并
   */
  bool use_index_;

  // ColMeta left_sel_col_;
  // ColMeta right_sel_col_;

  /**
   * @brief 左表连接列
   */
  Column left_sel_colu_;
  
  /**
   * @brief 右表连接列
   */
  Column right_sel_colu_;

  /**
   * @brief 左表排序器
   * @note 用于对左表进行排序（如果未排序）
   */
  std::unique_ptr<MergeSorter> leftSorter_;
  
  /**
   * @brief 右表排序器
   * @note 用于对右表进行排序（如果未排序）
   */
  std::unique_ptr<MergeSorter> rightSorter_;

  // RmRecord current_left_rec_;
  // RmRecord current_right_rec_;

  /**
   * @brief 当前左表元组
   */
  Tuple current_left_tup_;
  
  /**
   * @brief 当前右表元组
   */
  Tuple current_right_tup_;

  /**
   * @brief 当前左表元组的数据指针
   */
  char *current_left_data_;
  
  /**
   * @brief 当前右表元组的数据指针
   */
  char *current_right_data_;

  /**
   * @brief 左表元组大小（字节数）
   */
  uint32_t left_size_;
  
  /**
   * @brief 右表元组大小（字节数）
   */
  uint32_t right_size_;
  // std::vector<RmRecord> left_buffer_;
  // std::vector<RmRecord> right_buffer_;

  /**
   * @brief 左表元组缓冲区
   */
  std::vector<Tuple> left_buffer_;
  
  /**
   * @brief 右表元组缓冲区
   */
  std::vector<Tuple> right_buffer_;

  /**
   * @brief 左表缓冲区索引
   */
  int left_idx_;
  
  /**
   * @brief 右表缓冲区索引
   */
  int right_idx_;
  
  /**
   * @brief 上一个左表连接列的值
   */
  Value last_left_val_;
  
  /**
   * @brief 上一个右表连接列的值
   */
  Value last_right_val_;
  
  /**
   * @brief 上一个右表索引
   */
  int last_right_idx_;

  /**
   * @brief 左表文件流（用于外部排序）
   */
  std::fstream fd_left;
  
  /**
   * @brief 右表文件流（用于外部排序）
   */
  std::fstream fd_right;

  /**
   * @brief 初始化标志
   */
  bool initialize_flag_{false};

 public:
  MergeJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right,
                    std::vector<Condition> conds, bool use_index);

  ~MergeJoinExecutor();

  std::string getTabName() const override { return join_tab_name_; }

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
