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

/**
 * @brief 聚合执行器类
 *
 * AggregationExecutor 实现聚合操作，包括：
 * - 聚合函数（SUM、COUNT、MAX、MIN等）
 * - GROUP BY分组
 * - HAVING条件过滤
 *
 * 示例查询：
 * - SELECT SUM(score) FROM grade WHERE id = 1;
 * - SELECT course, COUNT(*) FROM grade GROUP BY course;
 * - SELECT id, MAX(score), MIN(score) FROM grade GROUP BY id HAVING COUNT(*) >
 * 1;
 */
class AggregationExecutor : public AbstractExecutor {
 private:
  /**
   * @brief 聚合操作的子节点执行器
   * @note 聚合操作的数据源
   */
  std::unique_ptr<AbstractExecutor> prev_;

  /**
   * @brief 聚合计算后得到的记录的长度（字节数）
   */
  size_t len_;

  // std::vector<ColMeta> cols_;               // 聚合计算后得到的字段
  // std::vector<ColMeta> prev_cols_;          // 原始字段
  // std::vector<ColMeta> sel_cols_;           // 聚合计算选择的字段
  // std::vector<ColMeta> group_cols_;         // groupby选择的字段

  /**
   * @brief 表名称
   */
  std::string tab_name_;

  /**
   * @brief 聚合计算后得到的字段模式
   */
  Schema schema_;

  /**
   * @brief 原始字段模式（输入元组的模式）
   */
  Schema prev_schema_;

  /**
   * @brief 聚合计算选择的字段列表
   * @note 包含聚合函数列（如SUM、COUNT等）和分组列
   */
  std::vector<Column> sel_colus_;
  // std::vector<Column> group_colus_;  // groupby选择的字段

  // std::vector<Condition> having_conds_;  // having算子的条件

  // std::vector<Tuple> all_records_;                             // 所有records
  // std::map<std::string, std::vector<Tuple>> key_records_map_;  //
  // 根据groupby条件分组的records <key,records>哈希表

  /**
   * @brief 用于返回的聚合结果记录
   */
  std::unique_ptr<Tuple> result_;
  // bool isend_;

  /**
   * @brief 分组键的长度（字节数）
   */
  int key_length;

  /**
   * @brief 分组记录的迭代器
   * @note 用于遍历分组后的记录
   */
  std::map<std::string, std::vector<Tuple>>::iterator it;

  /**
   * @brief 是否到达末尾标志
   */
  bool isend_;
  // int traverse_idx;

  /**
   * @brief 系统管理器指针
   */
  SmManager *sm_manager_;

 public:
  /**
   * @brief 构造函数
   * @param sm_manager 系统管理器指针
   * @param prev 子节点执行器的智能指针
   * @param sel_col_ 选择的列列表（包含聚合函数）
   */
  AggregationExecutor(SmManager *sm_manager,
                      std::unique_ptr<AbstractExecutor> prev,
                      std::vector<TabCol> sel_col_);
  // std::vector<TabCol> group_cols, std::vector<Condition> having_conds);

  /**
   * @brief 开始迭代元组
   * @note 初始化聚合计算，对所有输入元组进行分组和聚合
   */
  void beginTuple() override;

  /**
   * @brief 移动到下一个元组
   * @note 推进到下一个分组结果
   */
  void nextTuple() override;

  /**
   * @brief 获取下一个元组
   * @return 聚合结果的元组的智能指针，如果没有更多结果则返回nullptr
   */
  std::unique_ptr<Tuple> Next() override;

  /**
   * @brief 获取当前元组的RID
   * @return 抽象RID的引用
   */
  RID &rid() override { return _abstract_rid; }

  /**
   * @brief 获取元组长度
   * @return 元组的长度（字节数）
   */
  size_t tupleLen() const override { return len_; };

  // const std::vector<ColMeta> &cols() const override { return cols_; };

  /**
   * @brief 获取模式
   * @return 模式的常量引用
   */
  const Schema &schema() const override { return schema_; };

  /**
   * @brief 获取表名
   * @return 表名字符串
   */
  auto GetTabName() const -> std::string { return tab_name_; }

  /**
   * @brief 判断是否到达末尾
   * @return true 如果已到达末尾，false 否则
   */
  bool IsEnd() const override { return isend_; };
  // bool IsEnd() const override { return it == key_records_map_.end(); };

 private:
  // bool in_groupby_cols(Column target);

  // bool predicate(std::vector<Tuple> records);

  // Value aggregation_to_value(std::vector<Tuple> records, Column target_colu);

  /**
   * @brief 生成新的列名
   * @param col 列标识
   * @return 新的列名字符串
   * @note 用于处理聚合列的命名（如SUM(score) -> sum_score）
   */
  std::string generate_new_name(TabCol col);

  /**
   * @brief 计算聚合值
   * @param target_colu 目标列（包含聚合类型）
   * @return 聚合结果值
   * @note 根据聚合类型（SUM、COUNT、MAX、MIN）计算当前分组的聚合值
   */
  Value aggregation_to_value(Column target_colu);
};

}  // namespace easydb
