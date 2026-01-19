/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 *
 *-------------------------------------------------------------------------
 */

#pragma once
#include <functional>
#include "common/errors.h"
#include "defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "storage/index/ix_manager.h"
#include "storage/index/ix_scan.h"
#include "system/sm_defs.h"
#include "system/sm_meta.h"

namespace easydb {

/**
 * @brief 投影执行器类
 *
 * ProjectionExecutor 实现投影操作，从输入元组中选择指定的列，
 * 并可以执行去重操作（SELECT DISTINCT）。
 */
class ProjectionExecutor : public AbstractExecutor {
 private:
  /**
   * @brief 投影节点的子节点执行器
   * @note 投影操作的数据源，可以是扫描、连接等执行器
   */
  std::unique_ptr<AbstractExecutor> prev_;

  // std::string tab_name_;
  // std::vector<ColMeta> cols_;               // 需要投影的字段

  /**
   * @brief 投影后字段的总长度（字节数）
   */
  size_t len_;

  /**
   * @brief 投影字段对应的列索引（位置）
   * @note 存储要投影的列在输入元组中的索引
   */
  std::vector<uint32_t> sel_ids_;

  // RmRecord projection_records_;             // temp projection record(added
  // by flerovium)

  /**
   * @brief 临时投影记录
   * @note 存储当前投影后的元组数据
   */
  Tuple projection_records_;

  /**
   * @brief 投影后生成的记录的字段模式
   */
  Schema schema_;

  /**
   * @brief 已见过的元组集合（用于去重）
   * @note 存储已见过的元组的字符串表示，用于实现SELECT DISTINCT
   */
  std::unordered_set<std::string> seen_;

  /**
   * @brief 是否SELECT DISTINCT的结果集
   * @note true表示需要去重，false表示不去重
   */
  bool is_unique_;

 public:
  /**
   * @brief 构造函数
   * @param prev 子节点执行器的智能指针
   * @param sel_cols 要投影的列列表
   * @param is_unique 是否去重（默认false）
   */
  ProjectionExecutor(std::unique_ptr<AbstractExecutor> prev,
                     const std::vector<TabCol> &sel_cols,
                     bool is_unique = false);

  /**
   * @brief 开始迭代元组
   * @note 初始化子节点执行器，准备开始投影
   */
  void beginTuple() override;

  /**
   * @brief 移动到下一个元组
   * @note 推进子节点执行器，跳过重复的元组（如果is_unique_为true）
   */
  void nextTuple() override;

  /**
   * @brief 获取下一个元组
   * @return 投影后的元组的智能指针，如果没有更多元组则返回nullptr
   * @note 从子节点获取元组，提取指定的列，形成新的元组
   */
  std::unique_ptr<Tuple> Next() override;
  // std::unique_ptr<RmRecord> Next() override { return
  // std::make_unique<RmRecord>(projection_records_); }

  /**
   * @brief 获取当前元组的RID
   * @return 抽象RID的引用
   */
  RID &rid() override { return _abstract_rid; }

  /**
   * @brief 获取模式
   * @return 模式的常量引用
   */
  const Schema &schema() const override { return schema_; };
  // const std::vector<ColMeta> &cols() const override { return cols_; };

  /**
   * @brief 获取元组长度
   * @return 元组的长度（字节数）
   */
  size_t tupleLen() const override { return len_; };

  /**
   * @brief 判断是否到达末尾
   * @return true 如果子节点已到达末尾，false 否则
   */
  bool IsEnd() const override { return prev_->IsEnd(); };

  // ColMeta get_col_offset(const TabCol &target) override {
  //   for (auto &col : cols_) {
  //     if (target.col_name == col.name && target.tab_name == col.tab_name) {
  //       return col;
  //     }
  //   }
  //   throw ColumnNotFoundError(target.col_name);
  // };

 private:
  /**
   * @brief 执行投影操作
   * @return 投影后的元组
   * @note 从输入元组中提取指定的列，形成新的元组
   */
  // RmRecord projectRecord();
  Tuple projectRecord();

  /**
   * @brief 生成新的列名
   * @param col 列标识
   * @return 新的列名字符串
   * @note 用于处理列名冲突，生成唯一的列名
   */
  std::string generate_new_name(const TabCol &col);
};
}  // namespace easydb