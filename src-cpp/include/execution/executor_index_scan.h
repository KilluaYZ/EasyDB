/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * executor_index_scan.h
 *
 * Identification: src/include/execution/executor_index_scan.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <iostream>
#include <unordered_map>

#include <memory>
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
 * @brief 索引扫描执行器类
 *
 * IndexScanExecutor 实现索引扫描操作，利用索引快速定位满足条件的元组，
 * 比顺序扫描更高效。
 */
class IndexScanExecutor : public AbstractExecutor {
 private:
  /**
   * @brief 表名称
   */
  std::string tab_name_;

  /**
   * @brief 表的元数据
   */
  TabMeta tab_;

  /**
   * @brief 扫描条件列表
   * @note 用于过滤元组，只有满足所有条件的元组才会被返回
   */
  std::vector<Condition> conds_;

  /**
   * @brief 表的数据文件句柄
   * @note 用于访问表的实际数据
   */
  RmFileHandle *fh_;

  // std::vector<ColMeta> cols_;         // 需要读取的字段

  /**
   * @brief 扫描后生成的记录的字段模式
   */
  Schema schema_;

  /**
   * @brief 选取出来的一条记录的长度（字节数）
   */
  size_t len_;

  /**
   * @brief 已传递的扫描条件列表
   * @note 不一定和conds_字段相同，取决于索引的选择
   *       某些条件可能已经在索引扫描时被使用
   */
  std::vector<Condition> fed_conds_;

  /**
   * @brief 索引扫描涉及到的索引包含的字段名列表
   */
  std::vector<std::string> index_col_names_;

  /**
   * @brief 索引扫描涉及到的索引元数据
   */
  IndexMeta index_meta_;

  /**
   * @brief 当前元组的记录ID
   */
  RID rid_;

  /**
   * @brief 索引扫描器
   * @note 用于遍历索引，获取满足条件的RID列表
   */
  std::unique_ptr<IxScan> scan_;

  /**
   * @brief 系统管理器指针
   */
  SmManager *sm_manager_;

 public:
  /**
   * @brief 构造函数
   * @param sm_manager 系统管理器指针
   * @param tab_name 表名
   * @param conds 扫描条件列表
   * @param index_col_names 索引列名列表
   * @param context 事务上下文指针
   */
  IndexScanExecutor(SmManager *sm_manager, std::string tab_name,
                    std::vector<Condition> conds,
                    std::vector<std::string> index_col_names, Context *context);

  /**
   * @brief 获取表名
   * @return 表名字符串
   */
  std::string getTabName() const override { return tab_name_; }

  /**
   * @brief 获取元组长度
   * @return 元组的长度（字节数）
   */
  size_t tupleLen() const override { return len_; }

  // const std::vector<ColMeta> &cols() const override { return cols_; }

  /**
   * @brief 获取模式
   * @return 模式的常量引用
   */
  const Schema &schema() const override { return schema_; }

  /**
   * @brief 获取执行器类型名称
   * @return "IndexScanExecutor"
   */
  virtual std::string getType() override { return "IndexScanExecutor"; };

  /**
   * @brief 开始迭代元组
   * @note 初始化索引扫描器，定位到第一个满足条件的元组
   */
  void beginTuple() override;

  /**
   * @brief 移动到下一个元组
   * @note 推进索引扫描器到下一个位置，跳过不满足条件的元组
   */
  void nextTuple() override;

  /**
   * @brief 判断是否到达末尾
   * @return true 如果已到达末尾，false 否则
   */
  bool IsEnd() const override { return scan_->IsEnd(); }

  /**
   * @brief 获取当前元组的RID
   * @return 当前元组的RID的引用
   */
  RID &rid() override { return rid_; }

  /**
   * @brief 获取下一个元组
   * @return 下一个元组的智能指针，如果没有更多元组则返回nullptr
   * @note 根据RID从表中读取完整的元组数据
   */
  std::unique_ptr<Tuple> Next() override {
    // assert(!IsEnd());
    return fh_->GetTupleValue(rid_, context_);
  }

 private:
  /**
   * @brief 判断当前元组是否满足所有条件
   * @return true 如果满足所有条件，false 否则
   * @note 用于过滤不满足WHERE条件的元组
   *       注意：某些条件可能已经在索引扫描时被使用，但仍需要检查所有条件
   */
  bool predicate();
};

}  // namespace easydb
