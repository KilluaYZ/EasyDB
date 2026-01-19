/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <memory>
#include "catalog/schema.h"
#include "common/condition.h"
#include "common/errors.h"
#include "defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "storage/index/ix_manager.h"
#include "storage/index/ix_scan.h"
#include "system/sm_defs.h"
#include "system/sm_manager.h"
#include "system/sm_meta.h"
namespace easydb {

/**
 * @brief 顺序扫描执行器类
 * 
 * SeqScanExecutor 实现顺序扫描操作，逐页遍历表中的所有元组，
 * 并根据WHERE条件过滤元组。
 */
class SeqScanExecutor : public AbstractExecutor {
 private:
  /**
   * @brief 表的名称
   */
  std::string tab_name_;
  
  /**
   * @brief 扫描的条件列表
   * @note 用于过滤元组，只有满足所有条件的元组才会被返回
   */
  std::vector<Condition> conds_;
  
  /**
   * @brief 表的数据文件句柄
   * @note 用于访问表的实际数据
   */
  RmFileHandle *fh_;
  
  /**
   * @brief 扫描后生成的记录的字段元数据
   */
  std::vector<ColMeta> cols_;
  
  /**
   * @brief 扫描后生成的记录的字段模式
   */
  Schema schema_;
  
  /**
   * @brief 扫描后生成的每条记录的长度（字节数）
   */
  size_t len_;
  
  /**
   * @brief 已传递的条件列表
   * @note 同conds_，两个字段相同
   */
  std::vector<Condition> fed_conds_;

  /**
   * @brief 当前元组的记录ID
   */
  RID rid_;
  
  /**
   * @brief 表迭代器（记录扫描器）
   * @note 用于遍历表中的所有元组
   */
  std::unique_ptr<RecScan> scan_;

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
   * @param context 事务上下文指针
   */
  SeqScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, Context *context);
  // SeqScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds);

  /**
   * @brief 开始迭代元组
   * @note 初始化扫描器，定位到第一个元组
   */
  void beginTuple() override;

  /**
   * @brief 移动到下一个元组
   * @note 推进扫描器到下一个位置，跳过不满足条件的元组
   */
  void nextTuple() override;

  /**
   * @brief 获取下一个元组
   * @return 下一个元组的智能指针，如果没有更多元组则返回nullptr
   */
  std::unique_ptr<Tuple> Next() override;

  /**
   * @brief 获取当前元组的RID
   * @return 当前元组的RID的引用
   */
  RID &rid() override { return rid_; }

  /**
   * @brief 判断是否到达末尾
   * @return true 如果已到达末尾，false 否则
   */
  bool IsEnd() const override { return scan_->IsEnd(); };

  /**
   * @brief 获取列元数据向量
   * @return 列元数据向量的常量引用
   */
  const std::vector<ColMeta> &cols() const override { return cols_; };

  /**
   * @brief 获取模式
   * @return 模式的常量引用
   */
  const Schema &schema() const override { return schema_; };

  /**
   * @brief 获取元组长度
   * @return 元组的长度（字节数）
   */
  size_t tupleLen() const override { return len_; };

  /**
   * @brief 根据TabCol获取列元数据
   * @param target 目标列标识
   * @return 列元数据
   * @throws ColumnNotFoundError 如果列不存在
   */
  ColMeta get_col_offset(const TabCol &target) override {
    for (auto &col : cols_) {
      if (target.col_name == col.name && target.tab_name == col.tab_name) {
        return col;
      }
    }
    throw ColumnNotFoundError(target.col_name);
  };

  /**
   * @brief 获取表名
   * @return 表名字符串
   */
  std::string getTabName() const override { return tab_name_; }

 private:
  /**
   * @brief 判断当前元组是否满足所有条件
   * @return true 如果满足所有条件，false 否则
   * @note 用于过滤不满足WHERE条件的元组
   */
  bool predicate();
};
}  // namespace easydb
