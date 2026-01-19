/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * executor_delete.h
 *
 * Identification: src/include/execution/executor_delete.h
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
// #include "transaction/txn_defs.h"

namespace easydb {

/**
 * @brief 删除执行器类
 * 
 * DeleteExecutor 实现DELETE语句的执行，删除满足条件的元组。
 * 同时更新相关的索引。
 */
class DeleteExecutor : public AbstractExecutor {
 private:
  /**
   * @brief 表的元数据
   */
  TabMeta tab_;
  
  /**
   * @brief 删除的条件列表
   * @note 用于确定哪些元组需要被删除
   */
  std::vector<Condition> conds_;
  
  /**
   * @brief 表的数据文件句柄
   * @note 用于执行实际的删除操作
   */
  RmFileHandle *fh_;
  
  /**
   * @brief 需要删除的记录的位置列表
   * @note 存储所有满足条件的元组的RID
   */
  std::vector<RID> rids_;
  
  /**
   * @brief 表名称
   */
  std::string tab_name_;
  
  /**
   * @brief 系统管理器指针
   */
  SmManager *sm_manager_;

 public:
  /**
   * @brief 构造函数
   * @param sm_manager 系统管理器指针
   * @param tab_name 表名
   * @param conds 删除条件列表
   * @param rids 要删除的记录ID列表（如果为空，则根据条件查找）
   * @param context 事务上下文指针
   */
  DeleteExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<Condition> conds,
                 std::vector<RID> rids, Context *context);

  /**
   * @brief 获取下一个元组（执行删除操作）
   * @return 被删除的元组的智能指针，如果没有更多元组则返回nullptr
   * @note 
   *   - 遍历rids_列表，逐个删除元组
   *   - 删除操作包括：从表中删除、更新索引、记录日志
   */
  std::unique_ptr<Tuple> Next() override;

  /**
   * @brief 获取当前元组的RID
   * @return 抽象RID的引用
   */
  RID &rid() override { return _abstract_rid; }
};

}  // namespace easydb
