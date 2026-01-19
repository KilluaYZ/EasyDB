/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * executor_insert.h
 *
 * Identification: src/include/execution/executor_insert.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once
#include "common/errors.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "storage/index/ix_manager.h"
#include "storage/index/ix_scan.h"
#include "system/sm_defs.h"
#include "system/sm_meta.h"
#include "transaction/txn_defs.h"

namespace easydb {

/**
 * @brief 插入执行器类
 * 
 * InsertExecutor 实现INSERT语句的执行，将新元组插入到表中。
 * 同时更新相关的索引。
 */
class InsertExecutor : public AbstractExecutor {
 private:
  /**
   * @brief 表的元数据
   */
  TabMeta tab_;
  
  /**
   * @brief 需要插入的数据值列表
   * @note 按照表定义的列顺序排列
   */
  std::vector<Value> values_;
  
  /**
   * @brief 表的数据文件句柄
   * @note 用于执行实际的插入操作
   */
  RmFileHandle *fh_;
  
  /**
   * @brief 表名称
   */
  std::string tab_name_;
  
  /**
   * @brief 插入的位置（记录ID）
   * @note 由于系统默认插入时不指定位置，因此当前rid_在插入后才赋值
   */
  RID rid_;
  
  /**
   * @brief 系统管理器指针
   */
  SmManager *sm_manager_;

 public:
  /**
   * @brief 构造函数
   * @param sm_manager 系统管理器指针
   * @param tab_name 表名
   * @param values 要插入的值列表
   * @param context 事务上下文指针
   */
  InsertExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<Value> values, Context *context);

  /**
   * @brief 获取下一个元组（执行插入操作）
   * @return 插入的元组的智能指针
   * @note 
   *   - 第一次调用时执行插入操作，后续调用返回nullptr
   *   - 插入操作包括：创建元组、插入到表、更新索引、记录日志
   */
  std::unique_ptr<Tuple> Next() override;

  /**
   * @brief 获取插入的元组的RID
   * @return 插入的元组的RID的引用
   */
  RID &rid() override { return rid_; }
};

}  // namespace easydb
