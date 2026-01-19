/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <cassert>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "common/common.h"
#include "common/context.h"
#include "common/errors.h"
#include "defs.h"
#include "executor_abstract.h"
#include "planner/plan.h"
#include "planner/planner.h"
#include "record/record_printer.h"
#include "record/rm.h"
#include "system/sm_defs.h"
#include "system/sm_meta.h"
#include "transaction/transaction_manager.h"

// class Planner;
namespace easydb {

/**
 * @brief 查询语言管理器类（QL Manager）
 *
 * QlManager 负责执行查询计划，包括：
 * - DML语句的执行（SELECT、INSERT、UPDATE、DELETE）
 * - DDL语句的执行（CREATE TABLE、DROP TABLE等）
 * - 多查询的执行
 * - 子查询的执行
 */
class QlManager {
 private:
  /** @brief 系统管理器指针 */
  SmManager *sm_manager_;

  /** @brief 事务管理器指针 */
  TransactionManager *txn_mgr_;

  /** @brief 查询计划器指针 */
  Planner *planner_;

 public:
  /**
   * @brief 构造函数（完整版本）
   * @param sm_manager 系统管理器指针
   * @param txn_mgr 事务管理器指针
   * @param planner 查询计划器指针
   */
  QlManager(SmManager *sm_manager, TransactionManager *txn_mgr,
            Planner *planner)
      : sm_manager_(sm_manager), txn_mgr_(txn_mgr), planner_(planner) {}

  /**
   * @brief 构造函数（不带计划器）
   * @param sm_manager 系统管理器指针
   * @param txn_mgr 事务管理器指针
   */
  QlManager(SmManager *sm_manager, TransactionManager *txn_mgr)
      : sm_manager_(sm_manager), txn_mgr_(txn_mgr) {}

  /**
   * @brief 构造函数（只带系统管理器）
   * @param sm_manager 系统管理器指针
   */
  QlManager(SmManager *sm_manager) : sm_manager_(sm_manager) {}

  /**
   * @brief 执行多查询计划
   * @param plan 查询计划指针
   * @param context 上下文对象指针
   * @note 用于执行包含多个查询的复杂计划
   */
  void run_mutli_query(std::shared_ptr<Plan> plan, Context *context);

  /**
   * @brief 执行工具命令计划
   * @param plan 查询计划指针
   * @param txn_id 事务ID指针（输出参数）
   * @param context 上下文对象指针
   * @note 用于执行DDL语句和其他工具命令
   */
  void run_cmd_utility(std::shared_ptr<Plan> plan, txn_id_t *txn_id,
                       Context *context);

  /**
   * @brief 执行SELECT查询
   * @param executorTreeRoot 执行器树的根节点
   * @param sel_cols 选择的列列表
   * @param context 上下文对象指针
   * @note 遍历执行器树，获取结果并输出
   */
  void select_from(std::unique_ptr<AbstractExecutor> executorTreeRoot,
                   std::vector<TabCol> sel_cols, Context *context);

  /**
   * @brief 执行DML操作
   * @param exec 执行器指针
   * @note 用于执行INSERT、UPDATE、DELETE等DML操作
   */
  void run_dml(std::unique_ptr<AbstractExecutor> exec);
};

/**
 * @brief 执行子查询SELECT
 * @param executorTreeRoot 执行器树的根节点
 * @param sel_col 选择的列
 * @return 查询结果的值的向量
 * @note 用于WHERE子句中的子查询，返回单个列的值列表
 */
std::vector<Value> subquery_select_from(
    std::shared_ptr<AbstractExecutor> executorTreeRoot, TabCol sel_col);

}  // namespace easydb
