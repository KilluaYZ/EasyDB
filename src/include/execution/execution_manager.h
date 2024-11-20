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
#include "record/rm.h"
#include "record_printer.h"
#include "system/sm_defs.h"
#include "system/sm_meta.h"
#include "transaction/transaction_manager.h"

// class Planner;
namespace easydb {

class QlManager {
 private:
  SmManager *sm_manager_;
  TransactionManager *txn_mgr_;
  Planner *planner_;

 public:
  QlManager(SmManager *sm_manager, TransactionManager *txn_mgr, Planner *planner)
      : sm_manager_(sm_manager), txn_mgr_(txn_mgr), planner_(planner) {}
  QlManager(SmManager *sm_manager, TransactionManager *txn_mgr) : sm_manager_(sm_manager), txn_mgr_(txn_mgr) {}
  QlManager(SmManager *sm_manager) : sm_manager_(sm_manager) {}

  //   void run_mutli_query(std::shared_ptr<Plan> plan, Context *context);
  //   void run_cmd_utility(std::shared_ptr<Plan> plan, txn_id_t *txn_id, Context *context);
  void select_from(std::unique_ptr<AbstractExecutor> executorTreeRoot, std::vector<TabCol> sel_cols, Context *context);

  void run_dml(std::unique_ptr<AbstractExecutor> exec);
};

std::vector<Value> subquery_select_from(std::shared_ptr<AbstractExecutor> executorTreeRoot, TabCol sel_col);

}  // namespace easydb
