/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * executor_update.h
 *
 * Identification: src/include/execution/executor_update.h
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
#include "transaction/txn_defs.h"

namespace easydb {

class UpdateExecutor : public AbstractExecutor {
 private:
  TabMeta tab_;
  std::vector<Condition> conds_;
  RmFileHandle *fh_;
  std::vector<RID> rids_;
  std::string tab_name_;
  std::vector<SetClause> set_clauses_;
  SmManager *sm_manager_;

 public:
  UpdateExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<SetClause> set_clauses,
                 std::vector<Condition> conds, std::vector<RID> rids, Context *context);

  std::unique_ptr<Tuple> Next() override;

  RID &rid() override { return _abstract_rid; }
};
}  // namespace easydb
