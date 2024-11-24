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

class DeleteExecutor : public AbstractExecutor {
 private:
  TabMeta tab_;                   // 表的元数据
  std::vector<Condition> conds_;  // delete的条件
  RmFileHandle *fh_;              // 表的数据文件句柄
  std::vector<RID> rids_;         // 需要删除的记录的位置
  std::string tab_name_;          // 表名称
  SmManager *sm_manager_;

 public:
  DeleteExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<Condition> conds,
                 std::vector<RID> rids, Context *context);

  std::unique_ptr<Tuple> Next() override;

  RID &rid() override { return _abstract_rid; }
};

}  // namespace easydb
