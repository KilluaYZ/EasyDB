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

class InsertExecutor : public AbstractExecutor {
 private:
  TabMeta tab_;                // 表的元数据
  std::vector<Value> values_;  // 需要插入的数据
  RmFileHandle *fh_;           // 表的数据文件句柄
  std::string tab_name_;       // 表名称
  RID rid_;  // 插入的位置，由于系统默认插入时不指定位置，因此当前rid_在插入后才赋值
  SmManager *sm_manager_;

 public:
  InsertExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<Value> values, Context *context);

  std::unique_ptr<Tuple> Next() override;

  RID &rid() override { return rid_; }
};

}  // namespace easydb
