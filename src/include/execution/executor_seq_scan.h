/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
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

namespace easydb {

class SeqScanExecutor : public AbstractExecutor {
 private:
  std::string tab_name_;              // 表的名称
  std::vector<Condition> conds_;      // scan的条件
  RmFileHandle *fh_;                  // 表的数据文件句柄
  std::vector<ColMeta> cols_;         // scan后生成的记录的字段
  size_t len_;                        // scan后生成的每条记录的长度
  std::vector<Condition> fed_conds_;  // 同conds_，两个字段相同

  RID rid_;
  std::unique_ptr<RecScan> scan_;  // table_iterator

  SmManager *sm_manager_;

 public:
  // SeqScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, Context *context);
  SeqScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds);

  void beginTuple() override;

  void nextTuple() override;

  std::unique_ptr<Tuple> Next() override;

  RID &rid() override { return rid_; }

  bool IsEnd() const override { return scan_->IsEnd(); };

  const std::vector<ColMeta> &cols() const override { return cols_; };

  size_t tupleLen() const override { return len_; };

  ColMeta get_col_offset(const TabCol &target) override {
    for (auto &col : cols_) {
      if (target.col_name == col.name && target.tab_name == col.tab_name) {
        return col;
      }
    }
    throw ColumnNotFoundError(target.col_name);
  };

  std::string getTabName() const override { return tab_name_; }

 private:
  bool predicate();
};
}  // namespace easydb
