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

class IndexScanExecutor : public AbstractExecutor {
 private:
  std::string tab_name_;          // 表名称
  TabMeta tab_;                   // 表的元数据
  std::vector<Condition> conds_;  // 扫描条件
  RmFileHandle *fh_;              // 表的数据文件句柄
  // std::vector<ColMeta> cols_;         // 需要读取的字段
  Schema schema_;                     // scan后生成的记录的字段
  size_t len_;                        // 选取出来的一条记录的长度
  std::vector<Condition> fed_conds_;  // 扫描条件，不一定和conds_字段相同(取决于索引的选择)

  std::vector<std::string> index_col_names_;  // index scan涉及到的索引包含的字段
  IndexMeta index_meta_;                      // index scan涉及到的索引元数据

  RID rid_;
  std::unique_ptr<IxScan> scan_;

  SmManager *sm_manager_;

 public:
  IndexScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds,
                    std::vector<std::string> index_col_names, Context *context);

  std::string getTabName() const override { return tab_name_; }

  size_t tupleLen() const override { return len_; }

  // const std::vector<ColMeta> &cols() const override { return cols_; }

  const Schema &schema() const override { return schema_; }

  virtual std::string getType() override { return "IndexScanExecutor"; };

  void beginTuple() override;
  void nextTuple() override;

  bool IsEnd() const override { return scan_->IsEnd(); }

  RID &rid() override { return rid_; }

  std::unique_ptr<Tuple> Next() override {
    // assert(!IsEnd());
    return fh_->GetTupleValue(rid_, context_);
  }

 private:
  // return true only all the conditions were true
  bool predicate();
};

}  // namespace easydb
