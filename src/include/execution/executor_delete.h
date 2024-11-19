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
                 std::vector<RID> rids, Context *context) {
    sm_manager_ = sm_manager;
    tab_name_ = tab_name;
    tab_ = sm_manager_->db_.get_table(tab_name);
    fh_ = sm_manager_->fhs_.at(tab_name).get();
    conds_ = conds;
    rids_ = rids;
    context_ = context;

    // lock table
    if (context_ != nullptr) {
      context_->lock_mgr_->lock_IX_on_table(context_->txn_, fh_->GetFd());
    }
  }

  std::unique_ptr<RmRecord> Next() override {
    // auto indexHandle = sm_manager_->ihs_;
    // traverse records to be deleted
    int rid_size = rids_.size();
    for (int i = 0; i < rid_size; i++) {
      RID rid = rids_[i];

      // get records
      auto rec = fh_->GetTupleValue(rid, context_);

      // delete corresponding index
      int index_len = tab_.indexes.size();
      for (int i = 0; i < index_len; ++i) {
        auto &index = tab_.indexes[i];
        auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
        char *key = new char[index.col_tot_len];
        int offset = 0;
        for (int i = 0; i < index.col_num; ++i) {
          memcpy(key + offset, rec->data + index.cols[i].offset, index.cols[i].len);
          offset += index.cols[i].len;
        }
        // Wait for GAP lock first
        if (context_ != nullptr) {
          Iid lower = ih->lower_bound(key);
          context_->lock_mgr_->handle_index_gap_wait_die(context_->txn_, lower, fh_->GetFd());
        }
        ih->delete_entry(key, context_->txn_);
      }

      // delete records
      fh_->delete_record(rid, context_);

      // Log the delete operation
      DeleteLogRecord del_log_rec(context_->txn_->get_transaction_id(), *rec, rid, tab_name_);
      del_log_rec.prev_lsn_ = context_->txn_->get_prev_lsn();
      lsn_t lsn = context_->log_mgr_->add_log_to_buffer(&del_log_rec);
      context_->txn_->set_prev_lsn(lsn);
      // set lsn in page header
      fh_->set_page_lsn(rid.page_no, lsn);

      // Update context_ for rollback
      WriteRecord *write_record = new WriteRecord(WType::DELETE_TUPLE, tab_name_, rid, *rec);
      context_->txn_->append_write_record(write_record);

      sm_manager_->update_table_count(tab_name_, -1);
    }

    return nullptr;
  }

  RID &rid() override { return _abstract_rid; }
};

}  // namespace easydb
