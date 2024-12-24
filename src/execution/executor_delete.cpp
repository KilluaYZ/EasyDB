/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * executor_delete.cpp
 *
 * Identification: src/execution/executor_delete.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "execution/executor_delete.h"
#include "storage/table/tuple.h"

namespace easydb {

DeleteExecutor::DeleteExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<Condition> conds,
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

std::unique_ptr<Tuple> DeleteExecutor::Next() {
  // auto indexHandle = sm_manager_->ihs_;
  // traverse records to be deleted
  int rid_size = rids_.size();
  for (int i = 0; i < rid_size; i++) {
    RID rid = rids_[i];

    // get records
    auto rec = fh_->GetTupleValue(rid, context_);

    // delete corresponding index
    for (auto index : tab_.indexes) {
      auto ih = sm_manager_->ihs_.at(sm_manager_->GetIxManager()->GetIndexName(tab_name_, index.cols)).get();
      auto key_schema = Schema::CopySchema(&tab_.schema, index.col_ids);
      auto key_tuple = fh_->GetKeyTuple(tab_.schema, key_schema, index.col_ids, rid, context_);
      char *key = new char[index.col_tot_len];
      int offset = 0;
      for (int i = 0; i < index.col_num; ++i) {
        auto val = key_tuple.GetValue(&key_schema, i);
        ix_memcpy(key + offset, val, index.cols[i].len);
        offset += index.cols[i].len;
      }
      // Wait for GAP lock first
      if (context_ != nullptr) {
        Iid lower = ih->LowerBound(key);
        context_->lock_mgr_->handle_index_gap_wait_die(context_->txn_, lower, fh_->GetFd());
      }
      ih->DeleteEntry(key, context_->txn_);
    }

    // delete records
    fh_->DeleteTuple(rid, context_);

    // // Log the delete operation
    // DeleteLogRecord del_log_rec(context_->txn_->get_transaction_id(), *rec, rid, tab_name_);
    // del_log_rec.prev_lsn_ = context_->txn_->get_prev_lsn();
    // lsn_t lsn = context_->log_mgr_->add_log_to_buffer(&del_log_rec);
    // context_->txn_->set_prev_lsn(lsn);
    // // set lsn in page header
    // fh_->SetPageLSN(rid.GetPageId(), lsn);

    // Update context_ for rollback
    WriteRecord *write_record = new WriteRecord(WType::DELETE_TUPLE, tab_name_, rid, *rec);
    context_->txn_->AppendWriteRecord(write_record);

    sm_manager_->UpdateTableCount(tab_name_, -1);
  }

  return nullptr;
}

}  // namespace easydb
