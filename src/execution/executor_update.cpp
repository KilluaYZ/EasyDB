/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * executor_update.cpp
 *
 * Identification: src/execution/executor_update.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "execution/executor_update.h"

namespace easydb {

UpdateExecutor::UpdateExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<SetClause> set_clauses,
                               std::vector<Condition> conds, std::vector<RID> rids, Context *context) {
  sm_manager_ = sm_manager;
  tab_name_ = tab_name;
  set_clauses_ = set_clauses;
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

std::unique_ptr<Tuple> UpdateExecutor::Next() {
  // traverse records to be updated
  int rid_size = rids_.size();
  for (int i = 0; i < rid_size; i++) {
    RID rid = rids_[i];
    // get records and construct updated value buf
    auto tuple = fh_->GetTupleValue(rid, context_);
    auto old_values = tuple->GetValueVec(&tab_.schema);
    auto new_values = old_values;

    // replace the corresponding column
    for (auto &set_clause : set_clauses_) {
      auto col_tmp = tab_.get_col(set_clause.lhs.col_name);
      auto col_id = tab_.GetColId(set_clause.lhs.col_name);
      Value val;
      if (set_clause.is_rhs_exp) {
        // Value rhs_res = tuple->GetValue(&tab_.schema, set_clause.rhs_col.col_name);
        Value rhs_res = old_values[col_id];
        val = set_clause.cal_val(rhs_res);
      } else {
        val = set_clause.rhs;
      }
      val.CastAs(col_tmp->type);
      new_values[col_id] = val;
    }

    Tuple new_tuple{new_values, &tab_.schema};

    // update corresponding index
    // 1. construct key_d and key_i
    // 2. delete old index entry and insert new index entry
    for (auto index : tab_.indexes) {
      auto ih = sm_manager_->ihs_.at(sm_manager_->GetIxManager()->GetIndexName(tab_name_, index.cols)).get();
      auto ids = index.col_ids;
      char *key_d = new char[index.col_tot_len];
      char *key_i = new char[index.col_tot_len];
      int offset = 0;
      for (int i = 0; i < index.col_num; ++i) {
        // memcpy(key_d + offset, rec->data + index.cols[i].offset, index.cols[i].len);
        // memcpy(key_i + offset, buf.data + index.cols[i].offset, index.cols[i].len);
        auto id = ids[i];
        auto val_d = old_values[id];
        auto val_i = new_values[id];
        ix_memcpy(key_d + offset, val_d, index.cols[i].len);
        ix_memcpy(key_i + offset, val_i, index.cols[i].len);
        offset += index.cols[i].len;
      }
      // check if the key is the same as before
      if (memcmp(key_d, key_i, index.col_tot_len) == 0) {
        continue;
      }

      // Wait for GAP lock before insert
      if (context_ != nullptr) {
        Iid lower = ih->LowerBound(key_i);
        context_->lock_mgr_->handle_index_gap_wait_die(context_->txn_, lower, fh_->GetFd());
      }

      // check if the new key duplicated
      auto is_insert = ih->InsertEntry(key_i, rid, context_->txn_);
      if (is_insert == -1) {
        std::vector<std::string> col_names;
        for (auto col : index.cols) {
          col_names.emplace_back(col.name);
        }
        throw IndexExistsError(tab_name_, col_names);
      }

      // Wait for GAP lock before delete
      if (context_ != nullptr) {
        Iid lower = ih->LowerBound(key_d);
        context_->lock_mgr_->handle_index_gap_wait_die(context_->txn_, lower, fh_->GetFd());
      }

      ih->DeleteEntry(key_d, context_->txn_);
      delete[] key_d;
      delete[] key_i;
    }

    // // Log the update operation(before update old value: *rec)
    // UpdateLogRecord update_log_rec(context_->txn_->get_transaction_id(), *rec, buf, rid, tab_name_);

    // update records
    // fh_->UpdateTupleInPlace(TupleMeta{0, false}, new_tuple, context_);
    fh_->UpdateTupleInPlace(TupleMeta{0, false}, new_tuple, rid, context_);

    // // Log the update operation(after update)
    // update_log_rec.prev_lsn_ = context_->txn_->get_prev_lsn();
    // lsn_t lsn = context_->log_mgr_->add_log_to_buffer(&update_log_rec);
    // context_->txn_->set_prev_lsn(lsn);
    // // set lsn in page header
    // fh_->SetPageLSN(rid.page_no, lsn);

    // Update context_ for rollback
    WriteRecord *write_record = new WriteRecord(WType::UPDATE_TUPLE, tab_name_, rid, *tuple);
    context_->txn_->AppendWriteRecord(write_record);
  }

  return nullptr;
}

}  // namespace easydb
