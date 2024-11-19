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
  std::unique_ptr<RmRecord> Next() override {
    // auto rec_size = fh_->get_file_hdr().record_size;
    // traverse records to be updated
    int rid_size = rids_.size();
    for (int i = 0; i < rid_size; i++) {
      RID rid = rids_[i];
      // get records and construct updated value buf
      auto rec = fh_->get_record(rid, context_);
      // auto buf = std::make_unique<char[]>(rec->size);
      auto buf = RmRecord(rec->size, rec->data);
      // memcpy(buf.data, rec->data, rec->size);
      // replace the corresponding column
      for (auto &set_clause : set_clauses_) {
        auto col_tmp = tab_.get_col(set_clause.lhs.col_name);
        Value val;
        if (set_clause.is_rhs_exp) {
          Value rhs_res;
          if (rhs_res.get_value_from_record(*rec, tab_.cols, set_clause.rhs_col.col_name) == nullptr) {
            throw InternalError("target column not found.");
          }
          val = set_clause.cal_val(rhs_res);
        } else {
          val = set_clause.rhs;
        }
        val.type_cast(col_tmp->type);
        if (val.raw == nullptr) {
          val.init_raw(col_tmp->len);
        }
        memcpy(buf.data + col_tmp->offset, val.raw->data, val.raw->size);
      }
      // todo check conditions

      // update corresponding index
      // 1. construct key_d and key_i
      // 2.delete old index entry and insert new index entry
      int index_len = tab_.indexes.size();
      for (int i = 0; i < index_len; ++i) {
        auto &index = tab_.indexes[i];
        auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
        char *key_d = new char[index.col_tot_len];
        char *key_i = new char[index.col_tot_len];
        int offset = 0;
        for (int i = 0; i < index.col_num; ++i) {
          memcpy(key_d + offset, rec->data + index.cols[i].offset, index.cols[i].len);
          memcpy(key_i + offset, buf.data + index.cols[i].offset, index.cols[i].len);
          offset += index.cols[i].len;
        }
        // check if the key is the same as before
        if (memcmp(key_d, key_i, index.col_tot_len) == 0) {
          continue;
        }

        // Wait for GAP lock before insert
        if (context_ != nullptr) {
          Iid lower = ih->lower_bound(key_i);
          context_->lock_mgr_->handle_index_gap_wait_die(context_->txn_, lower, fh_->GetFd());
        }

        // check if the new key duplicated
        auto is_insert = ih->insert_entry(key_i, rid, context_->txn_);
        if (is_insert == -1) {
          std::vector<std::string> col_names;
          for (auto col : index.cols) {
            col_names.emplace_back(col.name);
          }
          throw IndexExistsError(tab_name_, col_names);
        }

        // Wait for GAP lock before delete
        if (context_ != nullptr) {
          Iid lower = ih->lower_bound(key_d);
          context_->lock_mgr_->handle_index_gap_wait_die(context_->txn_, lower, fh_->GetFd());
        }

        ih->delete_entry(key_d, context_->txn_);
      }

      // Log the update operation(before update old value: *rec)
      UpdateLogRecord update_log_rec(context_->txn_->get_transaction_id(), *rec, buf, rid, tab_name_);

      // update records
      fh_->update_record(rid, buf.data, context_);

      // Log the update operation(after update)
      update_log_rec.prev_lsn_ = context_->txn_->get_prev_lsn();
      lsn_t lsn = context_->log_mgr_->add_log_to_buffer(&update_log_rec);
      context_->txn_->set_prev_lsn(lsn);
      // set lsn in page header
      fh_->set_page_lsn(rid.page_no, lsn);

      // Update context_ for rollback
      WriteRecord *write_record = new WriteRecord(WType::UPDATE_TUPLE, tab_name_, rid, *rec);
      context_->txn_->append_write_record(write_record);
    }

    return nullptr;
  }

  RID &rid() override { return _abstract_rid; }
};
}  // namespace easydb
