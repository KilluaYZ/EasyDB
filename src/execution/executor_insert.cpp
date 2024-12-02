/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * executor_insert.cpp
 *
 * Identification: src/execution/executor_insert.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "execution/executor_insert.h"

namespace easydb {

InsertExecutor::InsertExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<Value> values,
                               Context *context) {
  sm_manager_ = sm_manager;
  tab_ = sm_manager_->db_.get_table(tab_name);
  values_ = values;
  tab_name_ = tab_name;
  if (values.size() != tab_.cols.size()) {
    throw InvalidValueCountError();
  }
  fh_ = sm_manager_->fhs_.at(tab_name).get();
  context_ = context;

  // // lock table
  // if (context_ != nullptr) {
  //   context_->lock_mgr_->lock_IX_on_table(context_->txn_, fh_->GetFd());
  // }
};

std::unique_ptr<Tuple> InsertExecutor::Next() {
  // Construct the tuple
  Tuple tuple{values_, &tab_.schema};
  // // Wait for GAP lock first
  // int index_len = tab_.indexes.size();
  // // TODO: keep the key to avoid copy again when insert into index
  // if (context_ != nullptr) {
  //   for (int i = 0; i < index_len; ++i) {
  //     auto &index = tab_.indexes[i];
  //     auto ih = sm_manager_->ihs_.at(sm_manager_->GetIxManager()->GetIndexName(tab_name_, index.cols)).get();
  //     char *key = new char[index.col_tot_len];
  //     int offset = 0;
  //     for (int i = 0; i < index.col_num; ++i) {
  //       memcpy(key + offset, rec.data + index.cols[i].offset, index.cols[i].len);
  //       offset += index.cols[i].len;
  //     }
  //     // wait
  //     Iid lower = ih->LowerBound(key);
  //     context_->lock_mgr_->handle_index_gap_wait_die(context_->txn_, lower, fh_->GetFd());
  //   }
  // }
  // // Now we can insert the record into the file and index safely

  // Insert into record file
  auto rid = fh_->InsertTuple(TupleMeta{0, false}, tuple);
  rid_ = RID{rid->GetPageId(), rid->GetSlotNum()};

  // // Log the insert operation
  // InsertLogRecord insert_log_rec(context_->txn_->get_transaction_id(), rec, rid_, tab_name_);
  // insert_log_rec.prev_lsn_ = context_->txn_->get_prev_lsn();
  // lsn_t lsn = context_->log_mgr_->add_log_to_buffer(&insert_log_rec);
  // context_->txn_->set_prev_lsn(lsn);
  // // set lsn in page header
  // fh_->SetPageLSN(rid_.GetPageId(), lsn);

  // Insert into index
  for (auto index : tab_.indexes) {
    auto ih = sm_manager_->ihs_.at(sm_manager_->GetIxManager()->GetIndexName(tab_name_, index.cols)).get();
    auto key_schema = Schema::CopySchema(&tab_.schema, index.col_ids);
    auto key_tuple = fh_->GetKeyTuple(tab_.schema, key_schema, index.col_ids, rid_);
    char *key = new char[index.col_tot_len];
    int offset = 0;
    for (int i = 0; i < index.col_num; ++i) {
      // memcpy(key + offset, rec.data + index.cols[i].offset, index.cols[i].len);
      auto val = key_tuple.GetValue(&key_schema, i);
      memcpy(key + offset, val.GetData(), val.GetStorageSize());
      offset += index.cols[i].len;
    }
    // auto is_insert = ih->InsertEntry(key, rid_, context_->txn_);
    auto is_insert = ih->InsertEntry(key, rid_);
    delete[] key;

    if (is_insert == -1) {
      // fh_->DeleteTuple(rid_, context_);
      fh_->DeleteTuple(rid_);
      std::vector<std::string> col_names;
      for (auto col : index.cols) {
        col_names.emplace_back(col.name);
      }
      throw IndexExistsError(tab_name_, col_names);
    }
  }

  // // Update context_ for rollback (be sure to update after record insert)
  // WriteRecord *write_record = new WriteRecord(WType::INSERT_TUPLE, tab_name_, rid_);
  // context_->txn_->append_write_record(write_record);

  sm_manager_->UpdateTableCount(tab_name_, 1);

  return nullptr;
}

}  // namespace easydb
