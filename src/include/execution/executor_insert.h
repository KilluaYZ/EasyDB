/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
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
  InsertExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<Value> values, Context *context) {
    sm_manager_ = sm_manager;
    tab_ = sm_manager_->db_.get_table(tab_name);
    values_ = values;
    tab_name_ = tab_name;
    if (values.size() != tab_.cols.size()) {
      throw InvalidValueCountError();
    }
    fh_ = sm_manager_->fhs_.at(tab_name).get();
    context_ = context;

    // lock table
    if (context_ != nullptr) {
      context_->lock_mgr_->lock_IX_on_table(context_->txn_, fh_->GetFd());
    }
  };

  std::unique_ptr<RmRecord> Next() override {
    // Make record buffer
    RmRecord rec(fh_->get_file_hdr().record_size);
    int value_len = values_.size();
    for (int i = 0; i < value_len; i++) {
      auto &col = tab_.cols[i];
      auto &val = values_[i];
      if (col.type != val.type) {
        if (col.type == TYPE_STRING || val.type == TYPE_STRING)
          throw IncompatibleTypeError(coltype2str(col.type), coltype2str(val.type));
        else
          val.type_cast(col.type);
      }
      val.init_raw(col.len);
      memcpy(rec.data + col.offset, val.raw->data, col.len);
    }
    // Wait for GAP lock first
    int index_len = tab_.indexes.size();
    // TODO: keep the key to avoid copy again when insert into index
    if (context_ != nullptr) {
      for (int i = 0; i < index_len; ++i) {
        auto &index = tab_.indexes[i];
        auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
        char *key = new char[index.col_tot_len];
        int offset = 0;
        for (int i = 0; i < index.col_num; ++i) {
          memcpy(key + offset, rec.data + index.cols[i].offset, index.cols[i].len);
          offset += index.cols[i].len;
        }
        // wait
        Iid lower = ih->lower_bound(key);
        context_->lock_mgr_->handle_index_gap_wait_die(context_->txn_, lower, fh_->GetFd());
      }
    }
    // Now we can insert the record into the file and index safely

    // Insert into record file
    rid_ = fh_->insert_record(rec.data, context_);

    // Log the insert operation
    InsertLogRecord insert_log_rec(context_->txn_->get_transaction_id(), rec, rid_, tab_name_);
    insert_log_rec.prev_lsn_ = context_->txn_->get_prev_lsn();
    lsn_t lsn = context_->log_mgr_->add_log_to_buffer(&insert_log_rec);
    context_->txn_->set_prev_lsn(lsn);
    // set lsn in page header
    fh_->set_page_lsn(rid_.page_no, lsn);

    // Insert into index
    for (int i = 0; i < index_len; ++i) {
      auto &index = tab_.indexes[i];
      auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
      char *key = new char[index.col_tot_len];
      int offset = 0;
      for (int i = 0; i < index.col_num; ++i) {
        memcpy(key + offset, rec.data + index.cols[i].offset, index.cols[i].len);
        offset += index.cols[i].len;
      }
      auto is_insert = ih->insert_entry(key, rid_, context_->txn_);
      if (is_insert == -1) {
        fh_->delete_record(rid_, context_);
        std::vector<std::string> col_names;
        for (auto col : index.cols) {
          col_names.emplace_back(col.name);
        }
        throw IndexExistsError(tab_name_, col_names);
      }
    }

    // Update context_ for rollback (be sure to update after record insert)
    WriteRecord *write_record = new WriteRecord(WType::INSERT_TUPLE, tab_name_, rid_);
    context_->txn_->append_write_record(write_record);

    sm_manager_->update_table_count(tab_name_, 1);

    return nullptr;
  }
  RID &rid() override { return rid_; }
};

}  // namespace easydb
