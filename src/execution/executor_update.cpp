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

/**
 * @description: 更新执行器的构造函数
 * @param storage_engine 系统管理器指针
 * @param tab_name 表名
 * @param set_clauses 更新子句列表
 * @param conds 更新条件
 * @param rids 要更新的记录ID列表
 * @param context 上下文指针
 */
UpdateExecutor::UpdateExecutor(StorageEngine *storage_engine,
                               const std::string &tab_name,
                               std::vector<SetClause> set_clauses,
                               std::vector<Condition> conds,
                               std::vector<Tuple> tuples, Context *context) {
  storage_engine_ = storage_engine;
  tab_name_ = tab_name;
  set_clauses_ = set_clauses;
  tab_ = storage_engine_->GetTableMeta(tab_name);
  conds_ = conds;
  tuples_ = std::move(tuples);
  context_ = context;

  // lock table
  if (context_ != nullptr) {
    context_->lock_mgr_->LockIXOnTable(
        context_->txn_, storage_engine_->GetTableLockId(tab_name));
  }
}

/**
 * @description: 执行更新操作，更新指定的记录并维护相关索引
 * @return 返回nullptr，因为更新操作不返回元组
 */
std::unique_ptr<Tuple> UpdateExecutor::Next() {
  // 遍历要更新的记录
  int tuple_count = tuples_.size();
  for (int i = 0; i < tuple_count; i++) {
    const auto &key_tuple = tuples_[i];
    // get records and construct updated value buf
    auto tuple = storage_engine_->ReadTuple(tab_name_, key_tuple, context_);
    auto old_values = tuple->GetValueVec(&tab_.schema);
    auto new_values = old_values;

    // replace the corresponding column
    for (auto &set_clause : set_clauses_) {
      auto col_tmp = tab_.get_col(set_clause.lhs.col_name);
      auto col_id = tab_.GetColId(set_clause.lhs.col_name);
      Value val;
      if (set_clause.is_rhs_exp) {
        // Value rhs_res = tuple->GetValue(&tab_.schema,
        // set_clause.rhs_col.col_name);
        Value rhs_res = old_values[col_id];
        val = set_clause.cal_val(rhs_res);
      } else {
        val = set_clause.rhs;
      }
      val.CastAs(col_tmp->type);
      new_values[col_id] = val;
    }

    Tuple new_tuple{new_values, &tab_.schema};

    if (context_ != nullptr && context_->txn_ != nullptr &&
        context_->log_mgr_ != nullptr) {
      RedoLogRecord redo(LogKind::REDO, UPDATE,
                         context_->txn_->GetTransactionId(),
                         context_->txn_->GetPrevLsn(), tab_name_, key_tuple,
                         *tuple, new_tuple);
      auto lsn = context_->log_mgr_->add_log_to_buffer(&redo);
      context_->txn_->SetPrevLsn(lsn);

      UndoLogRecord undo(LogKind::UNDO, UPDATE,
                         context_->txn_->GetTransactionId(),
                         context_->txn_->GetPrevLsn(), tab_name_, key_tuple,
                         *tuple, new_tuple);
      lsn = context_->log_mgr_->add_log_to_buffer(&undo);
      context_->txn_->SetPrevLsn(lsn);
      context_->log_mgr_->flush_log_to_disk();
    }

    // update records
    // fh_->UpdateTupleInPlace(TupleMeta{0, false}, new_tuple, context_);
    storage_engine_->UpdateTuple(tab_name_, key_tuple, new_tuple, context_);

    // Update context_ for rollback
    WriteRecord *write_record =
        new WriteRecord(WType::UPDATE_TUPLE, tab_name_, key_tuple, *tuple);
    context_->txn_->AppendWriteRecord(write_record);
  }

  return nullptr;
}

}  // namespace easydb
