/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "execution/executor_seq_scan.h"

namespace easydb {

SeqScanExecutor::SeqScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds) {
  //  Context *context) {
  sm_manager_ = sm_manager;
  tab_name_ = std::move(tab_name);
  conds_ = std::move(conds);
  TabMeta &tab = sm_manager_->db_.get_table(tab_name_);
  fh_ = sm_manager_->fhs_.at(tab_name_).get();
  schema_ = tab.schema;
  cols_ = tab.cols;
  // len_ = cols_.back().offset + cols_.back().len;
  len_ = schema_.length_;

  context_ = context;

  fed_conds_ = conds_;

  // // lock table
  // if (context_ != nullptr) {
  //   context_->lock_mgr_->lock_shared_on_table(context_->txn_, fh_->GetFd());
  // }
}

void SeqScanExecutor::beginTuple() override {
  scan_ = std::make_unique<RmScan>(fh_);
  rid_ = scan_->rid();
  while (!IsEnd() && !predicate()) {
    scan_->next();
    rid_ = scan_->rid();
  }
}

void SeqScanExecutor::nextTuple() override {
  do {
    scan_->next();
    rid_ = scan_->rid();
  } while (!IsEnd() && !predicate());
}

std::unique_ptr<Tuple> SeqScanExecutor::Next() override { return fh_->GetTupleValue(rid_); }

bool SeqScanExecutor::predicate() {
  auto tuple = *this->Next();
  bool satisfy = true;
  // return true only all the conditions were true
  // i.e. all conditions are connected with 'and' operator
  for (auto &cond : conds_) {
    // check subquery
    if (cond.is_rhs_stmt && !cond.is_rhs_exe_processed) {
      std::shared_ptr<AbstractExecutor> rhs_stmt_executor_tree_root =
          std::static_pointer_cast<AbstractExecutor>(cond.rhs_stmt_exe);
      std::vector<Value> results = subquery_select_from(rhs_stmt_executor_tree_root, cond.rhs_col);
      // 进行回填
      // comparison stmt, sub query should return only one value
      // Also legal if subquery doesn't use aggregation function, but return only a value.
      // Check is delayed until plan_query.
      // Remember to fill in cond.rhs_val real value after carrying executor.
      if (cond.op != OP_IN) {
        if (results.size() > 1) {
          throw SubqueryIllegalError("Result of subquery contains multiple tuples\n");
        } else if (results.size() == 1) {
          cond.rhs_val = results[0];
        } else {
          throw SubqueryIllegalError("Result of subquery is empty\n");
        }
      } else {
        cond.rhs_in_col = results;
      }
      cond.is_rhs_exe_processed = true;
    }
    Value lhs_v, rhs_v;
    lhs_v = tuple.GetValue(schema_,cond.lhs_col.col_name);
    if (lhs_v == nullptr) {
      throw InternalError("target column not found.");
    }
    if (cond.is_rhs_val) {
      rhs_v = cond.rhs_val;
    } else if (cond.op != OP_IN) {
      rhs_v = tuple.GetValue(schema_,cond.rhs_col.col_name);
      // rhs_v.get_value_from_tuple(tuple, cols_, cond.rhs_col.col_name)
      if(rhs_v == nullptr){
        throw InternalError("target column not found.");        
      }
    }
    if (!cond.satisfy(lhs_v, rhs_v)) {
      satisfy = false;
      break;
    }
  }
  return satisfy;
}

}  // namespace easydb
