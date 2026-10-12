#pragma once

#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "storage/easystore/table/tuple.h"

namespace easydb {

/** A transaction snapshot used by consistent reads. */
class ReadView {
 public:
  ReadView(timestamp_t snapshot_ts, txn_id_t creator_txn_id,
           std::unordered_set<txn_id_t> active_txns,
           std::unordered_map<txn_id_t, timestamp_t> committed_txns)
      : snapshot_ts_(snapshot_ts),
        creator_txn_id_(creator_txn_id),
        active_txns_(std::move(active_txns)),
        committed_txns_(std::move(committed_txns)) {}

  auto IsVisible(const TupleMeta &meta) const -> bool {
    if (meta.trx_id_ == INVALID_TXN_ID || meta.trx_id_ == creator_txn_id_) {
      return !meta.is_deleted_;
    }
    if (active_txns_.count(meta.trx_id_) != 0) return false;
    const auto it = committed_txns_.find(meta.trx_id_);
    return it != committed_txns_.end() && it->second <= snapshot_ts_ &&
           !meta.is_deleted_;
  }

 private:
  timestamp_t snapshot_ts_;
  txn_id_t creator_txn_id_;
  std::unordered_set<txn_id_t> active_txns_;
  std::unordered_map<txn_id_t, timestamp_t> committed_txns_;
};

}  // namespace easydb
