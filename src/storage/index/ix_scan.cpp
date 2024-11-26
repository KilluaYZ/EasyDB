/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * ix_scan.cpp
 *
 * Identification: src/storage/index/ix_scan.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "storage/index/ix_scan.h"

namespace easydb {

/**
 * @brief
 * @todo 加上读锁（需要使用缓冲池得到page）
 */
void IxScan::Next() {
  assert(!IsEnd());
  IxNodeHandle *node = ih_->FetchNode(iid_.page_id_);
  assert(node->IsLeafPage());
  assert(iid_.slot_num_ < static_cast<slot_id_t>(node->GetSize()));
  // increment slot no
  iid_.slot_num_++;
  if (iid_.page_id_ != ih_->file_hdr_->last_leaf_ && iid_.slot_num_ == node->GetSize()) {
    // go to Next leaf
    iid_.slot_num_ = 0;
    iid_.page_id_ = node->GetNextLeaf();
  }
  // Unpin the page that pinned in FetchNode()
  bpm_->UnpinPage(node->GetPageId(), false);
  delete node;
}

RID IxScan::GetRid() const { return ih_->GetRid(iid_); }

}  // namespace easydb
