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

#pragma once
#include "storage/index/ix_scan.h"

namespace easydb {

/**
 * @brief
 * @todo 加上读锁（需要使用缓冲池得到page）
 */
void IxScan::Next() {
  assert(!IsEnd());
  IxNodeHandle *node = ih_->FetchNode(iid_.page_no);
  assert(node->IsLeafPage());
  assert(iid_.slot_no < node->GetSize());
  // increment slot no
  iid_.slot_no++;
  if (iid_.page_no != ih_->file_hdr_->last_leaf_ && iid_.slot_no == node->GetSize()) {
    // go to Next leaf
    iid_.slot_no = 0;
    iid_.page_no = node->GetNextLeaf();
  }
  // Unpin the page that pinned in FetchNode()
  bpm_->UnpinPage(node->GetPageId(), false);
}

Rid IxScan::GetRid() const { return ih_->GetRid(iid_); }

}  // namespace easydb
