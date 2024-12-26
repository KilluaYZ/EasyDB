/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * rm_scan.cpp
 *
 * Identification: src/record/rm_scan.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "record/rm_scan.h"
#include <cstdint>
#include "record/rm_file_handle.h"

namespace easydb {

/**
 * @brief 初始化file_handle和rid
 * @param file_handle
 */
RmScan::RmScan(const RmFileHandle *file_handle) : file_handle_(file_handle) {
  // Initialize file_handle and set rid_ to the first valid record
  // Start from the first data page (page 0 is the file header)
  // Initialize slot_no to 0 to start scanning from the beginning
  rid_.Set(RM_FIRST_RECORD_PAGE, 0);
}

/**
 * @brief 找到文件中下一个存放了记录的位置
 */
void RmScan::Next() {
  auto page_no = rid_.GetPageId();
  auto slot_no = rid_.GetSlotNum() + 1;

  bool found_valid_record = false;
  // If we have not reached the end of the file
  while (page_no < file_handle_->file_hdr_.num_pages) {
    RmPageHandle page_handle = file_handle_->FetchPageHandle(page_no);
    uint32_t num_records = page_handle.GetNumTuples();

    while (slot_no < num_records) {
      // If not deleted, we have found a valid record
      if (!page_handle.IsTupleDeleted({page_no, slot_no})) {
        found_valid_record = true;
        break;
      }
      // Move to the next slot
      slot_no++;
    };

    // Unpin the page that was pinned in 'fetch_page_handle'
    file_handle_->buffer_pool_manager_->UnpinPage(page_handle.page->GetPageId(), false);

    // If we have reached the end of the page, move to the next page
    if (slot_no == num_records) {
      page_no++;
      slot_no = 0;
    }

    // If we have found a valid record, break out of the loop
    if (found_valid_record) {
      break;
    }
  }
  rid_.Set(page_no, slot_no);
}

/**
 * @brief ​ 判断是否到达文件末尾
 */
bool RmScan::IsEnd() const {
  // Check if we have reached the end of the file
  return rid_.GetPageId() >= file_handle_->file_hdr_.num_pages;
}

/**
 * @brief RmScan内部存放的rid
 */
RID RmScan::GetRid() const { return rid_; }
}  // namespace easydb
