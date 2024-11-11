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
  // Todo:
  // 初始化file_handle和rid（指向第一个存放了记录的位置）

  // Initialize file_handle and set rid_ to the first valid record
  // Start from the first data page (page 0 is the file header)
  // Initialize slot_no to -1 to start scanning from the beginning
  rid_.Set(RM_FIRST_RECORD_PAGE, 0);
  // Move to the first valid record
  Next();
}

/**
 * @brief 找到文件中下一个存放了记录的位置
 */
void RmScan::Next() {
  // Todo:
  // 找到文件中下一个存放了记录的非空闲位置，用rid_来指向这个位置

  auto page_no = rid_.GetPageId();
  auto slot_no = rid_.GetSlotNum();

  // If we have not reached the end of the file
  while (page_no < file_handle_->file_hdr_.num_pages) {
    RmPageHandle page_handle = file_handle_->FetchPageHandle(page_no);
    uint32_t num_records_per_page = file_handle_->file_hdr_.num_records_per_page;

    // Move to the next slot
    slot_no = Bitmap::next_bit(true, page_handle.bitmap, num_records_per_page, slot_no);

    // Unpin the page that was pinned in 'fetch_page_handle'
    file_handle_->buffer_pool_manager_->UnpinPage(page_handle.page->GetPageId(), false);

    if (slot_no < num_records_per_page) {
      // Found a valid record in the current page
      break;
    } else {
      // Move to the next page
      page_no++;
      slot_no = 0;
    }
  }
  rid_.Set(page_no, slot_no);
}

/**
 * @brief ​ 判断是否到达文件末尾
 */
bool RmScan::IsEnd() const {
  // Todo: 修改返回值
  // return false;

  // Check if we have reached the end of the file
  return rid_.GetPageId() >= file_handle_->file_hdr_.num_pages;
}

/**
 * @brief RmScan内部存放的rid
 */
RID RmScan::GetRid() const { return rid_; }
}  // namespace easydb
