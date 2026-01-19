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
 * @description: 初始化记录扫描器，设置文件句柄和记录ID
 * @param file_handle 记录文件句柄指针
 */
RmScan::RmScan(const RmFileHandle *file_handle) : file_handle_(file_handle) {
  // 初始化file_handle并将rid_设置为第一个有效记录
  // 从第一个数据页开始（第0页是文件头）
  // 将slot_no初始化为0，从头开始扫描
  rid_.Set(RM_FIRST_RECORD_PAGE, 0);
}

/**
 * @description: 找到文件中下一个存放了记录的位置
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
 * @description: 判断是否到达文件末尾
 * @return 如果到达文件末尾返回true，否则返回false
 */
bool RmScan::IsEnd() const {
  // 检查是否已经到达文件末尾
  return rid_.GetPageId() >= file_handle_->file_hdr_.num_pages;
}

/**
 * @description: 获取当前扫描位置的记录ID
 * @return 当前记录ID
 */
RID RmScan::GetRid() const { return rid_; }
}  // namespace easydb
