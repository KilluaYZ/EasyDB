/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * rm_file_handle.cpp
 *
 * Identification: src/record/rm_file_handle.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "record/rm_file_handle.h"

namespace easydb {

/**
 * @description: 获取当前表中记录号为rid的记录
 * @param {RID&} rid 记录号，指定记录的位置
 * @param {Context*} context
 * @return {unique_ptr<RmRecord>} rid对应的记录对象指针
 */
//  std::unique_ptr<RmRecord> RmFileHandle::get_record(const RID &rid, Context *context)
auto RmFileHandle::GetRecord(const RID &rid) -> std::unique_ptr<RmRecord> {
  // Todo:
  // 1. 获取指定记录所在的page handle
  // 2. 初始化一个指向RmRecord的指针（赋值其内部的data和size）
  // return nullptr;

  //   // lock manager
  //   if (context != nullptr) {
  //     context->lock_mgr_->lock_shared_on_record(context->txn_, rid, fd_);
  //   }

  // 1. Fetch the page handle for the page that contains the record
  RmPageHandle page_handle = FetchPageHandle(rid.GetPageId());

  // Check if the slot contains a valid record
  if (!Bitmap::is_set(page_handle.bitmap, rid.GetSlotNum())) {
    // Unpin the page before throwing an exception
    buffer_pool_manager_->UnpinPage({fd_, rid.GetPageId()}, false);
    throw RecordNotFoundError(rid.GetPageId(), rid.GetSlotNum());
    // throw InternalError("RmFileHandle::get_record: Record not found.");
  }

  // 2. Initialize a unique pointer to RmRecord
  char *record_data = page_handle.get_slot(rid.GetSlotNum());
  auto record = std::make_unique<RmRecord>(file_hdr_.record_size, record_data);

  // Unpin the page
  buffer_pool_manager_->UnpinPage({fd_, rid.GetPageId()}, false);

  return record;
}

/**
 * @description: 在当前表中插入一条记录，不指定插入位置
 * @param {char*} buf 要插入的记录的数据
 * @param {Context*} context
 * @return {RID} 插入的记录的记录号（位置）
 */
// RID RmFileHandle::insert_record(char *buf, Context *context) {
RID RmFileHandle::InsertRecord(char *buf) {
  // Todo:
  // 1. 获取当前未满的page handle
  // 2. 在page handle中找到空闲slot位置
  // 3. 将buf复制到空闲slot位置
  // 4. 更新page_handle.page_hdr中的数据结构
  // 注意考虑插入一条记录后页面已满的情况，需要更新file_hdr_.first_free_page_no
  // return RID{-1, -1};

  // 1. Fetch the current first free page handle
  RmPageHandle page_handle = CreatePageHandle();
  int page_no = page_handle.page->GetPageId().page_no;

  // 2. Find a free slot in the page handle
  int slot_no = Bitmap::first_bit(false, page_handle.bitmap, file_hdr_.num_records_per_page);
  if (slot_no == file_hdr_.num_records_per_page) {
    // This should not happen as we should have a free slot
    throw InternalError("RmFileHandle::insert_record Error: No free slot found in a supposedly free page");
  }

  // // lock manager
  // if (context != nullptr) {
  //   auto rid = RID{page_no, slot_no};
  //   context->lock_mgr_->lock_exclusive_on_record(context->txn_, rid, fd_);
  // }

  // 3. Copy the data to the free slot
  char *slot = page_handle.get_slot(slot_no);
  memcpy(slot, buf, file_hdr_.record_size);
  // Update the bitmap to mark the slot as used
  Bitmap::set(page_handle.bitmap, slot_no);

  // 4. Update the page header's num_records and next_free_page_no if necessary
  page_handle.page_hdr->num_records++;
  // Check if the page is full
  if (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page) {
    // Page is full, update file header to point to the next free page
    file_hdr_.first_free_page_no = page_handle.page_hdr->next_free_page_no;
    // Write the updated file header back to disk
    disk_manager_->WritePage(fd_, RM_FILE_HDR_PAGE, (char *)&file_hdr_, sizeof(file_hdr_));
  }

  // Unpin the page that was pinned in create_page_handle
  buffer_pool_manager_->UnpinPage(page_handle.page->GetPageId(), true);

  return RID{page_no, static_cast<slot_id_t>(slot_no)};
}

/**
 * @description: 在当前表中的指定位置插入一条记录
 * @param {RID&} rid 要插入记录的位置
 * @param {char*} buf 要插入记录的数据
 * @note 该函数主要用于事务的回滚和系统故障恢复
 */
// void RmFileHandle::insert_record(const RID &rid, char *buf) {
void RmFileHandle::InsertRecord(const RID &rid, char *buf) {
  int page_no = rid.GetPageId();
  int slot_no = rid.GetSlotNum();
  // 1. Fetch the page handle for the specified page number
  RmPageHandle page_handle = FetchPageHandle(page_no);

  // 2. Ensure that the specified slot is free
  if (Bitmap::is_set(page_handle.bitmap, slot_no)) {
    throw InternalError("RmFileHandle::insert_record Error: Slot is already occupied");
  }

  // 3. Copy the record data (buf) to the specified slot
  char *slot_data = page_handle.get_slot(slot_no);
  memcpy(slot_data, buf, file_hdr_.record_size);
  // Update the bitmap to mark the slot as used
  Bitmap::set(page_handle.bitmap, slot_no);

  // 4. Update the page header to reflect the addition of a new record
  page_handle.page_hdr->num_records++;
  // Check if the page is now full
  if (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page) {
    // Page is now full, update the file header's first_free_page_no
    file_hdr_.first_free_page_no = page_handle.page_hdr->next_free_page_no;
    // Write the updated file header back to disk
    disk_manager_->WritePage(fd_, RM_FILE_HDR_PAGE, (char *)&file_hdr_, sizeof(file_hdr_));
  }

  // Unpin the page that was pinned in FetchPageHandle
  buffer_pool_manager_->UnpinPage(page_handle.page->GetPageId(), true);
}

/**
 * @description: 删除记录文件中记录号为rid的记录
 * @param {RID&} rid 要删除的记录的记录号（位置）
 * @param {Context*} context
 */
// void RmFileHandle::delete_record(const RID &rid, Context *context) {
void RmFileHandle::DeleteRecord(const RID &rid) {
  // Todo:
  // 1. 获取指定记录所在的page handle
  // 2. 更新page_handle.page_hdr中的数据结构
  // 注意考虑删除一条记录后页面未满的情况，需要调用release_page_handle()

  //   // lock manager
  //   if (context != nullptr) {
  //     context->lock_mgr_->lock_exclusive_on_record(context->txn_, rid, fd_);
  //   }

  // 1. Fetch the page handle for the specified page number
  RmPageHandle page_handle = FetchPageHandle(rid.GetSlotNum());
  // Ensure that the specified slot contains a record
  if (!Bitmap::is_set(page_handle.bitmap, rid.GetSlotNum())) {
    throw InternalError("RmFileHandle::delete_record: Slot is already empty");
  }
  // Update the bitmap to mark the slot as free
  Bitmap::reset(page_handle.bitmap, rid.GetSlotNum());

  // 2. Update the page header to reflect the deletion of the record
  page_handle.page_hdr->num_records--;
  // Check if the page changed from full to non-full
  if (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page - 1) {
    ReleasePageHandle(page_handle);
  }

  // Unpin the page that was pinned in FetchPageHandle
  buffer_pool_manager_->UnpinPage(page_handle.page->GetPageId(), true);
}

/**
 * @description: 更新记录文件中记录号为rid的记录
 * @param {RID&} rid 要更新的记录的记录号（位置）
 * @param {char*} buf 新记录的数据
 * @param {Context*} context
 */
// void RmFileHandle::update_record(const RID &rid, char *buf, Context *context) {
void RmFileHandle::UpdateRecord(const RID &rid, char *buf) {
  // Todo:
  // 1. 获取指定记录所在的page handle
  // 2. 更新记录

  //   // lock manager
  //   if (context != nullptr) {
  //     context->lock_mgr_->lock_exclusive_on_record(context->txn_, rid, fd_);
  //   }

  // 1. Fetch the page handle for the specified page number
  RmPageHandle page_handle = FetchPageHandle(rid.GetPageId());
  // Ensure that the specified slot is occupied
  if (!Bitmap::is_set(page_handle.bitmap, rid.GetSlotNum())) {
    throw RecordNotFoundError(rid.GetPageId(), rid.GetSlotNum());
    // throw InternalError("RmFileHandle::update_record Error: RecordNotFoundError");
  }

  // 2. Update the slot data with the new record data from buf
  char *slot_data = page_handle.get_slot(rid.GetSlotNum());
  memcpy(slot_data, buf, file_hdr_.record_size);

  // Unpin the page that was pinned in FetchPageHandle
  buffer_pool_manager_->UnpinPage(page_handle.page->GetPageId(), true);
}

/**
 * 以下函数为辅助函数，仅提供参考，可以选择完成如下函数，也可以删除如下函数，在单元测试中不涉及如下函数接口的直接调用
 */

/**
 * @description: 获取指定页面的页面句柄
 * @param {int} page_no 页面号
 * @return {RmPageHandle} 指定页面的句柄
 * @note 该函数调用fetch_page进行pin操作，调用者需要调用UnpinPage进行unpin操作
 */
// RmPageHandle RmFileHandle::FetchPageHandle(int page_no) const {
RmPageHandle RmFileHandle::FetchPageHandle(page_id_t page_no) const {
  // Todo:
  // 使用缓冲池获取指定页面，并生成page_handle返回给上层
  // if page_no is invalid, throw PageNotExistError exception
  // return RmPageHandle(&file_hdr_, nullptr);

  // Ensure the page_no is within valid range
  if (page_no < 0 || page_no >= file_hdr_.num_pages) {
    throw PageNotExistError("", page_no);
    // throw InternalError("RmFileHandle::FetchPageHandle Error: Invalid page number.");
  }

  // Fetch the page from the buffer pool
  PageId page_id{fd_, page_no};
  Page *page = buffer_pool_manager_->FetchPage(page_id);

  // If the page is not found, throw an error
  if (page == nullptr) {
    throw InternalError("RmFileHandle::FetchPageHandle Error: Failed to fetch page");
  }

  // Return the page handle
  return RmPageHandle(&file_hdr_, page);
}

/**
 * @description: 创建一个新的page handle
 * @return {RmPageHandle} 新的PageHandle
 * @note 该函数调用new_page进行pin操作，调用者需要调用UnpinPage进行unpin操作；
 *       初始化page_hdr中的next_free_page_no(-1)和num_records(0);
 *       更新file_hdr_中的num_pages和first_free_page_no;
 *       写回文件头到磁盘
 */
RmPageHandle RmFileHandle::CreateNewPageHandle() {
  // Todo:
  // 1.使用缓冲池来创建一个新page
  // 2.更新page handle中的相关信息
  // 3.更新file_hdr_
  // return RmPageHandle(&file_hdr_, nullptr);

  // 1. Use the buffer pool to create a new page
  PageId new_page_id;
  new_page_id.fd = fd_;
  Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);

  if (new_page == nullptr) {
    throw InternalError("RmFileHandle::CreateNewPageHandle Error: Failed to create new page");
  }

  // 2. Initialize the new page handle
  RmPageHandle new_page_handle(&file_hdr_, new_page);
  // Initialize the new page header
  new_page_handle.page_hdr->next_free_page_no = RM_NO_PAGE;
  new_page_handle.page_hdr->num_records = 0;
  // Initialize the bitmap
  Bitmap::init(new_page_handle.bitmap, file_hdr_.bitmap_size);

  // 3. Update the file header
  file_hdr_.num_pages++;
  file_hdr_.first_free_page_no = new_page_id.page_no;

  // Write the updated file header back to the disk
  disk_manager_->WritePage(fd_, RM_FILE_HDR_PAGE, (char *)&file_hdr_, sizeof(file_hdr_));

  return new_page_handle;
}

// /**
//  * Sets the log sequence number (LSN) for a specific page in the file.
//  *
//  * @param page_no The page number of the page to set the LSN for.
//  * @param lsn The log sequence number to set for the page.
//  * @throws InternalError If the page cannot be fetched from the buffer pool.
//  */
// void RmFileHandle::set_page_lsn(int page_no, lsn_t lsn) {
//   // Fetch the page from the buffer pool
//   PageId page_id{fd_, page_no};
//   Page *page = buffer_pool_manager_->fetch_page(page_id);

//   // If the page is not found, throw an error
//   if (page == nullptr) {
//     throw InternalError("RmFileHandle::set_page_lsn: Failed to fetch page");
//   }
//   // Set the page's LSN
//   page->set_page_lsn(lsn);
//   // Unpin the page that was pinned in fetch_page
//   buffer_pool_manager_->UnpinPage(page_id, true);
// }

/**
 * @brief 创建或获取一个空闲的page handle
 *
 * @return RmPageHandle 返回生成的空闲page handle
 * @note pin the page, remember to unpin it outside!
 */
RmPageHandle RmFileHandle::CreatePageHandle() {
  // Todo:
  // 1. 判断file_hdr_中是否还有空闲页
  //     1.1 没有空闲页：使用缓冲池来创建一个新page；可直接调用create_new_page_handle()
  //     1.2 有空闲页：直接获取第一个空闲页
  // 2. 生成page handle并返回给上层
  // return RmPageHandle(&file_hdr_, nullptr);

  int page_no = file_hdr_.first_free_page_no;
  // 1. Check if there are free pages in file_hdr_
  if (page_no == RM_NO_PAGE) {
    // 1.1 No free pages: create a new page handle using the existing function
    return CreateNewPageHandle();
  }

  // 1.2 There are free pages: fetch the first free page
  RmPageHandle page_handle = FetchPageHandle(page_no);

  // 2. Return the page handle
  return page_handle;
}

/**
 * @description: 当一个页面从没有空闲空间的状态变为有空闲空间状态时，更新文件头和页头中空闲页面相关的元数据
 * @note 该函数更新
         文件头中的first_free_page_no为该空闲页面，
         页头中的next_free_page_no为原文件头中的first_free_page_no，
         写回文件头到磁盘
 */
void RmFileHandle::ReleasePageHandle(RmPageHandle &page_handle) {
  // Todo:
  // 当page从已满变成未满，考虑如何更新：
  // 1. page_handle.page_hdr->next_free_page_no
  // 2. file_hdr_.first_free_page_no

  // If the page becomes non-full, update the next_free_page_no and first_free_page_no
  page_handle.page_hdr->next_free_page_no = file_hdr_.first_free_page_no;
  file_hdr_.first_free_page_no = page_handle.page->GetPageId().page_no;

  // Write the updated file header back to disk
  disk_manager_->WritePage(fd_, RM_FILE_HDR_PAGE, (char *)&file_hdr_, sizeof(file_hdr_));
}

}  // namespace easydb
