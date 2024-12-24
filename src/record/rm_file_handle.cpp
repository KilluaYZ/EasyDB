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
#include "common/exception.h"
#include "common/macros.h"
#include "common/rid.h"

namespace easydb {

auto RmPageHandle::GetNextTupleOffset(const TupleMeta &meta, const Tuple &tuple) const -> std::optional<uint16_t> {
  size_t slot_end_offset;
  if (page_hdr_->num_records > 0) {
    auto &[offset, size, meta] = tuple_info_[page_hdr_->num_records - 1];
    slot_end_offset = offset;
  } else {
    slot_end_offset = PAGE_SIZE;
  }
  auto tuple_offset = slot_end_offset - tuple.GetLength();
  auto offset_size = TABLE_PAGE_HEADER_SIZE + TUPLE_INFO_SIZE * (page_hdr_->num_records + 1);
  // Note that we donnot use (tuple_offset < offset_size) because slot_end_offset may < tuple.GetLength()
  if (slot_end_offset < offset_size + tuple.GetLength()) {
    return std::nullopt;
  }
  return tuple_offset;
}

auto RmPageHandle::InsertTuple(const TupleMeta &meta, const Tuple &tuple) -> std::optional<uint16_t> {
  auto tuple_offset = GetNextTupleOffset(meta, tuple);
  if (tuple_offset == std::nullopt) {
    return std::nullopt;
  }
  auto tuple_id = page_hdr_->num_records;
  tuple_info_[tuple_id] = std::make_tuple(*tuple_offset, tuple.GetLength(), meta);
  page_hdr_->num_records++;
  memcpy(page_start_ + *tuple_offset, tuple.data_.data(), tuple.GetLength());
  return tuple_id;
}

void RmPageHandle::UpdateTupleMeta(const TupleMeta &meta, const RID &rid) {
  auto tuple_id = rid.GetSlotNum();
  if (tuple_id >= page_hdr_->num_records) {
    throw easydb::Exception("Tuple ID out of range");
  }
  auto &[offset, size, old_meta] = tuple_info_[tuple_id];
  if (!old_meta.is_deleted_ && meta.is_deleted_) {
    page_hdr_->num_deleted_records++;
  }
  tuple_info_[tuple_id] = std::make_tuple(offset, size, meta);
}

auto RmPageHandle::GetTuple(const RID &rid) const -> std::pair<TupleMeta, Tuple> {
  auto tuple_id = rid.GetSlotNum();
  if (tuple_id >= page_hdr_->num_records) {
    throw easydb::Exception("Tuple ID out of range");
  }
  auto &[offset, size, meta] = tuple_info_[tuple_id];
  Tuple tuple;
  tuple.data_.resize(size);
  memmove(tuple.data_.data(), page_start_ + offset, size);
  tuple.rid_ = rid;
  return std::make_pair(meta, std::move(tuple));
}

auto RmPageHandle::GetTupleMeta(const RID &rid) const -> TupleMeta {
  auto tuple_id = rid.GetSlotNum();
  if (tuple_id >= page_hdr_->num_records) {
    throw easydb::Exception("Tuple ID out of range");
  }
  auto &[_1, _2, meta] = tuple_info_[tuple_id];
  return meta;
}

void RmPageHandle::UpdateTupleInPlaceUnsafe(const TupleMeta &meta, const Tuple &tuple, RID rid) {
  auto tuple_id = rid.GetSlotNum();
  if (tuple_id >= page_hdr_->num_records) {
    throw easydb::Exception("Tuple ID out of range");
  }
  auto &[offset, size, old_meta] = tuple_info_[tuple_id];
  // if (size != tuple.GetLength()) {
  //   throw easydb::Exception("Tuple size mismatch");
  // }
  // If the tuple is larger than the old one, we throw an exception
  if (size < tuple.GetLength()) {
    throw easydb::Exception("Tuple size mismatch");
  }
  if (!old_meta.is_deleted_ && meta.is_deleted_) {
    page_hdr_->num_deleted_records++;
  }
  tuple_info_[tuple_id] = std::make_tuple(offset, size, meta);
  memcpy(page_start_ + offset, tuple.data_.data(), tuple.GetLength());
}

auto RmPageHandle::IsTupleDeleted(const RID &rid) -> bool {
  auto meta = GetTupleMeta(rid);
  return meta.is_deleted_;
}

auto RmFileHandle::InsertTuple(const TupleMeta &meta, const Tuple &tuple, Context *context) -> std::optional<RID> {
  // 1. Fetch the current first free page handle
  RmPageHandle page_handle = CreatePageHandle();
  int page_no = page_handle.page->GetPageId().page_no;
  std::optional<uint16_t> tuple_offset;

  // 2. Find a free slot in the page handle
  while (true) {
    tuple_offset = page_handle.GetNextTupleOffset(meta, tuple);
    if (tuple_offset != std::nullopt) {
      break;
    }

    // if there's no tuple in the page, and we can't insert the tuple, then this tuple is too large.
    EASYDB_ENSURE(page_handle.GetNumTuples() != 0, "tuple is too large, cannot insert");

    auto new_page_handle = CreateNewPageHandle();
    page_handle.SetNextPageId(new_page_handle.page->GetPageId().page_no);
    buffer_pool_manager_->UnpinPage(page_handle.page->GetPageId(), false);

    page_handle = std::move(new_page_handle);
    page_no = page_handle.page->GetPageId().page_no;
  }

  // 3. Insert the tuple to the free slot
  // auto slot_no = *page_handle.InsertTuple(meta, tuple);
  auto slot_no = page_handle.page_hdr_->num_records;
  auto rid = RID(page_no, slot_no);
  // lock manager
  if (context != nullptr) {
    context->lock_mgr_->lock_exclusive_on_record(context->txn_, rid, fd_);
  }

  page_handle.tuple_info_[slot_no] = std::make_tuple(*tuple_offset, tuple.GetLength(), meta);
  page_handle.page_hdr_->num_records++;
  memcpy(page_handle.page_start_ + *tuple_offset, tuple.data_.data(), tuple.GetLength());

  // Unpin the page that was pinned in create_page_handle
  buffer_pool_manager_->UnpinPage(page_handle.page->GetPageId(), true);

  return rid;
}

auto RmFileHandle::InsertTuple(RID rid, const TupleMeta &meta, const Tuple &tuple, Context *context) -> bool {
  // lock manager
  if (context != nullptr) {
    context->lock_mgr_->lock_exclusive_on_record(context->txn_, rid, fd_);
  }
  RmPageHandle page_handle = FetchPageHandle(rid.GetPageId());
  auto [old_meta, old_tup] = page_handle.GetTuple(rid);
  if (old_meta.is_deleted_) {
    old_meta.is_deleted_ = false;
    page_handle.UpdateTupleMeta(old_meta, rid);
  } else {
    throw Exception("RmFileHandle::InsertTuple(Rollback) Error: Tuple already exists");
  }
  buffer_pool_manager_->UnpinPage(page_handle.page->GetPageId(), true);
  return true;
}

auto RmFileHandle::DeleteTuple(RID rid, Context *context) -> bool {
  // lock manager
  if (context != nullptr) {
    context->lock_mgr_->lock_exclusive_on_record(context->txn_, rid, fd_);
  }

  RmPageHandle page_handle = FetchPageHandle(rid.GetPageId());
  auto [meta, tuple] = page_handle.GetTuple(rid);
  if (meta.is_deleted_) {
    throw InternalError("RmFileHandle::DeleteTuple Error: Tuple already deleted");
  }
  meta.is_deleted_ = true;
  page_handle.UpdateTupleMeta(meta, rid);
  buffer_pool_manager_->UnpinPage(page_handle.page->GetPageId(), true);
  return true;
}

auto RmFileHandle::UpdateTupleInPlace(const TupleMeta &meta, const Tuple &tuple, RID rid, Context *context,
                                      std::function<bool(const TupleMeta &meta, const Tuple &table, RID rid)> &&check)
    -> bool {
  // lock manager
  if (context != nullptr) {
    context->lock_mgr_->lock_exclusive_on_record(context->txn_, rid, fd_);
  }
  RmPageHandle page_handle = FetchPageHandle(rid.GetPageId());
  auto [old_meta, old_tup] = page_handle.GetTuple(rid);
  if (check == nullptr || check(old_meta, old_tup, rid)) {
    page_handle.UpdateTupleInPlaceUnsafe(meta, tuple, rid);
    buffer_pool_manager_->UnpinPage(page_handle.page->GetPageId(), true);
    return true;
  }
  buffer_pool_manager_->UnpinPage(page_handle.page->GetPageId(), false);
  return false;
}

void RmFileHandle::UpdateTupleMeta(const TupleMeta &meta, RID rid, Context *context) {
  // lock manager
  if (context != nullptr) {
    context->lock_mgr_->lock_exclusive_on_record(context->txn_, rid, fd_);
  }
  RmPageHandle page_handle = FetchPageHandle(rid.GetPageId());
  page_handle.UpdateTupleMeta(meta, rid);
  buffer_pool_manager_->UnpinPage(page_handle.page->GetPageId(), true);
}

auto RmFileHandle::GetTuple(RID rid, Context *context) -> std::pair<TupleMeta, Tuple> {
  // lock manager
  if (context != nullptr) {
    context->lock_mgr_->lock_shared_on_record(context->txn_, rid, fd_);
  }
  RmPageHandle page_handle = FetchPageHandle(rid.GetPageId());
  auto [meta, tuple] = page_handle.GetTuple(rid);
  buffer_pool_manager_->UnpinPage(page_handle.page->GetPageId(), false);
  tuple.rid_ = rid;
  return std::make_pair(meta, std::move(tuple));
}

auto RmFileHandle::GetTupleMeta(RID rid, Context *context) -> TupleMeta {
  // lock manager
  if (context != nullptr) {
    context->lock_mgr_->lock_shared_on_record(context->txn_, rid, fd_);
  }
  RmPageHandle page_handle = FetchPageHandle(rid.GetPageId());
  TupleMeta meat = page_handle.GetTupleMeta(rid);
  buffer_pool_manager_->UnpinPage(page_handle.page->GetPageId(), false);
  return meat;
}

/**
 * @description: 获取当前表中记录号为rid的记录
 * @param {RID&} rid 记录号，指定记录的位置
 * @param {Context*} context
 * @return {unique_ptr<RmRecord>} rid对应的记录对象指针
 */
//  std::unique_ptr<RmRecord> RmFileHandle::get_record(const RID &rid, Context *context)
// auto RmFileHandle::GetRecord(const RID &rid) -> std::unique_ptr<RmRecord> {
//   // Todo:
//   // 1. 获取指定记录所在的page handle
//   // 2. 初始化一个指向RmRecord的指针（赋值其内部的data和size）
//   // return nullptr;
//   //   // lock manager
//   //   if (context != nullptr) {
//   //     context->lock_mgr_->lock_shared_on_record(context->txn_, rid, fd_);
//   //   }
//   // 1. Fetch the page handle for the page that contains the record
//   RmPageHandle page_handle = FetchPageHandle(rid.GetPageId());
//   // 2. Initialize a unique pointer to RmRecord
//   auto [meta, tuple] = page_handle.GetTuple(rid);
//   tuple.rid_ = rid;
//   // return std::make_pair(meta, std::move(tuple));
//   auto record = std::make_unique<RmRecord>(tuple.GetLength(), tuple.data_.data());
//   // Unpin the page
//   buffer_pool_manager_->UnpinPage({fd_, rid.GetPageId()}, false);
//   return record;
// }

/**
 * @description: 获取当前表中记录号为rid的记录
 * @param {RID&} rid 记录号，指定记录的位置
 * @param {Context*} context
 * @return {unique_ptr<Tuple>} rid对应的记录对象指针
 */
auto RmFileHandle::GetTupleValue(const RID &rid, Context *context) -> std::unique_ptr<Tuple> {
  // lock manager
  if (context != nullptr) {
    context->lock_mgr_->lock_shared_on_record(context->txn_, rid, fd_);
  }

  // 1. Fetch the page handle for the page that contains the record
  RmPageHandle page_handle = FetchPageHandle(rid.GetPageId());

  // 2. Initialize a unique pointer to Tuple
  auto [meta, tuple] = page_handle.GetTuple(rid);

  // Unpin the page
  buffer_pool_manager_->UnpinPage({fd_, rid.GetPageId()}, false);

  return std::make_unique<Tuple>(tuple);
}

auto RmFileHandle::GetKeyTuple(const Schema &schema, const Schema &key_schema, const std::vector<uint32_t> &key_attrs,
                               const RID &rid, Context *context) -> Tuple {
  // lock manager
  if (context != nullptr) {
    context->lock_mgr_->lock_shared_on_record(context->txn_, rid, fd_);
  }

  // 1. Fetch the page handle for the page that contains the record
  RmPageHandle page_handle = FetchPageHandle(rid.GetPageId());

  // 2. Initialize a unique pointer to RmRecord
  auto [meta, tuple] = page_handle.GetTuple(rid);
  auto key_tuple = tuple.KeyFromTuple(schema, key_schema, key_attrs);

  // Unpin the page
  buffer_pool_manager_->UnpinPage({fd_, rid.GetPageId()}, false);
  return key_tuple;
}

// /**
//  * @description: 在当前表中插入一条记录，不指定插入位置
//  * @param {char*} buf 要插入的记录的数据
//  * @param {Context*} context
//  * @return {RID} 插入的记录的记录号（位置）
//  */
// // RID RmFileHandle::insert_record(char *buf, Context *context) {
// RID RmFileHandle::InsertRecord(char *buf) {
//   // Todo:
//   // 1. 获取当前未满的page handle
//   // 2. 在page handle中找到空闲slot位置
//   // 3. 将buf复制到空闲slot位置
//   // 4. 更新page_handle.page_hdr中的数据结构
//   // 注意考虑插入一条记录后页面已满的情况，需要更新file_hdr_.first_free_page_no
//   // return RID{-1, -1};
//   throw InternalError("RmFileHandle::insert_record removed, use InsertTuple instead.");
// }

// /**
//  * @description: 在当前表中的指定位置插入一条记录
//  * @param {RID&} rid 要插入记录的位置
//  * @param {char*} buf 要插入记录的数据
//  * @note 该函数主要用于事务的回滚和系统故障恢复
//  */
// void RmFileHandle::insert_record(const RID &rid, char *buf) {
// void RmFileHandle::InsertRecord(const RID &rid, char *buf) {
//   throw InternalError("RmFileHandle::insert_record not implemented");
// }

/**
 * @description: 删除记录文件中记录号为rid的记录
 * @param {RID&} rid 要删除的记录的记录号（位置）
 * @param {Context*} context
 */
// void RmFileHandle::delete_record(const RID &rid, Context *context) {
// void RmFileHandle::DeleteRecord(const RID &rid) {
//   // Todo:
//   // 1. 获取指定记录所在的page handle
//   // 2. 更新page_handle.page_hdr中的数据结构
//   // 注意考虑删除一条记录后页面未满的情况，需要调用release_page_handle()
//   throw InternalError("RmFileHandle::delete_record removed, use DeleteTuple instead.");
// }

/**
 * @description: 更新记录文件中记录号为rid的记录
 * @param {RID&} rid 要更新的记录的记录号（位置）
 * @param {char*} buf 新记录的数据
 * @param {Context*} context
 */
// void RmFileHandle::update_record(const RID &rid, char *buf, Context *context) {
// void RmFileHandle::UpdateRecord(const RID &rid, char *buf) {
//   // Todo:
//   // 1. 获取指定记录所在的page handle
//   // 2. 更新记录
//   throw InternalError("RmFileHandle::update_record removed, use UpdateTupleInPlace instead.");
// }

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
  new_page_handle.page_hdr_->Init();

  // 3. Update the file header
  file_hdr_.num_pages++;
  file_hdr_.first_free_page_no = new_page_id.page_no;

  // Write the updated file header back to the disk
  disk_manager_->WritePage(fd_, RM_FILE_HDR_PAGE, (char *)&file_hdr_, sizeof(file_hdr_));

  return new_page_handle;
}

/**
 * Sets the log sequence number (LSN) for a specific page in the file.
 *
 * @param page_no The page number of the page to set the LSN for.
 * @param lsn The log sequence number to set for the page.
 * @throws InternalError If the page cannot be fetched from the buffer pool.
 */
void RmFileHandle::SetPageLSN(page_id_t page_id_, lsn_t lsn) {
  // Fetch the page from the buffer pool
  PageId page_id{fd_, page_id_};
  Page *page = buffer_pool_manager_->FetchPage(page_id);

  // If the page is not found, throw an error
  if (page == nullptr) {
    throw InternalError("RmFileHandle::set_page_lsn: Failed to fetch page");
  }
  // Set the page's LSN
  page->SetLSN(lsn);
  // Unpin the page that was pinned in fetch_page
  buffer_pool_manager_->UnpinPage(page_id, true);
}

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
  // page_handle.page_hdr->next_free_page_no = file_hdr_.first_free_page_no;
  file_hdr_.first_free_page_no = page_handle.page->GetPageId().page_no;

  // Write the updated file header back to disk
  disk_manager_->WritePage(fd_, RM_FILE_HDR_PAGE, (char *)&file_hdr_, sizeof(file_hdr_));
}

}  // namespace easydb
