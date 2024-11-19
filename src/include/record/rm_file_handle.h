/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * rm_file_handle.h
 *
 * Identification: src/include/record/rm_file_handle.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once
#include <assert.h>

#include <memory>

#include "bitmap.h"
#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "common/rid.h"
#include "rm_defs.h"
#include "storage/disk/disk_manager.h"
#include "storage/page/page.h"

namespace easydb {

/**
 * Slotted page format:
 *  ---------------------------------------------------------
 *  | HEADER | ... FREE SPACE ... | ... INSERTED TUPLES ... |
 *  ---------------------------------------------------------
 *                                ^
 *                                free space pointer
 *
 *  Header format (size in bytes):
 *  ----------------------------------------------------------------------------
 *  | NextPageId (4)| NumTuples(2) | NumDeletedTuples(2) |
 *  ----------------------------------------------------------------------------
 *  ----------------------------------------------------------------
 *  | Tuple_1 offset+size (4) | Tuple_2 offset+size (4) | ... |
 *  ----------------------------------------------------------------
 *
 * Tuple format:
 * | meta | data |
 */

/* 表数据文件中每个页面的页头，记录每个页面的元信息 */
struct RmPageHdr {
  page_id_t next_page_id;  // 当前页面满了之后，下一个包含空闲空间的页面号（初始化为-1）
  uint16_t num_records;    // 当前页面中当前已经存储的记录个数（初始化为0）
  uint16_t num_deleted_records;  // 当前页面中已经删除的记录个数（初始化为0）
  // num_records 只增不减，删除记录则增 num_deleted_records，标记相应的slot为已删除

  void Init() {
    next_page_id = RM_NO_PAGE;
    num_records = 0;
    num_deleted_records = 0;
  }
};

static constexpr uint64_t TABLE_PAGE_HEADER_SIZE = Page::SIZE_PAGE_HEADER + sizeof(RmPageHdr);

/* 对表数据文件中的页面进行封装 */
class RmPageHandle {
  friend class RmFileHandle;
  friend class RmScan;

 public:
  RmPageHandle(const RmFileHdr *fhdr_, Page *page_) : file_hdr(fhdr_), page(page_) {
    page_hdr_ = reinterpret_cast<RmPageHdr *>(page->GetData() + page->OFFSET_PAGE_HDR);
    tuple_info_ = reinterpret_cast<TupleInfo *>(page->GetData() + sizeof(RmPageHdr) + page->OFFSET_PAGE_HDR);
    page_start_ = page->GetData();
  }

  // // 返回指定slot_no的slot存储收地址
  // char *get_slot(int slot_no) const {
  //   // return slots + slot_no * file_hdr->record_size;  // slots的首地址 + slot个数 *
  //   每个slot的大小(每个record的大小)
  // }

  /** @return number of tuples in this page */
  auto GetNumTuples() const -> uint32_t { return page_hdr_->num_records; }

  /** @return the page ID of the next table page */
  auto GetNextPageId() const -> page_id_t { return page_hdr_->next_page_id; }

  /** Set the page id of the next page in the table. */
  void SetNextPageId(page_id_t next_page_id) { page_hdr_->next_page_id = next_page_id; }

  /** Get the next offset to insert, return nullopt if this tuple cannot fit in this page */
  auto GetNextTupleOffset(const TupleMeta &meta, const Tuple &tuple) const -> std::optional<uint16_t>;

  /**
   * Insert a tuple into the table.
   * @param tuple tuple to insert
   * @return true if the insert is successful (i.e. there is enough space)
   */
  auto InsertTuple(const TupleMeta &meta, const Tuple &tuple) -> std::optional<uint16_t>;

  /**
   * Update a tuple.
   */
  void UpdateTupleMeta(const TupleMeta &meta, const RID &rid);

  /**
   * Read a tuple from a table.
   */
  auto GetTuple(const RID &rid) const -> std::pair<TupleMeta, Tuple>;

  /**
   * Read a tuple meta from a table.
   */
  auto GetTupleMeta(const RID &rid) const -> TupleMeta;

  /**
   * Update a tuple in place.
   */
  void UpdateTupleInPlaceUnsafe(const TupleMeta &meta, const Tuple &tuple, RID rid);

  /**
   * Check if a tuple is deleted.
   */
  auto IsTupleDeleted(const RID &rid) -> bool;

 private:
  const RmFileHdr *file_hdr;  // 当前页面所在文件的文件头指针
  Page *page;                 // 页面的实际数据，包括页面存储的数据、元信息等
  // 元组信息，包括slot号(offset)、大小(size)、元数据
  using TupleInfo = std::tuple<uint16_t, uint16_t, TupleMeta>;
  RmPageHdr *page_hdr_;  // page->data的第一部分，存储页面元信息，指针指向首地址，长度为sizeof(RmPageHdr)
  TupleInfo *tuple_info_;  // page->data的第二部分，存储页面的元组信息，长度为num_records * sizeof(TupleInfo)
  // char *bitmap;  // page->data的第二部分，存储页面的bitmap，指针指向首地址，长度为file_hdr->bitmap_size
  // char *slots;  // page->data的第三部分，存储表的记录，指针指向首地址，每个slot的长度为file_hdr->record_size
  char *page_start_;

  static constexpr size_t TUPLE_INFO_SIZE = 24;
  static_assert(sizeof(TupleInfo) == TUPLE_INFO_SIZE);
};

/* 每个RmFileHandle对应一个表的数据文件，里面有多个page，每个page的数据封装在RmPageHandle中 */
class RmFileHandle {
  friend class RmScan;
  friend class RmManager;

 private:
  DiskManager *disk_manager_;
  BufferPoolManager *buffer_pool_manager_;
  int fd_;              // 打开文件后产生的文件句柄
  RmFileHdr file_hdr_;  // 文件头，维护当前表文件的元数据

 public:
  RmFileHandle(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, int fd)
      : disk_manager_(disk_manager), buffer_pool_manager_(buffer_pool_manager), fd_(fd) {
    // 注意：这里从磁盘中读出文件描述符为fd的文件的file_hdr，读到内存中
    // 这里实际就是初始化file_hdr，只不过是从磁盘中读出进行初始化
    // init file_hdr_
    disk_manager_->ReadPage(fd, RM_FILE_HDR_PAGE, (char *)&file_hdr_, sizeof(file_hdr_));
    // disk_manager管理的fd对应的文件中，设置从file_hdr_.num_pages开始分配page_no
    disk_manager_->SetFd2Pageno(fd, file_hdr_.num_pages);
  }

  // RmFileHdr get_file_hdr() { return file_hdr_; }
  RmFileHdr GetFileHdr() { return file_hdr_; }
  int GetFd() { return fd_; }

  /**
   * Insert a tuple into the table. If the tuple is too large (>= page_size), return std::nullopt.
   * @param meta tuple meta
   * @param tuple tuple to insert
   * @return rid of the inserted tuple
   */
  auto InsertTuple(const TupleMeta &meta, const Tuple &tuple) -> std::optional<RID>;

  /**
   * Delete a tuple from the table.
   * @param rid rid of the tuple to delete
   * @return true if the delete is successful
   */
  auto DeleteTuple(RID rid) -> bool;

  /**
   * Update a tuple in place.
   * @param meta new tuple meta
   * @param tuple  new tuple
   * @param rid the rid of the tuple to be updated
   * @param check the check to run before actually update.
   */
  auto UpdateTupleInPlace(const TupleMeta &meta, const Tuple &tuple, RID rid,
                          std::function<bool(const TupleMeta &meta, const Tuple &table, RID rid)> &&check = nullptr)
      -> bool;

  /**
   * Update the meta of a tuple.
   * @param meta new tuple meta
   * @param rid the rid of the inserted tuple
   */
  void UpdateTupleMeta(const TupleMeta &meta, RID rid);

  /**
   * Read a tuple from the table.
   * @param rid rid of the tuple to read
   * @return the meta and tuple
   */
  auto GetTuple(RID rid) -> std::pair<TupleMeta, Tuple>;

  /**
   * Read a tuple from the table.
   * @param rid rid of the tuple to read
   * @return the tuple
   */
  auto GetTupleValue(const RID &rid) -> std::unique_ptr<Tuple> ;

  /**
   * Read a tuple meta from the table. Note: if you want to get tuple and meta together, use `GetTuple` instead
   * to ensure atomicity.
   * @param rid rid of the tuple to read
   * @return the meta
   */
  auto GetTupleMeta(RID rid) -> TupleMeta;

  // /* 判断指定位置上是否已经存在一条记录，通过Bitmap来判断 */
  // bool IsRecord(const RID  &rid) const {
  //   RmPageHandle page_handle = FetchPageHandle(rid.GetPageId());
  //   return Bitmap::is_set(page_handle.bitmap, rid.GetSlotNum());  // page的slot_no位置上是否有record
  // }

  //   std::unique_ptr<RmRecord> get_record(const RID  &rid, Context *context) const;
  auto GetRecord(const RID &rid) -> std::unique_ptr<RmRecord>;

  /**
   * Read a tuple from the table and generates a key tuple given schemas and attributes.
   * @param schema the schema of the table
   * @param key_schema the schema of the key
   * @param key_attrs the attributes of the key in the table schema
   * @param rid the rid of the tuple to read
   */
  auto GetKeyTuple(const Schema &schema, const Schema &key_schema, const std::vector<uint32_t> &key_attrs,
                   const RID &rid) -> Tuple;

  //   RID insert_record(char *buf, Context *context);
  RID InsertRecord(char *buf);

  //   void insert_record(const RID  &rid, char *buf);
  void InsertRecord(const RID &rid, char *buf);

  //   void delete_record(const RID  &rid, Context *context);
  void DeleteRecord(const RID &rid);

  //   void update_record(const RID  &rid, char *buf, Context *context);
  void UpdateRecord(const RID &rid, char *buf);

  // RmPageHandle create_new_page_handle();
  RmPageHandle CreateNewPageHandle();

  // RmPageHandle fetch_page_handle(int page_no) const;
  RmPageHandle FetchPageHandle(page_id_t page_no) const;

  //   void set_page_lsn(int page_no, lsn_t lsn);
  void SetPageLSN(page_id_t page_id_, lsn_t lsn);

 private:
  // RmPageHandle create_page_handle();
  RmPageHandle CreatePageHandle();

  // void release_page_handle(RmPageHandle &page_handle);
  void ReleasePageHandle(RmPageHandle &page_handle);
};
}  // namespace easydb
