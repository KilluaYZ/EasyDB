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
#include "common/context.h"
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

/**
 * @brief 表数据文件中每个页面的页头结构体，记录每个页面的元信息
 * 
 * RmPageHdr 存储页面级别的元数据，位于页面的头部。
 */
struct RmPageHdr {
  /**
   * @brief 当前页面满了之后，下一个包含空闲空间的页面号
   * @note 初始化为RM_NO_PAGE（-1），表示这是最后一个页面
   *       用于维护页面的链表结构
   */
  page_id_t next_page_id;
  
  /**
   * @brief 当前页面中已经存储的记录个数
   * @note 初始化为0，只增不减（即使记录被删除也不减少）
   */
  uint16_t num_records;
  
  /**
   * @brief 当前页面中已经删除的记录个数
   * @note 初始化为0，删除记录时增加此计数
   *       删除记录时，num_records不变，但num_deleted_records增加，并标记相应的slot为已删除
   */
  uint16_t num_deleted_records;

  /**
   * @brief 初始化页面头
   * @note 将所有字段设置为默认值
   */
  void Init() {
    next_page_id = RM_NO_PAGE;
    num_records = 0;
    num_deleted_records = 0;
  }
};

static constexpr uint64_t TABLE_PAGE_HEADER_SIZE = Page::SIZE_PAGE_HEADER + sizeof(RmPageHdr);

/**
 * @brief 对表数据文件中的页面进行封装的类
 * 
 * RmPageHandle 提供了对表页面数据的访问接口，包括：
 * - 元组的插入、删除、更新、读取
 * - 页面元数据的访问和修改
 * - 槽目录的管理
 */
class RmPageHandle {
  friend class RmFileHandle;
  friend class RmScan;

 public:
  /**
   * @brief 构造函数
   * @param fhdr_ 文件头指针
   * @param page_ 页面指针
   * @note 初始化页面头指针、元组信息指针和页面起始地址
   */
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

  /**
   * @brief 获取页面中元组的数量
   * @return 当前页面中存储的元组数量
   */
  auto GetNumTuples() const -> uint32_t { return page_hdr_->num_records; }

  /**
   * @brief 获取下一个表页面的页面ID
   * @return 下一个表页面的页面ID（如果这是最后一个页面，则为RM_NO_PAGE）
   */
  auto GetNextPageId() const -> page_id_t { return page_hdr_->next_page_id; }

  /**
   * @brief 设置下一个表页面的页面ID
   * @param next_page_id 下一个表页面的页面ID
   */
  void SetNextPageId(page_id_t next_page_id) { page_hdr_->next_page_id = next_page_id; }

  /**
   * @brief 获取下一个要插入元组的偏移量
   * @param meta 元组的元数据
   * @param tuple 要插入的元组
   * @return 如果元组可以放入此页面，返回偏移量；否则返回nullopt
   */
  auto GetNextTupleOffset(const TupleMeta &meta, const Tuple &tuple) const -> std::optional<uint16_t>;

  /**
   * @brief 向表中插入一个元组
   * @param meta 元组的元数据
   * @param tuple 要插入的元组
   * @return 如果插入成功（有足够空间），返回槽号；否则返回nullopt
   */
  auto InsertTuple(const TupleMeta &meta, const Tuple &tuple) -> std::optional<uint16_t>;

  /**
   * @brief 更新元组的元数据
   * @param meta 新的元数据
   * @param rid 要更新的元组的记录ID
   */
  void UpdateTupleMeta(const TupleMeta &meta, const RID &rid);

  /**
   * @brief 从表中读取一个元组
   * @param rid 要读取的元组的记录ID
   * @return 元组的元数据和数据的pair
   */
  auto GetTuple(const RID &rid) const -> std::pair<TupleMeta, Tuple>;

  /**
   * @brief 从表中读取元组的元数据
   * @param rid 要读取的元组的记录ID
   * @return 元组的元数据
   */
  auto GetTupleMeta(const RID &rid) const -> TupleMeta;

  /**
   * @brief 原地更新元组（不安全版本）
   * @param meta 新的元数据
   * @param tuple 新的元组数据
   * @param rid 要更新的元组的记录ID
   * @note "不安全"意味着不检查新元组的大小是否与旧元组匹配
   */
  void UpdateTupleInPlaceUnsafe(const TupleMeta &meta, const Tuple &tuple, RID rid);

  /**
   * @brief 检查元组是否已被删除
   * @param rid 要检查的元组的记录ID
   * @return true 如果元组已被删除，false 否则
   */
  auto IsTupleDeleted(const RID &rid) -> bool;

 private:
  /**
   * @brief 当前页面所在文件的文件头指针
   */
  const RmFileHdr *file_hdr;
  
  /**
   * @brief 页面的实际数据指针
   * @note 包括页面存储的数据、元信息等
   */
  Page *page;
  
  /**
   * @brief 元组信息类型定义
   * @note 包括槽号(offset)、大小(size)、元数据
   */
  using TupleInfo = std::tuple<uint16_t, uint16_t, TupleMeta>;
  
  /**
   * @brief 页面头指针
   * @note page->data的第一部分，存储页面元信息，指针指向首地址，长度为sizeof(RmPageHdr)
   */
  RmPageHdr *page_hdr_;
  
  /**
   * @brief 元组信息数组指针
   * @note page->data的第二部分，存储页面的元组信息，长度为num_records * sizeof(TupleInfo)
   */
  TupleInfo *tuple_info_;
  
  // char *bitmap;  // page->data的第二部分，存储页面的bitmap，指针指向首地址，长度为file_hdr->bitmap_size
  // char *slots;  // page->data的第三部分，存储表的记录，指针指向首地址，每个slot的长度为file_hdr->record_size
  
  /**
   * @brief 页面起始地址指针
   */
  char *page_start_;

  /**
   * @brief 每个元组信息的大小（字节）
   */
  static constexpr size_t TUPLE_INFO_SIZE = 24;
  static_assert(sizeof(TupleInfo) == TUPLE_INFO_SIZE);
};

/**
 * @brief 表数据文件句柄类
 * 
 * 每个RmFileHandle对应一个表的数据文件，文件中有多个page，
 * 每个page的数据封装在RmPageHandle中。
 * 
 * RmFileHandle 提供了对表数据文件的高级操作接口，包括：
 * - 元组的插入、删除、更新、读取
 * - 页面的创建和获取
 * - 文件元数据的管理
 */
class RmFileHandle {
  friend class RmScan;
  friend class RmManager;

 private:
  /** @brief 磁盘管理器指针 */
  DiskManager *disk_manager_;
  
  /** @brief 缓冲池管理器指针 */
  BufferPoolManager *buffer_pool_manager_;
  
  /**
   * @brief 打开文件后产生的文件句柄（文件描述符）
   */
  int fd_;
  
  /**
   * @brief 文件头，维护当前表文件的元数据
   * @note 包含文件的页面数量、第一个空闲页面号等信息
   */
  RmFileHdr file_hdr_;

 public:
  /**
   * @brief 构造函数
   * @param disk_manager 磁盘管理器指针
   * @param buffer_pool_manager 缓冲池管理器指针
   * @param fd 文件描述符
   * 
   * 初始化过程：
   * 1. 从磁盘读取文件头（第0页）到内存
   * 2. 设置磁盘管理器中该文件的页面号分配起点
   */
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
  
  /**
   * @brief 获取文件头
   * @return 文件头的副本
   */
  RmFileHdr GetFileHdr() { return file_hdr_; }
  
  /**
   * @brief 获取文件描述符
   * @return 文件描述符
   */
  int GetFd() { return fd_; }

  /**
   * @brief 向表中插入一个元组
   * @param meta 元组的元数据
   * @param tuple 要插入的元组
   * @param context 事务上下文指针
   * @return 如果元组太大（>= page_size），返回nullopt；否则返回插入的元组的RID
   * @note 自动分配新的RID，如果当前页面已满，会创建新页面
   */
  auto InsertTuple(const TupleMeta &meta, const Tuple &tuple, Context *context) -> std::optional<RID>;

  /**
   * @brief 向表中插入一个元组（用于回滚）
   * @param rid 要插入的元组的RID（指定位置）
   * @param meta 元组的元数据
   * @param tuple 要插入的元组
   * @param context 事务上下文指针
   * @return true 如果插入成功，false 否则
   * @note 用于事务回滚时恢复被删除的元组
   */
  auto InsertTuple(RID rid, const TupleMeta &meta, const Tuple &tuple, Context *context) -> bool;

  /**
   * @brief 从表中删除一个元组
   * @param rid 要删除的元组的RID
   * @param context 事务上下文指针
   * @return true 如果删除成功，false 否则
   * @note 标记元组为已删除，不立即释放空间
   */
  auto DeleteTuple(RID rid, Context *context) -> bool;

  /**
   * @brief 原地更新元组
   * @param meta 新的元数据
   * @param tuple 新的元组数据
   * @param rid 要更新的元组的RID
   * @param context 事务上下文指针
   * @param check 更新前执行的检查函数（可选）
   * @return true 如果更新成功，false 否则
   * @note 要求新元组的大小与旧元组匹配
   */
  auto UpdateTupleInPlace(const TupleMeta &meta, const Tuple &tuple, RID rid, Context *context,
                          std::function<bool(const TupleMeta &meta, const Tuple &table, RID rid)> &&check = nullptr)
      -> bool;

  /**
   * @brief 更新元组的元数据
   * @param meta 新的元数据
   * @param rid 要更新的元组的RID
   * @param context 事务上下文指针
   */
  void UpdateTupleMeta(const TupleMeta &meta, RID rid, Context *context);

  /**
   * @brief 从表中读取一个元组
   * @param rid 要读取的元组的RID
   * @param context 事务上下文指针
   * @return 元组的元数据和数据的pair
   */
  auto GetTuple(RID rid, Context *context) -> std::pair<TupleMeta, Tuple>;

  /**
   * @brief 从表中读取一个元组的值
   * @param rid 要读取的元组的RID
   * @param context 事务上下文指针
   * @return 元组的智能指针
   */
  auto GetTupleValue(const RID &rid, Context *context) -> std::unique_ptr<Tuple>;

  /**
   * @brief 从表中读取元组的元数据
   * @param rid 要读取的元组的RID
   * @param context 事务上下文指针
   * @return 元组的元数据
   * @note 如果需要同时获取元组和元数据，使用GetTuple以确保原子性
   */
  auto GetTupleMeta(RID rid, Context *context) -> TupleMeta;

  // /* 判断指定位置上是否已经存在一条记录，通过Bitmap来判断 */
  // bool IsRecord(const RID  &rid) const {
  //   RmPageHandle page_handle = FetchPageHandle(rid.GetPageId());
  //   return Bitmap::is_set(page_handle.bitmap, rid.GetSlotNum());  // page的slot_no位置上是否有record
  // }

  //   std::unique_ptr<RmRecord> get_record(const RID  &rid, Context *context) const;
  // auto GetRecord(const RID &rid) -> std::unique_ptr<RmRecord>;

  /**
   * @brief 从表中读取元组并生成键元组
   * @param schema 表的模式
   * @param key_schema 键的模式
   * @param key_attrs 键属性在表模式中的索引列表
   * @param rid 要读取的元组的RID
   * @param context 事务上下文指针
   * @return 键元组
   * @note 用于索引操作，从完整元组中提取键列
   */
  auto GetKeyTuple(const Schema &schema, const Schema &key_schema, const std::vector<uint32_t> &key_attrs,
                   const RID &rid, Context *context) -> Tuple;

  // //   RID insert_record(char *buf, Context *context);
  // RID InsertRecord(char *buf);

  // //   void insert_record(const RID  &rid, char *buf);
  // void InsertRecord(const RID &rid, char *buf);

  // //   void delete_record(const RID  &rid, Context *context);
  // void DeleteRecord(const RID &rid);

  // //   void update_record(const RID  &rid, char *buf, Context *context);
  // void UpdateRecord(const RID &rid, char *buf);

  /**
   * @brief 创建新的页面句柄
   * @return 新页面的句柄
   * @note 分配新页面并初始化页面头
   */
  RmPageHandle CreateNewPageHandle();

  /**
   * @brief 获取指定页面号的页面句柄
   * @param page_no 页面号
   * @return 页面句柄
   * @note 从缓冲池获取页面，如果不在缓冲池中则从磁盘加载
   */
  RmPageHandle FetchPageHandle(page_id_t page_no) const;

  /**
   * @brief 设置页面的LSN
   * @param page_id_ 页面ID
   * @param lsn 日志序列号
   */
  void SetPageLSN(page_id_t page_id_, lsn_t lsn);

 private:
  /**
   * @brief 创建页面句柄（内部方法）
   * @return 页面句柄
   * @note 分配新页面但不更新文件头
   */
  RmPageHandle CreatePageHandle();

  /**
   * @brief 释放页面句柄（内部方法）
   * @param page_handle 要释放的页面句柄
   * @note 取消固定页面，允许页面被替换
   */
  void ReleasePageHandle(RmPageHandle &page_handle);
};
}  // namespace easydb
