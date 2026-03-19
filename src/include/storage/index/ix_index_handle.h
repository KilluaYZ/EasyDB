/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * ix_index_handle.h
 *
 * Identification: src/include/storage/index/ix_index_handle.h
 *
 *-------------------------------------------------------------------------
 */

/**
 * @file ix_index_handle.h
 * @brief B+树索引句柄头文件
 * 
 * 该文件定义了B+树索引的核心类：
 * 1. IxNodeHandle：B+树节点句柄，用于操作单个节点
 * 2. IxIndexHandle：B+树索引句柄，用于操作整个B+树索引
 * 
 * 主要功能包括：
 * - 键值对的查找、插入、删除
 * - B+树节点的分裂、合并、重分配
 * - 叶子节点的遍历和范围查询
 */

#pragma once

#include <string>
#include "storage/disk/disk_manager.h"
#include "storage/index/ix_defs.h"
#include "storage/page/page.h"

#include "buffer/buffer_pool_manager.h"
#include "common/errors.h"
#include "common/rid.h"
#include "transaction/transaction.h"

namespace easydb {

/**
 * @enum Operation
 * @brief 索引操作类型枚举
 * 
 * 用于标识在查找叶子节点时将要执行的操作类型，
 * 不同的操作可能需要不同的并发控制策略
 */
enum class Operation {
  FIND = 0,    ///< 查找操作
  INSERT,      ///< 插入操作
  DELETE       ///< 删除操作
};

/** @brief 是否使用二分查找的标志（当前未使用） */
static const bool binary_search = false;

/**
 * @brief 比较两个键值（单列）- IxIndexHandle版本
 * @param a 第一个键值的指针
 * @param b 第二个键值的指针
 * @param type 列的数据类型
 * @param col_len 列的长度（字节）
 * @return 比较结果：-1表示a<b，0表示a==b，1表示a>b
 * 
 * 与ix_defs.h中的ix_compare功能相同，但使用不同的命名空间
 */
inline int IxCompare(const char *a, const char *b, ColType type, int col_len) {
  switch (type) {
    case TYPE_INT: {
      int ia = *(int *)a;
      int ib = *(int *)b;
      return (ia < ib) ? -1 : ((ia > ib) ? 1 : 0);
    }
    case TYPE_FLOAT: {
      float fa = *(float *)a;
      float fb = *(float *)b;
      return (fa < fb) ? -1 : ((fa > fb) ? 1 : 0);
    }
    case TYPE_CHAR:
    case TYPE_VARCHAR:
      return memcmp(a, b, col_len);
    default:
      throw InternalError("Unexpected data type");
  }
}

/**
 * @brief 比较两个复合键值（多列）- IxIndexHandle版本
 * @param a 第一个复合键值的指针
 * @param b 第二个复合键值的指针
 * @param col_types 各列的数据类型列表
 * @param col_lens 各列的长度列表（字节）
 * @return 比较结果：-1表示a<b，0表示a==b，1表示a>b
 * 
 * 与ix_defs.h中的ix_compare功能相同，但使用不同的命名空间
 */
inline int IxCompare(const char *a, const char *b,
                     const std::vector<ColType> &col_types,
                     const std::vector<int> &col_lens) {
  int offset = 0;
  for (size_t i = 0; i < col_types.size(); ++i) {
    int res = IxCompare(a + offset, b + offset, col_types[i], col_lens[i]);
    if (res != 0) return res;
    offset += col_lens[i];
  }
  return 0;
}

/* 管理B+树中的每个节点 */
class IxNodeHandle {
  friend class IxIndexHandle;
  friend class IxScan;

 private:
  const IxFileHdr *file_hdr;  // 节点所在文件的头部信息
  Page *page;                 // 存储节点的页面
  // page->data的第一部分，指针指向首地址，长度为sizeof(IxPageHdr)
  IxPageHdr *
      page_hdr;  
      
  char *
      keys;  
  RID *rids;  // page->data的第三部分，指针指向首地址

 public:
  IxNodeHandle() = default;

  IxNodeHandle(const IxFileHdr *file_hdr_, Page *page_)
      : file_hdr(file_hdr_), page(page_) {
    page_hdr = reinterpret_cast<IxPageHdr *>(page->GetData());
    keys = page->GetData() + sizeof(IxPageHdr);
    rids = reinterpret_cast<RID *>(keys + file_hdr->keys_size_);
  }

  // ==================== 访问器方法 ====================
  
  /** @brief 获取页面头指针 */
  const IxPageHdr *GetPageHdr() { return page_hdr; }
  
  /** @brief 获取文件头指针 */
  const IxFileHdr *GetFileHdr() { return file_hdr; }

  /** @brief 获取节点中键值对的数量 */
  int GetSize() { return page_hdr->num_key; }

  /** @brief 设置节点中键值对的数量 */
  void SetSize(int size) { page_hdr->num_key = size; }

  /** @brief 获取节点的最大容量（最多可容纳的键值对数量） */
  int GetMaxSize() { return file_hdr->btree_order_ + 1; }

  /** @brief 获取节点的最小容量（最少应保留的键值对数量，用于合并判断） */
  int GetMinSize() { return GetMaxSize() / 2; }

  /** @brief 获取索引列的数量 */
  int GetColNum() { return file_hdr->col_num_; }

  /** @brief 获取第i个键值（作为整数返回，仅适用于整数类型的键） */
  int KeyAt(int i) { return *(int *)GetKey(i); }

  /** 
   * @brief 获取第i个子节点的页号（用于内部节点）
   * @param i 子节点索引
   * @return 子节点的页号
   */
  page_id_t ValueAt(int i) { return GetRid(i)->GetPageId(); }

  /** @brief 获取当前节点的页号 */
  page_id_t GetPageNo() { return page->GetPageId().page_no; }

  /** @brief 获取当前节点的页面ID（包含文件描述符和页号） */
  PageId GetPageId() { return page->GetPageId(); }

  /** @brief 获取下一个叶子节点的页号（仅对叶子节点有效） */
  page_id_t GetNextLeaf() { return page_hdr->next_leaf; }

  /** @brief 获取上一个叶子节点的页号（仅对叶子节点有效） */
  page_id_t GetPrevLeaf() { return page_hdr->prev_leaf; }

  /** @brief 获取父节点的页号 */
  page_id_t GetParentPageNo() { return page_hdr->parent; }

  /** @brief 判断是否为叶子节点 */
  bool IsLeafPage() { return page_hdr->is_leaf; }

  /** @brief 判断是否为根节点（根节点的父节点页号为INVALID_PAGE_ID） */
  bool IsRootPage() { return GetParentPageNo() == INVALID_PAGE_ID; }

  // ==================== 修改器方法 ====================
  
  /** @brief 设置下一个叶子节点的页号 */
  void SetNextLeaf(page_id_t page_no) { page_hdr->next_leaf = page_no; }

  /** @brief 设置上一个叶子节点的页号 */
  void SetPrevLeaf(page_id_t page_no) { page_hdr->prev_leaf = page_no; }

  /** @brief 设置父节点的页号 */
  void SetParentPageNo(page_id_t parent) { page_hdr->parent = parent; }

  // ==================== 键值对访问方法 ====================
  
  /**
   * @brief 获取第key_idx个键值的指针
   * @param key_idx 键值索引
   * @return 键值的指针
   */
  char *GetKey(int key_idx) const {
    return keys + key_idx * file_hdr->col_tot_len_;
  }

  /**
   * @brief 获取第rid_idx个RID的指针
   * @param rid_idx RID索引
   * @return RID的指针
   */
  RID *GetRid(int rid_idx) const { return &rids[rid_idx]; }

  /**
   * @brief 设置第key_idx个键值
   * @param key_idx 键值索引
   * @param key 要设置的键值指针
   */
  void SetKey(int key_idx, const char *key) {
    memcpy(keys + key_idx * file_hdr->col_tot_len_, key,
           file_hdr->col_tot_len_);
  }

  /**
   * @brief 设置第rid_idx个RID
   * @param rid_idx RID索引
   * @param Rid 要设置的RID值
   */
  void SetRid(int rid_idx, const RID &Rid) { rids[rid_idx] = Rid; }

  // ==================== 查找方法 ====================
  
  /**
   * @brief 在当前节点中查找第一个大于等于target的键值索引
   * @param target 目标键值
   * @return 键值索引，范围为[0, num_key)，如果返回num_key则表示target大于所有键值
   * @note 返回的索引同时也是RID索引，可作为slot_no使用
   */
  int LowerBound(const char *target) const;

  /**
   * @brief 在当前节点中查找第一个大于target的键值索引
   * @param target 目标键值
   * @return 键值索引，范围为[0, num_key)，如果返回num_key则表示target大于等于所有键值
   */
  int UpperBound(const char *target) const;

  /**
   * @brief 在内部节点中查找目标key所在的子节点页号
   * @param key 目标键值
   * @return 子节点（子树）的页号
   */
  page_id_t InternalLookup(const char *key);

  /**
   * @brief 在叶子节点中查找目标key对应的RID
   * @param key 目标键值
   * @param[out] value 传出参数，如果找到则存储对应的RID指针
   * @return 是否找到目标key
   */
  bool LeafLookup(const char *key, RID **value);

  // ==================== 插入方法 ====================
  
  /**
   * @brief 在指定位置插入n个连续的键值对
   * @param pos 插入位置
   * @param key 键值数组的起始地址
   * @param rid RID数组的起始地址
   * @param n 要插入的键值对数量
   * @note 会更新当前节点的键数量（+=n）
   */
  void InsertPairs(int pos, const char *key, const RID *rid, int n);

  /**
   * @brief 在节点中插入单个键值对
   * @param key 要插入的键值
   * @param value 要插入的RID值
   * @return 插入后的键值对数量
   * @note 如果key重复，则不会插入，返回键值对数量不变
   */
  int Insert(const char *key, const RID &value);

  /**
   * @brief 在指定位置插入单个键值对
   * @param pos 插入位置
   * @param key 要插入的键值
   * @param Rid 要插入的RID值
   */
  void InsertPair(int pos, const char *key, const RID &Rid) {
    InsertPairs(pos, key, &Rid, 1);
  }

  // ==================== 删除方法 ====================
  
  /**
   * @brief 删除指定位置的键值对
   * @param pos 要删除的键值对位置
   */
  void ErasePair(int pos);

  /**
   * @brief 删除指定key的键值对
   * @param key 要删除的键值
   * @return 删除后的键值对数量
   * @note 如果要删除的键值对不存在，则返回键值对数量不变
   */
  int Remove(const char *key);

  /**
   * @brief 删除根节点中的最后一个键值对，并返回最后一个子节点
   * @return 最后一个子节点的页号
   * @note 仅在内部节点且大小为1时使用，用于调整根节点
   */
  page_id_t RemoveAndReturnOnlyChild() {
    assert(GetSize() == 1);
    page_id_t child_page_no = ValueAt(0);
    ErasePair(0);
    assert(GetSize() == 0);
    return child_page_no;
  }

  /**
   * @brief 查找子节点在父节点中的索引位置
   * @param child 子节点句柄
   * @return 子节点在父节点中的rid_idx，范围[0, num_key)
   * @note 由父节点调用，通过遍历RID数组找到匹配的子节点页号
   */
  int FindChild(IxNodeHandle *child) {
    int rid_idx;
    for (rid_idx = 0; rid_idx < page_hdr->num_key; rid_idx++) {
      if (GetRid(rid_idx)->GetPageId() == child->GetPageNo()) {
        break;
      }
    }
    assert(rid_idx < page_hdr->num_key);
    return rid_idx;
  }

  std::vector<std::vector<std::string>> GetDeserializeKeys() {
    if (file_hdr == nullptr || page_hdr == nullptr)
      return std::vector<std::vector<std::string>>();
    std::vector<std::vector<std::string>> result;
    int offset = 0;
    int col_types_size = file_hdr->col_types_.size();
    for (int i = 0; i < col_types_size; i++) {
      std::vector<std::string> tmp_res;
      for (int j = 0; j < page_hdr->num_key; j++) {
        auto cur_type = file_hdr->col_types_[i];
        auto cur_lens = file_hdr->col_lens_[i];
        std::string tmp_str;
        if (cur_type == TYPE_INT) {
          tmp_str = std::to_string(*(int *)(keys + offset));
        } else if (cur_type == TYPE_LONG) {
          tmp_str = std::to_string(*(long long *)(keys + offset));
        } else if (cur_type == TYPE_FLOAT) {
          tmp_str = std::to_string(*(float *)(keys + offset));
        } else if (cur_type == TYPE_DOUBLE) {
          tmp_str = std::to_string(*(double *)(keys + offset));
        } else if (cur_type == TYPE_CHAR) {
          tmp_str = std::string(keys + offset);
        } else if (cur_type == TYPE_VARCHAR) {
          tmp_str = std::string(keys + offset);
        }
        tmp_res.push_back(tmp_str);
        offset += cur_lens;
      }
      result.push_back(tmp_res);
    }
    return result;
  }
};

/**
 * @class IxIndexHandle
 * @brief B+树索引句柄类
 * 
 * 用于管理整个B+树索引，提供对索引的增删改查操作。
 * 
 * 主要功能：
 * - 键值对的查找、插入、删除
 * - B+树结构的维护（分裂、合并、重分配）
 * - 叶子节点的遍历和范围查询
 * - 节点页面的分配和释放
 * 
 * 文件结构：
 * - 第0页：文件头（IxFileHdr）
 * - 第1页：叶子链表头（虚拟节点）
 * - 第2页：根节点（初始状态）
 * - 第3页及以后：其他节点
 */
class IxIndexHandle {
  friend class IxScan;
  friend class IxManager;

 private:
  /** @brief 磁盘管理器指针，用于文件I/O操作 */
  DiskManager *disk_manager_;
  
  /** @brief 缓冲池管理器指针，用于页面缓存管理 */
  BufferPoolManager *buffer_pool_manager_;
  
  /** @brief 存储B+树的文件描述符 */
  int fd_;
  
  /** 
   * @brief 文件头指针（使用unique_ptr管理内存）
   * @note 文件头存储了root_page等信息
   * @note root_page初始化为2（第0页存FILE_HDR_PAGE，第1页存LEAF_HEADER_PAGE）
   */
  std::unique_ptr<IxFileHdr> file_hdr_;
  
  /** @brief 根节点互斥锁，用于并发控制 */
  std::mutex root_latch_;

 public:
  /**
   * @brief 构造函数
   * @param disk_manager 磁盘管理器指针
   * @param buffer_pool_manager 缓冲池管理器指针
   * @param fd 文件描述符
   * 
   * 从磁盘读取文件头并初始化索引句柄
   */
  IxIndexHandle(DiskManager *disk_manager,
                BufferPoolManager *buffer_pool_manager, int fd);

  /** @brief 获取文件描述符 */
  int GetFd() const { return fd_; }

  // ==================== 查找操作 ====================
  
  /**
   * @brief 查找指定键值对应的所有RID
   * @param key 要查找的键值
   * @param[out] result 用于存放结果的容器
   * @param transaction 事务指针
   * @return 是否找到目标键值
   */
  bool GetValue(const char *key, std::vector<RID> *result,
                Transaction *transaction);

  /**
   * @brief 查找包含指定键值的叶子节点
   * @param key 要查找的键值
   * @param operation 操作类型（FIND/INSERT/DELETE）
   * @param transaction 事务指针
   * @param find_first 是否查找第一个叶子节点（默认false）
   * @return [叶子节点句柄, 根节点是否加锁]的pair
   * @note 使用完FindLeafPage后必须unlatch和unpin叶子节点，否则下次latch会阻塞
   */
  std::pair<IxNodeHandle *, bool> FindLeafPage(const char *key,
                                               Operation operation,
                                               Transaction *transaction,
                                               bool find_first = false);

  // ==================== 插入操作 ====================
  
  /**
   * @brief 将键值对插入到B+树中
   * @param key 要插入的键值
   * @param value 要插入的RID值
   * @param transaction 事务指针
   * @return 插入到的叶子节点的页号，如果插入失败（重复key）则返回-1
   */
  page_id_t InsertEntry(const char *key, const RID &value,
                        Transaction *transaction);

  /**
   * @brief 分裂节点，在节点右边生成一个新节点
   * @param node 需要分裂的节点
   * @return 分裂得到的新节点句柄
   * @note 执行完毕后，原node和新node都需要在函数外面进行unpin
   */
  IxNodeHandle *Split(IxNodeHandle *node);

  /**
   * @brief 分裂后，将新节点的第一个key插入到父节点
   * @param old_node 原节点（分裂前的节点）
   * @param key 要插入到父节点的key（新节点的第一个key）
   * @param new_node 新节点（分裂后生成的右兄弟节点）
   * @param transaction 事务指针
   * @note 如果插入后父节点也需要分裂，则递归调用
   * @note 执行完毕后，new node和old node都需要在函数外面进行unpin
   */
  void InsertIntoParent(IxNodeHandle *old_node, const char *key,
                        IxNodeHandle *new_node, Transaction *transaction);

  // ==================== 删除操作 ====================
  
  /**
   * @brief 删除B+树中含有指定key的键值对
   * @param key 要删除的key值
   * @param transaction 事务指针
   * @return 是否删除成功
   */
  bool DeleteEntry(const char *key, Transaction *transaction);

  /**
   * @brief 处理合并和重分配的逻辑，用于删除键值对后调用
   * @param node 执行完删除操作的节点
   * @param transaction 事务指针
   * @param root_is_latched 传出参数：根节点是否上锁
   * @return 是否需要删除节点
   * @note 如果兄弟节点和当前节点的键值对数量之和 >= 2 * 最小容量，则重分配
   * @note 否则合并两个节点
   * @note 如果返回false，函数会unpin和delete节点（防止内存泄漏）
   */
  bool CoalesceOrRedistribute(IxNodeHandle *node,
                              Transaction *transaction = nullptr,
                              bool *root_is_latched = nullptr);

  /**
   * @brief 调整根节点（当根节点被删除键值对后调用）
   * @param old_root_node 原根节点
   * @return 根节点是否需要被删除
   * @note 根节点的size可以小于最小容量，此方法仅在CoalesceOrRedistribute中调用
   */
  bool AdjustRoot(IxNodeHandle *old_root_node);

  /**
   * @brief 重新分配node和兄弟节点的键值对
   * @param neighbor_node 兄弟节点
   * @param node 当前节点（刚被删除过key的节点）
   * @param parent 父节点
   * @param index node在parent中的rid_idx
   * @note 如果index=0，neighbor是node的后继节点；如果index>0，neighbor是node的前驱节点
   */
  void Redistribute(IxNodeHandle *neighbor_node, IxNodeHandle *node,
                    IxNodeHandle *parent, int index);

  /**
   * @brief 合并node和其前驱节点neighbor_node
   * @param neighbor_node 兄弟节点（node的前驱节点）
   * @param node 当前节点（需要被删除的节点）
   * @param parent 父节点
   * @param index node在parent中的rid_idx
   * @param transaction 事务指针
   * @param root_is_latched 根节点是否上锁
   * @return 父节点是否需要被删除
   * @note 假设neighbor_node是node的左兄弟节点（neighbor -> node）
   * @note 无论返回值如何，都会删除neighbor_node；如果返回false，也会删除parent_node
   */
  bool Coalesce(IxNodeHandle **neighbor_node, IxNodeHandle **node,
                IxNodeHandle **parent, int index, Transaction *transaction,
                bool *root_is_latched);

  // ==================== 范围查询和遍历 ====================
  
  /**
   * @brief 查找第一个大于等于key的Iid
   * @param key 目标键值
   * @return Iid对象，表示找到的位置
   */
  Iid LowerBound(const char *key);

  /**
   * @brief 查找第一个大于key的Iid
   * @param key 目标键值
   * @return Iid对象，表示找到的位置
   */
  Iid UpperBound(const char *key);

  /**
   * @brief 获取最后一个叶子节点的最后一个键值对之后的位置
   * @return Iid对象，可用作IxScan的结束位置
   */
  Iid LeafEnd() const;

  /**
   * @brief 获取第一个叶子节点的第一个键值对的位置
   * @return Iid对象，可用作IxScan的起始位置
   */
  Iid LeafBegin() const;

  // ==================== 节点管理 ====================
  
  /**
   * @brief 获取根节点句柄
   * @return 根节点句柄指针
   * @note 记得unpin和delete返回的节点句柄
   */
  IxNodeHandle *GetRoot() const;

  /**
   * @brief 获取指定页号的节点句柄
   * @param page_no 页面号
   * @return 节点句柄指针
   * @note 会pin页面，记得unpin和delete返回的节点句柄
   */
  IxNodeHandle *FetchNode(int page_no) const;

 private:
  // ==================== 辅助函数 ====================
  
  /**
   * @brief 更新根节点页号
   * @param root 新的根节点页号
   */
  void UpdateRootPageNo(page_id_t root) { file_hdr_->root_page_ = root; }

  /**
   * @brief 判断索引是否为空
   * @return 如果根节点页号为IX_NO_PAGE则返回true
   */
  bool IsEmpty() const { return file_hdr_->root_page_ == IX_NO_PAGE; }

  /**
   * @brief 创建一个新节点
   * @return 新节点句柄指针
   * @note 会pin页面，记得unpin和delete返回的节点句柄
   * @note 对于Index的处理：删除某个页面后，认为该被删除的页面是free_page
   * @note first_free_page实际上就是最新被删除的页面，初始为IX_NO_PAGE
   */
  IxNodeHandle *CreateNode();

  // ==================== 数据结构维护 ====================
  
  /**
   * @brief 从node开始向上更新其父节点的第一个key，直到根节点
   * @param node 起始节点
   * @note 当节点的第一个key发生变化时调用，用于维护B+树的正确性
   */
  void MaintainParent(IxNodeHandle *node);

  /**
   * @brief 删除叶子节点前调用，更新前驱和后继节点的指针
   * @param leaf 要删除的叶子节点
   */
  void EraseLeaf(IxNodeHandle *leaf);

  /**
   * @brief 释放节点句柄，更新file_hdr_.num_pages
   * @param node 要释放的节点句柄
   */
  void ReleaseNodeHandle(IxNodeHandle &node);

  /**
   * @brief 删除缓冲池中的索引页面（调试/测试辅助）
   * @return 是否删除成功
   */
  bool Erase();

  /**
   * @brief 将node的第child_idx个子节点的父节点设置为node
   * @param node 父节点句柄
   * @param child_idx 子节点索引
   */
  void MaintainChild(IxNodeHandle *node, int child_idx);

  // ==================== 测试辅助函数 ====================
  
  /**
   * @brief 将Iid转换为RID
   * @param iid 索引内部标识符
   * @return 对应的RID
   * @note iid和rid存的不是同一个东西：
   * @note - rid是上层传过来的记录位置（Record ID）
   * @note - iid是索引内部生成的索引槽位置
   */
  RID GetRid(const Iid &iid) const;
};

}  // namespace easydb
