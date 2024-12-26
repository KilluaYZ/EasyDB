/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * ix_defs.h
 *
 * Identification: src/include/storage/index/ix_defs.h
 *
 *-------------------------------------------------------------------------
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

enum class Operation { FIND = 0, INSERT, DELETE };  // 三种操作：查找、插入、删除

static const bool binary_search = false;

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

inline int IxCompare(const char *a, const char *b, const std::vector<ColType> &col_types,
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
  IxPageHdr *page_hdr;        // page->data的第一部分，指针指向首地址，长度为sizeof(IxPageHdr)
  char *keys;  // page->data的第二部分，指针指向首地址，长度为file_hdr->keys_size，每个key的长度为file_hdr->col_len
  RID *rids;   // page->data的第三部分，指针指向首地址

 public:
  IxNodeHandle() = default;

  IxNodeHandle(const IxFileHdr *file_hdr_, Page *page_) : file_hdr(file_hdr_), page(page_) {
    page_hdr = reinterpret_cast<IxPageHdr *>(page->GetData());
    keys = page->GetData() + sizeof(IxPageHdr);
    rids = reinterpret_cast<RID *>(keys + file_hdr->keys_size_);
  }

  const IxPageHdr *GetPageHdr() { return page_hdr; }
  const IxFileHdr *GetFileHdr() { return file_hdr; }

  int GetSize() { return page_hdr->num_key; }

  void SetSize(int size) { page_hdr->num_key = size; }

  int GetMaxSize() { return file_hdr->btree_order_ + 1; }

  int GetMinSize() { return GetMaxSize() / 2; }

  int GetColNum() { return file_hdr->col_num_; }

  int KeyAt(int i) { return *(int *)GetKey(i); }

  /* 得到第i个孩子结点的page_no */
  page_id_t ValueAt(int i) { return GetRid(i)->GetPageId(); }

  page_id_t GetPageNo() { return page->GetPageId().page_no; }

  PageId GetPageId() { return page->GetPageId(); }

  page_id_t GetNextLeaf() { return page_hdr->next_leaf; }

  page_id_t GetPrevLeaf() { return page_hdr->prev_leaf; }

  page_id_t GetParentPageNo() { return page_hdr->parent; }

  bool IsLeafPage() { return page_hdr->is_leaf; }

  bool IsRootPage() { return GetParentPageNo() == INVALID_PAGE_ID; }

  void SetNextLeaf(page_id_t page_no) { page_hdr->next_leaf = page_no; }

  void SetPrevLeaf(page_id_t page_no) { page_hdr->prev_leaf = page_no; }

  void SetParentPageNo(page_id_t parent) { page_hdr->parent = parent; }

  char *GetKey(int key_idx) const { return keys + key_idx * file_hdr->col_tot_len_; }

  RID *GetRid(int rid_idx) const { return &rids[rid_idx]; }

  void SetKey(int key_idx, const char *key) {
    memcpy(keys + key_idx * file_hdr->col_tot_len_, key, file_hdr->col_tot_len_);
  }

  void SetRid(int rid_idx, const RID &Rid) { rids[rid_idx] = Rid; }

  int LowerBound(const char *target) const;

  int UpperBound(const char *target) const;

  void InsertPairs(int pos, const char *key, const RID *rid, int n);

  page_id_t InternalLookup(const char *key);

  bool LeafLookup(const char *key, RID **value);

  int Insert(const char *key, const RID &value);

  // 用于在结点中的指定位置插入单个键值对
  void InsertPair(int pos, const char *key, const RID &Rid) { InsertPairs(pos, key, &Rid, 1); }

  void ErasePair(int pos);

  int Remove(const char *key);

  /**
   * @brief used in internal node to Remove the last key in root node, and return the last child
   *
   * @return the last child
   */
  page_id_t RemoveAndReturnOnlyChild() {
    assert(GetSize() == 1);
    page_id_t child_page_no = ValueAt(0);
    ErasePair(0);
    assert(GetSize() == 0);
    return child_page_no;
  }

  /**
   * @brief 由parent调用，寻找child，返回child在parent中的rid_idx∈[0,page_hdr->num_key)
   * @param child
   * @return int
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
    if (file_hdr == nullptr || page_hdr == nullptr) return std::vector<std::vector<std::string>>();
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

/* B+树 */
class IxIndexHandle {
  friend class IxScan;
  friend class IxManager;

 private:
  DiskManager *disk_manager_;
  BufferPoolManager *buffer_pool_manager_;
  int fd_;  // 存储B+树的文件
  // IxFileHdr *file_hdr_;  // 存了root_page，但其初始化为2（第0页存FILE_HDR_PAGE，第1页存LEAF_HEADER_PAGE）
  std::unique_ptr<IxFileHdr> file_hdr_;
  std::mutex root_latch_;

 public:
  IxIndexHandle(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, int fd);

  int GetFd() const { return fd_; }

  // for search
  bool GetValue(const char *key, std::vector<RID> *result, Transaction *transaction);

  std::pair<IxNodeHandle *, bool> FindLeafPage(const char *key, Operation operation, Transaction *transaction,
                                               bool find_first = false);

  // for insert
  page_id_t InsertEntry(const char *key, const RID &value, Transaction *transaction);

  IxNodeHandle *Split(IxNodeHandle *node);

  void InsertIntoParent(IxNodeHandle *old_node, const char *key, IxNodeHandle *new_node, Transaction *transaction);

  // for delete
  bool DeleteEntry(const char *key, Transaction *transaction);

  bool CoalesceOrRedistribute(IxNodeHandle *node, Transaction *transaction = nullptr, bool *root_is_latched = nullptr);

  bool AdjustRoot(IxNodeHandle *old_root_node);

  void Redistribute(IxNodeHandle *neighbor_node, IxNodeHandle *node, IxNodeHandle *parent, int index);

  bool Coalesce(IxNodeHandle **neighbor_node, IxNodeHandle **node, IxNodeHandle **parent, int index,
                Transaction *transaction, bool *root_is_latched);

  bool Erase();

  Iid LowerBound(const char *key);

  Iid UpperBound(const char *key);

  Iid LeafEnd() const;

  Iid LeafBegin() const;

  IxNodeHandle *GetRoot() const;

  // for get/create node
  IxNodeHandle *FetchNode(int page_no) const;

 private:
  // 辅助函数
  void UpdateRootPageNo(page_id_t root) { file_hdr_->root_page_ = root; }

  bool IsEmpty() const { return file_hdr_->root_page_ == IX_NO_PAGE; }

  IxNodeHandle *CreateNode();

  // for maintain data structure
  void MaintainParent(IxNodeHandle *node);

  void EraseLeaf(IxNodeHandle *leaf);

  void ReleaseNodeHandle(IxNodeHandle &node);

  void MaintainChild(IxNodeHandle *node, int child_idx);

  // for index test
  RID GetRid(const Iid &iid) const;
};

}  // namespace easydb
