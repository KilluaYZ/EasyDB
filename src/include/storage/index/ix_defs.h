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

#include <cassert>
#include <cstring>
#include <vector>

#include "common/config.h"
#include "common/errors.h"
#include "defs.h"

namespace easydb {

constexpr int IX_NO_PAGE = -1;
constexpr int IX_FILE_HDR_PAGE = 0;
constexpr int IX_LEAF_HEADER_PAGE = 1;
constexpr int IX_INIT_ROOT_PAGE = 2;
constexpr int IX_INIT_NUM_PAGES = 3;
constexpr int IX_MAX_COL_LEN = 512;

constexpr int IX_INIT_DIRECTORY_PAGE = 1;
constexpr int IX_INIT_BUCKET_0_PAGE = 2;
constexpr int IX_INIT_BUCKET_1_PAGE = 3;
constexpr int IX_INIT_HASH_NUM_PAGES = 4;
constexpr int IX_INIT_HASH_FIRST_FREE_PAGES = 4;

inline int ix_compare(const char *a, const char *b, ColType type, int col_len) {
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

inline int ix_compare(const char *a, const char *b, const std::vector<ColType> &col_types,
                      const std::vector<int> &col_lens) {
  int offset = 0;
  for (size_t i = 0; i < col_types.size(); ++i) {
    int res = ix_compare(a + offset, b + offset, col_types[i], col_lens[i]);
    if (res != 0) return res;
    offset += col_lens[i];
  }
  return 0;
}

class IxFileHdr {
 public:
  page_id_t first_free_page_no_;    // 文件中第一个空闲的磁盘页面的页面号
  int num_pages_;                   // 磁盘文件中页面的数量
  page_id_t root_page_;             // B+树根节点对应的页面号
  int col_num_;                     // 索引包含的字段数量
  std::vector<ColType> col_types_;  // 字段的类型
  std::vector<int> col_lens_;       // 字段的长度
  int col_tot_len_;                 // 索引包含的字段的总长度
  int btree_order_;                 // # children per page 每个结点最多可插入的键值对数量
  int keys_size_;                   // keys_size = (btree_order + 1) * col_tot_len
  // first_leaf初始化之后没有进行修改，只不过是在测试文件中遍历叶子结点的时候用了
  page_id_t first_leaf_;  // 首叶节点对应的页号，在上层IxManager的open函数进行初始化，初始化为root page_no
  page_id_t last_leaf_;  // 尾叶节点对应的页号
  int tot_len_;          // 记录结构体的整体长度(IxFileHdr的size)

  IxFileHdr() { tot_len_ = col_num_ = 0; }

  IxFileHdr(page_id_t first_free_page_no, int num_pages, page_id_t root_page, int col_num, int col_tot_len,
            int btree_order, int keys_size, page_id_t first_leaf, page_id_t last_leaf)
      : first_free_page_no_(first_free_page_no),
        num_pages_(num_pages),
        root_page_(root_page),
        col_num_(col_num),
        col_tot_len_(col_tot_len),
        btree_order_(btree_order),
        keys_size_(keys_size),
        first_leaf_(first_leaf),
        last_leaf_(last_leaf) {
    tot_len_ = 0;
  }

  void UpdateTotLen() {
    tot_len_ = 0;
    tot_len_ += sizeof(page_id_t) * 4 + sizeof(int) * 6;
    tot_len_ += sizeof(ColType) * col_num_ + sizeof(int) * col_num_;
  }

  void Serialize(char *dest) {
    int offset = 0;
    memcpy(dest + offset, &tot_len_, sizeof(int));
    offset += sizeof(int);
    memcpy(dest + offset, &first_free_page_no_, sizeof(page_id_t));
    offset += sizeof(page_id_t);
    memcpy(dest + offset, &num_pages_, sizeof(int));
    offset += sizeof(int);
    memcpy(dest + offset, &root_page_, sizeof(page_id_t));
    offset += sizeof(page_id_t);
    memcpy(dest + offset, &col_num_, sizeof(int));
    offset += sizeof(int);
    for (int i = 0; i < col_num_; ++i) {
      memcpy(dest + offset, &col_types_[i], sizeof(ColType));
      offset += sizeof(ColType);
    }
    for (int i = 0; i < col_num_; ++i) {
      memcpy(dest + offset, &col_lens_[i], sizeof(int));
      offset += sizeof(int);
    }
    memcpy(dest + offset, &col_tot_len_, sizeof(int));
    offset += sizeof(int);
    memcpy(dest + offset, &btree_order_, sizeof(int));
    offset += sizeof(int);
    memcpy(dest + offset, &keys_size_, sizeof(int));
    offset += sizeof(int);
    memcpy(dest + offset, &first_leaf_, sizeof(page_id_t));
    offset += sizeof(page_id_t);
    memcpy(dest + offset, &last_leaf_, sizeof(page_id_t));
    offset += sizeof(page_id_t);
    assert(offset == tot_len_);
  }

  void Deserialize(char *src) {
    int offset = 0;
    tot_len_ = *reinterpret_cast<const int *>(src + offset);
    offset += sizeof(int);
    first_free_page_no_ = *reinterpret_cast<const page_id_t *>(src + offset);
    offset += sizeof(int);
    num_pages_ = *reinterpret_cast<const int *>(src + offset);
    offset += sizeof(int);
    root_page_ = *reinterpret_cast<const page_id_t *>(src + offset);
    offset += sizeof(page_id_t);
    col_num_ = *reinterpret_cast<const int *>(src + offset);
    offset += sizeof(int);
    std::cout << col_num_ << "\n";
    for (int i = 0; i < col_num_; ++i) {
      // col_types_[i] = *reinterpret_cast<const ColType*>(src + offset);
      ColType type = *reinterpret_cast<const ColType *>(src + offset);
      offset += sizeof(ColType);
      col_types_.push_back(type);
    }
    for (int i = 0; i < col_num_; ++i) {
      // col_lens_[i] = *reinterpret_cast<const int*>(src + offset);
      int len = *reinterpret_cast<const int *>(src + offset);
      offset += sizeof(int);
      col_lens_.push_back(len);
    }
    col_tot_len_ = *reinterpret_cast<const int *>(src + offset);
    offset += sizeof(int);
    btree_order_ = *reinterpret_cast<const int *>(src + offset);
    offset += sizeof(int);
    keys_size_ = *reinterpret_cast<const int *>(src + offset);
    offset += sizeof(int);
    first_leaf_ = *reinterpret_cast<const page_id_t *>(src + offset);
    offset += sizeof(page_id_t);
    last_leaf_ = *reinterpret_cast<const page_id_t *>(src + offset);
    offset += sizeof(page_id_t);
    assert(offset == tot_len_);
  }
};

class ExtendibleHashIxFileHdr {
 public:
  page_id_t first_free_page_no_;    // 文件中第一个空闲的磁盘页面的页面号
  int num_pages_;                   // 磁盘文件中页面的数量
  page_id_t directory_page_;        // hash目录对应的页面号
  int col_num_;                     // 索引包含的字段数量
  std::vector<ColType> col_types_;  // 字段的类型
  std::vector<int> col_lens_;       // 字段的长度
  int col_tot_len_;                 // 索引包含的字段的总长度
  int keys_size_;                   // keys_size = (BUCKET_SIZE + 1) * col_tot_len
  int tot_len_;                     // 记录结构体的整体长度(IxFileHdr的size)

  ExtendibleHashIxFileHdr() { tot_len_ = col_num_ = 0; }

  ExtendibleHashIxFileHdr(page_id_t first_free_page_no, int num_pages, page_id_t directory_page, int col_num,
                          int col_tot_len, int keys_size)
      : first_free_page_no_(first_free_page_no),
        num_pages_(num_pages),
        directory_page_(directory_page),
        col_num_(col_num),
        col_tot_len_(col_tot_len),
        keys_size_(keys_size) {
    
    tot_len_ = 0;
  }

  void update_tot_len() {
    tot_len_ = 0;
    tot_len_ += sizeof(page_id_t) * 2 + sizeof(int) * 5;
    tot_len_ += sizeof(ColType) * col_num_ + sizeof(int) * col_num_;
  }

  void serialize(char *dest) {
    int offset = 0;
    memcpy(dest + offset, &tot_len_, sizeof(int));
    offset += sizeof(int);
    memcpy(dest + offset, &first_free_page_no_, sizeof(page_id_t));
    offset += sizeof(page_id_t);
    memcpy(dest + offset, &num_pages_, sizeof(int));
    offset += sizeof(int);
    memcpy(dest + offset, &directory_page_, sizeof(page_id_t));
    offset += sizeof(page_id_t);
    memcpy(dest + offset, &col_num_, sizeof(int));
    offset += sizeof(int);
    for (int i = 0; i < col_num_; ++i) {
      memcpy(dest + offset, &col_types_[i], sizeof(ColType));
      offset += sizeof(ColType);
    }
    for (int i = 0; i < col_num_; ++i) {
      memcpy(dest + offset, &col_lens_[i], sizeof(int));
      offset += sizeof(int);
    }
    memcpy(dest + offset, &col_tot_len_, sizeof(int));
    offset += sizeof(int);
    memcpy(dest + offset, &keys_size_, sizeof(int));
    offset += sizeof(int);
    assert(offset == tot_len_);
  }

  void deserialize(char *src) {
    int offset = 0;
    tot_len_ = *reinterpret_cast<const int *>(src + offset);
    offset += sizeof(int);
    first_free_page_no_ = *reinterpret_cast<const page_id_t *>(src + offset);
    offset += sizeof(int);
    num_pages_ = *reinterpret_cast<const int *>(src + offset);
    offset += sizeof(int);
    directory_page_ = *reinterpret_cast<const page_id_t *>(src + offset);
    offset += sizeof(page_id_t);
    col_num_ = *reinterpret_cast<const int *>(src + offset);
    offset += sizeof(int);
    std::cout << col_num_ << "\n";
    for (int i = 0; i < col_num_; ++i) {
      // col_types_[i] = *reinterpret_cast<const ColType*>(src + offset);
      ColType type = *reinterpret_cast<const ColType *>(src + offset);
      offset += sizeof(ColType);
      col_types_.push_back(type);
    }
    for (int i = 0; i < col_num_; ++i) {
      // col_lens_[i] = *reinterpret_cast<const int*>(src + offset);
      int len = *reinterpret_cast<const int *>(src + offset);
      offset += sizeof(int);
      col_lens_.push_back(len);
    }
    col_tot_len_ = *reinterpret_cast<const int *>(src + offset);
    offset += sizeof(int);
    keys_size_ = *reinterpret_cast<const int *>(src + offset);
    offset += sizeof(int);
    assert(offset == tot_len_);
  }
};

class IxPageHdr {
 public:
  page_id_t next_free_page_no;  // unused
  page_id_t parent;             // 父亲节点所在页面的叶号
  int num_key;          // # current keys (always equals to #child - 1) 已插入的keys数量，key_idx∈[0,num_key)
  bool is_leaf;         // 是否为叶节点
  page_id_t prev_leaf;  // previous leaf node's page_no, effective only when is_leaf is true
  page_id_t next_leaf;  // next leaf node's page_no, effective only when is_leaf is true
};

class IxExtendibleHashPageHdr {
 public:
  page_id_t next_free_page_no;  // unused
  // page_id_t prev_bucket;        // Page number of the previous bucket, default is -1.
  // page_id_t next_bucket;        // Page number of the next bucket, default is -1.
  bool is_valid;  // Indicates if the current bucket is valid. Some invalid buckets may be preallocated during a split;
                  // invalid buckets do not need to be flushed to disk.
  int local_depth;  // Depth of the current bucket
  int key_nums;     // Number of keys in the current bucket
  int size;         // Size of the bucket
};

class Iid {
 public:
  int page_no;
  int slot_no;

  friend bool operator==(const Iid &x, const Iid &y) { return x.page_no == y.page_no && x.slot_no == y.slot_no; }

  friend bool operator!=(const Iid &x, const Iid &y) { return !(x == y); }
};

}  // namespace easydb
