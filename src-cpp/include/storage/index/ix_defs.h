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

/**
 * @file ix_defs.h
 * @brief 索引定义头文件
 * 
 * 该文件定义了索引相关的常量、数据结构、比较函数等基础组件。
 * 包含：
 * 1. B+树索引和可扩展哈希索引的页面号常量
 * 2. 键值比较函数（支持多种数据类型）
 * 3. B+树索引文件头结构（IxFileHdr）
 * 4. 可扩展哈希索引文件头结构（ExtendibleHashIxFileHdr）
 * 5. B+树页面头结构（IxPageHdr）
 * 6. 可扩展哈希页面头结构（IxExtendibleHashPageHdr）
 * 7. 索引内部标识符（Iid）
 */

#pragma once

#include <cassert>
#include <cstring>
#include <vector>

#include "common/config.h"
#include "common/errors.h"
#include "type/type_id.h"
#include "type/value.h"

namespace easydb {

// ==================== B+树索引相关常量 ====================

/** @brief 无效页面号，表示不存在或未分配的页面 */
constexpr int IX_NO_PAGE = -1;

/** @brief B+树索引文件头页的页号（固定为0） */
constexpr int IX_FILE_HDR_PAGE = 0;

/** @brief B+树索引叶子链表头页的页号（固定为1） */
constexpr int IX_LEAF_HEADER_PAGE = 1;

/** @brief B+树索引初始根节点的页号（固定为2） */
constexpr int IX_INIT_ROOT_PAGE = 2;

/** @brief B+树索引初始页数（文件头+叶子头+根节点=3页） */
constexpr int IX_INIT_NUM_PAGES = 3;

/** @brief 索引列的最大总长度（字节） */
constexpr int IX_MAX_COL_LEN = 512;

// ==================== 可扩展哈希索引相关常量 ====================

/** @brief 可扩展哈希索引目录页的页号（固定为1） */
constexpr int IX_INIT_DIRECTORY_PAGE = 1;

/** @brief 可扩展哈希索引初始桶0的页号（固定为2） */
constexpr int IX_INIT_BUCKET_0_PAGE = 2;

/** @brief 可扩展哈希索引初始桶1的页号（固定为3） */
constexpr int IX_INIT_BUCKET_1_PAGE = 3;

/** @brief 可扩展哈希索引初始页数（文件头+目录+桶0+桶1=4页） */
constexpr int IX_INIT_HASH_NUM_PAGES = 4;

/** @brief 可扩展哈希索引第一个空闲页的页号（初始为4） */
constexpr int IX_INIT_HASH_FIRST_FREE_PAGES = 4;

/**
 * @brief 比较两个键值（单列）
 * @param a 第一个键值的指针
 * @param b 第二个键值的指针
 * @param type 列的数据类型
 * @param col_len 列的长度（字节）
 * @return 比较结果：-1表示a<b，0表示a==b，1表示a>b
 * 
 * 根据数据类型进行相应的比较：
 * - 整数类型：直接比较数值
 * - 浮点类型：直接比较数值
 * - 字符串类型：使用memcmp进行字节比较
 */
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

/**
 * @brief 比较两个复合键值（多列）
 * @param a 第一个复合键值的指针
 * @param b 第二个复合键值的指针
 * @param col_types 各列的数据类型列表
 * @param col_lens 各列的长度列表（字节）
 * @return 比较结果：-1表示a<b，0表示a==b，1表示a>b
 * 
 * 按列顺序依次比较，如果某一列不同则立即返回结果，否则继续比较下一列
 */
inline int ix_compare(const char *a, const char *b,
                      const std::vector<ColType> &col_types,
                      const std::vector<int> &col_lens) {
  int offset = 0;
  for (size_t i = 0; i < col_types.size(); ++i) {
    int res = ix_compare(a + offset, b + offset, col_types[i], col_lens[i]);
    if (res != 0) return res;
    offset += col_lens[i];
  }
  return 0;
}

/**
 * @brief 将Value对象复制到目标缓冲区（处理不同数据类型的包装函数）
 * @param dest 目标缓冲区指针
 * @param value 要复制的Value对象
 * @param len 要复制的长度（字节）
 * 
 * 对于字符串类型，直接使用memcpy复制数据；
 * 对于其他类型，使用Value的序列化方法
 */
inline void ix_memcpy(char *dest, Value &value, int len) {
  if (value.GetTypeId() == TYPE_CHAR || value.GetTypeId() == TYPE_VARCHAR) {
    memcpy(dest, value.GetData(), len);
  } else {
    assert(uint32_t(len) ==
           Type(value.GetTypeId()).GetTypeSize(value.GetTypeId()));
    value.SerializeTo(dest);
  }
}

/**
 * @class IxFileHdr
 * @brief B+树索引文件头结构
 * 
 * 存储B+树索引文件的元数据信息，包括：
 * - 文件页面管理信息（空闲页、总页数）
 * - 树结构信息（根节点、首尾叶子节点）
 * - 索引列信息（列数、类型、长度）
 * - B+树参数（阶数、键值对大小）
 * 
 * 文件头存储在索引文件的第0页（IX_FILE_HDR_PAGE）
 */
class IxFileHdr {
 public:
  /** @brief 文件中第一个空闲的磁盘页面的页面号，用于页面分配 */
  page_id_t first_free_page_no_;
  
  /** @brief 磁盘文件中页面的总数量 */
  int num_pages_;
  
  /** @brief B+树根节点对应的页面号 */
  page_id_t root_page_;
  
  /** @brief 索引包含的字段（列）数量 */
  int col_num_;
  
  /** @brief 各字段的数据类型列表 */
  std::vector<ColType> col_types_;
  
  /** @brief 各字段的长度列表（字节） */
  std::vector<int> col_lens_;
  
  /** @brief 索引包含的所有字段的总长度（字节） */
  int col_tot_len_;
  
  /** @brief B+树阶数，表示每个节点最多可插入的键值对数量 */
  int btree_order_;
  
  /** @brief 键值对数组的总大小，计算公式：keys_size = (btree_order + 1) * col_tot_len */
  int keys_size_;
  
  /** 
   * @brief 首叶节点对应的页号
   * @note 在IxManager的open函数中进行初始化，初始化为root_page_no
   * @note first_leaf初始化之后通常不会修改，主要用于测试文件中遍历叶子节点
   */
  page_id_t first_leaf_;
  
  /** @brief 尾叶节点对应的页号，用于快速定位最后一个叶子节点 */
  page_id_t last_leaf_;
  
  /** @brief 文件头结构体的整体长度（字节），包含所有动态数组的大小 */
  int tot_len_;

  /**
   * @brief 默认构造函数
   * 初始化tot_len_和col_num_为0
   */
  IxFileHdr() { tot_len_ = col_num_ = 0; }

  /**
   * @brief 带参数的构造函数
   * @param first_free_page_no 第一个空闲页号
   * @param num_pages 总页数
   * @param root_page 根节点页号
   * @param col_num 列数量
   * @param col_tot_len 列总长度
   * @param btree_order B+树阶数
   * @param keys_size 键值对数组大小
   * @param first_leaf 首叶节点页号
   * @param last_leaf 尾叶节点页号
   */
  IxFileHdr(page_id_t first_free_page_no, int num_pages, page_id_t root_page,
            int col_num, int col_tot_len, int btree_order, int keys_size,
            page_id_t first_leaf, page_id_t last_leaf)
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

  /**
   * @brief 更新文件头结构体的总长度
   * 
   * 计算包含所有固定字段和动态数组（col_types_和col_lens_）的总字节数
   * 固定字段：4个page_id_t + 6个int
   * 动态字段：col_num_个ColType + col_num_个int
   */
  void UpdateTotLen() {
    tot_len_ = 0;
    tot_len_ += sizeof(page_id_t) * 4 + sizeof(int) * 6;
    tot_len_ += sizeof(ColType) * col_num_ + sizeof(int) * col_num_;
  }

  /**
   * @brief 序列化文件头到缓冲区
   * @param dest 目标缓冲区指针，必须至少有tot_len_字节的空间
   * 
   * 将文件头结构体序列化为字节流，写入顺序：
   * 1. tot_len_（总长度）
   * 2. first_free_page_no_（第一个空闲页号）
   * 3. num_pages_（总页数）
   * 4. root_page_（根节点页号）
   * 5. col_num_（列数量）
   * 6. col_types_数组（各列类型）
   * 7. col_lens_数组（各列长度）
   * 8. col_tot_len_（列总长度）
   * 9. btree_order_（B+树阶数）
   * 10. keys_size_（键值对数组大小）
   * 11. first_leaf_（首叶节点页号）
   * 12. last_leaf_（尾叶节点页号）
   */
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

  /**
   * @brief 从缓冲区反序列化文件头
   * @param src 源缓冲区指针，包含序列化的文件头数据
   * 
   * 从字节流中读取文件头结构体的各个字段，按照Serialize的顺序进行读取
   * 注意：会清空并重新填充col_types_和col_lens_向量
   */
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

/**
 * @class ExtendibleHashIxFileHdr
 * @brief 可扩展哈希索引文件头结构
 * 
 * 存储可扩展哈希索引文件的元数据信息，包括：
 * - 文件页面管理信息（空闲页、总页数）
 * - 哈希目录信息（目录页号）
 * - 索引列信息（列数、类型、长度）
 * - 键值对大小信息
 * 
 * 文件头存储在索引文件的第0页（IX_FILE_HDR_PAGE）
 */
class ExtendibleHashIxFileHdr {
 public:
  /** @brief 文件中第一个空闲的磁盘页面的页面号，用于页面分配 */
  page_id_t first_free_page_no_;
  
  /** @brief 磁盘文件中页面的总数量 */
  int num_pages_;
  
  /** @brief 哈希目录对应的页面号 */
  page_id_t directory_page_;
  
  /** @brief 索引包含的字段（列）数量 */
  int col_num_;
  
  /** @brief 各字段的数据类型列表 */
  std::vector<ColType> col_types_;
  
  /** @brief 各字段的长度列表（字节） */
  std::vector<int> col_lens_;
  
  /** @brief 索引包含的所有字段的总长度（字节） */
  int col_tot_len_;
  
  /** @brief 键值对数组的总大小，计算公式：keys_size = (BUCKET_SIZE + 1) * col_tot_len */
  int keys_size_;
  
  /** @brief 文件头结构体的整体长度（字节），包含所有动态数组的大小 */
  int tot_len_;

  /**
   * @brief 默认构造函数
   * 初始化tot_len_和col_num_为0
   */
  ExtendibleHashIxFileHdr() { tot_len_ = col_num_ = 0; }

  /**
   * @brief 带参数的构造函数
   * @param first_free_page_no 第一个空闲页号
   * @param num_pages 总页数
   * @param directory_page 目录页号
   * @param col_num 列数量
   * @param col_tot_len 列总长度
   * @param keys_size 键值对数组大小
   */
  ExtendibleHashIxFileHdr(page_id_t first_free_page_no, int num_pages,
                          page_id_t directory_page, int col_num,
                          int col_tot_len, int keys_size)
      : first_free_page_no_(first_free_page_no),
        num_pages_(num_pages),
        directory_page_(directory_page),
        col_num_(col_num),
        col_tot_len_(col_tot_len),
        keys_size_(keys_size) {
    tot_len_ = 0;
  }

  /**
   * @brief 更新文件头结构体的总长度
   * 
   * 计算包含所有固定字段和动态数组（col_types_和col_lens_）的总字节数
   * 固定字段：2个page_id_t + 5个int
   * 动态字段：col_num_个ColType + col_num_个int
   */
  void update_tot_len() {
    tot_len_ = 0;
    tot_len_ += sizeof(page_id_t) * 2 + sizeof(int) * 5;
    tot_len_ += sizeof(ColType) * col_num_ + sizeof(int) * col_num_;
  }

  /**
   * @brief 序列化文件头到缓冲区
   * @param dest 目标缓冲区指针，必须至少有tot_len_字节的空间
   * 
   * 将文件头结构体序列化为字节流，写入顺序：
   * 1. tot_len_（总长度）
   * 2. first_free_page_no_（第一个空闲页号）
   * 3. num_pages_（总页数）
   * 4. directory_page_（目录页号）
   * 5. col_num_（列数量）
   * 6. col_types_数组（各列类型）
   * 7. col_lens_数组（各列长度）
   * 8. col_tot_len_（列总长度）
   * 9. keys_size_（键值对数组大小）
   */
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

  /**
   * @brief 从缓冲区反序列化文件头
   * @param src 源缓冲区指针，包含序列化的文件头数据
   * 
   * 从字节流中读取文件头结构体的各个字段，按照serialize的顺序进行读取
   * 注意：会清空并重新填充col_types_和col_lens_向量
   */
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

/**
 * @class IxPageHdr
 * @brief B+树节点页面头结构
 * 
 * 存储B+树中每个节点的元数据信息，包括：
 * - 节点在树中的位置信息（父节点、是否为叶子）
 * - 节点中的键值对数量
 * - 叶子节点链表信息（前驱、后继叶子节点）
 * 
 * 页面布局：|IxPageHdr|keys数组|RIDs数组|
 */
class IxPageHdr {
 public:
  /** @brief 下一个空闲页号（未使用） */
  page_id_t next_free_page_no;
  
  /** @brief 父节点所在页面的页号，根节点的parent为IX_NO_PAGE */
  page_id_t parent;
  
  /** 
   * @brief 当前节点中已插入的键值对数量
   * @note 对于内部节点：num_key = 子节点数量 - 1
   * @note 键值对的索引范围：key_idx ∈ [0, num_key)
   */
  int num_key;
  
  /** @brief 是否为叶子节点 */
  bool is_leaf;
  
  /** 
   * @brief 前一个叶子节点的页号
   * @note 仅在is_leaf为true时有效，用于维护叶子节点双向链表
   */
  page_id_t prev_leaf;
  
  /** 
   * @brief 下一个叶子节点的页号
   * @note 仅在is_leaf为true时有效，用于维护叶子节点双向链表
   */
  page_id_t next_leaf;
};

/**
 * @class IxExtendibleHashPageHdr
 * @brief 可扩展哈希索引页面头结构
 * 
 * 存储可扩展哈希索引中每个桶页面的元数据信息，包括：
 * - 桶的有效性标记
 * - 桶的本地深度（用于哈希值计算）
 * - 桶中的键值对数量
 * - 桶的容量大小
 * 
 * 注意：目录页使用local_depth=-1作为特殊标记
 */
class IxExtendibleHashPageHdr {
 public:
  /** @brief 下一个空闲页号（未使用） */
  page_id_t next_free_page_no;
  
  // 注释掉的字段：桶之间的链表指针（当前实现未使用）
  // page_id_t prev_bucket;  // 前一个桶的页号，默认为-1
  // page_id_t next_bucket;  // 下一个桶的页号，默认为-1
  
  /** 
   * @brief 指示当前桶是否有效
   * @note 在分裂操作期间可能会预分配一些无效的桶
   * @note 无效的桶不需要刷新到磁盘
   */
  bool is_valid;
  
  /** @brief 当前桶的本地深度，用于计算哈希值的高位位数 */
  int local_depth;
  
  /** @brief 当前桶中的键值对数量 */
  int key_nums;
  
  /** @brief 桶的容量大小（可容纳的最大键值对数量） */
  int size;
};

/**
 * @class Iid
 * @brief 索引内部标识符（Index Internal Identifier）
 * 
 * 用于在B+树索引内部定位一个键值对的位置，包含：
 * - page_id_：键值对所在的页面号
 * - slot_num_：键值对在页面中的槽位号（即键值对在节点中的索引位置）
 * 
 * 注意：Iid和RID是不同的概念：
 * - RID是上层传过来的记录位置（Record ID），指向实际数据记录
 * - Iid是索引内部生成的索引槽位置，指向索引中的键值对
 */
class Iid {
 public:
  /** @brief 键值对所在的页面号 */
  page_id_t page_id_;
  
  /** @brief 键值对在页面中的槽位号（索引位置） */
  slot_id_t slot_num_;

  /**
   * @brief 相等比较运算符
   * @param x 第一个Iid
   * @param y 第二个Iid
   * @return 如果两个Iid的page_id_和slot_num_都相等则返回true
   */
  friend bool operator==(const Iid &x, const Iid &y) {
    return x.page_id_ == y.page_id_ && x.slot_num_ == y.slot_num_;
  }

  /**
   * @brief 不等比较运算符
   * @param x 第一个Iid
   * @param y 第二个Iid
   * @return 如果两个Iid不相等则返回true
   */
  friend bool operator!=(const Iid &x, const Iid &y) { return !(x == y); }
};

}  // namespace easydb
