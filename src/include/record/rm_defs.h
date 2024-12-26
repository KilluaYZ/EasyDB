/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * rm_defs.h
 *
 * Identification: src/include/record/rm_defs.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once
#include <cstring>
// #include "common/config.h"
// #include "storage/page/page.h"
// #include "storage/table/tuple.h"

namespace easydb {

constexpr int RM_NO_PAGE = -1;
constexpr int RM_FILE_HDR_PAGE = 0;
constexpr int RM_FIRST_RECORD_PAGE = 1;
constexpr int RM_MAX_RECORD_SIZE = 512;

/* 文件头，记录表数据文件的元信息，写入磁盘中文件的第0号页面 */
struct RmFileHdr {
  int num_pages;           // 文件中分配的页面个数（初始化为1）
  int first_free_page_no;  // 文件中当前第一个包含空闲空间的页面号（初始化为-1）
  // int record_size;  // 表中每条记录的大小，由于不包含变长字段，因此当前字段初始化后保持不变
  // int num_records_per_page;  // 每个页面最多能存储的元组个数
  // int bitmap_size;           // 每个页面bitmap大小

  void Init() {
    num_pages = 1;
    first_free_page_no = RM_NO_PAGE;
  }
};

/**
 * Tuple format:
 * ---------------------------------------------------------------------
 * | FIXED-SIZE or VARIED-SIZED OFFSET | PAYLOAD OF VARIED-SIZED FIELD |
 * ---------------------------------------------------------------------
 */
/* 表中的记录 */
struct RmRecord {
  char *data;               // 记录的数据
  int size;                 // 记录的大小
  bool allocated_ = false;  // 是否已经为数据分配空间

  RmRecord() = default;

  RmRecord(const RmRecord &other) {
    size = other.size;
    data = new char[size];
    memcpy(data, other.data, size);
    allocated_ = true;
  };

  RmRecord &operator=(const RmRecord &other) {
    size = other.size;
    data = new char[size];
    memcpy(data, other.data, size);
    allocated_ = true;
    return *this;
  };

  RmRecord(int size_) {
    size = size_;
    data = new char[size_];
    allocated_ = true;
  }

  RmRecord(int size_, char *data_) {
    size = size_;
    data = new char[size_];
    memcpy(data, data_, size_);
    allocated_ = true;
  }

  void SetData(char *data_) { memcpy(data, data_, size); }

  void Deserialize(const char *data_) {
    size = *reinterpret_cast<const int *>(data_);
    if (allocated_) {
      delete[] data;
    }
    data = new char[size];
    memcpy(data, data_ + sizeof(int), size);
  }

  ~RmRecord() {
    if (allocated_) {
      delete[] data;
    }
    allocated_ = false;
    data = nullptr;
  }
};
}  // namespace easydb
