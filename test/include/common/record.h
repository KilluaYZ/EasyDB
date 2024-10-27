//
// Created by ziyang on 24-10-24.
//

#ifndef RECORD_H
#define RECORD_H

#include <cstring>
namespace easydb {
constexpr int EASY_NO_PAGE = -1;
constexpr int EASY_FILE_HDR_PAGE = 0;
constexpr int EASY_FIRST_RECORD_PAGE = 1;
constexpr int EASY_MAX_RECORD_SIZE = 512;

/* 文件头，记录表数据文件的元信息，写入磁盘中文件的第0号页面 */
struct EasyFileHdr {
  int record_size;  // 表中每条记录的大小，由于不包含变长字段，因此当前字段初始化后保持不变
  int num_pages;             // 文件中分配的页面个数（初始化为1）
  int num_records_per_page;  // 每个页面最多能存储的元组个数
  int first_free_page_no;    // 文件中当前第一个包含空闲空间的页面号（初始化为-1）
  int bitmap_size;           // 每个页面bitmap大小
};

/* 表数据文件中每个页面的页头，记录每个页面的元信息 */
struct EasyPageHdr {
  int next_free_page_no;  // 当前页面满了之后，下一个包含空闲空间的页面号（初始化为-1）
  int num_records;        // 当前页面中当前已经存储的记录个数（初始化为0）
};

/* 表中的记录 */
struct EasyRecord {
  char *data;               // 记录的数据
  int size;                 // 记录的大小
  bool allocated_ = false;  // 是否已经为数据分配空间

  EasyRecord() = default;

  EasyRecord(const EasyRecord &other) {
    size = other.size;
    data = new char[size];
    memcpy(data, other.data, size);
    allocated_ = true;
  };

  EasyRecord &operator=(const EasyRecord &other) {
    size = other.size;
    data = new char[size];
    memcpy(data, other.data, size);
    allocated_ = true;
    return *this;
  };

  EasyRecord(int size_) {
    size = size_;
    data = new char[size_];
    allocated_ = true;
  }

  EasyRecord(int size_, char *data_) {
    size = size_;
    data = new char[size_];
    memcpy(data, data_, size_);
    allocated_ = true;
  }

  void SetData(char* data_) {
    memcpy(data, data_, size);
  }

  void Deserialize(const char* data_) {
    size = *reinterpret_cast<const int*>(data_);
    if(allocated_) {
      delete[] data;
    }
    data = new char[size];
    memcpy(data, data_ + sizeof(int), size);
  }

  ~EasyRecord() {
    if(allocated_) {
      delete[] data;
    }
    allocated_ = false;
    data = nullptr;
  }
};
}

/* 字段元数据 */
struct ColMeta {
  std::string tab_name;   // 字段所属表名称
  std::string name;       // 字段名称
  ColType type;           // 字段类型
  int len;                // 字段长度
  int offset;             // 字段位于记录中的偏移量
  bool index;             /** unused */

  friend std::ostream &operator<<(std::ostream &os, const ColMeta &col) {
    // ColMeta中有各个基本类型的变量，然后调用重载的这些变量的操作符<<（具体实现逻辑在defs.h）
    return os << col.tab_name << ' ' << col.name << ' ' << col.type << ' ' << col.len << ' ' << col.offset << ' '
              << col.index;
  }

  friend std::istream &operator>>(std::istream &is, ColMeta &col) {
    return is >> col.tab_name >> col.name >> col.type >> col.len >> col.offset >> col.index;
  }
};

#endif //RECORD_H
