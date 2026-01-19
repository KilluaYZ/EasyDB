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

/**
 * @brief 文件头结构体，记录表数据文件的元信息
 * 
 * RmFileHdr 存储表数据文件的元数据信息，写入磁盘中文件的第0号页面。
 * 包含文件的基本信息，用于管理文件的页面分配和空闲空间。
 */
struct RmFileHdr {
  /**
   * @brief 文件中分配的页面个数
   * @note 初始化为1（只有文件头页面）
   */
  int num_pages;
  
  /**
   * @brief 文件中当前第一个包含空闲空间的页面号
   * @note 初始化为RM_NO_PAGE（-1），表示没有空闲页面
   *       用于快速定位有空闲空间的页面
   */
  int first_free_page_no;
  
  // int record_size;  // 表中每条记录的大小，由于不包含变长字段，因此当前字段初始化后保持不变
  // int num_records_per_page;  // 每个页面最多能存储的元组个数
  // int bitmap_size;           // 每个页面bitmap大小

  /**
   * @brief 初始化文件头
   * @note 将num_pages设置为1，first_free_page_no设置为RM_NO_PAGE
   */
  void Init() {
    num_pages = 1;
    first_free_page_no = RM_NO_PAGE;
  }
};

/**
 * @brief 元组格式说明
 * 
 * 元组格式：
 * ---------------------------------------------------------------------
 * | FIXED-SIZE or VARIED-SIZED OFFSET | PAYLOAD OF VARIED-SIZED FIELD |
 * ---------------------------------------------------------------------
 */
/**
 * @brief 表中的记录结构体
 * 
 * RmRecord 表示表中的一条记录，包含记录的数据和大小。
 * 支持内存管理和序列化/反序列化操作。
 */
struct RmRecord {
  /**
   * @brief 记录的数据指针
   * @note 指向存储记录实际数据的字符数组
   */
  char *data;
  
  /**
   * @brief 记录的大小（字节数）
   */
  int size;
  
  /**
   * @brief 是否已经为数据分配空间
   * @note true表示data指向的内存由RmRecord管理，析构时需要delete[]
   */
  bool allocated_ = false;

  /**
   * @brief 默认构造函数
   */
  RmRecord() = default;

  /**
   * @brief 拷贝构造函数
   * @param other 要复制的源记录对象
   * @note 分配新内存并复制数据
   */
  RmRecord(const RmRecord &other) {
    size = other.size;
    data = new char[size];
    memcpy(data, other.data, size);
    allocated_ = true;
  };

  /**
   * @brief 赋值运算符
   * @param other 要赋值的源记录对象
   * @return 当前对象的引用
   * @note 分配新内存并复制数据
   */
  RmRecord &operator=(const RmRecord &other) {
    size = other.size;
    data = new char[size];
    memcpy(data, other.data, size);
    allocated_ = true;
    return *this;
  };

  /**
   * @brief 根据大小构造记录
   * @param size_ 记录的大小（字节数）
   * @note 分配指定大小的内存空间
   */
  RmRecord(int size_) {
    size = size_;
    data = new char[size_];
    allocated_ = true;
  }

  /**
   * @brief 根据大小和数据构造记录
   * @param size_ 记录的大小（字节数）
   * @param data_ 记录数据的指针
   * @note 分配内存并复制数据
   */
  RmRecord(int size_, char *data_) {
    size = size_;
    data = new char[size_];
    memcpy(data, data_, size_);
    allocated_ = true;
  }

  /**
   * @brief 设置记录数据
   * @param data_ 源数据指针
   * @note 将源数据复制到已分配的内存中
   */
  void SetData(char *data_) { memcpy(data, data_, size); }

  /**
   * @brief 反序列化记录
   * @param data_ 序列化数据的指针
   * @note 
   *   - 格式：前4字节是大小，之后是实际数据
   *   - 如果已分配内存，先释放再重新分配
   */
  void Deserialize(const char *data_) {
    size = *reinterpret_cast<const int *>(data_);
    if (allocated_) {
      delete[] data;
    }
    data = new char[size];
    memcpy(data, data_ + sizeof(int), size);
  }

  /**
   * @brief 析构函数
   * @note 如果allocated_为true，释放data指向的内存
   */
  ~RmRecord() {
    if (allocated_) {
      delete[] data;
    }
    allocated_ = false;
    data = nullptr;
  }
};
}  // namespace easydb
