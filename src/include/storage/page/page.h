/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * page.h
 *
 * Identification: src/include/storage/page/page.h
 *
 *-------------------------------------------------------------------------
 */

/*
 * Original copyright:
 * Copyright (c) 2015-2019, Carnegie Mellon University Database Group
 */

#pragma once

#include <cstring>
#include <vector>

#include "common/config.h"
#include "common/rwlatch.h"

namespace easydb {

/**
 * @brief 页面标识符结构体，用于唯一标识数据库中的一个页面
 * 
 * PageId 由两部分组成：
 * - fd: 文件描述符，标识页面所在的磁盘文件
 * - page_no: 页面在文件中的编号
 * 
 * 通过fd和page_no的组合，可以唯一确定数据库中的一个页面。
 */
struct PageId {
  /**
   * @brief 文件描述符，标识页面所在的磁盘文件
   * @note 文件描述符是文件打开后在内存中的标识，用于定位打开的文件
   */
  int fd;
  
  /**
   * @brief 页面在文件中的编号
   * @note 默认值为INVALID_PAGE_ID，表示无效的页面ID
   */
  page_id_t page_no = INVALID_PAGE_ID;

  /**
   * @brief 相等运算符重载
   * @param x 左侧PageId对象
   * @param y 右侧PageId对象
   * @return true 如果两个PageId的fd和page_no都相等
   */
  friend bool operator==(const PageId &x, const PageId &y) { return x.fd == y.fd && x.page_no == y.page_no; }
  
  /**
   * @brief 小于运算符重载，用于排序
   * @param x 要比较的另一个PageId对象
   * @return true 如果当前PageId < x（先比较fd，再比较page_no）
   */
  bool operator<(const PageId &x) const {
    if (fd < x.fd) return true;
    return page_no < x.page_no;
  }

  /**
   * @brief 获取PageId的字符串表示
   * @return 格式为"{fd: X page_no: Y}"的字符串
   */
  std::string toString() { return "{fd: " + std::to_string(fd) + " page_no: " + std::to_string(page_no) + "}"; }

  /**
   * @brief 将PageId编码为64位整数
   * @return 64位整数，高16位是fd，低32位是page_no
   * @note 用于序列化和哈希计算
   */
  inline int64_t Get() const { return (static_cast<int64_t>(fd << 16) | page_no); }
};

/**
 * @brief PageId的自定义哈希函数，用于构建unordered_map<PageId, frame_id_t, PageIdHash>
 * 
 * 实现了PageId的哈希计算，将fd和page_no组合成一个哈希值。
 */
struct PageIdHash {
  /**
   * @brief 计算PageId的哈希值
   * @param x 要计算哈希值的PageId对象
   * @return 哈希值
   */
  size_t operator()(const PageId &x) const { return (x.fd << 16) | x.page_no; }
};

/**
 * @brief 页面类，数据库系统中的基本存储单元
 * 
 * Page 类提供了内存中数据页的封装，包含：
 * - 页面数据（PAGE_SIZE字节）
 * - 缓冲池管理器使用的簿记信息（pin count、dirty flag、page id等）
 * - 读写锁，用于保护页面的并发访问
 * - LSN（日志序列号），用于恢复
 * 
 * Page 是缓冲池管理的基本单位，所有磁盘页面的读写都通过Page对象进行。
 */
class Page {
  // 页面内部的簿记信息只应该与缓冲池管理器相关
  friend class BufferPoolManager;

 public:
  /**
   * @brief 构造函数，初始化页面数据为零
   * @note 调用ResetMemory()将页面数据清零并初始化所有字段
   */
  Page() { ResetMemory(); }

  /**
   * @brief 默认析构函数
   */
  ~Page() = default;

  /**
   * @brief 获取页面中实际数据的指针
   * @return 指向页面数据缓冲区的指针
   * @note 返回的指针指向PAGE_SIZE字节的数据缓冲区
   */
  inline auto GetData() -> char * { return data_.data(); }
  // inline auto GetData() -> char * { return data_; }

  /**
   * @brief 获取页面的ID
   * @return 页面的PageId对象
   */
  inline auto GetPageId() const -> PageId { return page_id_; }

  /**
   * @brief 获取页面的固定计数（pin count）
   * @return 当前有多少个线程/操作正在使用此页面
   * @note 
   *   - pin_count > 0 表示页面正在被使用，不能被替换
   *   - pin_count = 0 表示页面可以被替换
   */
  inline auto GetPinCount() const -> int { return pin_count_.load(std::memory_order_acquire); }

  /**
   * @brief 判断页面是否为脏页
   * @return true 如果页面在内存中已被修改（与磁盘上的版本不同），false 否则
   * @note 脏页需要在替换前写回磁盘
   */
  inline auto IsDirty() const -> bool { return is_dirty_.load(std::memory_order_acquire); }

  /**
   * @brief 获取页面的写锁（独占锁）
   * @note 用于修改页面数据时，确保独占访问
   */
  inline void WLatch() { rwlatch_.lock(); }

  /**
   * @brief 释放页面的写锁
   */
  inline void WUnlatch() { rwlatch_.unlock(); }

  /**
   * @brief 获取页面的读锁（共享锁）
   * @note 用于读取页面数据时，允许多个读者同时访问
   */
  inline void RLatch() { rwlatch_.lock_shared(); }

  /**
   * @brief 释放页面的读锁
   */
  inline void RUnlatch() { rwlatch_.unlock_shared(); }

  /**
   * @brief 获取页面的LSN（日志序列号）
   * @return 页面的LSN值
   * @note LSN用于恢复系统，记录最后修改此页面的日志记录位置
   */
  inline auto GetLSN() -> lsn_t { return *reinterpret_cast<lsn_t *>(GetData() + OFFSET_LSN); }

  /**
   * @brief 设置页面的LSN
   * @param lsn 新的LSN值
   * @note 当页面被修改时，需要更新其LSN
   */
  inline void SetLSN(lsn_t lsn) { memcpy(GetData() + OFFSET_LSN, &lsn, sizeof(lsn_t)); }

  /**
   * @brief 通用页面头部格式（大小以字节为单位）：
   * | page_id (4 bytes) | lsn (4 bytes) | ...(page-specific Header) |
   * 
   * 所有页面都遵循此头部格式，前8字节是通用头部，之后是页面特定的头部信息。
   */
  static_assert(sizeof(page_id_t) == 4);
  static_assert(sizeof(lsn_t) == 4);

  /** @brief 页面头部的大小（字节） */
  static constexpr size_t SIZE_PAGE_HEADER = 8;
  /** @brief 页面数据起始位置的偏移量 */
  static constexpr size_t OFFSET_PAGE_START = 0;
  /** @brief LSN在页面中的偏移量 */
  static constexpr size_t OFFSET_LSN = 4;
  /** @brief 页面特定头部开始位置的偏移量 */
  static constexpr size_t OFFSET_PAGE_HDR = 8;

 private:
  /**
   * @brief 重置页面帧
   * 
   * 将页面中的数据清零，并将所有字段设置为默认值。
   * 用于页面被替换或初始化时。
   */
  inline void ResetMemory() {
    data_.resize(PAGE_SIZE);
    std::fill(data_.begin(), data_.end(), 0);
    // memset(data_, 0, PAGE_SIZE);
    page_id_.page_no = INVALID_PAGE_ID;
    pin_count_.store(0, std::memory_order_release);
    is_dirty_.store(false, std::memory_order_release);
  }

  /**
   * @brief 页面中存储的实际数据
   * 
   * 实际应用中，这应该存储为 `char data_[PAGE_SIZE]`。
   * 但是，为了允许地址消毒器检测缓冲区溢出，我们将其存储为vector。
   * 
   * 注意：友元类应确保不增加此数据字段的大小。
   */
  std::vector<char> data_;
  // char data_[PAGE_SIZE];

  /** @brief 页面的ID */
  PageId page_id_;

  /**
   * @brief 页面的固定计数（pin count）
   * @note 
   *   - 使用原子操作确保线程安全
   *   - 表示当前有多少个操作正在使用此页面
   *   - 当pin_count > 0时，页面不能被替换
   */
  std::atomic<size_t> pin_count_;

  /**
   * @brief 页面是否为脏页的标志
   * @note 
   *   - 使用原子操作确保线程安全
   *   - true表示页面在内存中已被修改，与磁盘上的版本不同
   *   - 脏页在替换前需要写回磁盘
   */
  std::atomic<bool> is_dirty_;

  /**
   * @brief 保护页面数据访问的读写锁
   * @note 用于确保多线程环境下页面访问的线程安全性
   */
  std::shared_mutex rwlatch_;
};

}  // namespace easydb

namespace std {

template <>
struct std::hash<easydb::PageId> {
  size_t operator()(const easydb::PageId &obj) const { return std::hash<int64_t>()(obj.Get()); }
};

}  // namespace std
