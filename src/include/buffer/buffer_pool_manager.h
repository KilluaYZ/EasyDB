/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * buffer_pool_manager.h
 *
 * Identification: src/include/buffer/buffer_pool_manager.h
 *
 *-------------------------------------------------------------------------
 */

/*
 * Original copyright:
 * Copyright (c) 2015-2024, Carnegie Mellon University Database Group
 */

#pragma once

#include <list>
#include <memory>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

#include "buffer/lru_replacer.h"
#include "common/config.h"
#include "common/errors.h"
// #include "recovery/log_manager.h"
#include "storage/disk/disk_manager.h"
#include "storage/page/page.h"

namespace easydb {

class BufferPoolManager;

/**
 * @brief BufferPoolManager 类声明
 *
 * 缓冲池管理器负责在内存缓冲区和持久化存储之间移动物理数据页。
 * 它同时充当缓存角色，将频繁使用的页面保留在内存中以实现更快访问，
 * 并将未使用或冷页面驱逐回存储设备。
 *
 * 在实现缓冲池管理器之前，请确保完整阅读相关文档，并完成 LRUReplacer 和 DiskManager 类的实现。
 */
class BufferPoolManager {
 public:
  /**
   * @brief 构造函数：创建并初始化缓冲池管理器
   * @param num_frames 缓冲池中帧的数量
   * @param disk_manager 磁盘管理器指针，用于执行磁盘I/O操作
   */
  BufferPoolManager(size_t num_frames, DiskManager *disk_manager);

  /**
   * @brief 析构函数：销毁缓冲池管理器，释放所有内存资源
   */
  ~BufferPoolManager();

  /**
   * @brief 标记目标页面为脏页（已修改但未写回磁盘）
   * @param page 要标记为脏页的页面指针
   * @note 静态方法，用于外部标记页面为脏页
   */
  static void MarkDirty(Page *page) { page->is_dirty_ = true; }

  /**
   * @brief 返回缓冲池管理的帧数量
   * @return 缓冲池中帧的总数
   */
  auto Size() const -> size_t;

  /**
   * @brief 在磁盘上分配一个新页面
   * @param page_id 输出参数，返回新分配页面的PageId（需要预先设置fd）
   * @return 指向新分配页面的指针，如果分配失败返回nullptr
   * @note 新分配的页面会被固定（pin_count=1），使用后需要调用UnpinPage释放
   */
  auto NewPage(PageId *page_id) -> Page *;

  /**
   * @brief 从缓冲池或磁盘获取指定页面
   * @param page_id 目标页面的PageId
   * @return 指向目标页面的指针，如果获取失败返回nullptr
   * @note
   *   - 如果页面在缓冲池中（page_table_中找到），直接返回并增加pin_count
   *   - 如果页面不在缓冲池中，从磁盘加载到缓冲池，设置pin_count为1
   *   - 页面会被固定，使用后需要调用UnpinPage释放
   */
  auto FetchPage(PageId page_id) -> Page *;

  /**
   * @brief 释放缓冲池中的一个页面（取消固定）
   * @param page_id 要释放的页面PageId
   * @param is_dirty 是否标记页面为脏页
   * @return 如果页面pin_count <= 0返回false，否则返回true
   * @note
   *   - 减少页面的pin_count
   *   - 当pin_count变为0时，页面可以被替换器管理（可被替换）
   *   - 如果is_dirty为true，标记页面为脏页
   */
  auto UnpinPage(PageId page_id, bool is_dirty) -> bool;

  /**
   * @brief 从数据库删除一个页面（同时删除磁盘和内存中的页面）
   * @param page_id 要删除的页面PageId
   * @return
   *   - false: 页面存在但无法删除（例如页面被固定）
   *   - true: 页面不存在或删除成功
   * @note
   *   - 如果页面在缓冲池中被固定（pin_count != 0），则无法删除，返回false
   *   - 否则从磁盘和内存中删除页面，并将帧加入空闲帧列表
   */
  auto DeletePage(PageId page_id) -> bool;

  /**
   * @brief 将指定页面的数据刷新到磁盘
   * @param page_id 要刷新的页面PageId
   * @return 如果页面不在页表中返回false，否则返回true
   * @note 刷新后页面的is_dirty_标志会被设置为false
   */
  auto FlushPage(PageId page_id) -> bool;

  /**
   * @brief 将指定文件（通过fd区分）的所有页面从内存刷新到磁盘
   * @param fd 文件描述符，用于标识要刷新的文件
   * @note 遍历页表中所有属于该文件的页面并刷新到磁盘
   */
  void FlushAllPages(int fd);

  /**
   * @brief 将缓冲池中所有脏页刷新到磁盘
   * @note 使用作用域锁确保操作的线程安全性
   */
  void FlushAllDirtyPages();

  /**
   * @brief 移除缓冲池中属于指定文件的所有页面
   * @param fd 文件描述符，用于标识要移除的文件
   * @note
   *   - 在删除表/索引后使用，避免数据损坏
   *   - fd可能被重用，残留页面可能不是该文件的真实页面
   */
  void RemoveAllPages(int fd);

  /**
   * @brief 从磁盘恢复一个已知页面到缓冲池
   * @param page_id 要恢复的页面PageId（必须包含有效的fd）
   * @return 恢复的页面帧指针，失败返回nullptr
   * @note
   *   - 返回的页面pin_count为1，is_dirty为false
   *   - 这是FetchPage函数的包装版本
   */
  auto RecoverPage(PageId page_id) -> Page *;

 private:
  /**
   * @brief 从空闲帧列表或替换器中找到一个可用的受害者帧
   * @param frame_id 输出参数，返回找到的受害者帧ID
   * @return 找到受害者帧返回true，否则返回false
   * @note
   *   - 优先从free_frames_中获取空闲帧
   *   - 如果没有空闲帧，则通过replacer_选择可替换的帧
   */
  auto FindVictimPage(frame_id_t *frame_id) -> bool;

  /**
   * @brief 更新页面数据、页面元数据（data, is_dirty_, page_id）和页表
   * @param frame 要更新的帧指针
   * @param new_page_id 新的PageId
   * @param new_frame_id 新的帧ID
   * @note
   *   - 如果页面是脏页，先写回磁盘再更新
   *   - 更新后：PageId为新page_id，pin_count为0，is_dirty为false，数据重置为0
   */
  void UpdatePage(Page *frame, PageId new_page_id, frame_id_t new_frame_id);

  /**
   * @brief 缓冲池中帧的数量
   * @note 常量，在构造时确定，表示缓冲池的容量
   */
  const size_t num_frames_;

  /** @brief 下一个要分配的页面ID（已注释，未使用） */
  // std::atomic<PageId> next_page_id_;

  /**
   * @brief 保护缓冲池内部数据结构的互斥锁
   * @note 用于确保多线程环境下缓冲池操作的线程安全性
   */
  std::mutex latch_;
  // std::shared_ptr<std::mutex> bpm_latch_;

  /**
   * @brief 缓冲池管理的所有帧的数组
   * @note
   *   - 每个帧可以存储一个Page对象
   *   - 使用数组而非vector以提高性能
   *   - 在构造时一次性分配所有帧的内存
   */
  // std::vector<Page> frames_;
  Page *frames_;
  // std::vector<std::shared_ptr<FrameHeader>> frames_;

  /**
   * @brief 页表：记录磁盘页面和缓冲池帧之间的映射关系
   * @note
   *   - Key: PageId（磁盘页面的标识）
   *   - Value: frame_id_t（帧在frames_数组中的索引）
   *   - 用于快速查找页面是否在缓冲池中，以及所在的帧位置
   */
  std::unordered_map<PageId, frame_id_t, PageIdHash> page_table_;

  /**
   * @brief 空闲帧列表：记录当前未存储任何页面数据的帧ID列表
   * @note
   *   - 当需要分配新页面时，优先使用空闲帧
   *   - 当页面被删除或替换时，对应的帧会加入此列表
   */
  std::list<frame_id_t> free_frames_;

  /**
   * @brief LRU替换器：用于查找可被替换的未固定页面
   * @note
   *   - 管理所有pin_count为0的页面
   *   - 当缓冲池满且没有空闲帧时，通过替换器选择LRU页面进行替换
   *   - 使用LRU（最近最少使用）算法决定替换顺序
   */
  std::shared_ptr<LRUReplacer> replacer_;

  /**
   * @brief 磁盘管理器指针：负责执行磁盘I/O操作
   * @note
   *   - 用于读取页面从磁盘到内存（ReadPage）
   *   - 用于将页面从内存写入磁盘（WritePage）
   *   - 用于分配新的磁盘页面（AllocatePage）
   */
  // std::shared_ptr<DiskManager> disk_manager_;
  DiskManager *disk_manager_;
};
}  // namespace easydb
