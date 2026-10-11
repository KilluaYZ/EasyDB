/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * buffer_pool_manager.cpp
 *
 * Identification: src/buffer/buffer_pool_manager.cpp
 *
 *-------------------------------------------------------------------------
 */

/*
 * Original copyright:
 * Copyright (c) 2015-2024, Carnegie Mellon University Database Group
 */

/**
 * @file buffer_pool_manager.cpp
 * @brief 缓冲池管理器的实现文件
 *
 * 本文件实现了BufferPoolManager类的所有功能，包括：
 * - 缓冲池的初始化和销毁
 * - 页面的分配、获取、释放和删除
 * - 页面的刷新和恢复
 * - 页面替换策略的实现
 *
 * 缓冲池管理器是数据库系统的核心组件之一，负责在内存和磁盘之间管理页面数据，
 * 通过LRU替换算法优化内存使用，提高数据库访问性能。
 */

#include "buffer/buffer_pool_manager.h"
#include <iostream>
#include "common/config.h"

namespace easydb {

/**
 * @brief 构造函数：创建并初始化缓冲池管理器
 *
 * 构造函数负责创建缓冲池管理器实例，并进行以下初始化操作：
 * 1. 分配所有内存帧（一次性分配，提高性能）
 * 2. 初始化页表（预留空间）
 * 3. 将所有帧ID加入空闲帧列表（初始时所有帧都是空闲的）
 *
 * @param num_frames 缓冲池中帧的数量，决定了缓冲池的容量
 * @param disk_manager 磁盘管理器指针，用于执行磁盘I/O操作
 *
 * @note
 * - 使用new Page[num_frames_]一次性分配所有帧，保证内存连续性和缓存局部性
 * - 所有帧初始时都是空闲的，可以被立即使用
 * - 页表使用reserve预分配空间，避免后续扩容开销
 */
// TODO: 以下是旧版本的构造函数签名（已注释），保留用于参考
// BufferPoolManager::BufferPoolManager(size_t num_frames, DiskManager
// *disk_manager, size_t k_dist,
//                                      LogManager *log_manager)
//     : num_frames_(num_frames),
//       next_page_id_(0),
//       bpm_latch_(std::make_shared<std::mutex>()),
//       replacer_(std::make_shared<LRUReplacer>(num_frames, k_dist)),
//       disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)),
//       log_manager_(log_manager) {
//   // Not strictly necessary...
//   std::scoped_lock latch(*bpm_latch_);

BufferPoolManager::BufferPoolManager(size_t num_frames,
                                     DiskManager *disk_manager)
    : num_frames_(num_frames),
      replacer_(std::make_unique<LRUReplacer>(num_frames)),
      disk_manager_(disk_manager)
//   disk_manager_(std::make_unique<DiskManager>(disk_manager)) {
{
  // 一次性分配所有内存帧，使用数组而非vector以提高性能
  // 这样可以保证所有Page对象在内存中连续存储，提高缓存局部性
  frames_ = new Page[num_frames_];

  // 为页表预留空间，避免后续扩容时的内存重新分配
  // 页表最多需要num_frames_个槽位（每个帧对应一个页面）
  page_table_.reserve(num_frames_);

  // 初始化所有帧，并将所有帧ID加入空闲帧列表
  // 初始时所有帧都是空闲的，可以被立即使用
  for (size_t i = 0; i < num_frames_; i++) {
    // frames_.push_back(std::make_shared<PageId>());
    free_frames_.push_back(static_cast<int>(i));
  }
}

/**
 * @brief 析构函数：销毁缓冲池管理器并释放所有内存资源
 *
 * 析构函数负责清理缓冲池管理器占用的所有资源：
 * - 释放所有帧的内存（使用delete[]释放数组）
 *
 * @note
 * - 必须使用delete[]而不是delete，因为frames_是通过new[]分配的数组
 * - 其他成员（如replacer_）会通过智能指针自动释放
 */
// BufferPoolManager::~BufferPoolManager() = default;
BufferPoolManager::~BufferPoolManager() { delete[] frames_; };

/**
 * @brief 返回缓冲池管理的帧数量
 *
 * @return size_t 缓冲池中帧的总数，即缓冲池的容量
 *
 * @note 这是一个常量值，在构造时确定，之后不会改变
 */
auto BufferPoolManager::Size() const -> size_t { return num_frames_; }

/**
 * @brief 在磁盘上分配一个新页面，并将其加载到缓冲池中
 *
 * 该函数执行以下操作：
 * 1. 从空闲帧列表或替换器中选择一个受害者帧
 * 2. 从磁盘管理器分配一个新的页面ID（page_no）
 * 3. 如果受害者帧包含脏页，先将其写回磁盘
 * 4. 更新页表，建立新页面ID和帧的映射关系
 * 5. 重置帧并设置新的页面ID，固定页面（pin_count=1）
 *
 * @param page_id 输入输出参数：
 *                - 输入：必须包含有效的文件描述符（fd）
 *                - 输出：返回新分配页面的完整PageId（包含fd和page_no）
 *
 * @return Page* 指向新分配页面的指针，如果分配失败（找不到可用帧）返回nullptr
 *
 * @note
 * - 新分配的页面会被固定（pin_count=1），使用后必须调用UnpinPage释放
 * - 如果缓冲池已满且没有可替换的页面，函数返回nullptr
 * - 函数内部使用互斥锁保证线程安全
 */
auto BufferPoolManager::NewPage(PageId *page_id) -> Page * {
  // std::cerr << "[BufferPoolManager] NewPage" << std::endl;
  // 使用作用域锁保护整个操作，确保线程安全
  std::scoped_lock lock{latch_};

  // 步骤1：查找一个可用的受害者帧
  // 优先从空闲帧列表获取，如果没有空闲帧则通过LRU替换器选择
  frame_id_t frame_id;
  if (!FindVictimPage(&frame_id)) {
    // 如果找不到可用帧，说明缓冲池已满且所有页面都被固定，无法分配新页面
    return nullptr;
  }

  // 步骤2：从磁盘管理器分配一个新的页面ID
  // 从输入参数中获取文件描述符，然后分配页面号
  int fd = page_id->fd;                                // 获取文件描述符
  page_id->page_no = disk_manager_->AllocatePage(fd);  // 分配新的页面号

  // 获取对应的帧指针
  Page *frame = &frames_[frame_id];

  // 步骤3：如果受害者帧包含脏页，先将其写回磁盘
  // 这是为了保证数据一致性，避免丢失已修改的数据
  if (frame->is_dirty_) {
    disk_manager_->WritePage(frame->page_id_.fd, frame->page_id_.page_no,
                             frame->GetData(), PAGE_SIZE);
  }

  // 步骤4：更新页表
  // 删除旧页面的映射关系（如果存在）
  page_table_.erase(frame->page_id_);
  // 添加新页面的映射关系
  page_table_[*page_id] = frame_id;

  // 步骤5：更新帧的元数据并固定页面
  // 在替换器中标记该帧已被固定（从替换候选列表中移除）
  replacer_->Pin(frame_id);
  // 重置页面内存（清零数据）
  frame->ResetMemory();
  // 设置新的页面ID
  frame->page_id_ = *page_id;
  // 设置固定计数为1（表示有一个使用者）
  frame->pin_count_ = 1;
  // 新分配的页面初始状态不是脏页
  frame->is_dirty_ = false;

  return frame;
}

/**
 * @brief 从数据库中删除一个页面（同时从磁盘和内存中删除）
 *
 * 该函数执行以下操作：
 * 1. 在页表中查找目标页面
 * 2. 检查页面是否被固定（pin_count != 0），如果被固定则无法删除
 * 3. 如果页面是脏页，先将其写回磁盘
 * 4. 从页表中移除页面映射
 * 5. 重置帧的元数据，并将其加入空闲帧列表
 *
 * @param page_id 要删除的页面的PageId
 *
 * @return bool
 *         - true: 页面不存在或删除成功
 *         - false: 页面存在但无法删除（页面被固定）
 *
 * @note
 * - 如果页面在缓冲池中被固定（pin_count != 0），则无法删除，返回false
 * -
 * 如果页面不在缓冲池中，返回true（认为删除成功，因为磁盘上的页面会被磁盘管理器处理）
 * - 删除后，对应的帧会被加入空闲帧列表，可以被重新使用
 * - 函数内部使用互斥锁保证线程安全
 */
auto BufferPoolManager::DeletePage(PageId page_id) -> bool {
  // 使用作用域锁保护整个操作，确保线程安全
  std::scoped_lock lock{latch_};

  // 步骤1：在页表中查找目标页面
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    // 如果页面不在缓冲池中，返回true（认为删除成功）
    // 磁盘上的页面删除由磁盘管理器或其他组件处理
    return true;
  }

  // 获取页面所在的帧ID和帧指针
  frame_id_t frame_id = it->second;
  Page *frame = &frames_[frame_id];

  // 步骤2：检查页面是否被固定
  // 如果pin_count不为0，说明页面正在被使用，不能删除
  if (frame->pin_count_ != 0) {
    return false;
  }

  // 步骤3：如果页面是脏页，先将其写回磁盘
  // 这是为了保证数据一致性，避免丢失已修改的数据
  if (frame->is_dirty_) {
    disk_manager_->WritePage(frame->page_id_.fd, frame->page_id_.page_no,
                             frame->GetData(), PAGE_SIZE);
  }

  // 步骤4：从页表中移除页面映射
  page_table_.erase(it);

  // 步骤5：重置帧的元数据
  // 清零页面数据
  frame->ResetMemory();
  // 设置无效的页面ID（fd=-1, page_no=INVALID_PAGE_ID）
  frame->page_id_ = {-1, INVALID_PAGE_ID};
  // 重置脏页标志
  frame->is_dirty_ = false;
  // 重置固定计数
  frame->pin_count_ = 0;

  // 步骤6：将帧加入空闲帧列表，使其可以被重新使用
  free_frames_.push_back(frame_id);

  return true;
}

/**
 * @brief 将指定页面的数据刷新到磁盘
 *
 * 该函数将缓冲池中指定页面的数据写入磁盘，无论页面是否为脏页。
 * 刷新后，页面的is_dirty_标志会被设置为false。
 *
 * @param page_id 要刷新的页面的PageId
 *
 * @return bool
 *         - true: 刷新成功
 *         - false: 页面不在页表中（不在缓冲池中）
 *
 * @note
 * - 即使页面不是脏页，也会执行写操作（幂等性）
 * - 刷新后页面的is_dirty_标志会被设置为false
 * - 函数内部使用互斥锁保证线程安全
 */
auto BufferPoolManager::FlushPage(PageId page_id) -> bool {
  // std::cerr << "[BufferPoolManager] FlushPage" << std::endl;
  // 使用作用域锁保护整个操作，确保线程安全
  std::scoped_lock lock{latch_};

  // 步骤1：在页表中查找目标页面
  auto it = page_table_.find(page_id);
  // 如果页面不在缓冲池中，返回false
  if (it == page_table_.end()) {
    return false;
  }

  // 获取页面所在的帧ID和帧指针
  frame_id_t frame_id = it->second;
  Page *frame = &frames_[frame_id];

  // 步骤2：将页面的数据写入磁盘
  // 无论页面是否为脏页，都执行写操作（保证数据同步）
  disk_manager_->WritePage(page_id.fd, page_id.page_no, frame->GetData(),
                           PAGE_SIZE);

  // 步骤3：更新页面的脏页标志
  // 刷新后，页面与磁盘上的数据一致，不再是脏页
  frame->is_dirty_ = false;

  return true;
}

/**
 * @brief 将指定文件（通过fd区分）的所有页面从内存刷新到磁盘
 *
 * 该函数遍历页表中所有属于指定文件的页面，并将它们的数据写入磁盘。
 * 刷新后，所有相关页面的is_dirty_标志会被设置为false。
 *
 * @param fd 文件描述符，用于标识要刷新的文件
 *
 * @note
 * - 遍历页表中所有页面，只刷新属于指定文件的页面
 * - 即使页面不是脏页，也会执行写操作
 * - 刷新后所有相关页面的is_dirty_标志会被设置为false
 * - 函数内部使用互斥锁保证线程安全
 * - 常用于表关闭或检查点操作时，确保数据持久化
 */
void BufferPoolManager::FlushAllPages(int fd) {
  // std::cerr << "[BufferPoolManager] FlushAllPages" << std::endl;
  // 使用作用域锁保护整个操作，确保线程安全
  std::scoped_lock lock{latch_};

  // 遍历页表中的所有条目
  for (auto &entry : page_table_) {
    PageId page_id = entry.first;
    frame_id_t frame_id = entry.second;
    Page *frame = &frames_[frame_id];

    // 检查页面是否属于指定的文件描述符
    if (page_id.fd == fd) {
      // 将页面的数据写入磁盘
      disk_manager_->WritePage(page_id.fd, page_id.page_no, frame->GetData(),
                               PAGE_SIZE);

      // 更新页面的脏页标志
      // 刷新后，页面与磁盘上的数据一致，不再是脏页
      frame->is_dirty_ = false;
    }
  }
}

/**
 * @brief 将缓冲池中所有脏页刷新到磁盘
 *
 * 该函数遍历页表中所有页面，找出所有脏页（is_dirty_ == true），
 * 并将它们的数据写入磁盘。刷新后，所有脏页的is_dirty_标志会被设置为false。
 *
 * @note
 * - 遍历页表中所有页面，只刷新脏页
 * - 刷新后所有脏页的is_dirty_标志会被设置为false
 * - 函数内部使用作用域锁（scoped_lock）确保线程安全
 * - 常用于检查点操作或系统关闭时，确保所有修改都被持久化
 */
void BufferPoolManager::FlushAllDirtyPages() {
  // std::cerr << "[BufferPoolManager] FlushAllDirtyPages" << std::endl;
  // 使用作用域锁保护整个操作，确保线程安全
  std::scoped_lock lock{latch_};

  // 遍历页表中的所有条目
  for (auto &entry : page_table_) {
    PageId page_id = entry.first;
    frame_id_t frame_id = entry.second;
    Page *frame = &frames_[frame_id];

    // 检查页面是否为脏页
    if (frame->is_dirty_) {
      // 将脏页的数据写入磁盘
      disk_manager_->WritePage(page_id.fd, page_id.page_no, frame->GetData(),
                               PAGE_SIZE);

      // 更新页面的脏页标志
      // 刷新后，页面与磁盘上的数据一致，不再是脏页
      frame->is_dirty_ = false;
    }
  }
}

/**
 * @brief 移除缓冲池中属于指定文件的所有页面
 * @param {int} fd file descriptor
 * @return {void}
 * @note Used after drop table/index to avoid Data Corruption
        (fd maybe reused, so residual pages is not true pages from this file)
 */
void BufferPoolManager::RemoveAllPages(int fd) {
  // 使用作用域锁保护整个操作，确保线程安全
  std::scoped_lock lock{latch_};

  // 遍历页表，使用迭代器以便在遍历时安全删除元素
  for (auto it = page_table_.begin(); it != page_table_.end();) {
    PageId page_id = it->first;
    // 检查页面是否属于指定的文件描述符
    if (page_id.fd == fd) {
      // 获取对应的帧并重置其内存
      frame_id_t frame_id = it->second;
      Page *frame = &frames_[frame_id];
      frame->ResetMemory();
      // 从页表中删除页面映射
      // 注意：erase返回下一个有效的迭代器
      it = page_table_.erase(it);
    } else {
      // 如果页面不属于指定文件，继续遍历下一个
      it++;
    }
  }
}

/**
 * @brief 从磁盘恢复一个已知页面到缓冲池
 *
 * 该函数用于恢复场景，将磁盘上的页面加载到缓冲池中。
 * 执行以下操作：
 * 1. 检查页面是否已在缓冲池中，如果是则直接返回并固定
 * 2. 如果不在缓冲池中，查找一个受害者帧
 * 3. 如果受害者帧包含脏页，先更新它（写回磁盘并重置）
 * 4. 尝试从磁盘读取页面数据（如果页面尚未刷新到磁盘，可能抛出异常）
 * 5. 固定帧并设置pin_count为1
 *
 * @param page_id 要恢复的页面的PageId（必须包含有效的fd）
 *
 * @return Page* 恢复的页面帧指针，如果找不到受害者帧则抛出异常
 *
 * @note
 * - page_id必须包含有效的文件描述符（fd）
 * - 返回的页面pin_count为1，is_dirty为false
 * - 这是FetchPage函数的包装版本，但用于恢复场景
 * - 如果页面尚未刷新到磁盘（page_no < page_num），异常会被忽略
 * - 函数内部使用互斥锁保证线程安全
 *
 * @throws InternalError 如果找不到受害者帧
 */
auto BufferPoolManager::RecoverPage(PageId page_id) -> Page * {
  // 使用作用域锁保护整个操作，确保线程安全
  std::scoped_lock lock{latch_};

  // 步骤1：在页表中查找目标页面
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    // 如果目标页面已在缓冲池中，直接固定并返回
    frame_id_t frame_id = it->second;
    replacer_->Pin(frame_id);  // 在替换器中标记为已固定
    Page *frame = &frames_[frame_id];
    frame->pin_count_++;  // 增加固定计数
    return frame;
  }

  // 步骤2：如果页面不在缓冲池中，查找一个受害者帧
  frame_id_t frame_id;
  if (!FindVictimPage(&frame_id)) {
    // 如果找不到受害者帧，抛出异常
    throw InternalError(
        "BufferPoolManager::recover_page: No victim frame found");
  }

  // 获取受害者帧指针
  Page *frame = &frames_[frame_id];

  // 步骤3：如果受害者帧包含脏页，先更新它
  // UpdatePage会处理脏页写回、页表更新、帧重置等操作
  UpdatePage(frame, page_id, frame_id);

  // 步骤4：尝试从磁盘读取目标页面到帧中
  try {
    // 注意：这里使用的是WritePage而不是ReadPage，可能是代码错误
    // 应该是：disk_manager_->ReadPage(page_id.fd, page_id.page_no,
    // frame->GetData(), PAGE_SIZE);
    disk_manager_->WritePage(page_id.fd, page_id.page_no, frame->GetData(),
                             PAGE_SIZE);
    // disk_manager_->read_page(page_id.fd, page_id.page_no, page->GetData(),
    // PAGE_SIZE);
  } catch (InternalError &e) {
    // 如果页面尚未刷新到磁盘，可能会抛出异常
    // 获取文件中已分配的页面数量
    auto page_num = disk_manager_->GetFd2Pageno(page_id.fd);
    // 如果页面尚未刷新到磁盘，page_no应该 < page_num
    // 这种情况下可以忽略错误，否则重新抛出异常
    if (page_id.page_no >= page_num) {
      throw e;
    }
  }

  // 步骤5：固定帧并设置pin_count为1
  replacer_->Pin(frame_id);  // 在替换器中标记为已固定
  frame->pin_count_ = 1;     // 设置固定计数为1

  return frame;
}

// /**
//  * @brief Retrieves the pin count of a page. If the page does not exist in
//  memory, return `std::nullopt`.
//  * @param page_id The page ID of the page we want to get the pin count of.
//  * @return std::optional<size_t> The pin count if the page exists, otherwise
//  `std::nullopt`.
//  */
// auto BufferPoolManager::GetPinCount(page_id_t page_id) ->
// std::optional<size_t> {
//   std::scoped_lock latch(*bpm_latch_);
//   std::scoped_lock lock{latch_};

//   // 1. Search for the target page in page_table_
//   auto it = page_table_.find(page_id);
//   if (it != page_table_.end()) {
//     // 1.1 If the target page is found, return it
//     frame_id_t frame_id = it->second;
//     Page *frame = &frames_[frame_id];
//     // Page* page = &pages_[frame_id];
//     return frame->pin_count_;
//   }

//   return std::nullopt;
// }

/**
 * @brief 从空闲帧列表或替换器中找到一个可用的受害者帧
 *
 * 该函数按照以下优先级查找受害者帧：
 * 1. 优先从空闲帧列表（free_frames_）中获取空闲帧
 * 2. 如果没有空闲帧，则通过LRU替换器（replacer_）选择可替换的帧
 *
 * @param frame_id 输出参数，返回找到的受害者帧ID
 *
 * @return bool
 *         - true: 成功找到受害者帧
 *         - false: 无法找到受害者帧（缓冲池已满且所有页面都被固定）
 *
 * @note
 * - 这是一个私有辅助函数，由NewPage、FetchPage等函数调用
 * - 空闲帧是未被使用的帧，可以直接使用
 * - 通过替换器选择的帧是pin_count为0的页面，可以被替换
 * - 如果所有页面都被固定（pin_count > 0），则无法找到受害者帧
 * - 注意：此函数假设调用者已经持有锁，因此内部不获取锁
 */
auto BufferPoolManager::FindVictimPage(frame_id_t *frame_id) -> bool {
  // std::cerr << "[BufferPoolManager] FindVictimPage" << std::endl;
  // 注意：此函数假设调用者已经持有latch_锁，因此不需要再次获取锁
  // std::scoped_lock lock{latch_};

  // 步骤1：检查是否有空闲帧可用
  if (!free_frames_.empty()) {
    // 如果有空闲帧，使用第一个空闲帧
    *frame_id = free_frames_.front();
    free_frames_.pop_front();  // 从空闲帧列表中移除
    return true;
  }

  // 步骤2：如果没有空闲帧，使用LRU替换器查找可替换的帧
  // 替换器会选择pin_count为0且最近最少使用的页面
  if (replacer_->Victim(frame_id)) {
    return true;
  }

  // 如果无法找到受害者帧，返回false
  // 这通常发生在缓冲池已满且所有页面都被固定的情况下
  return false;
}

/**
 * @brief 更新页面数据、页面元数据和页表
 *
 * 该函数用于更新一个帧，使其存储新的页面。执行以下操作：
 * 1. 如果帧包含脏页，先将其写回磁盘
 * 2. 更新页表，删除旧页面的映射，添加新页面的映射
 * 3. 重置页面数据，更新PageId，重置pin_count和is_dirty标志
 *
 * @param frame 要更新的帧指针
 * @param new_page_id 新的PageId，帧将被用于存储这个页面
 * @param new_frame_id 新的帧ID（实际上就是frame在数组中的索引，用于一致性检查）
 *
 * @note
 * - 更新后：PageId为new_page_id；pin_count为0；is_dirty为false；数据重置为0
 * - 如果帧是脏页，会先写回磁盘再更新（保证数据一致性）
 * - 这是一个私有辅助函数，由FetchPage、RecoverPage等函数调用
 * - 注意：此函数假设调用者已经持有锁，因此内部不获取锁
 */
void BufferPoolManager::UpdatePage(Page *frame, PageId new_page_id,
                                   frame_id_t new_frame_id) {
  // std::cerr << "[BufferPoolManager] UpdatePage" << std::endl;
  // 注意：此函数假设调用者已经持有latch_锁，因此不需要再次获取锁
  // std::scoped_lock lock{latch_};

  // 步骤1：如果帧包含脏页，先将其写回磁盘
  // 这是为了保证数据一致性，避免丢失已修改的数据
  if (frame->is_dirty_) {
    disk_manager_->WritePage(frame->page_id_.fd, frame->page_id_.page_no,
                             frame->GetData(), PAGE_SIZE);
    // 注意：is_dirty_标志会在步骤3中被重置，这里不需要单独设置
  }

  // 步骤2：更新页表以反映新的映射关系
  // 删除旧页面的映射（如果存在）
  page_table_.erase(frame->page_id_);
  // 添加新页面的映射
  page_table_[new_page_id] = new_frame_id;

  // 步骤3：重置页面的数据并更新其PageId
  // 清零页面数据
  frame->ResetMemory();
  // 设置新的页面ID
  frame->page_id_ = new_page_id;
  // 重置固定计数为0（新页面还没有被固定）
  frame->pin_count_ = 0;
  // 重置脏页标志为false（新页面数据来自磁盘或刚分配）
  frame->is_dirty_ = false;
}

/**
 * @brief 从缓冲池或磁盘获取指定页面
 *
 * 该函数是缓冲池管理器的核心函数之一，用于获取页面。执行逻辑如下：
 *
 * 情况1：页面在缓冲池中（在page_table_中找到）
 *   - 直接返回页面指针
 *   - 增加页面的pin_count（固定页面）
 *   - 在替换器中标记为已固定
 *
 * 情况2：页面不在缓冲池中（在磁盘上）
 *   - 查找一个受害者帧（从空闲帧列表或替换器）
 *   - 如果受害者帧包含脏页，先更新它（写回磁盘并重置）
 *   - 从磁盘读取目标页面到帧中
 *   - 固定帧并设置pin_count为1
 *   - 返回页面指针
 *
 * @param page_id 目标页面的PageId
 *
 * @return Page* 指向目标页面的指针，如果获取失败（找不到可用帧）返回nullptr
 *
 * @note
 * - 页面会被固定（pin_count增加），使用后必须调用UnpinPage释放
 * - 如果页面不在缓冲池中，会从磁盘加载到缓冲池
 * - 如果缓冲池已满且所有页面都被固定，无法加载新页面，返回nullptr
 * - 函数内部使用互斥锁保证线程安全
 */
auto BufferPoolManager::FetchPage(PageId page_id) -> Page * {
  // std::cerr << "[BufferPoolManager] FetchPage" << std::endl;
  // 使用作用域锁保护整个操作，确保线程安全
  std::scoped_lock lock{latch_};

  // 步骤1：在页表中查找目标页面
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    // 情况1：目标页面已在缓冲池中
    // 获取帧ID和帧指针
    frame_id_t frame_id = it->second;
    // 在替换器中标记为已固定（从替换候选列表中移除）
    replacer_->Pin(frame_id);
    Page *frame = &frames_[frame_id];
    // 增加固定计数（表示又有一个使用者）
    frame->pin_count_++;
    return frame;
  }

  // 情况2：页面不在缓冲池中，需要从磁盘加载
  // 步骤2：查找一个受害者帧
  frame_id_t frame_id;
  if (!FindVictimPage(&frame_id)) {
    // 如果找不到可用帧，返回nullptr
    return nullptr;
  }

  // 获取受害者帧指针
  Page *frame = &frames_[frame_id];

  // 步骤3：如果受害者帧包含脏页，先更新它
  // UpdatePage会处理脏页写回、页表更新、帧重置等操作
  UpdatePage(frame, page_id, frame_id);

  // 步骤4：从磁盘读取目标页面到帧中
  disk_manager_->ReadPage(page_id.fd, page_id.page_no, frame->GetData(),
                          PAGE_SIZE);

  // 步骤5：固定帧并设置pin_count为1
  // 在替换器中标记为已固定（从替换候选列表中移除）
  replacer_->Pin(frame_id);
  // 设置固定计数为1（表示有一个使用者）
  frame->pin_count_ = 1;

  // 步骤6：返回目标页面
  return frame;
}

/**
 * @brief 释放缓冲池中的一个页面（取消固定）
 *
 * 该函数用于释放对页面的使用，执行以下操作：
 * 1. 在页表中查找目标页面
 * 2. 检查pin_count，如果已经是0则返回false
 * 3. 减少pin_count
 * 4. 如果pin_count变为0，调用替换器的Unpin方法（使页面可以被替换）
 * 5. 根据参数更新is_dirty标志
 *
 * @param page_id 要释放的页面PageId
 * @param is_dirty 是否标记页面为脏页
 *                - true: 标记页面为脏页（页面已被修改）
 *                - false: 不改变页面的脏页状态
 *
 * @return bool
 *         - true: 释放成功
 *         - false: 页面不在页表中或pin_count已经是0（无效操作）
 *
 * @note
 * - 每次调用FetchPage或NewPage后，必须调用UnpinPage释放页面
 * - 如果is_dirty为true，页面会被标记为脏页（即使之前不是脏页）
 * - 当pin_count变为0时，页面可以被替换器管理（可以被替换）
 * - 如果pin_count已经是0，说明页面已经被释放或从未被固定，返回false
 * - 函数内部使用互斥锁保证线程安全
 */
auto BufferPoolManager::UnpinPage(PageId page_id, bool is_dirty) -> bool {
  // std::cerr << "[BufferPoolManager] UnpinPage" << std::endl;
  // 使用作用域锁保护整个操作，确保线程安全
  std::scoped_lock lock{latch_};

  // 步骤1：在页表中查找目标页面
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    // 如果页面不在缓冲池中，返回false
    return false;
  }

  // 获取页面所在的帧ID和帧指针
  frame_id_t frame_id = it->second;
  Page *frame = &frames_[frame_id];

  // 步骤2：检查pin_count
  // 如果pin_count已经是0，说明页面已经被释放或从未被固定，返回false
  if (frame->pin_count_ == 0) {
    return false;
  }

  // 步骤3：减少pin_count（表示减少一个使用者）
  frame->pin_count_--;

  // 步骤4：如果pin_count变为0，调用替换器的Unpin方法
  // 使页面可以被替换器管理（可以被替换）
  if (frame->pin_count_ == 0) {
    replacer_->Unpin(frame_id);
  }

  // 步骤5：根据输入参数更新is_dirty标志
  // 如果is_dirty为true，标记页面为脏页（页面已被修改）
  if (is_dirty) {
    frame->is_dirty_ = true;
  }

  return true;
}

}  // namespace easydb
