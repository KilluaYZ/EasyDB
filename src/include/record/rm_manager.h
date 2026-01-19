/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * rm_manager.h
 *
 * Identification: src/include/record/rm_manager.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <assert.h>

#include "bitmap.h"
#include "rm_defs.h"
#include "rm_file_handle.h"

namespace easydb {

/**
 * @brief 记录管理器类，用于管理表的数据文件
 *
 * RmManager 负责表数据文件的创建、打开、删除、关闭等操作。
 * 它封装了底层磁盘管理器和缓冲池管理器的操作，提供高级的文件管理接口。
 */
class RmManager {
 private:
  /** @brief 磁盘管理器指针，用于执行磁盘I/O操作 */
  DiskManager *disk_manager_;

  /** @brief 缓冲池管理器指针，用于管理页面缓存 */
  BufferPoolManager *buffer_pool_manager_;

 public:
  /**
   * @brief 构造函数
   * @param disk_manager 磁盘管理器指针
   * @param buffer_pool_manager 缓冲池管理器指针
   */
  RmManager(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager)
      : disk_manager_(disk_manager),
        buffer_pool_manager_(buffer_pool_manager) {}

  /**
   * @brief 创建表的数据文件并初始化相关信息
   * @param filename 要创建的文件名称
   * @param record_size 表中记录的大小（字节数）
   * @throws InvalidRecordSizeError 如果记录大小无效
   *
   * 实现步骤：
   * 1. 验证记录大小的有效性
   * 2. 创建磁盘文件
   * 3. 打开文件获取文件描述符
   * 4. 初始化文件头（RmFileHdr）
   * 5. 将文件头写入磁盘文件的第0页
   * 6. 关闭文件
   */
  void CreateFile(const std::string &filename, int record_size) {
    if (record_size < 1 || record_size > RM_MAX_RECORD_SIZE) {
      throw InvalidRecordSizeError(record_size);
    }
    disk_manager_->CreateFile(filename);
    int fd = disk_manager_->OpenFile(filename);

    // 初始化file header
    RmFileHdr file_hdr{};
    file_hdr.Init();
    // file_hdr.record_size = record_size;
    // file_hdr.num_pages = 1;
    // file_hdr.first_free_page_no = RM_NO_PAGE;
    // We have: sizeof(hdr) + (n + 7) / 8 + n * record_size <= PAGE_SIZE
    // file_hdr.num_records_per_page =
    //     (BITMAP_WIDTH * (PAGE_SIZE - 1 - (int)sizeof(RmFileHdr)) + 1) / (1 +
    //     record_size * BITMAP_WIDTH);
    // file_hdr.bitmap_size = (file_hdr.num_records_per_page + BITMAP_WIDTH - 1)
    // / BITMAP_WIDTH;

    // 将file header写入磁盘文件（名为file name，文件描述符为fd）中的第0页
    // head page直接写入磁盘，没有经过缓冲区的NewPage，那么也就不需要FlushPage
    disk_manager_->WritePage(fd, RM_FILE_HDR_PAGE, (char *)&file_hdr,
                             sizeof(file_hdr));
    disk_manager_->CloseFile(fd);
  }

  /**
   * @brief 删除表的数据文件
   * @param filename 要删除的文件名称
   * @throws Exception 如果文件不存在或删除失败
   */
  void DestoryFile(const std::string &filename) {
    disk_manager_->DestroyFile(filename);
  }

  /**
   * @brief 打开表的数据文件，并返回文件句柄
   * @param filename 要打开的文件名称
   * @return 文件句柄的智能指针
   * @note 注意：这里打开文件，创建并返回了record file handle的指针
   *       调用者负责管理返回的智能指针的生命周期
   */
  std::unique_ptr<RmFileHandle> OpenFile(const std::string &filename) {
    int fd = disk_manager_->OpenFile(filename);
    return std::make_unique<RmFileHandle>(disk_manager_, buffer_pool_manager_,
                                          fd);
  }

  /**
   * @brief 关闭表的数据文件
   * @param file_handle 要关闭文件的句柄指针
   *
   * 实现步骤：
   * 1. 将文件头写回磁盘（更新元数据）
   * 2. 将缓冲区的所有页面刷新到磁盘（必须在close_file前面）
   * 3. 关闭文件描述符
   */
  void CloseFile(const RmFileHandle *file_handle) {
    disk_manager_->WritePage(file_handle->fd_, RM_FILE_HDR_PAGE,
                             (char *)&file_handle->file_hdr_,
                             sizeof(file_handle->file_hdr_));
    // 缓冲区的所有页刷到磁盘，注意这句话必须写在close_file前面
    buffer_pool_manager_->FlushAllPages(file_handle->fd_);
    disk_manager_->CloseFile(file_handle->fd_);
  }
};
}  // namespace easydb
