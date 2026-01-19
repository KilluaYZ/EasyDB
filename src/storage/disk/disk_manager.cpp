/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * disk_manager.cpp
 *
 * Identification: src/storage/disk/disk_manager.cpp
 *
 *-------------------------------------------------------------------------
 */

/*
 * Original copyright:
 * Copyright (c) 2015-2019, Carnegie Mellon University Database Group
 */

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>  // for lseek
#include <cassert>
#include <cstddef>
#include <cstring>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "storage/disk/disk_manager.h"

namespace easydb {

/**
 * @brief 构造函数：打开或创建数据库文件和日志文件的目录
 * @param db_dir 数据库目录名称
 * 
 * 初始化过程：
 * 1. 如果目录不存在，创建目录
 * 2. 打开或创建数据库元数据文件（.meta文件）
 * 3. 初始化文件描述符到页面号的映射数组
 */
DiskManager::DiskManager(const std::filesystem::path &db_dir) : dir_name_(db_dir) {
  // create directory if not exist
  if (!std::filesystem::exists(dir_name_)) {
    std::filesystem::create_directory(dir_name_);
  }
  // log_name_ = dir_name_ / (dir_name_.filename().stem().string() + ".log");

  // log_io_.open(log_name_, std::ios::binary | std::ios::in | std::ios::app | std::ios::out);
  // // directory or file does not exist
  // if (!log_io_.is_open()) {
  //   log_io_.clear();
  //   // create a new file
  //   log_io_.open(log_name_, std::ios::binary | std::ios::trunc | std::ios::out | std::ios::in);
  //   if (!log_io_.is_open()) {
  //     throw Exception("can't open dblog file");
  //   }
  // }

  // meta
  auto db_meta_file = dir_name_ / (dir_name_.filename().stem().string() + ".meta");
  // std::scoped_lock scoped_db_io_latch(db_io_latch_);
  db_meta_io_.open(db_meta_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_meta_io_.is_open()) {
    db_meta_io_.clear();
    // create a new file
    db_meta_io_.open(db_meta_file, std::ios::binary | std::ios::trunc | std::ios::out | std::ios::in);
    if (!db_meta_io_.is_open()) {
      throw Exception("can't open db file");
    }
  }
  // path2fd
  // fd2path
  // fd2pageno_
  memset(fd2pageno_, 0, MAX_FD * (sizeof(std::atomic<page_id_t>) / sizeof(char)));
}

/**
 * @brief 将指定页面的内容写入磁盘文件
 * @param fd 文件描述符，标识要写入的文件
 * @param page_id 页面ID，标识要写入的页面
 * @param page_data 页面数据的指针
 * @param num_bytes 要写入的字节数（通常为PAGE_SIZE）
 * 
 * 实现步骤：
 * 1. 计算页面在文件中的偏移量（page_id * PAGE_SIZE）
 * 2. 使用lseek()将文件指针移动到目标页面的起始位置
 * 3. 使用write()将页面数据写入文件
 */
void DiskManager::WritePage(int fd, page_id_t page_id, const char *page_data, size_t num_bytes) {
  // std::cerr << "[DiskManager] WritePage" << std::endl;
  // Calculate the offset in the file
  size_t offset = static_cast<size_t>(page_id) * PAGE_SIZE;

  // Set the write cursor to the page offset.

  // Use lseek() to move the file pointer to the beginning of the target page
  if (lseek(fd, offset, SEEK_SET) == -1) {
    LOG_DEBUG("lseek error");
    return;
  }

  // Write the page data to the file
  size_t write_count = write(fd, page_data, num_bytes);
  if (write_count != num_bytes) {
    LOG_DEBUG("write error");
    return;
  }
}

/**
 * @brief 从磁盘文件读取指定页面的内容到给定的内存区域
 * @param fd 文件描述符，标识要读取的文件
 * @param page_id 页面ID，标识要读取的页面
 * @param[out] page_data 输出缓冲区，用于存储读取的页面数据
 * @param num_bytes 要读取的字节数（通常为PAGE_SIZE）
 * 
 * 实现步骤：
 * 1. 计算页面在文件中的偏移量（page_id * PAGE_SIZE）
 * 2. 使用lseek()将文件指针移动到目标页面的起始位置
 * 3. 使用read()从文件读取页面数据
 * 4. 如果读取的字节数不足（文件末尾），将剩余部分填充为0
 */
void DiskManager::ReadPage(int fd, page_id_t page_id, char *page_data, size_t num_bytes) {
  // Calculate the offset in the file
  int offset = page_id * PAGE_SIZE;

  // Use lseek() to move the file pointer to the beginning of the target page
  if (lseek(fd, offset, SEEK_SET) == -1) {
    LOG_DEBUG("lseek error");
    return;
  }

  // Read the page data from the file
  size_t read_count = read(fd, page_data, num_bytes);
  if (read_count != num_bytes) {
    LOG_DEBUG("I/O error: Read hit the end of file at offset %d, missing %ld bytes", offset, num_bytes - read_count);
    memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    return;
  }
}

/**
 * @brief 在文件中分配一个新页面并返回其页面ID
 * @param fd 文件描述符，标识要分配页面的文件
 * @return 新分配页面的页面ID
 * 
 * 实现方式：
 * - 使用原子操作递增fd2pageno_[fd]，返回递增前的值作为新页面的ID
 * - 这确保了每个文件中的页面ID是唯一且递增的
 */
page_id_t DiskManager::AllocatePage(int fd) {
  assert(fd >= 0 && fd < MAX_FD);
  return fd2pageno_[fd]++;
}

/**
 * @brief 创建指定路径的目录
 * @param path 目录路径
 * @note 如果目录已存在，则不执行任何操作
 */
void DiskManager::CreateDir(const std::string &path) {
  if (std::filesystem::exists(path)) {
    return;
  }
  std::filesystem::create_directory(path);
}

/**
 * @brief 销毁指定路径的目录
 * @param path 目录路径
 * @note 递归删除目录及其所有内容
 */
void DiskManager::DestroyDir(const std::string &path) {
  if (std::filesystem::exists(path)) {
    std::filesystem::remove_all(path);
  }
}

/**
 * @brief 创建指定路径的文件
 * @param path 文件路径
 * @throws Exception 如果文件已存在或创建失败
 */
void DiskManager::CreateFile(const std::string &path) {
  if (IsFile(path)) {
    throw Exception("file " + path + " already exists");
  }
  int fd = open(path.c_str(), O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);

  if (fd == -1) {
    throw Exception("failed to create file " + path);
  }
  if (close(fd) == -1) {
    throw Exception("failed to close file " + path);
  }
}

/**
 * @brief 销毁指定路径的文件
 * @param path 文件路径
 * @throws Exception 如果文件不存在、文件仍被打开或删除失败
 */
void DiskManager::DestroyFile(const std::string &path) {
  if (IsFile(path)) {
    // Check if the file is still opened by any thread
    if (path2fd_.find(path) != path2fd_.end() && path2fd_[path] != -1) {
      throw Exception("file " + path + " is still opened by other threads");
    }
    if (!std::filesystem::remove(path)) {
      throw Exception("failed to remove file " + path);
    }
  } else {
    throw Exception("file " + path + " does not exist");
  }
}

/**
 * @brief 打开指定路径的文件并返回其文件描述符
 * @param path 文件路径
 * @return 文件描述符
 * @throws Exception 如果文件不存在、文件已被打开或打开失败
 * 
 * 实现步骤：
 * 1. 检查文件是否存在
 * 2. 检查文件是否已被打开（避免重复打开）
 * 3. 使用open()系统调用打开文件
 * 4. 在映射表中注册文件路径和文件描述符的对应关系
 */
int DiskManager::OpenFile(const std::string &path) {
  if (!IsFile(path)) {
    throw Exception("file " + path + " does not exist");
  }

  // Check if the file is already opened
  if (path2fd_.find(path) != path2fd_.end() && path2fd_[path] != -1) {
    throw Exception("file " + path + " is already opened by thread " + std::to_string(path2fd_[path]));
  }

  // Open the file
  int fd = open(path.c_str(), O_RDWR, S_IRUSR | S_IWUSR);

  if (fd == -1) {
    throw Exception("failed to open file " + path);
  }

  // Register the file in the map
  path2fd_[path] = fd;
  fd2path_[fd] = path;

  return fd;
}

/**
 * @brief 关闭指定文件描述符的文件
 * @param fd 文件描述符
 * 
 * 实现步骤：
 * 1. 验证文件描述符的有效性
 * 2. 检查文件是否已被关闭
 * 3. 使用close()系统调用关闭文件
 * 4. 从映射表中移除文件路径和文件描述符的对应关系
 */
void DiskManager::CloseFile(int fd) {
  if (fd < 0 || fd >= MAX_FD) {
    LOG_ERROR("invalid file descriptor %d", fd);
    return;
  }

  // Check if the file is already closed
  if (fd2path_.find(fd) == fd2path_.end()) {
    LOG_WARN("file %s is already closed", fd2path_[fd].c_str());
    return;
  }

  // Close the file
  if (close(fd) == -1) {
    LOG_ERROR("failed to close file %s", fd2path_[fd].c_str());
    return;
  }

  // Unregister the file in the map
  path2fd_.erase(fd2path_[fd]);
  fd2path_.erase(fd);
}

auto DiskManager::GetFileSize(const std::string &path) -> int {
  struct stat stat_buf;
  int rc = stat(path.c_str(), &stat_buf);
  return rc == 0 ? static_cast<int>(stat_buf.st_size) : -1;
}

/**
 * @brief 根据文件描述符获取文件名
 * @param fd 文件描述符
 * @return 文件的路径
 * @throws Exception 如果文件描述符无效或文件未打开
 */
auto DiskManager::GetFileName(int fd) -> std::filesystem::path {
  if (fd < 0 || fd >= MAX_FD) {
    // LOG_ERROR("invalid file descriptor %d", fd);
    throw Exception("invalid file descriptor");
  }

  if (fd2path_.find(fd) == fd2path_.end()) {
    // LOG_ERROR("file %d is not opened", fd);
    throw Exception("file is not opened");
  }

  return fd2path_[fd];
}

/**
 * @brief 获取指定路径文件的文件描述符
 * @param path 文件路径
 * @return 文件描述符
 * @note 如果文件未打开，会自动打开文件并返回其文件描述符
 */
int DiskManager::GetFileFd(const std::string &path) {
  if (path2fd_.find(path) == path2fd_.end()) {
    return OpenFile(path);
  }

  return path2fd_[path];
}

}  // namespace easydb
