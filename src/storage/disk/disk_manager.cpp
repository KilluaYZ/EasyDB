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
 * Constructor: open/create a directory of database files & log files
 * @input db_dir: database directory name
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
 * Write the contents of the specified page into disk file
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
 * Read the contents of the specified page into the given memory area
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
 * Allocate a new page in the file and return its page id
 */
page_id_t DiskManager::AllocatePage(int fd) {
  assert(fd >= 0 && fd < MAX_FD);
  return fd2pageno_[fd]++;
}

/**
 * Create a directory with the given path
 */
void DiskManager::CreateDir(const std::string &path) {
  if (std::filesystem::exists(path)) {
    return;
  }
  std::filesystem::create_directory(path);
}

/**
 * Destroy a directory with the given path
 */
void DiskManager::DestroyDir(const std::string &path) {
  if (std::filesystem::exists(path)) {
    std::filesystem::remove_all(path);
  }
}

/**
 * Create a file with the given path
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
 * Destroy a file with the given path
 */
void DiskManager::DestroyFile(const std::string &path) {
  if (IsFile(path)) {
    // Check if the file is still opened by any thread
    if (path2fd_.find(path) != path2fd_.end() && path2fd_[path] != -1) {
      throw Exception("file " + path + " is still opened by other threads");
    }
    if (std::filesystem::remove(path) != 0) {
      throw Exception("failed to remove file " + path);
    }
  } else {
    throw Exception("file " + path + " does not exist");
  }
}

/**
 * Open a file with the given path and return its file descriptor
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
 * Close a file with the given file descriptor
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
 * Get the name of the file with the given file descriptor
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
 * Get the file descriptor of the file with the given path
 * If the file is not opened, open it and return its file descriptor
 */
int DiskManager::GetFileFd(const std::string &path) {
  if (path2fd_.find(path) == path2fd_.end()) {
    return OpenFile(path);
  }

  return path2fd_[path];
}

}  // namespace easydb
