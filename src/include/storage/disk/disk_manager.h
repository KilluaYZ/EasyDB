/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * disk_manager.h
 *
 * Identification: src/include/storage/disk/disk_manager.h
 *
 *-------------------------------------------------------------------------
 */

/*
 * Original copyright:
 * Copyright (c) 2015-2019, Carnegie Mellon University Database Group
 */

#pragma once

#include <atomic>
#include <cstddef>
#include <filesystem>
#include <fstream>
#include <future>
#include <mutex>
#include <string>
#include <unordered_map>

#include "common/config.h"

namespace easydb {

/**
 * DiskManager takes care of the allocation and deallocation of pages within a database. It performs the reading and
 * writing of pages to and from disk, providing a logical file layer within the context of a database management system.
 */
class DiskManager {
 public:
  /**
   * Creates a new disk manager that writes to the specified database directory.
   * @param db_dir the directory name of the database directory to write to
   */
  explicit DiskManager(const std::filesystem::path &db_dir);

  virtual ~DiskManager() = default;

  /**
   * Write a page to the database file.
   * @param fd file descriptor of the database file
   * @param page_id id of the page
   * @param page_data raw page data
   * @param num_bytes number of bytes to write
   */
  virtual void WritePage(int fd, page_id_t page_id, const char *page_data, size_t num_bytes);

  /**
   * Read a page from the database file.
   * @param fd file descriptor of the database file
   * @param page_id id of the page
   * @param[out] page_data output buffer
   * @param num_bytes number of bytes to read
   */
  virtual void ReadPage(int fd, page_id_t page_id, char *page_data, size_t num_bytes);

  /**
   * Allocate a new page in the database file.
   * @param fd file descriptor of the database file
   * @return the id of the allocated page
   */
  virtual page_id_t AllocatePage(int fd);

  // Directory operations
  bool IsDir(const std::string &path) { return std::filesystem::is_directory(path); }

  void CreateDir(const std::string &path);

  void DestroyDir(const std::string &path);

  // File operations
  bool IsFile(const std::string &path) { return std::filesystem::is_regular_file(path); }

  void CreateFile(const std::string &path);

  void DestroyFile(const std::string &path);

  int OpenFile(const std::string &path);

  void CloseFile(int fd);

  auto GetFileSize(const std::string &path) -> int;

  auto GetFileName(int fd) -> std::filesystem::path;

  int GetFileFd(const std::string &path);

  /**
   * Sets the mapping of file descriptor to page number.
   * @param fd file descriptor of the database file
   * @param pageno page number
   */
  inline void SetFd2Pageno(int fd, page_id_t pageno) { fd2pageno_[fd] = pageno; }

  /**
   * Gets the mapping of file descriptor to page number.
   * @param fd file descriptor of the database file
   * @return the page number
   */
  inline auto GetFd2Pageno(int fd) -> page_id_t { return fd2pageno_[fd]; }

  // Log operations

 protected:
  static constexpr int MAX_FD = 8192;

  // streams to write db directory
  std::filesystem::path dir_name_;
  std::fstream db_meta_io_;
  // path to fd and fd to path mapping
  std::unordered_map<std::filesystem::path, int> path2fd_;
  std::unordered_map<int, std::filesystem::path> fd2path_;
  int log_fd_{-1};
  std::atomic<page_id_t> fd2pageno_[MAX_FD]{};
};

}  // namespace easydb
