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
 * @brief The declaration of the `BufferPoolManager` class.
 *
 * As stated in the writeup, the buffer pool is responsible for moving physical pages of data back and forth from
 * buffers in main memory to persistent storage. It also behaves as a cache, keeping frequently used pages in memory for
 * faster access, and evicting unused or cold pages back out to storage.
 *
 * Make sure you read the writeup in its entirety before attempting to implement the buffer pool manager. You also need
 * to have completed the implementation of both the `LRUKReplacer` and `DiskManager` classes.
 */
class BufferPoolManager {
 public:
  BufferPoolManager(size_t num_frames, DiskManager *disk_manager);
  ~BufferPoolManager();

  /**
  * @description: mark target page dirty
  * @param {Page*} page: dirty page
  */
  static void MarkDirty(Page* page) { page->is_dirty_ = true; }

  /**
 * @brief Returns the number of frames that this buffer pool manages.
 */
  auto Size() const -> size_t;

 /**
 * @brief Allocates a new page on disk.
 * @return The page ID of the newly allocated page.
 */
  auto NewPage(PageId*  page_id) -> Page*;

  /**
  * @description: fetch a page;
  *              if find page_id from page_table_, the page is in buffer pool, return it and pin_count++;
  *              if can not find page_id from page_table_, the page in on disk, load it to disk, and return it. Set
  * pin_count to 1;
  * @return {Page*} the target page or nullptr.
  * @param {PageId} page_id : PageId of the target page.
  * @note: pin the page, need to unpin the page outside
  */
  auto FetchPage(PageId  page_id) -> Page*;


  /**
  * @description: unpin a frame in buffer pool.
  * @return {bool} return false if the target frame.pin_count_ <= 0, else return true.
  * @param {PageId} page_id: page_id of the target page.
  * @param {bool} is_dirty: mark if the target frame need to be marked dirty
  */
  auto UnpinPage(PageId page_id, bool is_dirty) -> bool;

  /**
  * @brief Removes a page from the database, both on disk and in memory.
  *
  * If the page is pinned in the buffer pool, this function does nothing and returns `false`. Otherwise, this function
  * removes the page from both disk and memory (if it is still in the buffer pool), returning `true`.
  *
  * @param page_id The page ID of the page we want to delete.
  * @return `false` if the page exists but could not be deleted, `true` if the page didn't exist or deletion succeeded.
  */
  auto DeletePage(PageId page_id) -> bool;

  /**
  * @brief Flushes a page's data out to disk.
  * @param page_id The page ID of the page to be flushed.
  * @return `false` if the page could not be found in the page table, otherwise `true`.
  */
  auto FlushPage(PageId page_id) -> bool;

  /**
  * @brief Flushes all page data in a table (distinguished by fd) that is in memory to disk.
  * @param {int} fd file descriptor
  */
  void FlushAllPages(int fd);

  /**
  * @description: This function flushes all dirty pages in the buffer pool to disk.
  * @return {void}
  * @note This function uses a scoped lock to ensure thread safety during the operation.
  */
  void FlushAllDirtyPages();

  /**
  * @brief Recover a known page from disk to the buffer bool.
  * @return {Page*} return recovered frame，otherwise return nullptr
  * @param {PageId} page_id: the page_id of the page to be recovered
  * @note: page_id must have valid fd；
  *        the pin_count of the output frame is 1，is_dirty is false;
  *        the page is a wrapper of FetchPage function
  *
  */
  auto RecoverPage(PageId page_id) -> Page*;

 private:

  /**
  * @brief Find a victim frame from the free_frame_list or the replacer.
  * @return {bool} true: find a victim frame , false: fail to find a victim frame
  * @param {frame_id_t*} return the frame_id of the found victim frame
  *
  */
  auto FindVictimPage(frame_id_t* frame_id) -> bool;


  /**
  * @brief Update the page data, page meta data (data, is_dirty_, page_id) and page table.
  * If it is dirty, it should be write back to disk first before update.
  * @param {Page*} frame : frame to be updated
  * @param {PageId} new_page_id : new page_id
  * @param {frame_id_t} new_frame_id : new frame_id
  * @note after update : PageId is new_page_id; pin_count is 0; is_dirty is false; data reset to 0
  *
  */
  void UpdatePage(Page* frame, PageId new_page_id, frame_id_t new_frame_id);

  /** @brief The number of frames in the buffer pool. */
  const size_t num_frames_;

  /** @brief The next page ID to be allocated.  */
  // std::atomic<PageId> next_page_id_;

  /**
   * @brief The latch protecting the buffer pool's inner data structures.
   */
  std::mutex latch_;      
  // std::shared_ptr<std::mutex> bpm_latch_;

  /** @brief The frame headers of the frames that this buffer pool manages. */
  std::vector<Page> frames_;
  // std::vector<std::shared_ptr<FrameHeader>> frames_;

  /** @brief The page table that keeps track of the mapping between pages and buffer pool frames. */
  std::unordered_map<PageId, frame_id_t, PageIdHash> page_table_;

  /** @brief A list of free frames that do not hold any page's data. */
  std::list<frame_id_t> free_frames_;

  /** @brief The replacer to find unpinned / candidate pages for eviction. */
  std::shared_ptr<LRUReplacer> replacer_;

  // std::shared_ptr<DiskManager> disk_manager_;
  DiskManager* disk_manager_;

};
}  // namespace easydb
