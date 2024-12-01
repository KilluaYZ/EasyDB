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

#include "buffer/buffer_pool_manager.h"
#include <iostream>
#include "common/config.h"

namespace easydb {

/**
 * @brief Creates a new `BufferPoolManager` instance and initializes all fields.
 *
 * See the documentation for `BufferPoolManager` in "buffer/buffer_pool_manager.h" for more information.
 * @param num_frames The size of the buffer pool.
 * @param disk_manager The disk manager.
 * @param k_dist The backward k-distance for the LRU-K replacer.
 * @param log_manager The log manager. Please ignore this for P1.
 */
// TODO:
// BufferPoolManager::BufferPoolManager(size_t num_frames, DiskManager *disk_manager, size_t k_dist,
//                                      LogManager *log_manager)
//     : num_frames_(num_frames),
//       next_page_id_(0),
//       bpm_latch_(std::make_shared<std::mutex>()),
//       replacer_(std::make_shared<LRUReplacer>(num_frames, k_dist)),
//       disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)),
//       log_manager_(log_manager) {
//   // Not strictly necessary...
//   std::scoped_lock latch(*bpm_latch_);

BufferPoolManager::BufferPoolManager(size_t num_frames, DiskManager *disk_manager)
    : num_frames_(num_frames),
      replacer_(std::make_unique<LRUReplacer>(num_frames)),
      disk_manager_(disk_manager)
//   disk_manager_(std::make_unique<DiskManager>(disk_manager)) {
{
  // Allocate all of the in-memory frames up front.
  frames_ = new Page[num_frames_];

  // // The page table should have exactly `num_frames_` slots, corresponding to exactly `num_frames_` frames.
  page_table_.reserve(num_frames_);

  // Initialize all of the frame headers, and fill the free frame list with all possible frame IDs (since all frames are
  // initially free).
  for (size_t i = 0; i < num_frames_; i++) {
    // frames_.push_back(std::make_shared<PageId>());
    free_frames_.push_back(static_cast<int>(i));
  }
}

/**
 * @brief Destroys the `BufferPoolManager`, freeing up all memory that the buffer pool was using.
 */
// BufferPoolManager::~BufferPoolManager() = default;
BufferPoolManager::~BufferPoolManager() { delete[] frames_; };

/**
 * @brief Returns the number of frames that this buffer pool manages.
 */
auto BufferPoolManager::Size() const -> size_t { return num_frames_; }

/**
 * @brief Allocates a new page on disk.
 * @return The page ID of the newly allocated page.
 */
auto BufferPoolManager::NewPage(PageId *page_id) -> Page * {
  // std::cerr << "[BufferPoolManager] NewPage" << std::endl;
  //  std::scoped_lock latch(*bpm_latch_);
  std::scoped_lock lock{latch_};

  // 1. Find a victim frame
  frame_id_t frame_id;
  if (!FindVictimPage(&frame_id)) {
    return nullptr;
  }

  // 2. Allocate a new PageId from the DiskManager
  int fd = page_id->fd;  // Get the file descriptor from page_id
  page_id->page_no = disk_manager_->AllocatePage(fd);

  // Page* page = &pages_[frame_id];
  Page *frame = &frames_[frame_id];

  // 3. If the frame is dirty, flush its content to disk
  if (frame->is_dirty_) {
    disk_manager_->WritePage(frame->page_id_.fd, frame->page_id_.page_no, frame->GetData(), PAGE_SIZE);
  }

  // Update the page table
  page_table_.erase(frame->page_id_);
  page_table_[*page_id] = frame_id;

  // 4. Update the frame with the new page ID and pin it
  replacer_->Pin(frame_id);
  // Reset the page memory
  frame->ResetMemory();
  frame->page_id_ = *page_id;
  frame->pin_count_ = 1;
  frame->is_dirty_ = false;

  return frame;
}

/**
 * @brief Removes a page from the database, both on disk and in memory.
 *
 * If the page is pinned in the buffer pool, this function does nothing and returns `false`. Otherwise, this function
 * removes the page from both disk and memory (if it is still in the buffer pool), returning `true`.
 *
 * @param page_id The page ID of the page we want to delete.
 * @return `false` if the page exists but could not be deleted, `true` if the page didn't exist or deletion succeeded.
 */
auto BufferPoolManager::DeletePage(PageId page_id) -> bool {
  // std::scoped_lock latch(*bpm_latch_);
  std::scoped_lock lock{latch_};

  // 1. Search for the target page in the page_table_
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    // If the page is not found, return true
    return true;
  }

  frame_id_t frame_id = it->second;
  // Page* page = &pages_[frame_id];
  Page *frame = &frames_[frame_id];

  // 2. If the target page's pin_count_ is not 0, return false
  if (frame->pin_count_ != 0) {
    return false;
  }

  // 3. If the page is dirty, write it back to disk
  if (frame->is_dirty_) {
    disk_manager_->WritePage(frame->page_id_.fd, frame->page_id_.page_no, frame->GetData(), PAGE_SIZE);
  }

  // Remove the page from the page_table_
  page_table_.erase(it);

  // Reset the page's metadata
  frame->ResetMemory();
  frame->page_id_ = {-1, INVALID_PAGE_ID};
  frame->is_dirty_ = false;
  frame->pin_count_ = 0;

  // Add the frame to the free_frames_
  free_frames_.push_back(frame_id);

  return true;
}

/**
 * @brief Flushes a page's data out to disk.
 * @param page_id The page ID of the page to be flushed.
 * @return `false` if the page could not be found in the page table, otherwise `true`.
 */
auto BufferPoolManager::FlushPage(PageId page_id) -> bool {
  // std::cerr << "[BufferPoolManager] FlushPage" << std::endl;
  //  std::scoped_lock latch(*bpm_latch_);
  std::scoped_lock lock{latch_};

  // 1. Search for the page in the page_table_
  auto it = page_table_.find(page_id);
  // 1.1 If the page is not found, return false
  if (it == page_table_.end()) {
    return false;
  }

  frame_id_t frame_id = it->second;
  // Page* page = &pages_[frame_id];
  Page *frame = &frames_[frame_id];

  // 2. Write the page's data to the disk
  disk_manager_->WritePage(page_id.fd, page_id.page_no, frame->GetData(), PAGE_SIZE);

  // 3. Update the is_dirty_ flag of the page
  frame->is_dirty_ = false;

  return true;
}

/**
 * @brief Flushes all page data in a table (distinguished by fd) that is in memory to disk.
 * @param {int} fd file descriptor
 */
void BufferPoolManager::FlushAllPages(int fd) {
  // std::cerr << "[BufferPoolManager] FlushAllPages" << std::endl;
  // std::scoped_lock latch(*bpm_latch_);
  std::scoped_lock lock{latch_};

  // Iterate through the page_table_
  for (auto &entry : page_table_) {
    PageId page_id = entry.first;
    frame_id_t frame_id = entry.second;
    Page *frame = &frames_[frame_id];
    // Page* page = &pages_[frame_id];

    // Check if the page belongs to the specified file descriptor
    if (page_id.fd == fd) {
      // Write the page's data to the disk
      disk_manager_->WritePage(page_id.fd, page_id.page_no, frame->GetData(), PAGE_SIZE);

      // Update the is_dirty_ flag of the page
      frame->is_dirty_ = false;
    }
  }
}

/**
 * @description: This function flushes all dirty pages in the buffer pool to disk.
 * @return {void}
 * @note This function uses a scoped lock to ensure thread safety during the operation.
 */
void BufferPoolManager::FlushAllDirtyPages() {
  // std::cerr << "[BufferPoolManager] FlushAllDirtyPages" << std::endl;
  // std::scoped_lock latch(*bpm_latch_);
  std::scoped_lock lock{latch_};

  // Iterate through the page_table_
  for (auto &entry : page_table_) {
    PageId page_id = entry.first;
    frame_id_t frame_id = entry.second;
    Page *frame = &frames_[frame_id];
    // Page* page = &pages_[frame_id];

    // Check if the page belongs to the specified file descriptor
    if (frame->is_dirty_) {
      // Write the page's data to the disk
      disk_manager_->WritePage(page_id.fd, page_id.page_no, frame->GetData(), PAGE_SIZE);

      // Update the is_dirty_ flag of the page
      frame->is_dirty_ = false;
    }
  }
}

/**
 * @description: Remove all pages in the buffer pool that belong to a specific file.
 * @param {int} fd file descriptor
 * @return {void}
 * @note Used after drop table/index to avoid Data Corruption
        (fd maybe reused, so residual pages is not true pages from this file)
 */
void BufferPoolManager::RemoveAllPages(int fd) {
  std::scoped_lock lock{latch_};

  // Iterate through the page_table_
  for (auto it = page_table_.begin(); it != page_table_.end();) {
    PageId page_id = it->first;
    if (page_id.fd == fd) {
      frame_id_t frame_id = it->second;
      Page *frame = &frames_[frame_id];
      frame->ResetMemory();
      // Remove the page from the page_table_
      it = page_table_.erase(it);
    } else {
      it++;
    }
  }
}

/**
 * @brief Recover a known page from disk to the buffer bool.
 * @return {Page*} return recovered frame，otherwise return nullptr
 * @param {PageId} page_id: the page_id of the page to be recovered
 * @note: page_id must have valid fd；
 *        the pin_count of the output frame is 1，is_dirty is false;
 *        the page is a wrapper of FetchPage function
 *
 */
auto BufferPoolManager::RecoverPage(PageId page_id) -> Page * {
  // std::scoped_lock latch(*bpm_latch_);
  std::scoped_lock lock{latch_};

  // 1. Search for the target page in page_table_
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    // 1.1 If the target page is found, pin it and return it
    frame_id_t frame_id = it->second;
    replacer_->Pin(frame_id);
    Page *frame = &frames_[frame_id];
    // Page* page = &pages_[frame_id];
    frame->pin_count_++;
    return frame;
  }

  // 1.2 If the page is not found, find a victim frame
  frame_id_t frame_id;
  if (!FindVictimPage(&frame_id)) {
    throw InternalError("BufferPoolManager::recover_page: No victim frame found");
  }

  // Page* page = &pages_[frame_id];
  Page *frame = &frames_[frame_id];

  // 2. If the frame is dirty, update it
  UpdatePage(frame, page_id, frame_id);

  // 3. Try to read the target page from disk into the frame
  try {
    disk_manager_->WritePage(page_id.fd, page_id.page_no, frame->GetData(), PAGE_SIZE);
    // disk_manager_->read_page(page_id.fd, page_id.page_no, page->GetData(), PAGE_SIZE);
  } catch (InternalError &e) {
    // may happen if the page is not flushed to disk yet
    auto page_num = disk_manager_->GetFd2Pageno(page_id.fd);
    // if not flushed to disk yet, page_no should < page_num,
    // we can just ignore this error, otherwise, error occurs
    if (page_id.page_no >= page_num) {
      throw e;
    }
  }

  // 4. Pin the frame and set pin_count_ to 1
  replacer_->Pin(frame_id);
  frame->pin_count_ = 1;

  return frame;
}

// /**
//  * @brief Retrieves the pin count of a page. If the page does not exist in memory, return `std::nullopt`.
//  * @param page_id The page ID of the page we want to get the pin count of.
//  * @return std::optional<size_t> The pin count if the page exists, otherwise `std::nullopt`.
//  */
// auto BufferPoolManager::GetPinCount(page_id_t page_id) -> std::optional<size_t> {
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
 * @brief Find a victim frame from the free_frame_list or the replacer.
 * @return {bool} true: find a victim frame , false: fail to find a victim frame
 * @param {frame_id_t*} return the frame_id of the found victim frame
 *
 */
auto BufferPoolManager::FindVictimPage(frame_id_t *frame_id) -> bool {
  // std::cerr << "[BufferPoolManager] FindVictimPage" << std::endl;
  // std::scoped_lock latch(*bpm_latch_);
  // std::scoped_lock lock{latch_};

  // 1. Check if there are any free frames available
  if (!free_frames_.empty()) {
    // 1.1 If free frames are available, use one
    *frame_id = free_frames_.front();
    free_frames_.pop_front();
    return true;
  }

  // 1.2 If no free frames are available, use the LRUReplacer to find a victim frame
  if (replacer_->Victim(frame_id)) {
    return true;
  }

  // If no victim frame can be found, return false
  return false;
}

/**
 * @brief Update the page data, page meta data (data, is_dirty_, page_id) and page table.
 * If it is dirty, it should be write back to disk first before update.
 * @param {Page*} frame : frame to be updated
 * @param {PageId} new_page_id : new page_id
 * @param {frame_id_t} new_frame_id : new frame_id
 * @note after update : PageId is new_page_id; pin_count is 0; is_dirty is false; data reset to 0
 *
 */
void BufferPoolManager::UpdatePage(Page *frame, PageId new_page_id, frame_id_t new_frame_id) {
  // std::cerr << "[BufferPoolManager] UpdatePage" << std::endl;
  // std::scoped_lock latch(*bpm_latch_);
  // std::scoped_lock lock{latch_};

  if (frame->is_dirty_) {
    disk_manager_->WritePage(frame->page_id_.fd, frame->page_id_.page_no, frame->GetData(), PAGE_SIZE);
    // page->is_dirty_ = false;
  }

  // 2. Update the page table to reflect the new mapping
  // Remove the old page id mapping
  page_table_.erase(frame->page_id_);
  // Add the new page id mapping
  page_table_[new_page_id] = new_frame_id;

  // 3. Reset the page's data and update its PageId
  frame->ResetMemory();
  frame->page_id_ = new_page_id;
  frame->pin_count_ = 0;
  frame->is_dirty_ = false;
}

/**
 * @description: fetch a page;
 *              if find page_id from page_table_, the page is in buffer pool, return it and pin_count++;
 *              if can not find page_id from page_table_, the page in on disk, load it to disk, and return it. Set
 * pin_count to 1;
 * @return {Page*} the target page or nullptr.
 * @param {PageId} page_id : PageId of the target page.
 * @note: pin the page, need to unpin the page outside
 */
auto BufferPoolManager::FetchPage(PageId page_id) -> Page * {
  // std::cerr << "[BufferPoolManager] FetchPage" << std::endl;
  // std::scoped_lock latch(*bpm_latch_);
  std::scoped_lock lock{latch_};

  // 1. Search for the target page in page_table_
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    // 1.1 If the target page is found, pin it and return it
    frame_id_t frame_id = it->second;
    replacer_->Pin(frame_id);
    Page *frame = &frames_[frame_id];
    // Page* page = &pages_[frame_id];
    frame->pin_count_++;
    return frame;
  }

  // 1.2 If the page is not found, find a victim frame
  frame_id_t frame_id;
  if (!FindVictimPage(&frame_id)) {
    return nullptr;
  }

  Page *frame = &frames_[frame_id];
  // Page* page = &pages_[frame_id];

  // 2. If the victim frame contains a dirty page, update it
  UpdatePage(frame, page_id, frame_id);

  // 3. Read the target page from disk into the frame
  disk_manager_->ReadPage(page_id.fd, page_id.page_no, frame->GetData(), PAGE_SIZE);

  // 4. Pin the frame and set pin_count_ to 1
  replacer_->Pin(frame_id);
  frame->pin_count_ = 1;

  // 5. Return the target page
  return frame;
}

/**
 * @description: unpin a frame in buffer pool.
 * @return {bool} return false if the target frame.pin_count_ <= 0, else return true.
 * @param {PageId} page_id: page_id of the target page.
 * @param {bool} is_dirty: mark if the target frame need to be marked dirty
 */
auto BufferPoolManager::UnpinPage(PageId page_id, bool is_dirty) -> bool {
  // std::cerr << "[BufferPoolManager] UnpinPage" << std::endl;
  // std::scoped_lock latch(*bpm_latch_);
  std::scoped_lock lock{latch_};

  // 1. Search for the page in the page_table_
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    // 1.1 If the page is not found, return false
    return false;
  }

  // 1.2 If the page is found, get its pin_count_
  frame_id_t frame_id = it->second;
  Page *frame = &frames_[frame_id];
  // Page* page = &pages_[frame_id];

  // Check the pin_count_
  // 2.1 If pin_count_ is already 0, return false
  if (frame->pin_count_ == 0) {
    return false;
  }

  // 2.2 If pin_count_ is greater than 0, decrement it
  frame->pin_count_--;

  // 2.2.1 If pin_count_ becomes 0 after decrementing, call replacer's Unpin
  if (frame->pin_count_ == 0) {
    replacer_->Unpin(frame_id);
  }

  // 3. Update the is_dirty flag based on the input parameter
  if (is_dirty) {
    frame->is_dirty_ = true;
  }

  return true;
}

}  // namespace easydb
