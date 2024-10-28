/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * lru_replacer.cpp
 *
 * Identification: src/buffer/lru_replacer.cpp
 *
 *-------------------------------------------------------------------------
 */

/*
 * Original copyright:
 * Copyright (c) 2015-2019, Carnegie Mellon University Database Group
 */

#include "buffer/lru_replacer.h"
#include <algorithm>

namespace easydb {

LRUReplacer::LRUReplacer(size_t num_frames) : replacer_size_(num_frames) {}

LRUReplacer::~LRUReplacer() = default;

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  for (auto it = lru_list_.rbegin(); it != lru_list_.rend(); ++it) {
    if (evictable_[*it]) {
      *frame_id = *it;
      evictable_.erase(*frame_id);
      frame_map_.erase(*frame_id);
      lru_list_.erase(std::next(it).base());
      return true;
    }
  }
  return false;
}

// Record the event that the given frame id is accessed at the current timestamp
void LRUReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frame_map_.count(frame_id)) {
    lru_list_.erase(frame_map_[frame_id]);
  } else if (lru_list_.size() >= replacer_size_) {
    frame_id_t old_frame_id = lru_list_.back();
    lru_list_.pop_back();
    frame_map_.erase(old_frame_id);
    evictable_.erase(old_frame_id);
  }
  lru_list_.emplace_front(frame_id);
  frame_map_[frame_id] = lru_list_.begin();
  evictable_[frame_id] = true;
}

// Set a frame as evictable or non-evictable based on the parameter
void LRUReplacer::SetEvictable(frame_id_t frame_id, bool Evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frame_map_.count(frame_id)) {
    evictable_[frame_id] = Evictable;
    // if (!Evictable) {
    //   lru_list_.remove(frame_id);  // Remove from LRU if it becomes non-evictable
    // } else if (std::find(lru_list_.begin(), lru_list_.end(), frame_id) == lru_list_.end()) {
    //   // Add to front if not already present when marked evictable
    //   lru_list_.emplace_front(frame_id);
    //   frame_map_[frame_id] = lru_list_.begin();
    // }
  }
}

// Return the number of evictable frames
auto LRUReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t size = 0;
  for (const auto &[frame_id, is_evictable] : evictable_) {
    if (is_evictable) {
      size++;
    }
  }
  return size;
}

}  // namespace easydb
