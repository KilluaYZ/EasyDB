/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * lru_replacer.h
 *
 * Identification: src/include/buffer/lru_replacer.h
 *
 *-------------------------------------------------------------------------
 */

/*
 * Original copyright:
 * Copyright (c) 2015-2021, Carnegie Mellon University Database Group
 */

#pragma once

#include <limits>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"
#include "common/macros.h"

namespace easydb {

/**
 * LRUReplacer implements the Least Recently Used replacement policy.
 */
class LRUReplacer : public Replacer {
 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);

  DISALLOW_COPY_AND_MOVE(LRUReplacer);
  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  // Remove the object that was accessed least recently compared to all the other elements being tracked by the
  // Replacer, store its contents in the output parameter and return True. If the Replacer is empty return False.
  auto Victim(frame_id_t *frame_id) -> bool override;

  // Set a frame as evictable or non-evictable based on the parameter
  void SetEvictable(frame_id_t frame_id, bool Evictable) override;

  // Record the event that the given frame id is accessed at the current timestamp
  void RecordAccess(frame_id_t frame_id);

  auto Size() -> size_t override;

 private:
  size_t replacer_size_;
  std::list<frame_id_t> lru_list_;  // List of frame ids in LRU order
  std::unordered_map<frame_id_t, std::list<frame_id_t>::iterator> frame_map_;
  std::unordered_map<frame_id_t, bool> evictable_;  // Track pin status of frames
  std::mutex latch_;
};

}  // namespace easydb
