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

/**
 * @brief 构造函数：创建并初始化LRU替换器
 * @param num_pages 最大页面数量（当前实现中未使用，保留用于未来扩展）
 */
LRUReplacer::LRUReplacer(size_t num_pages) {}

/**
 * @brief 析构函数：销毁LRU替换器，释放所有链表节点占用的内存
 *
 * 遍历整个双向链表，逐个删除所有节点，防止内存泄漏。
 */
LRUReplacer::~LRUReplacer() {
  LinkListNode *p = head_;
  while (p != nullptr) {
    LinkListNode *tmp = p;
    p = p->next_;
    delete (tmp);
  }
}

/**
 * @brief 从双向链表中删除指定的节点
 * @param curr 要删除的节点指针
 *
 * 处理以下情况：
 * 1. 链表只有一个节点：删除后head_和tail_都设为nullptr
 * 2. 删除头节点：更新head_指针，并设置新头节点的prev_为nullptr
 * 3. 删除尾节点：更新tail_指针，并设置新尾节点的next_为nullptr
 * 4. 删除中间节点：更新前后节点的指针连接
 *
 * 最后释放节点内存。
 */
void LRUReplacer::DeleteNode(LinkListNode *curr) {
  // 情况1：链表只有一个节点
  if (curr == head_ && curr == tail_) {
    head_ = nullptr;
    tail_ = nullptr;
  }
  // 情况2：删除头节点
  else if (curr == head_) {
    head_ = head_->next_;
    curr->next_->prev_ = curr->prev_;
  }
  // 情况3：删除尾节点
  else if (curr == tail_) {
    tail_ = tail_->prev_;
    curr->prev_->next_ = curr->next_;
  }
  // 情况4：删除中间节点
  else {
    curr->prev_->next_ = curr->next_;
    curr->next_->prev_ = curr->prev_;
  }
  delete (curr);
}

/**
 * @brief 根据LRU策略选择受害者帧（最久未使用的帧）
 * @param[out] frame_id 输出参数，返回被选中的受害者帧ID
 * @return true 如果找到了受害者帧，false 如果替换器为空
 *
 * 实现步骤：
 * 1. 加锁保护临界区
 * 2. 检查替换器是否为空，如果为空则返回false
 * 3. 选择链表头部的帧作为受害者（最久未使用）
 * 4. 从哈希表中移除该帧的映射
 * 5. 从链表中删除该节点
 * 6. 解锁并返回true
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  data_latch_.lock();

  // 如果替换器为空，没有可替换的帧
  if (data_idx_.empty()) {
    data_latch_.unlock();
    return false;
  }

  // 选择链表头部的帧作为受害者（最久未使用）
  *frame_id = head_->val_;

  // 从哈希表中移除该帧的映射
  data_idx_.erase(head_->val_);

  // 从链表中删除头节点
  LinkListNode *tmp = head_;
  DeleteNode(tmp);

  data_latch_.unlock();
  return true;
}

/**
 * @brief 固定一个帧，将其从LRU链表中移除
 * @param frame_id 要固定的帧ID
 *
 * 实现步骤：
 * 1. 加锁保护临界区
 * 2. 在哈希表中查找该帧
 * 3. 如果找到，从链表中删除对应节点，并从哈希表中移除映射
 * 4. 如果未找到（已被固定或从未取消固定），不做任何操作
 * 5. 解锁
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
  data_latch_.lock();

  // 在哈希表中查找该帧
  auto it = data_idx_.find(frame_id);
  if (it != data_idx_.end()) {
    // 如果找到，从链表中删除对应节点
    DeleteNode(data_idx_[frame_id]);
    // 从哈希表中移除映射
    data_idx_.erase(it);
  }
  // 如果未找到，说明该帧已被固定或从未取消固定，不做任何操作

  data_latch_.unlock();
}

/**
 * @brief 取消固定一个帧，将其加入LRU链表的尾部
 * @param frame_id 要取消固定的帧ID
 *
 * 实现步骤：
 * 1. 加锁保护临界区
 * 2. 检查该帧是否已在链表中
 * 3. 如果不在链表中，创建新节点并加入链表尾部（标记为最近使用）
 * 4. 更新哈希表映射
 * 5. 如果已在链表中，不做任何操作（避免重复）
 * 6. 解锁
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
  data_latch_.lock();

  // 检查该帧是否已在链表中
  auto it = data_idx_.find(frame_id);
  if (it == data_idx_.end()) {
    // 如果不在链表中，创建新节点
    LinkListNode *new_node = new LinkListNode(frame_id);

    // 如果链表为空，新节点既是头节点也是尾节点
    if (data_idx_.empty()) {
      head_ = tail_ = new_node;
    }
    // 否则，将新节点加入链表尾部
    else {
      tail_->next_ = new_node;
      new_node->prev_ = tail_;
      tail_ = new_node;
    }

    // 更新哈希表映射
    data_idx_[frame_id] = tail_;
  }
  // 如果已在链表中，不做任何操作（避免重复）

  data_latch_.unlock();
}

/**
 * @brief 返回当前LRU替换器中可被替换的帧数量
 * @return 链表中节点的数量，即可以被选为受害者的帧数量
 *
 * 实现步骤：
 * 1. 加锁保护临界区
 * 2. 返回哈希表的大小（等于链表中节点的数量）
 * 3. 解锁
 */
size_t LRUReplacer::Size() {
  data_latch_.lock();
  size_t ret = data_idx_.size();
  data_latch_.unlock();
  return ret;
}

}  // namespace easydb
