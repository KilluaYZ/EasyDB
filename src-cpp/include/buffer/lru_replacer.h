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
 * @brief 双向链表的节点类，用于实现LRU替换器的链表结构
 *
 * LinkListNode 表示LRU链表中的一个节点，包含帧ID和指向前后节点的指针。
 * 使用双向链表可以高效地实现LRU策略：最近使用的帧移到尾部，最少使用的帧在头部。
 */
class LinkListNode {
 public:
  /** @brief 存储的帧ID值 */
  frame_id_t val_{0};

  /** @brief 指向前一个节点的指针 */
  LinkListNode *prev_{nullptr};

  /** @brief 指向后一个节点的指针 */
  LinkListNode *next_{nullptr};

  /**
   * @brief 构造函数，创建一个新的链表节点
   * @param Val 要存储的帧ID值
   */
  explicit LinkListNode(frame_id_t Val) : val_(Val) {}
};

/**
 * @brief LRU替换器类，实现最近最少使用（Least Recently Used）替换策略
 *
 * LRUReplacer 继承自 Replacer 抽象类，使用双向链表和哈希表实现LRU替换策略。
 *
 * 工作原理：
 * - 使用双向链表维护帧的访问顺序：头部是最久未使用的帧，尾部是最近使用的帧
 * - 使用哈希表（data_idx_）实现O(1)时间复杂度的帧查找
 * - 当需要选择受害者时，选择链表头部的帧（最久未使用）
 * - 当帧被访问时，将其移到链表尾部（标记为最近使用）
 * - 当帧被固定时，从链表中移除
 * - 当帧被取消固定时，将其加入链表尾部
 */
class LRUReplacer : public Replacer {
 public:
  /**
   * @brief 创建一个新的LRU替换器实例
   * @param num_pages
   * LRU替换器需要管理的最大页面数量（用于预留空间，实际不限制）
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * @brief 析构函数，销毁LRU替换器并释放所有链表节点占用的内存
   */
  ~LRUReplacer() override;

  /**
   * @brief 根据LRU策略选择一个受害者帧（最久未使用的帧）
   * @param[out] frame_id 输出参数，返回被选中的受害者帧ID
   * @return true 如果找到了受害者帧，false 如果替换器为空（没有可替换的帧）
   * @note 选中的帧是链表头部的帧（最久未使用），会被从替换器中移除
   */
  bool Victim(frame_id_t *frame_id) override;

  /**
   * @brief 固定一个帧，将其从LRU链表中移除
   * @param frame_id 要固定的帧ID
   * @note
   *   - 固定的帧不会被选为受害者
   *   - 如果帧不在链表中（已被固定或从未取消固定），则不做任何操作
   */
  void Pin(frame_id_t frame_id) override;

  /**
   * @brief 取消固定一个帧，将其加入LRU链表的尾部
   * @param frame_id 要取消固定的帧ID
   * @note
   *   - 取消固定后，该帧可以被选为受害者
   *   - 如果帧已经在链表中，则不做任何操作（避免重复）
   *   - 新加入的帧会被放在链表尾部（标记为最近使用）
   */
  void Unpin(frame_id_t frame_id) override;

  /**
   * @brief 返回当前LRU替换器中可被替换的帧数量
   * @return 链表中节点的数量，即可以被选为受害者的帧数量
   */
  size_t Size() override;

  /**
   * @brief 从双向链表中删除指定的节点
   * @param curr 要删除的节点指针
   * @note
   *   - 处理链表为空、只有一个节点、删除头节点、删除尾节点等边界情况
   *   - 删除节点后释放其内存
   *   - 更新head_和tail_指针
   */
  void DeleteNode(LinkListNode *curr);

 private:
  /**
   * @brief 哈希表：从帧ID到链表节点的映射，用于O(1)时间查找节点
   * @note Key: 帧ID，Value: 指向链表中对应节点的指针
   */
  std::unordered_map<frame_id_t, LinkListNode *> data_idx_;

  /**
   * @brief 双向链表的头指针，指向最久未使用的帧（LRU策略的受害者候选）
   */
  LinkListNode *head_{nullptr};

  /**
   * @brief 双向链表的尾指针，指向最近使用的帧
   */
  LinkListNode *tail_{nullptr};

  /**
   * @brief 保护LRU替换器内部数据结构的互斥锁
   * @note 确保多线程环境下LRU替换器操作的线程安全性
   */
  std::mutex data_latch_;
};

}  // namespace easydb
