/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * table_page.h
 *
 * Identification: src/include/storage/page/table_page.h
 *
 *-------------------------------------------------------------------------
 */

/*
 * Original copyright:
 * Copyright (c) 2015-2019, Carnegie Mellon University Database Group
 */

#pragma once

#include <cstring>
#include <optional>
#include <tuple>
#include <utility>

#include "common/config.h"
#include "common/rid.h"
// #include "concurrency/lock_manager.h"
// #include "recovery/log_manager.h"
#include "storage/page/page.h"
// #include "storage/table/table_heap.h"
#include "storage/table/tuple.h"

namespace easydb {

/** @brief 表页面头部的大小（字节） */
static constexpr uint64_t TABLE_PAGE_HEADER_SIZE = 8;

/**
 * @brief 槽式页面格式说明
 * 
 * 槽式页面（Slotted Page）格式：
 *  ---------------------------------------------------------
 *  | HEADER | ... FREE SPACE ... | ... INSERTED TUPLES ... |
 *  ---------------------------------------------------------
 *                                ^
 *                                free space pointer
 *
 * 页面布局：
 * - 头部（Header）：包含页面元数据和槽目录
 * - 空闲空间（Free Space）：页面中间的空闲区域
 * - 已插入的元组（Inserted Tuples）：从页面末尾向前插入的元组数据
 *
 * 头部格式（大小以字节为单位）：
 *  ----------------------------------------------------------------------------
 *  |     NextPageId (4)    |    NumTuples(2)   |     NumDeletedTuples(2)      |
 *  ----------------------------------------------------------------------------
 *  ----------------------------------------------------------------
 *  | Tuple_1 offset+size (4)   | Tuple_2 offset+size (4)  |  ...  |
 *  ----------------------------------------------------------------
 *
 * 元组格式：
 * | meta | data |
 * 
 * 说明：
 * - NextPageId: 指向下一个表页面的页面ID（用于链表结构）
 * - NumTuples: 页面中元组的数量
 * - NumDeletedTuples: 页面中已删除元组的数量
 * - 槽目录：每个槽存储一个元组的偏移量和大小
 * - 元组从页面末尾向前插入，空闲空间在中间
 */

/**
 * @brief 表页面类，用于存储数据库表的元组数据
 * 
 * TablePage 实现了槽式页面格式，用于在页面中存储和管理元组。
 * 页面使用槽目录来跟踪每个元组的位置和大小，元组从页面末尾向前插入。
 */
class TablePage {
 public:
  /**
   * @brief 初始化表页面头部
   * @note 将页面头部字段初始化为默认值（next_page_id、num_tuples、num_deleted_tuples等）
   */
  void Init();

  /**
   * @brief 获取页面中元组的数量
   * @return 当前页面中存储的元组数量
   */
  auto GetNumTuples() const -> uint32_t { return num_tuples_; }

  /**
   * @brief 获取下一个表页面的页面ID
   * @return 下一个表页面的页面ID（如果这是最后一个页面，则为INVALID_PAGE_ID）
   * @note 表页面通过链表连接，用于遍历整个表
   */
  auto GetNextPageId() const -> page_id_t { return next_page_id_; }

  /**
   * @brief 设置下一个表页面的页面ID
   * @param next_page_id 下一个表页面的页面ID
   * @note 用于维护表页面的链表结构
   */
  void SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

  /**
   * @brief 获取下一个要插入元组的偏移量
   * @param meta 元组的元数据
   * @param tuple 要插入的元组
   * @return 如果元组可以放入此页面，返回偏移量；否则返回nullopt
   * @note 检查页面是否有足够的空间存储元组
   */
  auto GetNextTupleOffset(const TupleMeta &meta, const Tuple &tuple) const -> std::optional<uint16_t>;

  /**
   * @brief 向表中插入一个元组
   * @param meta 元组的元数据（时间戳、删除标志等）
   * @param tuple 要插入的元组
   * @return 如果插入成功（有足够空间），返回槽号；否则返回nullopt
   * @note 
   *   - 元组从页面末尾向前插入
   *   - 在槽目录中添加新条目
   *   - 更新元组计数
   */
  auto InsertTuple(const TupleMeta &meta, const Tuple &tuple) -> std::optional<uint16_t>;

  /**
   * @brief 更新元组的元数据
   * @param meta 新的元数据
   * @param rid 要更新的元组的记录ID
   * @note 用于更新元组的时间戳或删除标志
   */
  void UpdateTupleMeta(const TupleMeta &meta, const RID &rid);

  /**
   * @brief 从表中读取一个元组
   * @param rid 要读取的元组的记录ID
   * @return 元组的元数据和数据的pair
   * @note 根据RID中的槽号从槽目录中找到元组的位置并读取
   */
  auto GetTuple(const RID &rid) const -> std::pair<TupleMeta, Tuple>;

  /**
   * @brief 从表中读取元组的元数据
   * @param rid 要读取的元组的记录ID
   * @return 元组的元数据
   * @note 只读取元数据，不读取实际数据（更高效）
   */
  auto GetTupleMeta(const RID &rid) const -> TupleMeta;

  /**
   * @brief 原地更新元组（不安全版本）
   * @param meta 新的元数据
   * @param tuple 新的元组数据
   * @param rid 要更新的元组的记录ID
   * @note 
   *   - "不安全"意味着不检查新元组的大小是否与旧元组匹配
   *   - 调用者需要确保新元组的大小不超过旧元组
   *   - 用于更新操作，当新值大小不超过旧值时使用
   */
  void UpdateTupleInPlaceUnsafe(const TupleMeta &meta, const Tuple &tuple, RID rid);

  static_assert(sizeof(page_id_t) == 4);

 private:
  /**
   * @brief 元组信息类型，包含偏移量、大小和元数据
   * @note 格式：<offset(2字节), size(2字节), TupleMeta(16字节)>
   */
  using TupleInfo = std::tuple<uint16_t, uint16_t, TupleMeta>;
  
  /** @brief 页面起始位置的占位符（用于计算偏移量） */
  char page_start_[0];
  
  /** @brief 下一个表页面的页面ID */
  page_id_t next_page_id_;
  
  /** @brief 页面中元组的数量 */
  uint16_t num_tuples_;
  
  /** @brief 页面中已删除元组的数量 */
  uint16_t num_deleted_tuples_;
  
  /**
   * @brief 元组信息数组（槽目录）
   * @note 
   *   - 这是一个灵活数组成员，实际大小由num_tuples_决定
   *   - 每个元素对应一个元组的槽信息
   *   - 包含元组的偏移量、大小和元数据
   */
  TupleInfo tuple_info_[0];

  /** @brief 每个元组信息的大小（字节） */
  static constexpr size_t TUPLE_INFO_SIZE = 24;
  static_assert(sizeof(TupleInfo) == TUPLE_INFO_SIZE);
};

static_assert(sizeof(TablePage) == TABLE_PAGE_HEADER_SIZE);

}  // namespace easydb
