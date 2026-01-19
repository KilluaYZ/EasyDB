/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * table_page.cpp
 *
 * Identification: src/storage/page/table_page.cpp
 *
 *-------------------------------------------------------------------------
 */

/*
 * Original copyright:
 * Copyright (c) 2015-2019, Carnegie Mellon University Database Group
 */

#include "storage/page/table_page.h"

#include <cassert>
#include <cstring>
#include <optional>
#include <tuple>
#include "common/config.h"
#include "common/exception.h"
#include "storage/table/tuple.h"

namespace easydb {

/**
 * @brief 初始化表页面头部
 * 
 * 将页面头部字段初始化为默认值：
 * - next_page_id_: 设置为INVALID_PAGE_ID（表示这是最后一个页面）
 * - num_tuples_: 设置为0（页面中没有元组）
 * - num_deleted_tuples_: 设置为0（没有已删除的元组）
 */
void TablePage::Init() {
  next_page_id_ = INVALID_PAGE_ID;
  num_tuples_ = 0;
  num_deleted_tuples_ = 0;
}

/**
 * @brief 获取下一个要插入元组的偏移量
 * @param meta 元组的元数据
 * @param tuple 要插入的元组
 * @return 如果元组可以放入此页面，返回偏移量；否则返回nullopt
 * 
 * 计算逻辑：
 * 1. 确定最后一个元组的结束位置（如果页面为空，则为PAGE_SIZE）
 * 2. 计算新元组的起始位置（从末尾向前）
 * 3. 检查是否有足够空间（新元组位置不能与槽目录重叠）
 */
auto TablePage::GetNextTupleOffset(const TupleMeta &meta, const Tuple &tuple) const -> std::optional<uint16_t> {
  size_t slot_end_offset;
  if (num_tuples_ > 0) {
    auto &[offset, size, meta] = tuple_info_[num_tuples_ - 1];
    slot_end_offset = offset;
  } else {
    slot_end_offset = PAGE_SIZE;
  }
  auto tuple_offset = slot_end_offset - tuple.GetLength();
  auto offset_size = TABLE_PAGE_HEADER_SIZE + TUPLE_INFO_SIZE * (num_tuples_ + 1);
  if (tuple_offset < offset_size) {
    return std::nullopt;
  }
  return tuple_offset;
}

/**
 * @brief 向表中插入一个元组
 * @param meta 元组的元数据
 * @param tuple 要插入的元组
 * @return 如果插入成功，返回槽号；否则返回nullopt
 * 
 * 实现步骤：
 * 1. 检查是否有足够空间
 * 2. 在槽目录中添加新条目（包含偏移量、大小和元数据）
 * 3. 将元组数据复制到页面中的指定位置
 * 4. 更新元组计数
 */
auto TablePage::InsertTuple(const TupleMeta &meta, const Tuple &tuple) -> std::optional<uint16_t> {
  auto tuple_offset = GetNextTupleOffset(meta, tuple);
  if (tuple_offset == std::nullopt) {
    return std::nullopt;
  }
  auto tuple_id = num_tuples_;
  tuple_info_[tuple_id] = std::make_tuple(*tuple_offset, tuple.GetLength(), meta);
  num_tuples_++;
  memcpy(page_start_ + *tuple_offset, tuple.data_.data(), tuple.GetLength());
  return tuple_id;
}

/**
 * @brief 更新元组的元数据
 * @param meta 新的元数据
 * @param rid 要更新的元组的记录ID
 * 
 * 实现步骤：
 * 1. 验证槽号的有效性
 * 2. 如果元组从未删除变为已删除，增加已删除元组计数
 * 3. 更新槽目录中的元数据
 */
void TablePage::UpdateTupleMeta(const TupleMeta &meta, const RID &rid) {
  auto tuple_id = rid.GetSlotNum();
  if (tuple_id >= num_tuples_) {
    throw easydb::Exception("Tuple ID out of range");
  }
  auto &[offset, size, old_meta] = tuple_info_[tuple_id];
  if (!old_meta.is_deleted_ && meta.is_deleted_) {
    num_deleted_tuples_++;
  }
  tuple_info_[tuple_id] = std::make_tuple(offset, size, meta);
}

/**
 * @brief 从表中读取一个元组
 * @param rid 要读取的元组的记录ID
 * @return 元组的元数据和数据的pair
 * 
 * 实现步骤：
 * 1. 验证槽号的有效性
 * 2. 从槽目录中获取元组的偏移量、大小和元数据
 * 3. 从页面中复制元组数据
 * 4. 设置元组的RID
 */
auto TablePage::GetTuple(const RID &rid) const -> std::pair<TupleMeta, Tuple> {
  auto tuple_id = rid.GetSlotNum();
  if (tuple_id >= num_tuples_) {
    throw easydb::Exception("Tuple ID out of range");
  }
  auto &[offset, size, meta] = tuple_info_[tuple_id];
  Tuple tuple;
  tuple.data_.resize(size);
  memmove(tuple.data_.data(), page_start_ + offset, size);
  tuple.rid_ = rid;
  return std::make_pair(meta, std::move(tuple));
}

/**
 * @brief 从表中读取元组的元数据
 * @param rid 要读取的元组的记录ID
 * @return 元组的元数据
 * @note 只读取元数据，不读取实际数据（更高效）
 */
auto TablePage::GetTupleMeta(const RID &rid) const -> TupleMeta {
  auto tuple_id = rid.GetSlotNum();
  if (tuple_id >= num_tuples_) {
    throw easydb::Exception("Tuple ID out of range");
  }
  auto &[_1, _2, meta] = tuple_info_[tuple_id];
  return meta;
}

/**
 * @brief 原地更新元组（不安全版本）
 * @param meta 新的元数据
 * @param tuple 新的元组数据
 * @param rid 要更新的元组的记录ID
 * 
 * 实现步骤：
 * 1. 验证槽号的有效性
 * 2. 验证新元组的大小与旧元组匹配（否则抛出异常）
 * 3. 如果元组从未删除变为已删除，增加已删除元组计数
 * 4. 更新槽目录中的元数据
 * 5. 将新元组数据复制到页面中的原位置
 * 
 * @note "不安全"意味着不检查新元组的大小是否与旧元组匹配，调用者需要确保大小匹配
 */
void TablePage::UpdateTupleInPlaceUnsafe(const TupleMeta &meta, const Tuple &tuple, RID rid) {
  auto tuple_id = rid.GetSlotNum();
  if (tuple_id >= num_tuples_) {
    throw easydb::Exception("Tuple ID out of range");
  }
  auto &[offset, size, old_meta] = tuple_info_[tuple_id];
  if (size != tuple.GetLength()) {
    throw easydb::Exception("Tuple size mismatch");
  }
  if (!old_meta.is_deleted_ && meta.is_deleted_) {
    num_deleted_tuples_++;
  }
  tuple_info_[tuple_id] = std::make_tuple(offset, size, meta);
  memcpy(page_start_ + offset, tuple.data_.data(), tuple.GetLength());
}

}  // namespace easydb
