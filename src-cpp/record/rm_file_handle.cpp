/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * rm_file_handle.cpp
 *
 * Identification: src/record/rm_file_handle.cpp
 *
 *-------------------------------------------------------------------------
 */

/**
 * @file rm_file_handle.cpp
 * @brief 记录管理（Record Manager）文件句柄实现
 *
 * 本文件实现了表数据文件的页面和文件级别的操作接口，主要包括：
 * 1. RmPageHandle：页面级别的操作，包括元组的插入、删除、更新、查询
 * 2. RmFileHandle：文件级别的操作，包括页面的创建、获取、元组管理等
 *
 * 页面布局（Slotted Page Format）：
 * +--------------------------------------------------------+
 * | HEADER | ... FREE SPACE ... | ... INSERTED TUPLES ... |
 * +--------------------------------------------------------+
 *                              ^
 *                              free space pointer
 *
 * Header格式：
 * +----------------------------------------------------------------------------+
 * | NextPageId (4) | NumTuples(2) | NumDeletedTuples(2) | Tuple_1 info | ... |
 * +----------------------------------------------------------------------------+
 *
 * 元组格式：
 * | meta | data |
 */

#include "record/rm_file_handle.h"
#include "common/exception.h"
#include "common/macros.h"
#include "common/rid.h"

namespace easydb {

/**
 * @brief 计算下一个元组在页面中的偏移量
 * @param meta 元组的元数据（未使用，保留接口一致性）
 * @param tuple 要插入的元组
 * @return 如果页面有足够空间，返回元组的偏移量；否则返回 std::nullopt
 *
 * 算法说明：
 * 1. 页面采用从后向前分配空间的策略（slotted page）
 * 2. 如果页面已有记录，新元组插入到最后一个元组之前
 * 3. 如果页面为空，新元组插入到页面末尾（PAGE_SIZE位置）
 * 4. 检查是否有足够空间：需要确保元组数据区域不与槽目录区域重叠
 *
 * 空间检查逻辑：
 * - slot_end_offset：当前最后一个元组的起始位置（或页面末尾）
 * - tuple_offset：新元组的起始位置 = slot_end_offset - tuple.GetLength()
 * - offset_size：槽目录所需的空间 = 页面头 + (num_records + 1) * 元组信息大小
 * - 检查条件：slot_end_offset >= offset_size + tuple.GetLength()
 *   即：最后一个元组的位置 >= 槽目录结束位置 + 新元组大小
 */
auto RmPageHandle::GetNextTupleOffset(const TupleMeta &meta,
                                      const Tuple &tuple) const
    -> std::optional<uint16_t> {
  // 计算当前最后一个元组的结束位置（即新元组的起始位置）
  size_t slot_end_offset;
  if (page_hdr_->num_records > 0) {
    // 如果页面已有记录，获取最后一个元组的起始位置
    auto &[offset, size, meta] = tuple_info_[page_hdr_->num_records - 1];
    slot_end_offset = offset;
  } else {
    // 如果页面为空，从页面末尾开始分配
    slot_end_offset = PAGE_SIZE;
  }
  
  // 计算新元组的起始偏移量（从后向前分配）
  auto tuple_offset = slot_end_offset - tuple.GetLength();
  
  // 计算槽目录所需的总空间
  // TABLE_PAGE_HEADER_SIZE：页面头大小
  // TUPLE_INFO_SIZE * (page_hdr_->num_records + 1)：当前槽目录 + 新槽
  auto offset_size =
      TABLE_PAGE_HEADER_SIZE + TUPLE_INFO_SIZE * (page_hdr_->num_records + 1);
  
  // 检查是否有足够空间
  // 注意：不能使用 (tuple_offset < offset_size) 来判断，因为
  // slot_end_offset 可能小于 tuple.GetLength()（导致 tuple_offset 为负数）
  // 正确的检查：最后一个元组的位置必须 >= 槽目录结束位置 + 新元组大小
  if (slot_end_offset < offset_size + tuple.GetLength()) {
    return std::nullopt;  // 空间不足
  }
  
  return tuple_offset;
}

/**
 * @brief 在页面中插入一个元组
 * @param meta 元组的元数据（包含是否删除等标志）
 * @param tuple 要插入的元组数据
 * @return 如果插入成功，返回元组的槽号（slot number）；否则返回 std::nullopt
 *
 * 插入流程：
 * 1. 检查页面是否有足够空间
 * 2. 分配槽号（使用当前记录数作为新槽号）
 * 3. 更新槽目录：记录元组的偏移量、大小和元数据
 * 4. 增加页面记录计数
 * 5. 将元组数据复制到页面中
 *
 * @note 调用者需要确保页面有足够空间（通过 GetNextTupleOffset 检查）
 */
auto RmPageHandle::InsertTuple(const TupleMeta &meta, const Tuple &tuple)
    -> std::optional<uint16_t> {
  // 1. 检查并获取元组的插入位置
  auto tuple_offset = GetNextTupleOffset(meta, tuple);
  if (tuple_offset == std::nullopt) {
    return std::nullopt;  // 空间不足，插入失败
  }
  
  // 2. 分配槽号（使用当前记录数作为新槽号）
  auto tuple_id = page_hdr_->num_records;
  
  // 3. 更新槽目录：记录元组的偏移量、大小和元数据
  tuple_info_[tuple_id] =
      std::make_tuple(*tuple_offset, tuple.GetLength(), meta);
  
  // 4. 增加页面记录计数
  page_hdr_->num_records++;
  
  // 5. 将元组数据复制到页面的指定位置
  memcpy(page_start_ + *tuple_offset, tuple.data_.data(), tuple.GetLength());
  
  return tuple_id;
}

/**
 * @brief 更新指定元组的元数据
 * @param meta 新的元数据
 * @param rid 元组的记录ID（包含页面号和槽号）
 *
 * 更新流程：
 * 1. 验证槽号的有效性
 * 2. 如果元组从非删除状态变为删除状态，增加删除计数
 * 3. 更新槽目录中的元数据（保持偏移量和大小不变）
 *
 * @note 此函数只更新元数据，不修改元组的实际数据
 * @note 如果槽号超出范围，抛出异常
 */
void RmPageHandle::UpdateTupleMeta(const TupleMeta &meta, const RID &rid) {
  // 1. 获取槽号并验证有效性
  auto tuple_id = rid.GetSlotNum();
  if (tuple_id >= page_hdr_->num_records) {
    throw easydb::Exception("Tuple ID out of range");
  }
  
  // 2. 获取旧的元数据
  auto &[offset, size, old_meta] = tuple_info_[tuple_id];
  
  // 3. 如果元组从非删除状态变为删除状态，更新删除计数
  if (!old_meta.is_deleted_ && meta.is_deleted_) {
    page_hdr_->num_deleted_records++;
  }
  
  // 4. 更新槽目录中的元数据（保持偏移量和大小不变）
  tuple_info_[tuple_id] = std::make_tuple(offset, size, meta);
}

/**
 * @brief 从页面中读取指定元组
 * @param rid 元组的记录ID（包含页面号和槽号）
 * @return 返回元组的元数据和数据的pair
 *
 * 读取流程：
 * 1. 验证槽号的有效性
 * 2. 从槽目录获取元组的偏移量、大小和元数据
 * 3. 从页面中复制元组数据到 Tuple 对象
 * 4. 设置 Tuple 的 RID
 *
 * @note 使用 memmove 而不是 memcpy，因为源和目标可能重叠
 * @note 如果槽号超出范围，抛出异常
 */
auto RmPageHandle::GetTuple(const RID &rid) const
    -> std::pair<TupleMeta, Tuple> {
  // 1. 获取槽号并验证有效性
  auto tuple_id = rid.GetSlotNum();
  if (tuple_id >= page_hdr_->num_records) {
    throw easydb::Exception("Tuple ID out of range");
  }
  
  // 2. 从槽目录获取元组信息（偏移量、大小、元数据）
  auto &[offset, size, meta] = tuple_info_[tuple_id];
  
  // 3. 创建 Tuple 对象并分配空间
  Tuple tuple;
  tuple.data_.resize(size);
  
  // 4. 从页面中复制元组数据（使用 memmove 处理可能的重叠）
  memmove(tuple.data_.data(), page_start_ + offset, size);
  
  // 5. 设置元组的 RID
  tuple.rid_ = rid;
  
  return std::make_pair(meta, std::move(tuple));
}

/**
 * @brief 获取指定元组的元数据（不读取实际数据）
 * @param rid 元组的记录ID（包含页面号和槽号）
 * @return 元组的元数据
 *
 * 此函数只读取元数据，不复制元组的实际数据，性能更高。
 *
 * @note 如果槽号超出范围，抛出异常
 */
auto RmPageHandle::GetTupleMeta(const RID &rid) const -> TupleMeta {
  // 1. 获取槽号并验证有效性
  auto tuple_id = rid.GetSlotNum();
  if (tuple_id >= page_hdr_->num_records) {
    throw easydb::Exception("Tuple ID out of range");
  }
  
  // 2. 从槽目录中提取元数据（忽略偏移量和大小）
  auto &[_1, _2, meta] = tuple_info_[tuple_id];
  
  return meta;
}

/**
 * @brief 原地更新元组（不安全版本）
 * @param meta 新的元数据
 * @param tuple 新的元组数据
 * @param rid 要更新的元组的记录ID
 *
 * "不安全"的含义：
 * - 不检查新元组的大小是否与旧元组完全匹配
 * - 只要求新元组大小 <= 旧元组大小（允许缩小）
 * - 如果新元组更大，会抛出异常
 *
 * 更新流程：
 * 1. 验证槽号的有效性
 * 2. 检查新元组大小是否超过旧元组大小
 * 3. 如果元组从非删除状态变为删除状态，更新删除计数
 * 4. 更新槽目录中的元数据
 * 5. 将新元组数据复制到页面中的原位置
 *
 * @note 此函数假设新元组可以放入原位置，不进行空间检查
 * @note 如果新元组更大，抛出异常
 */
void RmPageHandle::UpdateTupleInPlaceUnsafe(const TupleMeta &meta,
                                            const Tuple &tuple, RID rid) {
  // 1. 获取槽号并验证有效性
  auto tuple_id = rid.GetSlotNum();
  if (tuple_id >= page_hdr_->num_records) {
    throw easydb::Exception("Tuple ID out of range");
  }
  
  // 2. 获取旧的元组信息
  auto &[offset, size, old_meta] = tuple_info_[tuple_id];
  
  // 3. 检查新元组大小是否超过旧元组大小
  // 注意：允许新元组更小（缩小），但不允许更大（扩大）
  if (size < tuple.GetLength()) {
    throw easydb::Exception("Tuple size mismatch");
  }
  
  // 4. 如果元组从非删除状态变为删除状态，更新删除计数
  if (!old_meta.is_deleted_ && meta.is_deleted_) {
    page_hdr_->num_deleted_records++;
  }
  
  // 5. 更新槽目录中的元数据（保持偏移量和大小不变）
  tuple_info_[tuple_id] = std::make_tuple(offset, size, meta);
  
  // 6. 将新元组数据复制到页面中的原位置
  memcpy(page_start_ + offset, tuple.data_.data(), tuple.GetLength());
}

/**
 * @brief 检查指定元组是否已被删除
 * @param rid 元组的记录ID（包含页面号和槽号）
 * @return true 如果元组已被删除，false 否则
 *
 * 此函数通过读取元数据的 is_deleted_ 标志来判断元组是否被删除。
 */
auto RmPageHandle::IsTupleDeleted(const RID &rid) -> bool {
  auto meta = GetTupleMeta(rid);
  return meta.is_deleted_;
}

/**
 * @brief 在文件中插入一个元组（自动分配位置）
 * @param meta 元组的元数据
 * @param tuple 要插入的元组数据
 * @param context 事务上下文（用于锁管理，可为 nullptr）
 * @return 如果插入成功，返回元组的记录ID（RID）；否则返回 std::nullopt
 *
 * 插入流程：
 * 1. 获取或创建第一个空闲页面
 * 2. 在页面中查找空闲槽位：
 *    - 如果当前页面有空间，直接插入
 *    - 如果当前页面已满，创建新页面并链接
 *    - 如果空页面都无法容纳元组，说明元组太大，抛出异常
 * 3. 获取排他锁（如果提供了上下文）
 * 4. 在页面中插入元组（更新槽目录和复制数据）
 * 5. 取消固定页面（标记为脏页）
 *
 * 页面链接策略：
 * - 当页面满时，创建新页面
 * - 将旧页面的 next_page_id 设置为新页面号
 * - 新页面成为新的空闲页面
 *
 * @note 调用者不需要手动管理页面的 pin/unpin（函数内部处理）
 * @note 如果元组太大无法放入空页面，会抛出异常
 */
auto RmFileHandle::InsertTuple(const TupleMeta &meta, const Tuple &tuple,
                               Context *context) -> std::optional<RID> {
  // 1. 获取或创建第一个空闲页面句柄
  RmPageHandle page_handle = CreatePageHandle();
  int page_no = page_handle.page->GetPageId().page_no;
  std::optional<uint16_t> tuple_offset;

  // 2. 在页面中查找空闲槽位
  while (true) {
    // 检查当前页面是否有足够空间
    tuple_offset = page_handle.GetNextTupleOffset(meta, tuple);
    if (tuple_offset != std::nullopt) {
      break;  // 找到空闲槽位，退出循环
    }

    // 如果页面为空且无法插入，说明元组太大
    // 注意：空页面应该能容纳任何合理大小的元组
    EASYDB_ENSURE(page_handle.GetNumTuples() != 0,
                  "tuple is too large, cannot insert");

    // 当前页面已满，创建新页面
    auto new_page_handle = CreateNewPageHandle();
    
    // 将旧页面的 next_page_id 设置为新页面号（链接页面）
    page_handle.SetNextPageId(new_page_handle.page->GetPageId().page_no);
    
    // 取消固定旧页面（未修改，所以 is_dirty = false）
    buffer_pool_manager_->UnpinPage(page_handle.page->GetPageId(), false);

    // 切换到新页面继续查找
    page_handle = std::move(new_page_handle);
    page_no = page_handle.page->GetPageId().page_no;
  }

  // 3. 插入元组到找到的空闲槽位
  // 分配槽号（使用当前记录数作为新槽号）
  auto slot_no = page_handle.page_hdr_->num_records;
  auto rid = RID(page_no, slot_no);
  
  // 4. 获取排他锁（如果提供了事务上下文）
  if (context != nullptr) {
    context->lock_mgr_->LockExclusiveOnRecord(context->txn_, rid, fd_);
  }

  // 5. 更新槽目录：记录元组的偏移量、大小和元数据
  page_handle.tuple_info_[slot_no] =
      std::make_tuple(*tuple_offset, tuple.GetLength(), meta);
  
  // 6. 增加页面记录计数
  page_handle.page_hdr_->num_records++;
  
  // 7. 将元组数据复制到页面的指定位置
  memcpy(page_handle.page_start_ + *tuple_offset, tuple.data_.data(),
         tuple.GetLength());

  // 8. 取消固定页面（标记为脏页，需要写回磁盘）
  buffer_pool_manager_->UnpinPage(page_handle.page->GetPageId(), true);

  return rid;
}

/**
 * @brief 在指定位置插入元组（用于事务回滚和恢复）
 * @param rid 指定的记录ID（页面号和槽号）
 * @param meta 元组的元数据
 * @param tuple 要插入的元组数据（未使用，保留接口一致性）
 * @param context 事务上下文（用于锁管理，可为 nullptr）
 * @return 如果插入成功返回 true
 *
 * 此函数主要用于事务回滚和系统故障恢复场景：
 * - 恢复被删除的元组（将删除标记设为 false）
 * - 如果元组已存在且未删除，抛出异常
 *
 * 流程：
 * 1. 获取排他锁（如果提供了上下文）
 * 2. 获取指定页面句柄
 * 3. 读取旧元组信息
 * 4. 如果元组已被删除，恢复它（取消删除标记）
 * 5. 如果元组已存在且未删除，抛出异常
 *
 * @note 此函数不实际插入新数据，只恢复已删除的元组
 * @note 主要用于事务回滚和 WAL 恢复
 */
auto RmFileHandle::InsertTuple(RID rid, const TupleMeta &meta,
                               const Tuple &tuple, Context *context) -> bool {
  // 1. 获取排他锁（如果提供了事务上下文）
  if (context != nullptr) {
    context->lock_mgr_->LockExclusiveOnRecord(context->txn_, rid, fd_);
  }
  
  // 2. 获取指定页面句柄
  RmPageHandle page_handle = FetchPageHandle(rid.GetPageId());
  
  // 3. 读取旧元组信息
  auto [old_meta, old_tup] = page_handle.GetTuple(rid);
  
  // 4. 如果元组已被删除，恢复它
  if (old_meta.is_deleted_) {
    old_meta.is_deleted_ = false;
    page_handle.UpdateTupleMeta(old_meta, rid);
  } else {
    // 5. 如果元组已存在且未删除，抛出异常
    throw Exception(
        "RmFileHandle::InsertTuple(Rollback) Error: Tuple already exists");
  }
  
  // 6. 取消固定页面（标记为脏页）
  buffer_pool_manager_->UnpinPage(page_handle.page->GetPageId(), true);
  
  return true;
}

/**
 * @brief 删除指定位置的元组（逻辑删除）
 * @param rid 要删除的元组的记录ID（页面号和槽号）
 * @param context 事务上下文（用于锁管理，可为 nullptr）
 * @return 如果删除成功返回 true
 *
 * 删除流程：
 * 1. 获取排他锁（如果提供了上下文）
 * 2. 获取指定页面句柄
 * 3. 读取元组信息
 * 4. 检查元组是否已被删除（防止重复删除）
 * 5. 将元组的删除标记设为 true（逻辑删除）
 * 6. 更新页面元数据（增加删除计数）
 *
 * 注意：
 * - 这是逻辑删除，不会实际删除数据，只是标记为已删除
 * - 删除的元组空间可以被后续插入重用
 * - 如果元组已被删除，抛出异常
 *
 * @note 调用者不需要手动管理页面的 pin/unpin（函数内部处理）
 */
auto RmFileHandle::DeleteTuple(RID rid, Context *context) -> bool {
  // 1. 获取排他锁（如果提供了事务上下文）
  if (context != nullptr) {
    context->lock_mgr_->LockExclusiveOnRecord(context->txn_, rid, fd_);
  }

  // 2. 获取指定页面句柄
  RmPageHandle page_handle = FetchPageHandle(rid.GetPageId());
  
  // 3. 读取元组信息
  auto [meta, tuple] = page_handle.GetTuple(rid);
  
  // 4. 检查元组是否已被删除
  if (meta.is_deleted_) {
    throw InternalError(
        "RmFileHandle::DeleteTuple Error: Tuple already deleted");
  }
  
  // 5. 将元组的删除标记设为 true（逻辑删除）
  meta.is_deleted_ = true;
  page_handle.UpdateTupleMeta(meta, rid);
  
  // 6. 取消固定页面（标记为脏页）
  buffer_pool_manager_->UnpinPage(page_handle.page->GetPageId(), true);
  
  return true;
}

/**
 * @brief 原地更新指定位置的元组
 * @param meta 新的元数据
 * @param tuple 新的元组数据
 * @param rid 要更新的元组的记录ID（页面号和槽号）
 * @param context 事务上下文（用于锁管理，可为 nullptr）
 * @param check 可选的检查函数，用于验证是否可以更新
 * @return 如果更新成功返回 true，否则返回 false
 *
 * 更新流程：
 * 1. 获取排他锁（如果提供了上下文）
 * 2. 获取指定页面句柄
 * 3. 读取旧元组信息
 * 4. 如果提供了检查函数，执行检查：
 *    - 如果检查失败，取消固定页面（不标记为脏）并返回 false
 *    - 如果检查通过或未提供检查函数，继续更新
 * 5. 调用 UpdateTupleInPlaceUnsafe 执行更新
 * 6. 取消固定页面（标记为脏页）
 *
 * 检查函数说明：
 * - check 函数接收旧元数据、旧元组和 RID 作为参数
 * - 返回 true 表示允许更新，false 表示不允许更新
 * - 可用于实现乐观并发控制（OCC）等机制
 *
 * @note 新元组大小必须 <= 旧元组大小（不允许扩大）
 * @note 如果检查失败，页面不会被标记为脏
 */
auto RmFileHandle::UpdateTupleInPlace(
    const TupleMeta &meta, const Tuple &tuple, RID rid, Context *context,
    std::function<bool(const TupleMeta &meta, const Tuple &table, RID rid)>
        &&check) -> bool {
  // 1. 获取排他锁（如果提供了事务上下文）
  if (context != nullptr) {
    context->lock_mgr_->LockExclusiveOnRecord(context->txn_, rid, fd_);
  }
  
  // 2. 获取指定页面句柄
  RmPageHandle page_handle = FetchPageHandle(rid.GetPageId());
  
  // 3. 读取旧元组信息
  auto [old_meta, old_tup] = page_handle.GetTuple(rid);
  
  // 4. 执行可选的检查函数
  if (check == nullptr || check(old_meta, old_tup, rid)) {
    // 5. 检查通过，执行更新
    page_handle.UpdateTupleInPlaceUnsafe(meta, tuple, rid);
    
    // 6. 取消固定页面（标记为脏页）
    buffer_pool_manager_->UnpinPage(page_handle.page->GetPageId(), true);
    return true;
  }
  
  // 检查失败，取消固定页面（不标记为脏）
  buffer_pool_manager_->UnpinPage(page_handle.page->GetPageId(), false);
  return false;
}

/**
 * @brief 更新指定元组的元数据（不修改实际数据）
 * @param meta 新的元数据
 * @param rid 要更新的元组的记录ID（页面号和槽号）
 * @param context 事务上下文（用于锁管理，可为 nullptr）
 *
 * 此函数只更新元数据（如删除标记），不修改元组的实际数据。
 * 主要用于：
 * - 标记元组为已删除
 * - 更新元组的其他元数据字段
 *
 * 流程：
 * 1. 获取排他锁（如果提供了上下文）
 * 2. 获取指定页面句柄
 * 3. 调用页面句柄的 UpdateTupleMeta 更新元数据
 * 4. 取消固定页面（标记为脏页）
 *
 * @note 调用者不需要手动管理页面的 pin/unpin（函数内部处理）
 */
void RmFileHandle::UpdateTupleMeta(const TupleMeta &meta, RID rid,
                                   Context *context) {
  // 1. 获取排他锁（如果提供了事务上下文）
  if (context != nullptr) {
    context->lock_mgr_->LockExclusiveOnRecord(context->txn_, rid, fd_);
  }
  
  // 2. 获取指定页面句柄
  RmPageHandle page_handle = FetchPageHandle(rid.GetPageId());
  
  // 3. 更新元数据
  page_handle.UpdateTupleMeta(meta, rid);
  
  // 4. 取消固定页面（标记为脏页）
  buffer_pool_manager_->UnpinPage(page_handle.page->GetPageId(), true);
}

/**
 * @brief 从文件中读取指定位置的元组
 * @param rid 要读取的元组的记录ID（页面号和槽号）
 * @param context 事务上下文（用于锁管理，可为 nullptr）
 * @return 返回元组的元数据和数据的pair
 *
 * 读取流程：
 * 1. 获取共享锁（如果提供了上下文）
 * 2. 获取指定页面句柄
 * 3. 从页面中读取元组
 * 4. 设置元组的 RID
 * 5. 取消固定页面（未修改，所以不标记为脏）
 *
 * @note 使用共享锁，允许多个事务同时读取
 * @note 调用者不需要手动管理页面的 pin/unpin（函数内部处理）
 */
auto RmFileHandle::GetTuple(RID rid, Context *context)
    -> std::pair<TupleMeta, Tuple> {
  // 1. 获取共享锁（如果提供了事务上下文）
  if (context != nullptr) {
    context->lock_mgr_->LockSharedOnRecord(context->txn_, rid, fd_);
  }
  
  // 2. 获取指定页面句柄
  RmPageHandle page_handle = FetchPageHandle(rid.GetPageId());
  
  // 3. 从页面中读取元组
  auto [meta, tuple] = page_handle.GetTuple(rid);
  
  // 4. 取消固定页面（未修改，所以 is_dirty = false）
  buffer_pool_manager_->UnpinPage(page_handle.page->GetPageId(), false);
  
  // 5. 设置元组的 RID（确保 RID 正确）
  tuple.rid_ = rid;
  
  return std::make_pair(meta, std::move(tuple));
}

/**
 * @brief 获取指定元组的元数据（不读取实际数据）
 * @param rid 要读取的元组的记录ID（页面号和槽号）
 * @param context 事务上下文（用于锁管理，可为 nullptr）
 * @return 元组的元数据
 *
 * 此函数只读取元数据，不复制元组的实际数据，性能更高。
 * 主要用于：
 * - 检查元组是否被删除
 * - 获取元组的其他元数据信息
 *
 * 流程：
 * 1. 获取共享锁（如果提供了上下文）
 * 2. 获取指定页面句柄
 * 3. 从页面中读取元数据
 * 4. 取消固定页面（未修改，所以不标记为脏）
 *
 * @note 使用共享锁，允许多个事务同时读取
 * @note 调用者不需要手动管理页面的 pin/unpin（函数内部处理）
 */
auto RmFileHandle::GetTupleMeta(RID rid, Context *context) -> TupleMeta {
  // 1. 获取共享锁（如果提供了事务上下文）
  if (context != nullptr) {
    context->lock_mgr_->LockSharedOnRecord(context->txn_, rid, fd_);
  }
  
  // 2. 获取指定页面句柄
  RmPageHandle page_handle = FetchPageHandle(rid.GetPageId());
  
  // 3. 从页面中读取元数据（不读取实际数据）
  TupleMeta meta = page_handle.GetTupleMeta(rid);
  
  // 4. 取消固定页面（未修改，所以 is_dirty = false）
  buffer_pool_manager_->UnpinPage(page_handle.page->GetPageId(), false);
  
  return meta;
}

/**
 * @description: 获取当前表中记录号为rid的记录
 * @param {RID&} rid 记录号，指定记录的位置
 * @param {Context*} context
 * @return {unique_ptr<RmRecord>} rid对应的记录对象指针
 */
//  std::unique_ptr<RmRecord> RmFileHandle::get_record(const RID &rid, Context
//  *context)
// auto RmFileHandle::GetRecord(const RID &rid) -> std::unique_ptr<RmRecord> {
//   // Todo:
//   // 1. 获取指定记录所在的page handle
//   // 2. 初始化一个指向RmRecord的指针（赋值其内部的data和size）
//   // return nullptr;
//   //   // lock manager
//   //   if (context != nullptr) {
//   //     context->lock_mgr_->LockSharedOnRecord(context->txn_, rid, fd_);
//   //   }
//   // 1. Fetch the page handle for the page that contains the record
//   RmPageHandle page_handle = FetchPageHandle(rid.GetPageId());
//   // 2. Initialize a unique pointer to RmRecord
//   auto [meta, tuple] = page_handle.GetTuple(rid);
//   tuple.rid_ = rid;
//   // return std::make_pair(meta, std::move(tuple));
//   auto record = std::make_unique<RmRecord>(tuple.GetLength(),
//   tuple.data_.data());
//   // Unpin the page
//   buffer_pool_manager_->UnpinPage({fd_, rid.GetPageId()}, false);
//   return record;
// }

/**
 * @brief 获取指定位置的元组值（返回 unique_ptr）
 * @param rid 要读取的元组的记录ID（页面号和槽号）
 * @param context 事务上下文（用于锁管理，可为 nullptr）
 * @return 指向元组的 unique_ptr
 *
 * 此函数与 GetTuple 功能类似，但返回 unique_ptr 而不是 pair。
 * 主要用于需要返回指针的场景。
 *
 * 流程：
 * 1. 获取共享锁（如果提供了上下文）
 * 2. 获取指定页面句柄
 * 3. 从页面中读取元组
 * 4. 取消固定页面（未修改，所以不标记为脏）
 * 5. 返回元组的 unique_ptr
 *
 * @note 使用共享锁，允许多个事务同时读取
 * @note 调用者不需要手动管理页面的 pin/unpin（函数内部处理）
 */
auto RmFileHandle::GetTupleValue(const RID &rid, Context *context)
    -> std::unique_ptr<Tuple> {
  // 1. 获取共享锁（如果提供了事务上下文）
  if (context != nullptr) {
    context->lock_mgr_->LockSharedOnRecord(context->txn_, rid, fd_);
  }

  // 2. 获取指定页面句柄
  RmPageHandle page_handle = FetchPageHandle(rid.GetPageId());

  // 3. 从页面中读取元组
  auto [meta, tuple] = page_handle.GetTuple(rid);

  // 4. 取消固定页面（未修改，所以 is_dirty = false）
  buffer_pool_manager_->UnpinPage({fd_, rid.GetPageId()}, false);

  // 5. 返回元组的 unique_ptr
  return std::make_unique<Tuple>(tuple);
}

/**
 * @brief 从元组中提取键值元组（用于索引）
 * @param schema 表的完整模式
 * @param key_schema 键的模式（索引列的模式）
 * @param key_attrs 键属性的索引列表（在完整模式中的位置）
 * @param rid 要读取的元组的记录ID（页面号和槽号）
 * @param context 事务上下文（用于锁管理，可为 nullptr）
 * @return 提取的键值元组
 *
 * 此函数用于从完整元组中提取索引键，主要用于：
 * - 构建索引条目
 * - 索引查找和更新
 *
 * 流程：
 * 1. 获取共享锁（如果提供了上下文）
 * 2. 获取指定页面句柄
 * 3. 从页面中读取完整元组
 * 4. 从完整元组中提取键值（根据 key_attrs）
 * 5. 取消固定页面（未修改，所以不标记为脏）
 * 6. 返回键值元组
 *
 * @note 使用共享锁，允许多个事务同时读取
 * @note 调用者不需要手动管理页面的 pin/unpin（函数内部处理）
 */
auto RmFileHandle::GetKeyTuple(const Schema &schema, const Schema &key_schema,
                               const std::vector<uint32_t> &key_attrs,
                               const RID &rid, Context *context) -> Tuple {
  // 1. 获取共享锁（如果提供了事务上下文）
  if (context != nullptr) {
    context->lock_mgr_->LockSharedOnRecord(context->txn_, rid, fd_);
  }

  // 2. 获取指定页面句柄
  RmPageHandle page_handle = FetchPageHandle(rid.GetPageId());

  // 3. 从页面中读取完整元组
  auto [meta, tuple] = page_handle.GetTuple(rid);
  
  // 4. 从完整元组中提取键值（根据 key_attrs 指定的属性）
  auto key_tuple = tuple.KeyFromTuple(schema, key_schema, key_attrs);

  // 5. 取消固定页面（未修改，所以 is_dirty = false）
  buffer_pool_manager_->UnpinPage({fd_, rid.GetPageId()}, false);
  
  return key_tuple;
}

// /**
//  * @description: 在当前表中插入一条记录，不指定插入位置
//  * @param {char*} buf 要插入的记录的数据
//  * @param {Context*} context
//  * @return {RID} 插入的记录的记录号（位置）
//  */
// // RID RmFileHandle::insert_record(char *buf, Context *context) {
// RID RmFileHandle::InsertRecord(char *buf) {
//   // Todo:
//   // 1. 获取当前未满的page handle
//   // 2. 在page handle中找到空闲slot位置
//   // 3. 将buf复制到空闲slot位置
//   // 4. 更新page_handle.page_hdr中的数据结构
//   //
//   注意考虑插入一条记录后页面已满的情况，需要更新file_hdr_.first_free_page_no
//   // return RID{-1, -1};
//   throw InternalError("RmFileHandle::insert_record removed, use InsertTuple
//   instead.");
// }

// /**
//  * @description: 在当前表中的指定位置插入一条记录
//  * @param {RID&} rid 要插入记录的位置
//  * @param {char*} buf 要插入记录的数据
//  * @note 该函数主要用于事务的回滚和系统故障恢复
//  */
// void RmFileHandle::insert_record(const RID &rid, char *buf) {
// void RmFileHandle::InsertRecord(const RID &rid, char *buf) {
//   throw InternalError("RmFileHandle::insert_record not implemented");
// }

/**
 * @description: 删除记录文件中记录号为rid的记录
 * @param {RID&} rid 要删除的记录的记录号（位置）
 * @param {Context*} context
 */
// void RmFileHandle::delete_record(const RID &rid, Context *context) {
// void RmFileHandle::DeleteRecord(const RID &rid) {
//   // Todo:
//   // 1. 获取指定记录所在的page handle
//   // 2. 更新page_handle.page_hdr中的数据结构
//   // 注意考虑删除一条记录后页面未满的情况，需要调用release_page_handle()
//   throw InternalError("RmFileHandle::delete_record removed, use DeleteTuple
//   instead.");
// }

/**
 * @description: 更新记录文件中记录号为rid的记录
 * @param {RID&} rid 要更新的记录的记录号（位置）
 * @param {char*} buf 新记录的数据
 * @param {Context*} context
 */
// void RmFileHandle::update_record(const RID &rid, char *buf, Context *context)
// { void RmFileHandle::UpdateRecord(const RID &rid, char *buf) {
//   // Todo:
//   // 1. 获取指定记录所在的page handle
//   // 2. 更新记录
//   throw InternalError("RmFileHandle::update_record removed, use
//   UpdateTupleInPlace instead.");
// }

/**
 * 以下函数为辅助函数，仅提供参考，可以选择完成如下函数，也可以删除如下函数，在单元测试中不涉及如下函数接口的直接调用
 */

/**
 * @brief 获取指定页面的页面句柄
 * @param page_no 页面号
 * @return 指定页面的句柄
 *
 * 此函数从缓冲池中获取指定页面，并创建页面句柄返回给上层。
 *
 * 流程：
 * 1. 验证页面号的有效性（必须在有效范围内）
 * 2. 构造页面ID（文件描述符 + 页面号）
 * 3. 从缓冲池中获取页面（会 pin 页面）
 * 4. 创建并返回页面句柄
 *
 * 注意事项：
 * - 此函数会 pin 页面，调用者必须调用 UnpinPage 进行 unpin
 * - 如果页面号无效，抛出 PageNotExistError 异常
 * - 如果缓冲池获取失败，抛出 InternalError 异常
 *
 * @note 调用者必须负责 unpin 页面，否则会导致页面无法被替换
 */
RmPageHandle RmFileHandle::FetchPageHandle(page_id_t page_no) const {
  // 1. 验证页面号的有效性
  // 页面号必须在 [0, file_hdr_.num_pages) 范围内
  if (page_no < 0 || page_no >= file_hdr_.num_pages) {
    throw PageNotExistError("", page_no);
  }

  // 2. 构造页面ID（文件描述符 + 页面号）
  PageId page_id{fd_, page_no};
  
  // 3. 从缓冲池中获取页面（会 pin 页面，增加引用计数）
  Page *page = buffer_pool_manager_->FetchPage(page_id);

  // 4. 检查页面是否获取成功
  if (page == nullptr) {
    throw InternalError(
        "RmFileHandle::FetchPageHandle Error: Failed to fetch page");
  }

  // 5. 创建并返回页面句柄
  return RmPageHandle(&file_hdr_, page);
}

/**
 * @brief 创建一个新的页面句柄
 * @return 新的页面句柄
 *
 * 此函数创建一个新的数据页面，并初始化页面头和文件头。
 *
 * 流程：
 * 1. 使用缓冲池创建新页面（会 pin 页面）
 * 2. 创建页面句柄并初始化页面头：
 *    - next_page_id = RM_NO_PAGE (-1，表示最后一个页面)
 *    - num_records = 0（初始无记录）
 *    - num_deleted_records = 0（初始无删除记录）
 * 3. 更新文件头：
 *    - num_pages++（增加页面计数）
 *    - first_free_page_no = 新页面号（新页面成为第一个空闲页面）
 * 4. 将更新的文件头写回磁盘
 *
 * 注意事项：
 * - 此函数会 pin 页面，调用者必须调用 UnpinPage 进行 unpin
 * - 新页面自动成为第一个空闲页面
 * - 文件头会立即写回磁盘，确保一致性
 *
 * @note 调用者必须负责 unpin 页面，否则会导致页面无法被替换
 */
RmPageHandle RmFileHandle::CreateNewPageHandle() {
  // 1. 使用缓冲池创建新页面
  PageId new_page_id;
  new_page_id.fd = fd_;
  Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);

  // 检查页面创建是否成功
  if (new_page == nullptr) {
    throw InternalError(
        "RmFileHandle::CreateNewPageHandle Error: Failed to create new page");
  }

  // 2. 创建页面句柄并初始化页面头
  RmPageHandle new_page_handle(&file_hdr_, new_page);
  
  // 初始化新页面头（设置默认值）
  // - next_page_id = RM_NO_PAGE (-1)
  // - num_records = 0
  // - num_deleted_records = 0
  new_page_handle.page_hdr_->Init();

  // 3. 更新文件头
  file_hdr_.num_pages++;  // 增加页面计数
  file_hdr_.first_free_page_no = new_page_id.page_no;  // 新页面成为第一个空闲页面

  // 4. 将更新的文件头写回磁盘（确保一致性）
  disk_manager_->WritePage(fd_, RM_FILE_HDR_PAGE, (char *)&file_hdr_,
                           sizeof(file_hdr_));

  return new_page_handle;
}

/**
 * @brief 设置指定页面的日志序列号（LSN）
 * @param page_id_ 要设置 LSN 的页面号
 * @param lsn 要设置的日志序列号
 *
 * LSN（Log Sequence Number）用于：
 * - 故障恢复：确定页面需要从哪个日志位置开始恢复
 * - 并发控制：跟踪页面的修改历史
 *
 * 流程：
 * 1. 从缓冲池获取指定页面（会 pin 页面）
 * 2. 设置页面的 LSN
 * 3. 取消固定页面（标记为脏页，因为 LSN 已更新）
 *
 * @note 此函数会自动处理页面的 pin/unpin
 * @note 如果页面获取失败，抛出 InternalError 异常
 */
void RmFileHandle::SetPageLSN(page_id_t page_id_, lsn_t lsn) {
  // 1. 从缓冲池获取指定页面
  PageId page_id{fd_, page_id_};
  Page *page = buffer_pool_manager_->FetchPage(page_id);

  // 检查页面是否获取成功
  if (page == nullptr) {
    throw InternalError("RmFileHandle::set_page_lsn: Failed to fetch page");
  }
  
  // 2. 设置页面的 LSN
  page->SetLSN(lsn);
  
  // 3. 取消固定页面（标记为脏页，因为 LSN 已更新）
  buffer_pool_manager_->UnpinPage(page_id, true);
}

/**
 * @brief 创建或获取一个空闲的页面句柄
 * @return 返回生成的空闲页面句柄
 *
 * 此函数用于获取一个可以插入新元组的页面：
 * - 如果文件中有空闲页面，返回第一个空闲页面
 * - 如果文件中没有空闲页面，创建新页面并返回
 *
 * 流程：
 * 1. 检查文件头中是否有空闲页面（first_free_page_no）
 * 2. 如果没有空闲页面（first_free_page_no == RM_NO_PAGE）：
 *    - 调用 CreateNewPageHandle 创建新页面
 * 3. 如果有空闲页面：
 *    - 调用 FetchPageHandle 获取第一个空闲页面
 * 4. 返回页面句柄
 *
 * 注意事项：
 * - 此函数会 pin 页面，调用者必须调用 UnpinPage 进行 unpin
 * - 空闲页面是指有空间可以插入新元组的页面
 * - 新创建的页面自动成为第一个空闲页面
 *
 * @note 调用者必须负责 unpin 页面，否则会导致页面无法被替换
 */
RmPageHandle RmFileHandle::CreatePageHandle() {
  // 1. 检查文件头中是否有空闲页面
  int page_no = file_hdr_.first_free_page_no;
  
  if (page_no == RM_NO_PAGE) {
    // 1.1 没有空闲页面：创建新页面
    // CreateNewPageHandle 会：
    // - 创建新页面
    // - 初始化页面头
    // - 更新文件头（num_pages++, first_free_page_no = 新页面号）
    // - 写回文件头到磁盘
    return CreateNewPageHandle();
  }

  // 1.2 有空闲页面：获取第一个空闲页面
  // FetchPageHandle 会：
  // - 验证页面号有效性
  // - 从缓冲池获取页面（pin 页面）
  RmPageHandle page_handle = FetchPageHandle(page_no);

  // 2. 返回页面句柄
  return page_handle;
}

/**
 * @brief 释放页面句柄（当页面从已满变为未满时调用）
 * @param page_handle 要释放的页面句柄
 *
 * 当一个页面从没有空闲空间的状态变为有空闲空间状态时，需要更新空闲页面链表。
 * 此函数用于维护空闲页面的链表结构。
 *
 * 更新逻辑：
 * 1. 将文件头中的 first_free_page_no 设置为当前页面号
 *    （当前页面成为第一个空闲页面）
 * 2. 将当前页面的 next_free_page_no 设置为原 first_free_page_no
 *    （将当前页面插入到空闲页面链表的头部）
 * 3. 将更新的文件头写回磁盘
 *
 * 空闲页面链表结构：
 * file_hdr_.first_free_page_no -> page1 -> page2 -> ... -> RM_NO_PAGE
 *
 * 注意：
 * - 此函数应该在删除元组后调用，当页面从已满变为未满时
 * - 当前实现只更新了文件头，未更新页面头的 next_free_page_no
 *   （代码中有注释掉的更新逻辑）
 *
 * @note 文件头会立即写回磁盘，确保一致性
 */
void RmFileHandle::ReleasePageHandle(RmPageHandle &page_handle) {
  // 当页面从已满变成未满时，需要更新空闲页面链表：
  // 1. 将当前页面插入到空闲页面链表的头部
  // 2. 更新文件头中的 first_free_page_no
  
  // 注意：当前实现只更新了文件头，未更新页面头的 next_free_page_no
  // 理想情况下应该执行：
  // page_handle.page_hdr_->next_free_page_no = file_hdr_.first_free_page_no;
  
  // 将文件头中的 first_free_page_no 设置为当前页面号
  // （当前页面成为第一个空闲页面）
  file_hdr_.first_free_page_no = page_handle.page->GetPageId().page_no;

  // 将更新的文件头写回磁盘（确保一致性）
  disk_manager_->WritePage(fd_, RM_FILE_HDR_PAGE, (char *)&file_hdr_,
                           sizeof(file_hdr_));
}

}  // namespace easydb
