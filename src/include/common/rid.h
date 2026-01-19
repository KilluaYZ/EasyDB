/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * rid.h
 *
 * Identification: src/include/common/rid.h
 *
 *-------------------------------------------------------------------------
 */

/*
 * Original copyright:
 * Copyright (c) 2015-2019, Carnegie Mellon University Database Group
 */

#pragma once

#include <cstdint>
#include <sstream>
#include <string>

#include "common/config.h"

namespace easydb {

/**
 * @brief 记录标识符（Record Identifier，RID）类
 *
 * RID 用于唯一标识数据库中的一个记录（元组），它由两部分组成：
 * - page_id_: 记录所在的页面ID
 * - slot_num_: 记录在页面中的槽号（逻辑偏移量）
 *
 * RID 可以编码为一个64位整数：高32位存储page_id_，低32位存储slot_num_
 */
class RID {
 public:
  /**
   * @brief 默认构造函数，创建一个无效的RID
   * @note page_id_会被初始化为INVALID_PAGE_ID，表示这是一个无效的RID
   */
  RID() = default;

  /**
   * @brief 根据页面ID和槽号创建RID
   * @param page_id 页面标识符
   * @param slot_num 槽号（记录在页面中的逻辑偏移量，从0开始）
   */
  RID(page_id_t page_id, slot_id_t slot_num)
      : page_id_(page_id), slot_num_(slot_num) {}

  /**
   * @brief 从64位整数解码创建RID
   * @param rid 64位整数，高32位是page_id，低32位是slot_num
   * @note 用于从序列化的RID值恢复RID对象
   */
  explicit RID(int64_t rid)
      : page_id_(static_cast<page_id_t>(rid >> 32)),
        slot_num_(static_cast<slot_id_t>(rid)) {}

  /**
   * @brief 将RID编码为64位整数
   * @return 64位整数，高32位是page_id_，低32位是slot_num_
   * @note 用于序列化RID，便于存储和传输
   */
  inline auto Get() const -> int64_t {
    return (static_cast<int64_t>(page_id_)) << 32 | slot_num_;
  }

  /**
   * @brief 获取页面ID
   * @return 页面标识符
   */
  inline auto GetPageId() const -> page_id_t { return page_id_; }

  /**
   * @brief 获取槽号
   * @return 槽号（记录在页面中的逻辑偏移量）
   */
  inline auto GetSlotNum() const -> slot_id_t { return slot_num_; }

  /**
   * @brief 设置页面ID和槽号
   * @param page_id 新的页面ID
   * @param slot_num 新的槽号
   */
  inline void Set(page_id_t page_id, slot_id_t slot_num) {
    page_id_ = page_id;
    slot_num_ = slot_num;
  }

  /**
   * @brief 设置页面ID
   * @param page_id 新的页面ID
   */
  inline void SetPageId(page_id_t page_id) { page_id_ = page_id; }

  /**
   * @brief 设置槽号
   * @param slot_num 新的槽号
   */
  inline void SetSlotNum(slot_id_t slot_num) { slot_num_ = slot_num; }

  /**
   * @brief 获取RID的字符串表示
   * @return 格式为"(page_id,slot_num)"的字符串
   */
  inline auto ToString() const -> std::string {
    std::stringstream os;
    // os << "page_id: " << page_id_;
    // os << " slot_num: " << slot_num_ << "\n";
    os << "(" << page_id_ << "," << slot_num_ << ")";
    return os.str();
  }

  /**
   * @brief 流输出运算符重载，用于将RID输出到流
   * @param os 输出流
   * @param rid 要输出的RID对象
   * @return 输出流的引用
   */
  friend auto operator<<(std::ostream &os, const RID &rid) -> std::ostream & {
    os << rid.ToString();
    return os;
  }

  /**
   * @brief 相等运算符重载，判断两个RID是否相等
   * @param other 要比较的另一个RID对象
   * @return true 如果两个RID的page_id_和slot_num_都相等，false 否则
   */
  auto operator==(const RID &other) const -> bool {
    return page_id_ == other.page_id_ && slot_num_ == other.slot_num_;
  }

 private:
  /** @brief 页面标识符，表示记录所在的页面 */
  page_id_t page_id_{INVALID_PAGE_ID};

  /** @brief 槽号，表示记录在页面中的逻辑偏移量（从0开始计数） */
  uint32_t slot_num_{0};
};

}  // namespace easydb

namespace std {
template <>
struct hash<easydb::RID> {
  auto operator()(const easydb::RID &obj) const -> size_t {
    return hash<int64_t>()(obj.Get());
  }
};
}  // namespace std
