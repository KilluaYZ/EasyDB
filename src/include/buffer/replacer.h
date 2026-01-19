/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * replacer.h
 *
 * Identification: src/include/buffer/replacer.h
 *
 *-------------------------------------------------------------------------
 */

/*
 * Original copyright:
 * Copyright (c) 2015-2019, Carnegie Mellon University Database Group
 */

#pragma once

#include "common/config.h"

namespace easydb {

/**
 * @brief 替换器抽象基类，用于跟踪页面使用情况并实现页面替换策略
 * 
 * Replacer 是一个抽象类，定义了缓冲池中页面替换策略的接口。
 * 当缓冲池满时，需要选择一个页面（受害者）从内存中移除，为新页面腾出空间。
 * 不同的替换策略（如LRU、LFU等）通过继承此类来实现。
 */
class Replacer {
 public:
  /**
   * @brief 默认构造函数
   */
  Replacer() = default;
  
  /**
   * @brief 虚析构函数，确保派生类对象能够正确析构
   */
  virtual ~Replacer() = default;

  /**
   * @brief 根据替换策略选择一个受害者帧（可以被替换的帧）
   * @param[out] frame_id 输出参数，返回被选中的受害者帧ID；如果没有找到受害者则保持不变
   * @return true 如果找到了受害者帧，false 如果没有可替换的帧
   * @note 此方法会从替换器中移除选中的帧，表示该帧即将被新页面占用
   */
  virtual auto Victim(frame_id_t *frame_id) -> bool = 0;

  /**
   * @brief 固定一个帧，表示该帧不应该被选为受害者，直到它被取消固定
   * @param frame_id 要固定的帧ID
   * @note 
   *   - 当页面正在被使用时，应该调用此方法固定对应的帧
   *   - 固定的帧不会被替换策略选中作为受害者
   *   - 固定操作通常会增加引用计数或从替换器中移除该帧
   */
  virtual void Pin(frame_id_t frame_id) = 0;

  /**
   * @brief 取消固定一个帧，表示该帧现在可以被选为受害者
   * @param frame_id 要取消固定的帧ID
   * @note 
   *   - 当页面不再被使用时，应该调用此方法取消固定对应的帧
   *   - 取消固定后，该帧可以被替换策略管理，并在需要时被选为受害者
   *   - 取消固定操作通常会将帧加入替换器的管理列表
   */
  virtual void Unpin(frame_id_t frame_id) = 0;

  /**
   * @brief 返回替换器中可以被选为受害者的元素数量
   * @return 当前可以被替换的帧数量
   * @note 这个数量表示缓冲池中有多少帧是"可替换"的（未被固定且可以被驱逐）
   */
  virtual auto Size() -> size_t = 0;
};

}  // namespace easydb
