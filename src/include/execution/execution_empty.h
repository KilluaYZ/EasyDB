#pragma once

#include <memory>
#include <unordered_map>
#include <vector>
#include "common/common.h"
#include "common/condition.h"
#include "common/errors.h"
#include "common/hashutil.h"
#include "defs.h"
#include "executor_abstract.h"
#include "planner/plan.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace easydb {

/**
 * @brief 空执行器类
 * 
 * EmptyExecutor 表示空结果集的执行器，用于某些特殊情况
 * （如WHERE条件永远为false的查询）。
 */
class EmptyExecutor : public AbstractExecutor {
 public:
  /**
   * @brief 构造函数
   * @param ctx 事务上下文指针
   */
  EmptyExecutor(Context *ctx) : context_(ctx), end_(false) {}

  /**
   * @brief 开始迭代元组
   * @note 一开始就设为true，表示无数据行
   */
  void beginTuple() { end_ = true; }
  
  /**
   * @brief 判断是否到达末尾
   * @return true（始终为true，因为结果集为空）
   */
  bool IsEnd() { return end_; }
  
  /**
   * @brief 获取当前元组的RID
   * @return RID的引用
   */
  RID &rid() { return rid(); }
  
  /**
   * @brief 获取下一个元组
   * @return nullptr（始终返回nullptr，因为结果集为空）
   */
  std::unique_ptr<Tuple> Next() { return nullptr; }
  
  /**
   * @brief 移动到下一个元组
   * @note 空操作，因为结果集为空
   */
  void nextTuple() override {}
  
  /**
   * @brief 获取元组元数据
   * @return 空的元组元数据
   */
  TupleMeta tuple_meta() { return TupleMeta(); }
  
  /**
   * @brief 获取元组
   * @return 空的元组
   */
  Tuple tuple() { return Tuple(); }
  
  /**
   * @brief 获取模式
   * @return 空模式（没有列）
   */
  Schema GetSchema() {
    // 空schema，没有列
    return Schema();
  }

 private:
  /**
   * @brief 事务上下文指针
   */
  Context *context_;
  
  /**
   * @brief 是否到达末尾标志（始终为true）
   */
  bool end_;
};

}  // namespace easydb