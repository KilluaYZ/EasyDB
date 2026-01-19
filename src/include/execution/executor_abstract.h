/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/common.h"
#include "common/context.h"
#include "common/errors.h"
#include "defs.h"
#include "storage/index/ix_manager.h"
#include "storage/index/ix_scan.h"
#include "system/sm_defs.h"
#include "system/sm_meta.h"
#include "type/type_id.h"

namespace easydb {

/**
 * @brief 执行器抽象基类
 *
 * AbstractExecutor 是所有执行器的基类，定义了执行器的通用接口。
 * 执行器负责执行查询计划中的各个操作，如扫描、连接、投影、聚合等。
 */
class AbstractExecutor {
 public:
  /** @brief 抽象RID（用于某些执行器） */
  RID _abstract_rid;

  /** @brief 事务上下文指针 */
  Context *context_;

  /**
   * @brief 默认构造函数
   */
  AbstractExecutor() : _abstract_rid(), context_(nullptr) {}

  /**
   * @brief 从shared_ptr构造执行器
   * @param ptr 指向派生类对象的shared_ptr
   * @note 使用派生类指针初始化AbstractExecutor的成员变量
   */
  AbstractExecutor(std::shared_ptr<void> &ptr) {
    auto derived_ptr = std::static_pointer_cast<AbstractExecutor>(ptr);
    // 使用 derived_ptr 初始化 AbstractExecutor 的成员变量
    _abstract_rid = derived_ptr->_abstract_rid;
    context_ = derived_ptr->context_;
  }

  /**
   * @brief 虚析构函数
   */
  virtual ~AbstractExecutor() = default;

  /**
   * @brief 获取元组长度
   * @return 元组的长度（字节数），默认返回0
   */
  virtual size_t tupleLen() const { return 0; };

  /**
   * @brief 获取列元数据向量
   * @return 列元数据向量的常量引用，默认返回空向量
   */
  virtual const std::vector<ColMeta> &cols() const {
    std::vector<ColMeta> *_cols = nullptr;
    return *_cols;
  };

  /**
   * @brief 获取模式
   * @return 模式的常量引用，默认返回空模式
   */
  virtual const Schema &schema() const {
    Schema tp;
    return tp;
  };

  /**
   * @brief 获取执行器类型名称
   * @return 执行器类型的字符串表示
   */
  virtual std::string getType() { return "AbstractExecutor"; };

  /**
   * @brief 开始迭代元组
   * @note 初始化迭代器，准备开始遍历
   */
  virtual void beginTuple() {};

  /**
   * @brief 移动到下一个元组
   * @note 推进迭代器到下一个位置
   */
  virtual void nextTuple() {};

  /**
   * @brief 判断是否到达末尾
   * @return true 如果已到达末尾，false 否则
   */
  virtual bool IsEnd() const { return true; };

  /**
   * @brief 获取表名
   * @return 表名字符串，默认返回空字符串
   */
  virtual std::string getTabName() const {}

  /**
   * @brief 获取当前元组的RID
   * @return 当前元组的RID的引用
   * @note 纯虚函数，派生类必须实现
   */
  virtual RID &rid() = 0;

  /**
   * @brief 获取下一个元组
   * @return 下一个元组的智能指针，如果没有更多元组则返回nullptr
   * @note 纯虚函数，派生类必须实现
   */
  virtual std::unique_ptr<Tuple> Next() = 0;

  // virtual std::unique_ptr<RmRecord> Next() = 0;

  /**
   * @brief 根据TabCol获取列元数据
   * @param target 目标列标识
   * @return 列元数据，默认返回空ColMeta
   */
  virtual ColMeta get_col_offset(const TabCol &target) { return ColMeta(); };

  /**
   * @brief 根据TabCol获取列对象
   * @param target 目标列标识
   * @return 列对象，默认返回空Column
   */
  virtual Column get_colu_offset(const TabCol &target) { return Column(); };

  /**
   * @brief 在列元数据向量中查找指定列（根据TabCol）
   * @param rec_cols 列元数据向量
   * @param target 目标列标识
   * @return 找到的列的迭代器
   * @throws ColumnNotFoundError 如果列不存在
   */
  std::vector<ColMeta>::const_iterator get_col(
      const std::vector<ColMeta> &rec_cols, const TabCol &target) {
    auto pos =
        std::find_if(rec_cols.begin(), rec_cols.end(), [&](const ColMeta &col) {
          return col.tab_name == target.tab_name && col.name == target.col_name;
        });
    if (pos == rec_cols.end()) {
      throw ColumnNotFoundError(target.tab_name + '.' + target.col_name);
    }
    return pos;
  }

  /**
   * @brief 在列元数据向量中查找指定列（根据表名和列名）
   * @param rec_cols 列元数据向量
   * @param target_tab_name 目标表名
   * @param target_col_name 目标列名
   * @return 找到的列的迭代器
   * @throws ColumnNotFoundError 如果列不存在
   */
  std::vector<ColMeta>::const_iterator get_col(
      const std::vector<ColMeta> &rec_cols, const std::string &target_tab_name,
      const std::string &target_col_name) {
    auto pos =
        std::find_if(rec_cols.begin(), rec_cols.end(), [&](const ColMeta &col) {
          return col.tab_name == target_tab_name && col.name == target_col_name;
        });
    if (pos == rec_cols.end()) {
      throw ColumnNotFoundError(target_tab_name + '.' + target_col_name);
    }
    return pos;
  }

  /**
   * @brief 在列向量中查找指定列（根据表名和列名）
   * @param rec_cols 列向量
   * @param target_tab_name 目标表名
   * @param target_col_name 目标列名
   * @return 找到的列的迭代器
   * @throws ColumnNotFoundError 如果列不存在
   * @note 只比较列名，不比较表名（用于某些场景）
   */
  std::vector<Column>::const_iterator get_col(
      const std::vector<Column> &rec_cols, const std::string &target_tab_name,
      const std::string &target_col_name) {
    auto pos = std::find_if(
        rec_cols.begin(), rec_cols.end(),
        [&](const Column &col) { return col.GetName() == target_col_name; });
    if (pos == rec_cols.end()) {
      throw ColumnNotFoundError(target_tab_name + '.' + target_col_name);
    }
    return pos;
  }
};

}  // namespace easydb
