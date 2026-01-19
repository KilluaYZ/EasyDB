/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 *
 *-------------------------------------------------------------------------
 */

#pragma once
#include <memory>
#include "catalog/column.h"
#include "common/errors.h"
#include "common/mergeSorter.h"
#include "defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "storage/index/ix_manager.h"
#include "storage/index/ix_scan.h"
#include "storage/table/tuple.h"
#include "system/sm_defs.h"
#include "system/sm_meta.h"

namespace easydb {

/**
 * @brief 排序执行器类
 *
 * SortExecutor 实现排序操作，根据指定列对输入元组进行排序。
 * 支持升序和降序排序，使用外部归并排序算法处理大数据集。
 */
class SortExecutor : public AbstractExecutor {
 private:
  /**
   * @brief 排序操作的子节点执行器
   * @note 排序操作的数据源
   */
  std::unique_ptr<AbstractExecutor> prev_;

  // ColMeta cols_;   //
  // 框架中只支持一个键排序，需要自行修改数据结构支持多个键排序

  /**
   * @brief 排序后生成的记录的字段模式
   */
  Schema schema_;

  /**
   * @brief 排序的列
   * @note 框架中只支持一个键排序，需要自行修改数据结构支持多个键排序
   */
  Column colus_;

  /**
   * @brief 元组数量
   */
  size_t tuple_num;

  /**
   * @brief 元组长度（字节数）
   */
  size_t len_;

  /**
   * @brief 最大物理长度（字节数）
   */
  size_t max_physical_len_;

  /**
   * @brief 是否降序标志
   * @note true表示降序，false表示升序
   */
  bool is_desc_;

  /**
   * @brief 是否到达末尾标志
   */
  bool isend_ = false;

  /**
   * @brief 已使用的元组索引列表
   */
  std::vector<size_t> used_tuple;

  /**
   * @brief 当前元组的数据指针
   */
  char *current_data_;
  // std::unique_ptr<Tuple> current_tuple;

  /**
   * @brief 归并排序器
   * @note 用于执行外部归并排序
   */
  std::unique_ptr<MergeSorter> sorter;

 public:
  /**
   * @brief 构造函数
   * @param prev 子节点执行器的智能指针
   * @param sel_cols 排序的列
   * @param is_desc 是否降序
   */
  SortExecutor(std::unique_ptr<AbstractExecutor> prev, TabCol sel_cols,
               bool is_desc);

  /**
   * @brief 析构函数
   */
  ~SortExecutor();

  /**
   * @brief 开始迭代元组
   * @note 初始化排序器，对所有输入元组进行排序
   */
  void beginTuple() override;

  /**
   * @brief 移动到下一个元组
   * @note 推进到排序后的下一个元组
   */
  void nextTuple() override;

  /**
   * @brief 获取下一个元组
   * @return 排序后的元组的智能指针，如果没有更多元组则返回nullptr
   */
  std::unique_ptr<Tuple> Next() override {
    Tuple tp;
    tp.DeserializeFrom(current_data_);
    return std::make_unique<Tuple>(tp);
  }
  // std::unique_ptr<RmRecord> Next() override { return
  // std::move(current_tuple); }

  /**
   * @brief 获取当前元组的RID
   * @return 抽象RID的引用
   */
  RID &rid() override { return _abstract_rid; }

  // const std::vector<ColMeta> &cols() const override { return prev_->cols();
  // };

  /**
   * @brief 获取模式
   * @return 模式的常量引用
   */
  const Schema &schema() const override { return schema_; };

  /**
   * @brief 获取执行器类型名称
   * @return "SortExecutor"
   */
  std::string getType() override { return "SortExecutor"; };

  /**
   * @brief 判断是否到达末尾
   * @return true 如果已到达末尾，false 否则
   */
  bool IsEnd() const override {
    // return sorter->IsEnd();
    return isend_;
  };

  /**
   * @brief 获取元组长度
   * @return 元组的长度（字节数）
   */
  size_t tupleLen() const override { return len_; };

  /**
   * @brief 打印记录（用于调试）
   * @param record 记录对象
   * @param cols 列元数据向量
   */
  void printRecord(RmRecord record, std::vector<ColMeta> cols);

  /**
   * @brief 打印记录（用于调试）
   * @param data 记录数据指针
   * @param cols 列元数据向量
   */
  void printRecord(char *data, std::vector<ColMeta> cols);

  // Column get_colu_offset(const TabCol &target) {
  //   auto cols = schema_.GetColumns();
  //   for (auto &col : cols) {
  //     if (target.col_name == col.GetName()) {
  //       return col;
  //     }
  //   }
  //   throw ColumnNotFoundError(target.col_name);
  //  };
};

}  // namespace easydb
