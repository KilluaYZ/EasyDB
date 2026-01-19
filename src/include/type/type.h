/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * type.h
 *
 * Identification: src/include/type/type.h
 *
 *-------------------------------------------------------------------------
 */

/*
 * Original copyright:
 * Copyright (c) 2015-2019, Carnegie Mellon University Database Group
 */

#pragma once

#include <cstdint>
#include <string>

#include "type/type_id.h"

namespace easydb {

class Value;

/**
 * @brief 比较结果枚举
 * @note 用于表示两个值的比较结果，包括NULL值的处理
 */
enum class CmpBool {
  CmpFalse = 0,  /**< 比较结果为false */
  CmpTrue = 1,   /**< 比较结果为true */
  CmpNull = 2    /**< 比较结果涉及NULL值 */
};

/**
 * @brief 类型基类
 * 
 * Type 是所有数据类型的基类，提供了类型相关的通用操作接口。
 * 每种具体类型（如IntegerType、VarlenType等）都继承自此类。
 */
class Type {
 public:
  /**
   * @brief 构造函数
   * @param type_id 类型ID
   */
  explicit Type(TypeId type_id) : type_id_(type_id) {}

  /**
   * @brief 虚析构函数
   */
  virtual ~Type() = default;

  /**
   * @brief 获取数据类型的字节大小
   * @param type_id 类型ID
   * @return 该类型的字节大小
   */
  static auto GetTypeSize(TypeId type_id) -> uint64_t;

  /**
   * @brief 判断此类型是否可以从另一个类型强制转换
   * @param type_id 源类型ID
   * @return true 如果可以强制转换，false 否则
   */
  auto IsCoercableFrom(TypeId type_id) const -> bool;

  /**
   * @brief 将类型ID转换为字符串（用于调试）
   * @param type_id 类型ID
   * @return 类型名称的字符串
   */
  static auto TypeIdToString(TypeId type_id) -> std::string;

  /**
   * @brief 获取类型的最小值
   * @param type_id 类型ID
   * @return 该类型的最小值
   */
  static auto GetMinValue(TypeId type_id) -> Value;
  
  /**
   * @brief 获取类型的最大值
   * @param type_id 类型ID
   * @return 该类型的最大值
   */
  static auto GetMaxValue(TypeId type_id) -> Value;

  /**
   * @brief 获取类型的单例实例
   * @param type_id 类型ID
   * @return 该类型的单例指针
   */
  inline static auto GetInstance(TypeId type_id) -> Type * { return k_types[type_id]; }

  /**
   * @brief 获取类型ID
   * @return 当前类型的ID
   */
  inline auto GetTypeId() const -> TypeId { return type_id_; }

  // ==================== 比较函数 ====================
  //
  // 注意：
  // 理论上只需要CompareLessThan()是纯虚函数，因为其他比较函数可以从
  // CompareLessThan()推导出来。例如：
  //
  //    CompareEquals(o) = !CompareLessThan(o) && !o.CompareLessThan(this)
  //    CompareNotEquals(o) = !CompareEquals(o)
  //    CompareLessThanEquals(o) = CompareLessThan(o) || CompareEquals(o)
  //    CompareGreaterThan(o) = !CompareLessThanEquals(o)
  //    ... 等等 ...
  //
  // 我们不这样做有两个原因：
  // (1) 重复调用CompareLessThan()可能是性能问题，由于Value是执行引擎的核心组件，
  //     我们希望尽可能提高性能。
  // (2) 通过使所有函数都是纯虚函数来保持接口的一致性。
  
  /**
   * @brief 比较两个值是否相等
   * @param left 左侧值
   * @param right 右侧值
   * @return 比较结果（CmpBool）
   */
  virtual auto CompareEquals(const Value &left, const Value &right) const -> CmpBool;
  
  /**
   * @brief 比较两个值是否不相等
   * @param left 左侧值
   * @param right 右侧值
   * @return 比较结果（CmpBool）
   */
  virtual auto CompareNotEquals(const Value &left, const Value &right) const -> CmpBool;
  
  /**
   * @brief 比较左侧值是否小于右侧值
   * @param left 左侧值
   * @param right 右侧值
   * @return 比较结果（CmpBool）
   */
  virtual auto CompareLessThan(const Value &left, const Value &right) const -> CmpBool;
  
  /**
   * @brief 比较左侧值是否小于等于右侧值
   * @param left 左侧值
   * @param right 右侧值
   * @return 比较结果（CmpBool）
   */
  virtual auto CompareLessThanEquals(const Value &left, const Value &right) const -> CmpBool;
  
  /**
   * @brief 比较左侧值是否大于右侧值
   * @param left 左侧值
   * @param right 右侧值
   * @return 比较结果（CmpBool）
   */
  virtual auto CompareGreaterThan(const Value &left, const Value &right) const -> CmpBool;
  
  /**
   * @brief 比较左侧值是否大于等于右侧值
   * @param left 左侧值
   * @param right 右侧值
   * @return 比较结果（CmpBool）
   */
  virtual auto CompareGreaterThanEquals(const Value &left, const Value &right) const -> CmpBool;

  // ==================== 数学函数 ====================
  
  /**
   * @brief 加法运算
   * @param left 左侧值
   * @param right 右侧值
   * @return 运算结果
   */
  virtual auto Add(const Value &left, const Value &right) const -> Value;
  
  /**
   * @brief 减法运算
   * @param left 左侧值
   * @param right 右侧值
   * @return 运算结果
   */
  virtual auto Subtract(const Value &left, const Value &right) const -> Value;
  
  /**
   * @brief 乘法运算
   * @param left 左侧值
   * @param right 右侧值
   * @return 运算结果
   */
  virtual auto Multiply(const Value &left, const Value &right) const -> Value;
  
  /**
   * @brief 除法运算
   * @param left 左侧值
   * @param right 右侧值
   * @return 运算结果
   */
  virtual auto Divide(const Value &left, const Value &right) const -> Value;
  
  /**
   * @brief 取模运算
   * @param left 左侧值
   * @param right 右侧值
   * @return 运算结果
   */
  virtual auto Modulo(const Value &left, const Value &right) const -> Value;
  
  /**
   * @brief 取最小值
   * @param left 左侧值
   * @param right 右侧值
   * @return 两个值中的较小者
   */
  virtual auto Min(const Value &left, const Value &right) const -> Value;
  
  /**
   * @brief 取最大值
   * @param left 左侧值
   * @param right 右侧值
   * @return 两个值中的较大者
   */
  virtual auto Max(const Value &left, const Value &right) const -> Value;
  
  /**
   * @brief 平方根运算
   * @param val 值
   * @return 平方根结果
   */
  virtual auto Sqrt(const Value &val) const -> Value;
  
  /**
   * @brief NULL值运算
   * @param val 值
   * @param right 右侧值
   * @return 运算结果（如果涉及NULL，返回NULL）
   */
  virtual auto OperateNull(const Value &val, const Value &right) const -> Value;
  
  /**
   * @brief 判断值是否为零
   * @param val 值
   * @return true 如果值为零，false 否则
   */
  virtual auto IsZero(const Value &val) const -> bool;

  /**
   * @brief 判断数据是否内联存储在此类的存储空间中
   * @param val 值
   * @return true 如果数据内联存储，false 如果必须通过间接/指针访问
   * @note 内联类型（如INT）直接存储在元组中，非内联类型（如VARCHAR）存储指针
   */
  virtual auto IsInlined(const Value &val) const -> bool;

  /**
   * @brief 返回值的字符串表示
   * @param val 值
   * @return 值的字符串表示
   */
  virtual auto ToString(const Value &val) const -> std::string;

  /**
   * @brief 将值序列化到给定的存储空间
   * @param val 要序列化的值
   * @param storage 存储缓冲区指针
   */
  virtual void SerializeTo(const Value &val, char *storage) const;

  /**
   * @brief 从给定的存储空间反序列化一个值
   * @param storage 存储缓冲区指针
   * @return 反序列化的值
   */
  virtual auto DeserializeFrom(const char *storage) const -> Value;

  /**
   * @brief 创建值的副本
   * @param val 要复制的值
   * @return 值的副本
   */
  virtual auto Copy(const Value &val) const -> Value;

  /**
   * @brief 将值转换为指定类型
   * @param val 要转换的值
   * @param type_id 目标类型ID
   * @return 转换后的值
   */
  virtual auto CastAs(const Value &val, TypeId type_id) const -> Value;

  /**
   * @brief 访问从元组存储中存储的原始变长数据
   * @param val 值
   * @return 指向原始数据的常量指针
   */
  virtual auto GetData(const Value &val) const -> const char *;

  /**
   * @brief 获取值的存储大小
   * @param val 值
   * @return 值的存储大小（字节数）
   */
  virtual auto GetStorageSize(const Value &val) const -> uint32_t;

 protected:
  /**
   * @brief 实际的类型ID
   */
  TypeId type_id_;
  
  /**
   * @brief 单例实例数组
   * @note 每种类型都有一个单例实例，通过GetInstance()访问
   */
  static Type *k_types[10];
};
}  // namespace easydb
