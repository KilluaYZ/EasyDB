/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * value.h
 *
 * Identification: src/include/type/value.h
 *
 *-------------------------------------------------------------------------
 */

/*
 * Original copyright:
 * Copyright (c) 2015-2019, Carnegie Mellon University Database Group
 */

#pragma once

#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

// #include "fmt/format.h"
#include "catalog/schema.h"
#include "common/logger.h"
#include "common/macros.h"
#include "type/limits.h"
#include "type/type.h"

namespace easydb {

class Column;

/**
 * @brief 将布尔值转换为CmpBool
 * @param boolean 布尔值
 * @return CmpBool枚举值
 */
inline auto GetCmpBool(bool boolean) -> CmpBool { return boolean ? CmpBool::CmpTrue : CmpBool::CmpFalse; }

/**
 * @brief 值类，表示SQL数据的视图
 * 
 * Value 类表示存储在某种物化状态中的SQL数据的视图。
 * 所有值都有类型和比较函数，但子类实现其他特定于类型的功能。
 * 
 * Value 类提供了：
 * - 类型信息
 * - 比较操作（==, !=, <, <=, >, >=）
 * - 数学运算（+, -, *, /, %）
 * - 序列化/反序列化
 * - 类型转换
 */
class Value {
  // Friend Type classes
  friend class Type;
  friend class NumericType;
  friend class IntegerParentType;
  friend class TinyintType;
  friend class SmallintType;
  friend class IntegerType;
  friend class BigintType;
  friend class DecimalType;
  friend class TimestampType;
  friend class BooleanType;
  friend class VarlenType;
  friend class VectorType;

 public:
  /**
   * @brief 根据类型ID构造值（创建NULL值）
   * @param type 类型ID
   */
  explicit Value(const TypeId type) : manage_data_(false), type_id_(type) { size_.len_ = EASYDB_VALUE_NULL; }
  
  /**
   * @brief 根据int8_t值构造值（用于BOOLEAN和TINYINT类型）
   * @param type 类型ID
   * @param i 8位整数值
   */
  Value(TypeId type, int8_t i);
  
  /**
   * @brief 根据double值构造值（用于DECIMAL类型）
   * @param type 类型ID
   * @param d 双精度浮点数值
   */
  Value(TypeId type, double d);
  
  /**
   * @brief 根据int16_t值构造值（用于SMALLINT类型）
   * @param type 类型ID
   * @param i 16位整数值
   */
  Value(TypeId type, int16_t i);
  
  /**
   * @brief 根据int32_t值构造值（用于INTEGER类型）
   * @param type 类型ID
   * @param i 32位整数值
   */
  Value(TypeId type, int32_t i);
  
  /**
   * @brief 根据int64_t值构造值（用于BIGINT类型）
   * @param type 类型ID
   * @param i 64位整数值
   */
  Value(TypeId type, int64_t i);
  
  /**
   * @brief 根据long long值构造值（用于LONG类型）
   * @param type 类型ID
   * @param ll long long值
   */
  Value(TypeId type, long long ll) { Value(type, static_cast<int64_t>(ll)); }
  
  /**
   * @brief 根据uint64_t值构造值（用于TIMESTAMP类型）
   * @param type 类型ID
   * @param i 64位无符号整数值
   */
  Value(TypeId type, uint64_t i);
  
  /**
   * @brief 根据字符数组构造值（用于CHAR和VARCHAR类型）
   * @param type 类型ID
   * @param data 字符数组指针
   * @param len 数据长度（字节数）
   * @param manage_data 是否由Value对象管理内存（true表示需要delete[]）
   */
  Value(TypeId type, const char *data, uint32_t len, bool manage_data);
  
  /**
   * @brief 根据字符串构造值（用于CHAR和VARCHAR类型）
   * @param type 类型ID
   * @param data 字符串引用
   */
  Value(TypeId type, const std::string &data);
  
  /**
   * @brief 根据double向量构造值（用于VECTOR类型）
   * @param type 类型ID
   * @param data double向量
   */
  Value(TypeId type, const std::vector<double> &data);

  /**
   * @brief 默认构造函数，创建空值
   */
  Value() : Value(TypeId::TYPE_EMPTY) {}
  
  /**
   * @brief 拷贝构造函数
   * @param other 要复制的源值对象
   */
  Value(const Value &other);
  
  /**
   * @brief 赋值运算符（使用copy-and-swap惯用法）
   * @param other 要赋值的源值对象
   * @return 当前对象的引用
   */
  auto operator=(Value other) -> Value &;
  
  /**
   * @brief 析构函数
   * @note 如果manage_data_为true，释放变长数据的内存
   */
  ~Value();
  
  /**
   * @brief 交换两个Value对象的内容
   * @param first 第一个Value对象
   * @param second 第二个Value对象
   * @note 用于copy-and-swap惯用法
   */
  // NOLINTNEXTLINE
  friend void Swap(Value &first, Value &second) {
    std::swap(first.value_, second.value_);
    std::swap(first.size_, second.size_);
    std::swap(first.manage_data_, second.manage_data_);
    std::swap(first.type_id_, second.type_id_);
  }
  
  /**
   * @brief 检查值是否为整数类型
   * @return true 如果是整数类型（INT、LONG等），false 否则
   */
  auto CheckInteger() const -> bool;
  
  /**
   * @brief 检查两个值是否可比较
   * @param o 另一个值对象
   * @return true 如果两个值可以进行比较，false 否则
   */
  auto CheckComparable(const Value &o) const -> bool;

  /**
   * @brief 获取值的类型ID
   * @return 值的类型ID
   */
  inline auto GetTypeId() const -> TypeId { return type_id_; }

  /**
   * @brief 获取值的列对象表示
   * @return 表示此值的列对象
   */
  auto GetColumn() const -> Column;

  /**
   * @brief 获取变长数据的长度
   * @return 变长数据的存储大小（字节数）
   */
  inline auto GetStorageSize() const -> uint32_t { return Type::GetInstance(type_id_)->GetStorageSize(*this); }
  
  /**
   * @brief 访问原始变长数据
   * @return 指向原始数据的常量指针
   */
  inline auto GetData() const -> const char * { return Type::GetInstance(type_id_)->GetData(*this); }

  /**
   * @brief 将值转换为指定类型T
   * @tparam T 目标类型
   * @return 转换后的值
   * @note 使用reinterpret_cast进行类型转换，适用于定长类型
   */
  template <class T>
  inline auto GetAs() const -> T {
    return *reinterpret_cast<const T *>(&value_);
  }

  /**
   * @brief 获取向量值（用于VECTOR类型）
   * @return double向量
   */
  auto GetVector() const -> std::vector<double>;

  /**
   * @brief 将值转换为指定类型
   * @param type_id 目标类型ID
   * @return 转换后的值
   */
  inline auto CastAs(const TypeId type_id) const -> Value {
    return Type::GetInstance(type_id_)->CastAs(*this, type_id);
  }
  
  /**
   * @brief 精确相等比较（用于项目4）
   * @param o 另一个值对象
   * @return true 如果两个值完全相等（包括NULL值），false 否则
   * @note 如果两个值都是NULL，返回true
   */
  inline auto CompareExactlyEquals(const Value &o) const -> bool {
    if (this->IsNull() && o.IsNull()) {
      return true;
    }
    return (Type::GetInstance(type_id_)->CompareEquals(*this, o)) == CmpBool::CmpTrue;
  }
  
  // ==================== 比较方法 ====================
  
  /**
   * @brief 比较两个值是否相等
   * @param o 另一个值对象
   * @return 比较结果（CmpBool）
   */
  inline auto CompareEquals(const Value &o) const -> CmpBool {
    return Type::GetInstance(type_id_)->CompareEquals(*this, o);
  }
  
  /**
   * @brief 比较两个值是否不相等
   * @param o 另一个值对象
   * @return 比较结果（CmpBool）
   */
  inline auto CompareNotEquals(const Value &o) const -> CmpBool {
    return Type::GetInstance(type_id_)->CompareNotEquals(*this, o);
  }
  
  /**
   * @brief 比较当前值是否小于另一个值
   * @param o 另一个值对象
   * @return 比较结果（CmpBool）
   */
  inline auto CompareLessThan(const Value &o) const -> CmpBool {
    return Type::GetInstance(type_id_)->CompareLessThan(*this, o);
  }
  
  /**
   * @brief 比较当前值是否小于等于另一个值
   * @param o 另一个值对象
   * @return 比较结果（CmpBool）
   */
  inline auto CompareLessThanEquals(const Value &o) const -> CmpBool {
    return Type::GetInstance(type_id_)->CompareLessThanEquals(*this, o);
  }
  
  /**
   * @brief 比较当前值是否大于另一个值
   * @param o 另一个值对象
   * @return 比较结果（CmpBool）
   */
  inline auto CompareGreaterThan(const Value &o) const -> CmpBool {
    return Type::GetInstance(type_id_)->CompareGreaterThan(*this, o);
  }
  
  /**
   * @brief 比较当前值是否大于等于另一个值
   * @param o 另一个值对象
   * @return 比较结果（CmpBool）
   */
  inline auto CompareGreaterThanEquals(const Value &o) const -> CmpBool {
    return Type::GetInstance(type_id_)->CompareGreaterThanEquals(*this, o);
  }

  // ==================== 比较运算符 ====================
  
  /**
   * @brief 相等运算符重载
   * @param o 另一个值对象
   * @return true 如果两个值相等
   */
  inline auto operator==(const Value &o) const -> bool { return CompareEquals(o) == CmpBool::CmpTrue; }
  
  /**
   * @brief 不等运算符重载
   * @param o 另一个值对象
   * @return true 如果两个值不相等
   */
  inline auto operator!=(const Value &o) const -> bool { return CompareNotEquals(o) == CmpBool::CmpTrue; }
  
  /**
   * @brief 小于运算符重载
   * @param o 另一个值对象
   * @return true 如果当前值小于另一个值
   */
  inline auto operator<(const Value &o) const -> bool { return CompareLessThan(o) == CmpBool::CmpTrue; }
  
  /**
   * @brief 小于等于运算符重载
   * @param o 另一个值对象
   * @return true 如果当前值小于等于另一个值
   */
  inline auto operator<=(const Value &o) const -> bool { return CompareLessThanEquals(o) == CmpBool::CmpTrue; }
  
  /**
   * @brief 大于运算符重载
   * @param o 另一个值对象
   * @return true 如果当前值大于另一个值
   */
  inline auto operator>(const Value &o) const -> bool { return CompareGreaterThan(o) == CmpBool::CmpTrue; }
  
  /**
   * @brief 大于等于运算符重载
   * @param o 另一个值对象
   * @return true 如果当前值大于等于另一个值
   */
  inline auto operator>=(const Value &o) const -> bool { return CompareGreaterThanEquals(o) == CmpBool::CmpTrue; }

  // ==================== 数学函数 ====================
  
  /**
   * @brief 加法运算
   * @param o 另一个值对象
   * @return 运算结果
   */
  inline auto Add(const Value &o) const -> Value { return Type::GetInstance(type_id_)->Add(*this, o); }
  
  /**
   * @brief 减法运算
   * @param o 另一个值对象
   * @return 运算结果
   */
  inline auto Subtract(const Value &o) const -> Value { return Type::GetInstance(type_id_)->Subtract(*this, o); }
  
  /**
   * @brief 乘法运算
   * @param o 另一个值对象
   * @return 运算结果
   */
  inline auto Multiply(const Value &o) const -> Value { return Type::GetInstance(type_id_)->Multiply(*this, o); }
  
  /**
   * @brief 除法运算
   * @param o 另一个值对象
   * @return 运算结果
   */
  inline auto Divide(const Value &o) const -> Value { return Type::GetInstance(type_id_)->Divide(*this, o); }
  
  /**
   * @brief 取模运算
   * @param o 另一个值对象
   * @return 运算结果
   */
  inline auto Modulo(const Value &o) const -> Value { return Type::GetInstance(type_id_)->Modulo(*this, o); }
  
  /**
   * @brief 取最小值
   * @param o 另一个值对象
   * @return 两个值中的较小者
   */
  inline auto Min(const Value &o) const -> Value { return Type::GetInstance(type_id_)->Min(*this, o); }
  
  /**
   * @brief 取最大值
   * @param o 另一个值对象
   * @return 两个值中的较大者
   */
  inline auto Max(const Value &o) const -> Value { return Type::GetInstance(type_id_)->Max(*this, o); }
  
  /**
   * @brief 平方根运算
   * @return 平方根结果
   */
  inline auto Sqrt() const -> Value { return Type::GetInstance(type_id_)->Sqrt(*this); }

  // ==================== 数学运算符 ====================
  
  /**
   * @brief 加法运算符重载
   * @param o 另一个值对象
   * @return 运算结果
   */
  inline auto operator+(const Value &o) const -> Value { return Add(o); }
  
  /**
   * @brief 减法运算符重载
   * @param o 另一个值对象
   * @return 运算结果
   */
  inline auto operator-(const Value &o) const -> Value { return Subtract(o); }
  
  /**
   * @brief 乘法运算符重载
   * @param o 另一个值对象
   * @return 运算结果
   */
  inline auto operator*(const Value &o) const -> Value { return Multiply(o); }
  
  /**
   * @brief 除法运算符重载
   * @param o 另一个值对象
   * @return 运算结果
   */
  inline auto operator/(const Value &o) const -> Value { return Divide(o); }
  
  /**
   * @brief 取模运算符重载
   * @param o 另一个值对象
   * @return 运算结果
   */
  inline auto operator%(const Value &o) const -> Value { return Modulo(o); }

  /**
   * @brief NULL值运算
   * @param o 另一个值对象
   * @return 运算结果（如果涉及NULL，返回NULL）
   */
  inline auto OperateNull(const Value &o) const -> Value { return Type::GetInstance(type_id_)->OperateNull(*this, o); }
  
  /**
   * @brief 判断值是否为零
   * @return true 如果值为零，false 否则
   */
  inline auto IsZero() const -> bool { return Type::GetInstance(type_id_)->IsZero(*this); }
  
  /**
   * @brief 判断值是否为NULL
   * @return true 如果值为NULL，false 否则
   */
  inline auto IsNull() const -> bool { return size_.len_ == EASYDB_VALUE_NULL; }

  /**
   * @brief 将值序列化到给定的存储空间
   * @param storage 存储缓冲区指针
   * @note 
   *   - inlined参数指示是否允许将此值内联到存储空间中
   *   - 如果inlined为false，可能使用提供的数据池为此值分配空间
   *   - 在存储中只存储指向分配池空间的引用
   */
  inline void SerializeTo(char *storage) const { Type::GetInstance(type_id_)->SerializeTo(*this, storage); }

  /**
   * @brief 从给定的存储空间反序列化一个指定类型的值
   * @param storage 存储缓冲区指针
   * @param type_id 值的类型ID
   * @return 反序列化的值
   */
  inline static auto DeserializeFrom(const char *storage, const TypeId type_id) -> Value {
    return Type::GetInstance(type_id)->DeserializeFrom(storage);
  }

  /**
   * @brief 从给定的存储空间反序列化一个值（根据模式和列名）
   * @param storage 存储缓冲区指针
   * @param schema 模式指针
   * @param column_name 列名
   * @return 反序列化的值
   */
  inline static auto DeserializeFrom(const char *storage, const Schema *schema, std::string column_name) -> Value {
    assert(schema);
    const TypeId column_type = schema->GetColumn(column_name).GetType();
    return Value::DeserializeFrom(storage, column_type);
  }

  /**
   * @brief 返回值的字符串版本
   * @return 值的字符串表示
   */
  inline auto ToString() const -> std::string { return Type::GetInstance(type_id_)->ToString(*this); }
  
  /**
   * @brief 创建值的副本
   * @return 值的副本
   */
  inline auto Copy() const -> Value { return Type::GetInstance(type_id_)->Copy(*this); }

 protected:
  /**
   * @brief 实际的值项（联合体）
   * @note 使用联合体节省内存，不同类型的值共享同一块内存
   */
  union Val {
    int8_t boolean_;         /**< 布尔值 */
    int8_t tinyint_;         /**< 8位整数 */
    int16_t smallint_;       /**< 16位整数 */
    int32_t integer_;        /**< 32位整数 */
    int64_t bigint_;         /**< 64位整数 */
    double decimal_;         /**< 双精度浮点数 */
    uint64_t timestamp_;     /**< 时间戳 */
    char *varlen_;           /**< 变长数据指针（可修改） */
    const char *const_varlen_; /**< 变长数据常量指针（不可修改） */
  } value_;

  /**
   * @brief 大小或元素类型ID（联合体）
   * @note 对于变长类型，存储长度；对于其他类型，可能存储元素类型ID
   */
  union {
    uint32_t len_;          /**< 变长数据的长度（字节数） */
    TypeId elem_type_id_;   /**< 元素类型ID */
  } size_;

  /**
   * @brief 是否由Value对象管理数据内存
   * @note true表示需要delete[]释放内存，false表示不管理内存
   */
  bool manage_data_;
  
  /**
   * @brief 数据类型ID
   */
  TypeId type_id_;
};
}  // namespace easydb

// template <typename T>
// struct fmt::formatter<T, std::enable_if_t<std::is_base_of<easydb::Value, T>::value, char>>
//     : fmt::formatter<std::string> {
//   template <typename FormatCtx>
//   auto format(const easydb::Value &x, FormatCtx &ctx) const {
//     return fmt::formatter<std::string>::format(x.ToString(), ctx);
//   }
// };

// template <typename T>
// struct fmt::formatter<std::unique_ptr<T>, std::enable_if_t<std::is_base_of<easydb::Value, T>::value, char>>
//     : fmt::formatter<std::string> {
//   template <typename FormatCtx>
//   auto format(const std::unique_ptr<easydb::Value> &x, FormatCtx &ctx) const {
//     return fmt::formatter<std::string>::format(x->ToString(), ctx);
//   }
// };
