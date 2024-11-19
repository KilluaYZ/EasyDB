/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * decimal_type.cpp
 *
 * Identification: src/type/decimal_type.cpp
 *
 *-------------------------------------------------------------------------
 */

/*
 * Original copyright:
 * Copyright (c) 2015-2019, Carnegie Mellon University Database Group
 */

#include <cassert>
#include <cmath>
#include <iostream>
#include <string>

#include "common/exception.h"
#include "type/decimal_type.h"

namespace easydb {
#define DECIMAL_COMPARE_FUNC(OP)                                           \
  switch (right.GetTypeId()) {                                             \
    case TypeId::TYPE_INT:                                                 \
      return GetCmpBool(left.value_.decimal_ OP right.GetAs<int32_t>());   \
    case TypeId::TYPE_LONG:                                                \
      return GetCmpBool(left.value_.decimal_ OP right.GetAs<int64_t>());   \
    case TypeId::TYPE_FLOAT:                                               \
    case TypeId::TYPE_DOUBLE:                                              \
      return GetCmpBool(left.value_.decimal_ OP right.GetAs<double>());    \
    case TypeId::TYPE_CHAR:                                                \
    case TypeId::TYPE_VARCHAR: {                                           \
      auto r_value = right.CastAs(TypeId::TYPE_DOUBLE);                    \
      return GetCmpBool(left.value_.decimal_ OP r_value.GetAs<int64_t>()); \
    }                                                                      \
    default:                                                               \
      break;                                                               \
  }  // SWITCH

#define DECIMAL_MODIFY_FUNC(OP)                                                           \
  switch (right.GetTypeId()) {                                                            \
    case TypeId::TYPE_INT:                                                                \
      return Value(TypeId::TYPE_DOUBLE, left.value_.decimal_ OP right.GetAs<int32_t>());  \
    case TypeId::TYPE_LONG:                                                               \
      return Value(TypeId::TYPE_DOUBLE, left.value_.decimal_ OP right.GetAs<int64_t>());  \
    case TypeId::TYPE_FLOAT:                                                              \
    case TypeId::TYPE_DOUBLE:                                                             \
      return Value(TypeId::TYPE_DOUBLE, left.value_.decimal_ OP right.GetAs<double>());   \
    case TypeId::TYPE_CHAR:                                                               \
    case TypeId::TYPE_VARCHAR: {                                                          \
      auto r_value = right.CastAs(TypeId::TYPE_DOUBLE);                                   \
      return Value(TypeId::TYPE_DOUBLE, left.value_.decimal_ OP r_value.GetAs<double>()); \
    }                                                                                     \
    default:                                                                              \
      break;                                                                              \
  }  // SWITCH

// static inline double ValMod(double x, double y) {
//  return x - std::trunc((double)x / (double)y) * y;
//}

DecimalType::DecimalType() : NumericType(TypeId::TYPE_DOUBLE) {}

auto DecimalType::IsZero(const Value &val) const -> bool {
  assert(GetTypeId() == TypeId::TYPE_DOUBLE);
  return (val.value_.decimal_ == 0);
}

auto DecimalType::Add(const Value &left, const Value &right) const -> Value {
  assert(GetTypeId() == TypeId::TYPE_DOUBLE);
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return left.OperateNull(right);
  }

  DECIMAL_MODIFY_FUNC(+);  // NOLINT
  throw Exception("type error");
}

auto DecimalType::Subtract(const Value &left, const Value &right) const -> Value {
  assert(GetTypeId() == TypeId::TYPE_DOUBLE);
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return left.OperateNull(right);
  }

  DECIMAL_MODIFY_FUNC(-);  // NOLINT

  throw Exception("type error");
}

auto DecimalType::Multiply(const Value &left, const Value &right) const -> Value {
  assert(GetTypeId() == TypeId::TYPE_DOUBLE);
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return left.OperateNull(right);
  }

  DECIMAL_MODIFY_FUNC(*);  // NOLINT
  throw Exception("type error");
}

auto DecimalType::Divide(const Value &left, const Value &right) const -> Value {
  assert(GetTypeId() == TypeId::TYPE_DOUBLE);
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return left.OperateNull(right);
  }

  if (right.IsZero()) {
    throw Exception(ExceptionType::DIVIDE_BY_ZERO, "Division by zero on right-hand side");
  }

  DECIMAL_MODIFY_FUNC(/);  // NOLINT

  throw Exception("type error");
}

auto DecimalType::Modulo(const Value &left, const Value &right) const -> Value {
  assert(GetTypeId() == TypeId::TYPE_DOUBLE);
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return OperateNull(left, right);
  }

  if (right.IsZero()) {
    throw Exception(ExceptionType::DIVIDE_BY_ZERO, "Division by zero on right-hand side");
  }
  switch (right.GetTypeId()) {
    // case TypeId::TINYINT:
    //   return {TypeId::TYPE_DOUBLE, ValMod(left.value_.decimal_, right.GetAs<int8_t>())};
    // case TypeId::SMALLINT:
    //   return {TypeId::TYPE_DOUBLE, ValMod(left.value_.decimal_, right.GetAs<int16_t>())};
    case TypeId::TYPE_INT:
      return {TypeId::TYPE_DOUBLE, ValMod(left.value_.decimal_, right.GetAs<int32_t>())};
    case TypeId::TYPE_LONG:
      return {TypeId::TYPE_DOUBLE, ValMod(left.value_.decimal_, right.GetAs<int64_t>())};
    case TypeId::TYPE_FLOAT:
    case TypeId::TYPE_DOUBLE:
      return {TypeId::TYPE_DOUBLE, ValMod(left.value_.decimal_, right.GetAs<double>())};
    case TypeId::TYPE_CHAR:
    case TypeId::TYPE_VARCHAR: {
      auto r_value = right.CastAs(TypeId::TYPE_DOUBLE);
      return {TypeId::TYPE_DOUBLE, ValMod(left.value_.decimal_, r_value.GetAs<double>())};
    }
    default:
      break;
  }
  throw Exception("type error");
}

auto DecimalType::Min(const Value &left, const Value &right) const -> Value {
  assert(GetTypeId() == TypeId::TYPE_DOUBLE);
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return left.OperateNull(right);
  }

  if (left.CompareLessThanEquals(right) == CmpBool::CmpTrue) {
    return left.Copy();
  }
  return right.Copy();
}

auto DecimalType::Max(const Value &left, const Value &right) const -> Value {
  assert(GetTypeId() == TypeId::TYPE_DOUBLE);
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return left.OperateNull(right);
  }

  if (left.CompareGreaterThanEquals(right) == CmpBool::CmpTrue) {
    return left.Copy();
  }
  return right.Copy();
}

auto DecimalType::Sqrt(const Value &val) const -> Value {
  assert(GetTypeId() == TypeId::TYPE_DOUBLE);
  if (val.IsNull()) {
    return {TypeId::TYPE_DOUBLE, EASYDB_DECIMAL_NULL};
  }
  if (val.value_.decimal_ < 0) {
    throw Exception(ExceptionType::DECIMAL, "Cannot take square root of a negative number.");
  }
  return {TypeId::TYPE_DOUBLE, std::sqrt(val.value_.decimal_)};
}

auto DecimalType::OperateNull(const Value &left __attribute__((unused)),
                              const Value &right __attribute__((unused))) const -> Value {
  return {TypeId::TYPE_DOUBLE, EASYDB_DECIMAL_NULL};
}

auto DecimalType::CompareEquals(const Value &left, const Value &right) const -> CmpBool {
  assert(GetTypeId() == TypeId::TYPE_DOUBLE);
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::CmpNull;
  }

  DECIMAL_COMPARE_FUNC(==);  // NOLINT

  throw Exception("type error");
}

auto DecimalType::CompareNotEquals(const Value &left, const Value &right) const -> CmpBool {
  assert(GetTypeId() == TypeId::TYPE_DOUBLE);
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::CmpNull;
  }

  DECIMAL_COMPARE_FUNC(!=);  // NOLINT

  throw Exception("type error");
}

auto DecimalType::CompareLessThan(const Value &left, const Value &right) const -> CmpBool {
  assert(GetTypeId() == TypeId::TYPE_DOUBLE);
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::CmpNull;
  }

  DECIMAL_COMPARE_FUNC(<);  // NOLINT

  throw Exception("type error");
}

auto DecimalType::CompareLessThanEquals(const Value &left, const Value &right) const -> CmpBool {
  assert(GetTypeId() == TypeId::TYPE_DOUBLE);
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::CmpNull;
  }

  DECIMAL_COMPARE_FUNC(<=);  // NOLINT

  throw Exception("type error");
}

auto DecimalType::CompareGreaterThan(const Value &left, const Value &right) const -> CmpBool {
  assert(GetTypeId() == TypeId::TYPE_DOUBLE);
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::CmpNull;
  }

  DECIMAL_COMPARE_FUNC(>);  // NOLINT

  throw Exception("type error");
}

auto DecimalType::CompareGreaterThanEquals(const Value &left, const Value &right) const -> CmpBool {
  assert(GetTypeId() == TypeId::TYPE_DOUBLE);
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::CmpNull;
  }

  DECIMAL_COMPARE_FUNC(>=);  // NOLINT

  throw Exception("type error");
}

auto DecimalType::CastAs(const Value &val, const TypeId type_id) const -> Value {
  switch (type_id) {
    // case TypeId::TINYINT: {
    //   if (val.IsNull()) {
    //     return {type_id, EASYDB_INT8_NULL};
    //   }
    //   if (val.GetAs<double>() > EASYDB_INT8_MAX || val.GetAs<double>() < EASYDB_INT8_MIN) {
    //     throw Exception(ExceptionType::OUT_OF_RANGE, "Numeric value out of range.");
    //   }
    //   return {type_id, static_cast<int8_t>(val.GetAs<double>())};
    // }
    // case TypeId::SMALLINT: {
    //   if (val.IsNull()) {
    //     return {type_id, EASYDB_INT16_NULL};
    //   }
    //   if (val.GetAs<double>() > EASYDB_INT16_MAX || val.GetAs<double>() < EASYDB_INT16_MIN) {
    //     throw Exception(ExceptionType::OUT_OF_RANGE, "Numeric value out of range.");
    //   }
    //   return {type_id, static_cast<int16_t>(val.GetAs<double>())};
    // }
    case TypeId::TYPE_INT: {
      if (val.IsNull()) {
        return {type_id, EASYDB_INT32_NULL};
      }
      if (val.GetAs<double>() > EASYDB_INT32_MAX || val.GetAs<double>() < EASYDB_INT32_MIN) {
        throw Exception(ExceptionType::OUT_OF_RANGE, "Numeric value out of range.");
      }
      return {type_id, static_cast<int32_t>(val.GetAs<double>())};
    }
    case TypeId::TYPE_LONG: {
      if (val.IsNull()) {
        return {type_id, EASYDB_INT64_NULL};
      }
      if (val.GetAs<double>() >= static_cast<double>(EASYDB_INT64_MAX) ||
          val.GetAs<double>() < static_cast<double>(EASYDB_INT64_MIN)) {
        throw Exception(ExceptionType::OUT_OF_RANGE, "Numeric value out of range.");
      }
      return {type_id, static_cast<int64_t>(val.GetAs<double>())};
    }
    case TypeId::TYPE_FLOAT:
    case TypeId::TYPE_DOUBLE:
      return val.Copy();
    case TypeId::TYPE_CHAR:
    case TypeId::TYPE_VARCHAR: {
      if (val.IsNull()) {
        return {TypeId::TYPE_VARCHAR, nullptr, 0, false};
      }
      return {TypeId::TYPE_VARCHAR, val.ToString()};
    }
    default:
      break;
  }
  throw Exception("DECIMAL is not coercable to " + Type::TypeIdToString(type_id));
}

auto DecimalType::ToString(const Value &val) const -> std::string {
  if (val.IsNull()) {
    return "decimal_null";
  }
  return std::to_string(val.value_.decimal_);
}

void DecimalType::SerializeTo(const Value &val, char *storage) const {
  *reinterpret_cast<double *>(storage) = val.value_.decimal_;
}

// Deserialize a value of the given type from the given storage space.
auto DecimalType::DeserializeFrom(const char *storage) const -> Value {
  double val = *reinterpret_cast<const double *>(storage);
  return {type_id_, val};
}

auto DecimalType::Copy(const Value &val) const -> Value { return {TypeId::TYPE_DOUBLE, val.value_.decimal_}; }

}  // namespace easydb