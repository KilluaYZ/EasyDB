/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * bigint_type.cpp
 *
 * Identification: src/type/bigint_type.cpp
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
#include "type/type_id.h"

#include "type/bigint_type.h"

namespace easydb {
#define BIGINT_COMPARE_FUNC(OP)                                           \
  switch (right.GetTypeId()) {                                            \
    case TypeId::TYPE_INT:                                                \
      return GetCmpBool(left.value_.bigint_ OP right.GetAs<int32_t>());   \
    case TypeId::TYPE_LONG:                                               \
      return GetCmpBool(left.value_.bigint_ OP right.GetAs<int64_t>());   \
    case TypeId::TYPE_FLOAT:                                              \
    case TypeId::TYPE_DOUBLE:                                             \
      return GetCmpBool(left.value_.bigint_ OP right.GetAs<double>());    \
    case TypeId::TYPE_CHAR:                                               \
    case TypeId::TYPE_VARCHAR: {                                          \
      auto r_value = right.CastAs(TypeId::TYPE_LONG);                     \
      return GetCmpBool(left.value_.bigint_ OP r_value.GetAs<int64_t>()); \
    }                                                                     \
    default:                                                              \
      break;                                                              \
  }  // SWITCH

#define BIGINT_MODIFY_FUNC(METHOD, OP)                                                 \
  switch (right.GetTypeId()) {                                                         \
    case TypeId::TYPE_INT:                                                             \
      /* NOLINTNEXTLINE */                                                             \
      return METHOD<int64_t, int32_t>(left, right);                                    \
    case TypeId::TYPE_LONG:                                                            \
      /* NOLINTNEXTLINE */                                                             \
      return METHOD<int64_t, int64_t>(left, right);                                    \
    case TypeId::TYPE_FLOAT:                                                           \
    case TypeId::TYPE_DOUBLE:                                                          \
      /* NOLINTNEXTLINE */                                                             \
      return Value(TypeId::TYPE_DOUBLE, left.value_.bigint_ OP right.GetAs<double>()); \
    case TypeId::TYPE_CHAR:                                                            \
    case TypeId::TYPE_VARCHAR: {                                                       \
      auto r_value = right.CastAs(TypeId::TYPE_LONG);                                  \
      /* NOLINTNEXTLINE */                                                             \
      return METHOD<int64_t, int64_t>(left, r_value);                                  \
    }                                                                                  \
    default:                                                                           \
      break;                                                                           \
  }  // SWITCH

BigintType::BigintType() : IntegerParentType(TYPE_LONG) {}

auto BigintType::IsZero(const Value &val) const -> bool { return (val.value_.bigint_ == 0); }

auto BigintType::Add(const Value &left, const Value &right) const -> Value {
  assert(left.CheckInteger());
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return left.OperateNull(right);
  }

  BIGINT_MODIFY_FUNC(AddValue, +);

  throw Exception("type error");
}

auto BigintType::Subtract(const Value &left, const Value &right) const -> Value {
  assert(left.CheckInteger());
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return left.OperateNull(right);
  }

  BIGINT_MODIFY_FUNC(SubtractValue, -);

  throw Exception("type error");
}

auto BigintType::Multiply(const Value &left, const Value &right) const -> Value {
  assert(left.CheckInteger());
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return left.OperateNull(right);
  }

  BIGINT_MODIFY_FUNC(MultiplyValue, *);

  throw Exception("type error");
}

auto BigintType::Divide(const Value &left, const Value &right) const -> Value {
  assert(left.CheckInteger());
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return left.OperateNull(right);
  }

  if (right.IsZero()) {
    throw Exception(ExceptionType::DIVIDE_BY_ZERO, "Division by zero on right-hand side");
  }

  BIGINT_MODIFY_FUNC(DivideValue, /);
  throw Exception("type error");
}

auto BigintType::Modulo(const Value &left, const Value &right) const -> Value {
  assert(left.CheckInteger());
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return left.OperateNull(right);
  }

  if (right.IsZero()) {
    throw Exception(ExceptionType::DIVIDE_BY_ZERO, "Division by zero on right-hand side");
  }

  switch (right.GetTypeId()) {
    // case TypeId::TINYINT:
    //   return ModuloValue<int64_t, int8_t>(left, right);
    // case TypeId::SMALLINT:
    //   return ModuloValue<int64_t, int16_t>(left, right);
    case TypeId::TYPE_INT:
      return ModuloValue<int64_t, int32_t>(left, right);
    case TypeId::TYPE_LONG:
      return ModuloValue<int64_t, int64_t>(left, right);
    case TypeId::TYPE_FLOAT:
    case TypeId::TYPE_DOUBLE:
      return {TypeId::TYPE_DOUBLE, ValMod(left.value_.bigint_, right.GetAs<double>())};
    case TypeId::TYPE_CHAR:
    case TypeId::TYPE_VARCHAR: {
      auto r_value = right.CastAs(TypeId::TYPE_LONG);
      return ModuloValue<int64_t, int64_t>(left, r_value);
    }
    default:
      break;
  }
  throw Exception("type error");
}

auto BigintType::Sqrt(const Value &val) const -> Value {
  assert(val.CheckInteger());
  if (val.IsNull()) {
    return {TypeId::TYPE_DOUBLE, EASYDB_DECIMAL_NULL};
  }

  if (val.value_.bigint_ < 0) {
    throw Exception(ExceptionType::DECIMAL, "Cannot take square root of a negative number.");
  }
  return {TypeId::TYPE_DOUBLE, std::sqrt(val.value_.bigint_)};
}

auto BigintType::OperateNull(const Value &left __attribute__((unused)), const Value &right) const -> Value {
  switch (right.GetTypeId()) {
    // case TypeId::TINYINT:
    // case TypeId::SMALLINT:
    case TypeId::TYPE_INT:
    case TypeId::TYPE_LONG:
      return {TypeId::TYPE_LONG, static_cast<int64_t>(EASYDB_INT64_NULL)};
    case TypeId::TYPE_FLOAT:
    case TypeId::TYPE_DOUBLE:
      return {TypeId::TYPE_DOUBLE, static_cast<double>(EASYDB_DECIMAL_NULL)};
    default:
      break;
  }
  throw Exception("type error");
}

auto BigintType::CompareEquals(const Value &left, const Value &right) const -> CmpBool {
  assert(left.CheckInteger());
  assert(left.CheckComparable(right));

  if (left.IsNull() || right.IsNull()) {
    return CmpBool::CmpNull;
  }

  BIGINT_COMPARE_FUNC(==);  // NOLINT

  throw Exception("type error");
}

auto BigintType::CompareNotEquals(const Value &left, const Value &right) const -> CmpBool {
  assert(left.CheckInteger());
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::CmpNull;
  }

  BIGINT_COMPARE_FUNC(!=);  // NOLINT

  throw Exception("type error");
}

auto BigintType::CompareLessThan(const Value &left, const Value &right) const -> CmpBool {
  assert(left.CheckInteger());
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::CmpNull;
  }

  BIGINT_COMPARE_FUNC(<);  // NOLINT

  throw Exception("type error");
}

auto BigintType::CompareLessThanEquals(const Value &left, const Value &right) const -> CmpBool {
  assert(left.CheckInteger());
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::CmpNull;
  }

  BIGINT_COMPARE_FUNC(<=);  // NOLINT

  throw Exception("type error");
}

auto BigintType::CompareGreaterThan(const Value &left, const Value &right) const -> CmpBool {
  assert(left.CheckInteger());
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::CmpNull;
  }

  BIGINT_COMPARE_FUNC(>);  // NOLINT

  throw Exception("type error");
}

auto BigintType::CompareGreaterThanEquals(const Value &left, const Value &right) const -> CmpBool {
  assert(left.CheckInteger());
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::CmpNull;
  }

  BIGINT_COMPARE_FUNC(>=);  // NOLINT
  throw Exception("type error");
}

auto BigintType::ToString(const Value &val) const -> std::string {
  assert(val.CheckInteger());

  if (val.IsNull()) {
    return "bigint_null";
  }
  return std::to_string(val.value_.bigint_);
}

void BigintType::SerializeTo(const Value &val, char *storage) const {
  *reinterpret_cast<int64_t *>(storage) = val.value_.bigint_;
}

// Deserialize a value of the given type from the given storage space.
auto BigintType::DeserializeFrom(const char *storage) const -> Value {
  int64_t val = *reinterpret_cast<const int64_t *>(storage);
  return {type_id_, val};
}

auto BigintType::Copy(const Value &val) const -> Value { return {TypeId::TYPE_LONG, val.value_.bigint_}; }

auto BigintType::CastAs(const Value &val, const TypeId type_id) const -> Value {
  switch (type_id) {
    // case TypeId::TINYINT: {
    //   if (val.IsNull()) {
    //     return {type_id, EASYDB_INT8_NULL};
    //   }
    //   if (val.GetAs<int64_t>() > EASYDB_INT8_MAX || val.GetAs<int64_t>() < EASYDB_INT8_MIN) {
    //     throw Exception(ExceptionType::OUT_OF_RANGE, "Numeric value out of range.");
    //   }
    //   return {type_id, static_cast<int8_t>(val.GetAs<int64_t>())};
    // }
    // case TypeId::SMALLINT: {
    //   if (val.IsNull()) {
    //     return {type_id, EASYDB_INT16_NULL};
    //   }
    //   if (val.GetAs<int64_t>() > EASYDB_INT16_MAX || val.GetAs<int64_t>() < EASYDB_INT16_MIN) {
    //     throw Exception(ExceptionType::OUT_OF_RANGE, "Numeric value out of range.");
    //   }
    //   return {type_id, static_cast<int16_t>(val.GetAs<int64_t>())};
    // }
    case TypeId::TYPE_INT: {
      if (val.IsNull()) {
        return {type_id, EASYDB_INT32_NULL};
      }
      if (val.GetAs<int64_t>() > EASYDB_INT32_MAX || val.GetAs<int64_t>() < EASYDB_INT32_MIN) {
        throw Exception(ExceptionType::OUT_OF_RANGE, "Numeric value out of range.");
      }
      return {type_id, static_cast<int32_t>(val.GetAs<int64_t>())};
    }

    case TypeId::TYPE_LONG: {
      if (val.IsNull()) {
        return {type_id, EASYDB_INT64_NULL};
      }
      return Copy(val);
    }

    case TypeId::TYPE_FLOAT:
    case TypeId::TYPE_DOUBLE: {
      if (val.IsNull()) {
        return {type_id, EASYDB_DECIMAL_NULL};
      }
      return {type_id, static_cast<double>(val.GetAs<int64_t>())};
    }

    case TypeId::TYPE_CHAR:
    case TypeId::TYPE_VARCHAR: {
      if (val.IsNull()) {
        return {TYPE_VARCHAR, nullptr, 0, false};
      }
      return {TYPE_VARCHAR, val.ToString()};
    }
    default:
      break;
  }
  throw Exception("bigint is not coercable to " + Type::TypeIdToString(type_id));
}

}  // namespace easydb