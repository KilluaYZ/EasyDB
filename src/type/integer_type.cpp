/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * integer_type.cpp
 *
 * Identification: src/type/integer_type.cpp
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

#include "type/integer_type.h"

namespace easydb {
#define INT_COMPARE_FUNC(OP)                                               \
  switch (right.GetTypeId()) {                                             \
    case TypeId::TYPE_INT:                                                 \
      return GetCmpBool(left.value_.integer_ OP right.GetAs<int32_t>());   \
    case TypeId::TYPE_LONG:                                                \
      return GetCmpBool(left.value_.integer_ OP right.GetAs<int64_t>());   \
    case TypeId::TYPE_FLOAT:                                               \
    case TypeId::TYPE_DOUBLE:                                              \
      return GetCmpBool(left.value_.integer_ OP right.GetAs<double>());    \
    case TypeId::TYPE_CHAR:                                                \
    case TypeId::TYPE_VARCHAR: {                                           \
      auto r_value = right.CastAs(TypeId::TYPE_INT);                       \
      return GetCmpBool(left.value_.integer_ OP r_value.GetAs<int32_t>()); \
    }                                                                      \
    default:                                                               \
      break;                                                               \
  }  // SWITCH

#define INT_MODIFY_FUNC(METHOD, OP)                                                     \
  switch (right.GetTypeId()) {                                                          \
    case TypeId::TYPE_INT:                                                              \
      /* NOLINTNEXTLINE */                                                              \
      return METHOD<int32_t, int32_t>(left, right);                                     \
    case TypeId::TYPE_LONG:                                                             \
      /* NOLINTNEXTLINE */                                                              \
      return METHOD<int32_t, int64_t>(left, right);                                     \
    case TypeId::TYPE_FLOAT:                                                            \
    case TypeId::TYPE_DOUBLE:                                                           \
      /* NOLINTNEXTLINE */                                                              \
      return Value(TypeId::TYPE_DOUBLE, left.value_.integer_ OP right.GetAs<double>()); \
    case TypeId::TYPE_CHAR:                                                             \
    case TypeId::TYPE_VARCHAR: {                                                        \
      auto r_value = right.CastAs(TypeId::TYPE_INT);                                    \
      /* NOLINTNEXTLINE */                                                              \
      return METHOD<int32_t, int32_t>(left, r_value);                                   \
    }                                                                                   \
    default:                                                                            \
      break;                                                                            \
  }  // SWITCH

IntegerType::IntegerType(TypeId type) : IntegerParentType(type) {}

auto IntegerType::IsZero(const Value &val) const -> bool { return (val.value_.integer_ == 0); }

auto IntegerType::Add(const Value &left, const Value &right) const -> Value {
  assert(left.CheckInteger());
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return left.OperateNull(right);
  }

  INT_MODIFY_FUNC(AddValue, +);
  throw Exception("type error");
}

auto IntegerType::Subtract(const Value &left, const Value &right) const -> Value {
  assert(left.CheckInteger());
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return left.OperateNull(right);
  }

  INT_MODIFY_FUNC(SubtractValue, -);

  throw Exception("type error");
}

auto IntegerType::Multiply(const Value &left, const Value &right) const -> Value {
  assert(left.CheckInteger());
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return left.OperateNull(right);
  }

  INT_MODIFY_FUNC(MultiplyValue, *);

  throw Exception("type error");
}

auto IntegerType::Divide(const Value &left, const Value &right) const -> Value {
  assert(left.CheckInteger());
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return left.OperateNull(right);
  }

  if (right.IsZero()) {
    throw Exception(ExceptionType::DIVIDE_BY_ZERO, "Division by zero on right-hand side");
  }

  INT_MODIFY_FUNC(DivideValue, /);

  throw Exception("type error");
}

auto IntegerType::Modulo(const Value &left, const Value &right) const -> Value {
  assert(left.CheckInteger());
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return left.OperateNull(right);
  }

  if (right.IsZero()) {
    throw Exception(ExceptionType::DIVIDE_BY_ZERO, "Division by zero on right-hand side");
  }

  switch (right.GetTypeId()) {
      //   case TypeId::TINYINT:
      //     return ModuloValue<int32_t, int8_t>(left, right);
      //   case TypeId::SMALLINT:
      //     return ModuloValue<int32_t, int16_t>(left, right);
    case TypeId::TYPE_INT:
      return ModuloValue<int32_t, int32_t>(left, right);
    case TypeId::TYPE_LONG:
      return ModuloValue<int32_t, int64_t>(left, right);
    case TypeId::TYPE_FLOAT:
    case TypeId::TYPE_DOUBLE:
      return {TypeId::TYPE_DOUBLE, ValMod(left.value_.integer_, right.GetAs<double>())};
    case TypeId::TYPE_CHAR:
    case TypeId::TYPE_VARCHAR: {
      auto r_value = right.CastAs(TypeId::TYPE_INT);
      return ModuloValue<int32_t, int32_t>(left, r_value);
    }
    default:
      break;
  }

  throw Exception("type error");
}

auto IntegerType::Sqrt(const Value &val) const -> Value {
  assert(val.CheckInteger());
  if (val.IsNull()) {
    return OperateNull(val, val);
  }

  if (val.value_.integer_ < 0) {
    throw Exception(ExceptionType::DECIMAL, "Cannot take square root of a negative number.");
  }
  return {TypeId::TYPE_DOUBLE, std::sqrt(val.value_.integer_)};
}

auto IntegerType::OperateNull(const Value &left __attribute__((unused)), const Value &right) const -> Value {
  switch (right.GetTypeId()) {
      //   case TypeId::TINYINT:
      //   case TypeId::SMALLINT:
    case TypeId::TYPE_INT:
      return {TypeId::TYPE_INT, static_cast<int32_t>(EASYDB_INT32_NULL)};
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

auto IntegerType::CompareEquals(const Value &left, const Value &right) const -> CmpBool {
  assert(left.CheckInteger());
  assert(left.CheckComparable(right));

  if (left.IsNull() || right.IsNull()) {
    return CmpBool::CmpNull;
  }

  INT_COMPARE_FUNC(==);  // NOLINT

  throw Exception("type error");
}

auto IntegerType::CompareNotEquals(const Value &left, const Value &right) const -> CmpBool {
  assert(left.CheckInteger());
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::CmpNull;
  }

  INT_COMPARE_FUNC(!=);  // NOLINT

  throw Exception("type error");
}

auto IntegerType::CompareLessThan(const Value &left, const Value &right) const -> CmpBool {
  assert(left.CheckInteger());
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::CmpNull;
  }

  INT_COMPARE_FUNC(<);  // NOLINT

  throw Exception("type error");
}

auto IntegerType::CompareLessThanEquals(const Value &left, const Value &right) const -> CmpBool {
  assert(left.CheckInteger());
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::CmpNull;
  }

  INT_COMPARE_FUNC(<=);  // NOLINT

  throw Exception("type error");
}

auto IntegerType::CompareGreaterThan(const Value &left, const Value &right) const -> CmpBool {
  assert(left.CheckInteger());
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::CmpNull;
  }

  INT_COMPARE_FUNC(>);  // NOLINT

  throw Exception("type error");
}

auto IntegerType::CompareGreaterThanEquals(const Value &left, const Value &right) const -> CmpBool {
  assert(left.CheckInteger());
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::CmpNull;
  }

  INT_COMPARE_FUNC(>=);  // NOLINT

  throw Exception("type error");
}

auto IntegerType::ToString(const Value &val) const -> std::string {
  assert(val.CheckInteger());

  if (val.IsNull()) {
    return "integer_null";
  }
  return std::to_string(val.value_.integer_);
}

void IntegerType::SerializeTo(const Value &val, char *storage) const {
  *reinterpret_cast<int32_t *>(storage) = val.value_.integer_;
}

// Deserialize a value of the given type from the given storage space.
auto IntegerType::DeserializeFrom(const char *storage) const -> Value {
  int32_t val = *reinterpret_cast<const int32_t *>(storage);
  return {type_id_, val};
}

auto IntegerType::Copy(const Value &val) const -> Value {
  assert(val.CheckInteger());
  return {val.GetTypeId(), val.value_.integer_};
}

auto IntegerType::CastAs(const Value &val, const TypeId type_id) const -> Value {
  switch (type_id) {
      //   case TypeId::TINYINT: {
      //     if (val.IsNull()) {
      //       return {type_id, EASYDB_INT8_NULL};
      //     }
      //     if (val.GetAs<int32_t>() > EASYDB_INT8_MAX || val.GetAs<int32_t>() < EASYDB_INT8_MIN) {
      //       throw Exception(ExceptionType::OUT_OF_RANGE, "Numeric value out of range.");
      //     }
      //     return {type_id, static_cast<int8_t>(val.GetAs<int32_t>())};
      //   }
      //   case TypeId::SMALLINT: {
      //     if (val.IsNull()) {
      //       return {type_id, EASYDB_INT16_NULL};
      //     }
      //     if (val.GetAs<int32_t>() > EASYDB_INT16_MAX || val.GetAs<int32_t>() < EASYDB_INT16_MIN) {
      //       throw Exception(ExceptionType::OUT_OF_RANGE, "Numeric value out of range.");
      //     }
      //     return {type_id, static_cast<int16_t>(val.GetAs<int32_t>())};
      //   }
    case TypeId::TYPE_INT: {
      if (val.IsNull()) {
        return {type_id, EASYDB_INT32_NULL};
      }
      return {type_id, static_cast<int32_t>(val.GetAs<int32_t>())};
    }
    case TypeId::TYPE_LONG: {
      if (val.IsNull()) {
        return {type_id, EASYDB_INT64_NULL};
      }
      return {type_id, static_cast<int64_t>(val.GetAs<int32_t>())};
    }
    case TypeId::TYPE_FLOAT:
    case TypeId::TYPE_DOUBLE: {
      if (val.IsNull()) {
        return {type_id, EASYDB_DECIMAL_NULL};
      }
      return {type_id, static_cast<double>(val.GetAs<int32_t>())};
    }
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
  throw Exception("Integer is not coercable to " + Type::TypeIdToString(type_id));
}

}  // namespace easydb