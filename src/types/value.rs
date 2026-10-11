//! SQL 值。
//!
//! C++ 版中这是 `type/value.h` + `type/*.cpp` 的 `Value`/`Type` 双继承体系
//! （~2.4k 行，含 `manage_data_` 手工内存管理），Rust 版收敛为一个枚举：
//! 比较、算术、格式化全部用 `match` 分发。

use std::cmp::Ordering;
use std::fmt;

use crate::common::errors::EasyDbError;
use crate::types::ColType;

/// SQL 值。
///
/// 存储口径与 C++ 版一致：DATE 为 i64 编码的时间戳；FLOAT 与 DOUBLE 均以
/// f64 存储；CHAR/VARCHAR 共用 [`Value::Str`]（定长/变长属性由列定义决定）。
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// 空值（对应 C++ TYPE_EMPTY）
    Empty,
    Int(i32),
    Long(i64),
    Float(f64),
    Double(f64),
    /// CHAR / VARCHAR 字符串
    Str(String),
    /// DATE：i64 编码的时间戳
    Date(i64),
}

impl Value {
    /// 值对应的列类型。`Str` 一律映射为 VARCHAR（CHAR 与 VARCHAR 在值层面
    /// 行为一致，定长属性只属于列元数据）。
    pub fn col_type(&self) -> ColType {
        match self {
            Value::Empty => ColType::Empty,
            Value::Int(_) => ColType::Int,
            Value::Long(_) => ColType::Long,
            Value::Float(_) => ColType::Float,
            Value::Double(_) => ColType::Double,
            Value::Str(_) => ColType::VarChar,
            Value::Date(_) => ColType::Date,
        }
    }

    /// 同域比较。
    ///
    /// Int/Long 互通、Float/Double 互通；跨族（如 INT vs VARCHAR）返回
    /// [`EasyDbError::IncompatibleType`]，对应 C++ 的 `IncompatibleTypeError`。
    ///
    /// 浮点 NaN 与 C++ 版一致地“不等于任何值”；为使排序结果全序化，
    /// NaN 参与比较时归约为 [`Ordering::Equal`]。
    pub fn compare(&self, other: &Value) -> Result<Ordering, EasyDbError> {
        let ord = match (self, other) {
            (Value::Int(a), Value::Int(b)) => a.cmp(b),
            (Value::Long(a), Value::Long(b)) => a.cmp(b),
            (Value::Int(a), Value::Long(b)) => i64::from(*a).cmp(b),
            (Value::Long(a), Value::Int(b)) => a.cmp(&i64::from(*b)),
            (Value::Float(a) | Value::Double(a), Value::Float(b) | Value::Double(b)) => {
                a.partial_cmp(b).unwrap_or(Ordering::Equal)
            }
            (Value::Str(a), Value::Str(b)) => a.cmp(b),
            (Value::Date(a), Value::Date(b)) => a.cmp(b),
            (Value::Empty, Value::Empty) => Ordering::Equal,
            _ => {
                return Err(EasyDbError::IncompatibleType(
                    self.col_type().as_str().to_string(),
                    other.col_type().as_str().to_string(),
                ));
            }
        };
        Ok(ord)
    }

    /// 数值加法（聚合 SUM 等使用）。
    ///
    /// 整数溢出返回 [`EasyDbError::Internal`] 而非静默回绕；Int/Long 混算
    /// 提升为 Long；Float/Double 混算提升为 Double。
    pub fn checked_add(&self, other: &Value) -> Result<Value, EasyDbError> {
        let overflow = || EasyDbError::Internal("integer addition overflow".to_string());
        Ok(match (self, other) {
            (Value::Int(a), Value::Int(b)) => Value::Int(a.checked_add(*b).ok_or_else(overflow)?),
            (Value::Long(a), Value::Long(b)) => {
                Value::Long(a.checked_add(*b).ok_or_else(overflow)?)
            }
            (Value::Int(a), Value::Long(b)) => {
                Value::Long(i64::from(*a).checked_add(*b).ok_or_else(overflow)?)
            }
            (Value::Long(a), Value::Int(b)) => {
                Value::Long(a.checked_add(i64::from(*b)).ok_or_else(overflow)?)
            }
            (Value::Float(a) | Value::Double(a), Value::Float(b) | Value::Double(b)) => {
                Value::Double(a + b)
            }
            _ => {
                return Err(EasyDbError::IncompatibleType(
                    self.col_type().as_str().to_string(),
                    other.col_type().as_str().to_string(),
                ));
            }
        })
    }
}

impl fmt::Display for Value {
    /// 数值/字符串的基础格式。SELECT 结果行的精确输出格式（如 DATE 的显示、
    /// 浮点精度）将在 P5 执行器阶段与 C++ 版黑盒输出对比后统一对齐。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Empty => Ok(()),
            Value::Int(v) => write!(f, "{v}"),
            Value::Long(v) => write!(f, "{v}"),
            Value::Float(v) | Value::Double(v) => write!(f, "{v}"),
            Value::Str(s) => f.write_str(s),
            Value::Date(v) => write!(f, "{v}"),
        }
    }
}

impl From<i32> for Value {
    fn from(v: i32) -> Self {
        Value::Int(v)
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::Long(v)
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::Double(v)
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::Str(v)
    }
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::Str(v.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn col_type_mapping() {
        assert_eq!(Value::Int(1).col_type(), ColType::Int);
        assert_eq!(Value::Long(1).col_type(), ColType::Long);
        assert_eq!(Value::Float(1.0).col_type(), ColType::Float);
        assert_eq!(Value::Double(1.0).col_type(), ColType::Double);
        assert_eq!(Value::Str("a".into()).col_type(), ColType::VarChar);
        assert_eq!(Value::Date(0).col_type(), ColType::Date);
    }

    #[test]
    fn compare_within_and_across_integer_family() {
        assert_eq!(
            Value::Int(1).compare(&Value::Int(2)).unwrap(),
            Ordering::Less
        );
        assert_eq!(
            Value::Long(3).compare(&Value::Long(2)).unwrap(),
            Ordering::Greater
        );
        // Int 与 Long 互通比较
        assert_eq!(
            Value::Int(2).compare(&Value::Long(2_i64)).unwrap(),
            Ordering::Equal
        );
        assert_eq!(
            Value::Long(5).compare(&Value::Int(4)).unwrap(),
            Ordering::Greater
        );
    }

    #[test]
    fn compare_float_family() {
        assert_eq!(
            Value::Float(0.5).compare(&Value::Double(1.5)).unwrap(),
            Ordering::Less
        );
        // NaN 归约为 Equal（见 compare 文档）
        let nan = Value::Double(f64::NAN);
        assert_eq!(nan.compare(&Value::Double(1.0)).unwrap(), Ordering::Equal);
    }

    #[test]
    fn compare_incompatible_types_error() {
        let err = Value::Int(1).compare(&Value::Str("a".into())).unwrap_err();
        assert!(err.to_string().contains("Incompatible type error"));
    }

    #[test]
    fn checked_add_variants() {
        assert_eq!(
            Value::Int(1).checked_add(&Value::Int(2)).unwrap(),
            Value::Int(3)
        );
        assert_eq!(
            Value::Int(1).checked_add(&Value::Long(2)).unwrap(),
            Value::Long(3)
        );
        assert_eq!(
            Value::Float(0.5).checked_add(&Value::Double(0.25)).unwrap(),
            Value::Double(0.75)
        );
        // 溢出报错而非回绕
        assert!(Value::Int(i32::MAX).checked_add(&Value::Int(1)).is_err());
        // 字符串不可加
        assert!(
            Value::Str("a".into())
                .checked_add(&Value::Str("b".into()))
                .is_err()
        );
    }

    #[test]
    fn display_basics() {
        assert_eq!(Value::Int(-3).to_string(), "-3");
        assert_eq!(Value::Long(42).to_string(), "42");
        assert_eq!(Value::Double(1.5).to_string(), "1.5");
        assert_eq!(Value::Str("alice".into()).to_string(), "alice");
        assert_eq!(Value::Empty.to_string(), "");
    }

    #[test]
    fn from_conversions() {
        assert_eq!(Value::from(1_i32), Value::Int(1));
        assert_eq!(Value::from(1_i64), Value::Long(1));
        assert_eq!(Value::from(1.5_f64), Value::Double(1.5));
        assert_eq!(Value::from("hi"), Value::Str("hi".to_string()));
    }
}
