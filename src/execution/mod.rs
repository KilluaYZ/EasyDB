//! 查询执行：火山模型执行器。
//!
//! 对应 C++ 版 `src-cpp/execution/`。C++ 用 `AbstractExecutor` 虚继承 +
//! `shared_ptr<void>` 装载算子树，Rust 版为 `Box<dyn Executor>` trait 对象。
//!
//! 行表示：扫描节点解码出 `Vec<Value>` 并携带 RID（DML 需要），
//! 上层算子（投影/聚合/连接）直接以值传递，避免 C++ 版反复的
//! 字节解码。子查询在计划期即求解（仅支持非相关子查询）。

pub mod dml;
pub mod query_execs;
pub mod scan;

use std::cmp::Ordering;

use crate::common::errors::EasyDbError;
use crate::common::rid::Rid;
use crate::parser::ast::{ArithOp, CompOp};
use crate::types::Value;

/// 一行：RID + 已解码的列值。
#[derive(Debug, Clone)]
pub struct Row {
    pub rid: Rid,
    pub values: Vec<Value>,
}

/// 行模式：列的 (表名, 列名) 顺序，用于把列引用解析为下标。
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RowSchema {
    pub cols: Vec<(String, String)>,
}

impl RowSchema {
    pub fn new(cols: Vec<(String, String)>) -> Self {
        Self { cols }
    }

    /// 解析列引用为下标；未限定名要求唯一匹配。
    pub fn index_of(&self, tab: &str, col: &str) -> Option<usize> {
        if tab.is_empty() {
            let mut found = None;
            for (i, (_, c)) in self.cols.iter().enumerate() {
                if c == col {
                    if found.is_some() {
                        return None; // 歧义
                    }
                    found = Some(i);
                }
            }
            found
        } else {
            self.cols.iter().position(|(t, c)| t == tab && c == col)
        }
    }
}

/// 条件求值所需的已解析信息（列引用 → 行内下标）。
#[derive(Debug, Clone)]
pub struct ResolvedCond {
    pub lhs: usize,
    pub op: CompOp,
    pub rhs: ResolvedRhs,
}

#[derive(Debug, Clone)]
pub enum ResolvedRhs {
    Value(Value),
    Col(usize),
    List(Vec<Value>),
}

/// 执行器接口（火山模型）。
pub trait Executor {
    /// 输出行模式。
    fn schema(&self) -> &RowSchema;
    /// 取下一行；结束返回 `None`。
    fn next(&mut self) -> Result<Option<Row>, EasyDbError>;
}

/// 带数值提升的比较：Int/Long/Float/Double 互相可比较，
/// 字符串按字节、DATE 按其 i64 值；其余组合报不兼容。
pub fn cmp_values(a: &Value, b: &Value) -> Result<Ordering, EasyDbError> {
    use Value::*;
    let incompatible = || {
        Err(EasyDbError::IncompatibleType(
            a.col_type().as_str().to_string(),
            b.col_type().as_str().to_string(),
        ))
    };
    Ok(match (a, b) {
        // 数值族：整型精确比较，跨整型/浮点经 f64
        (Int(_) | Long(_), Int(_) | Long(_)) => {
            let ai = match a {
                Int(v) => i64::from(*v),
                Long(v) => *v,
                _ => unreachable!(),
            };
            let bi = match b {
                Int(v) => i64::from(*v),
                Long(v) => *v,
                _ => unreachable!(),
            };
            ai.cmp(&bi)
        }
        (Float(_) | Double(_), Int(_) | Long(_)) | (Int(_) | Long(_), Float(_) | Double(_)) => {
            let fa = match a {
                Int(v) => i64::from(*v) as f64,
                Long(v) => *v as f64,
                Float(v) | Double(v) => *v,
                _ => unreachable!(),
            };
            let fb = match b {
                Int(v) => i64::from(*v) as f64,
                Long(v) => *v as f64,
                Float(v) | Double(v) => *v,
                _ => unreachable!(),
            };
            fa.partial_cmp(&fb).unwrap_or(Ordering::Equal)
        }
        (Float(_) | Double(_), Float(_) | Double(_)) => {
            let (fa, fb) = (as_f64(a), as_f64(b));
            fa.partial_cmp(&fb).unwrap_or(Ordering::Equal)
        }
        (Str(x), Str(y)) => x.cmp(y),
        (Date(x), Date(y)) => x.cmp(y),
        _ => return incompatible(),
    })
}

fn as_f64(v: &Value) -> f64 {
    match v {
        Value::Float(x) | Value::Double(x) => *x,
        Value::Int(x) => i64::from(*x) as f64,
        Value::Long(x) => *x as f64,
        _ => 0.0,
    }
}

/// 条件求值。
pub fn eval_cond(cond: &ResolvedCond, row: &Row) -> Result<bool, EasyDbError> {
    let lhs = &row.values[cond.lhs];
    let satisfied = match &cond.rhs {
        ResolvedRhs::Value(v) => apply_op(cond.op, &cmp_values(lhs, v)?) && cond.op != CompOp::In,
        ResolvedRhs::Col(idx) => {
            apply_op(cond.op, &cmp_values(lhs, &row.values[*idx])?) && cond.op != CompOp::In
        }
        ResolvedRhs::List(list) => {
            debug_assert_eq!(cond.op, CompOp::In);
            list.iter().any(|v| {
                cmp_values(lhs, v)
                    .map(|o| o == Ordering::Equal)
                    .unwrap_or(false)
            })
        }
    };
    Ok(satisfied)
}

fn apply_op(op: CompOp, ord: &Ordering) -> bool {
    use Ordering::*;
    match op {
        CompOp::Eq => ord == &Equal,
        CompOp::Ne => ord != &Equal,
        CompOp::Lt => ord == &Less,
        CompOp::Gt => ord == &Greater,
        CompOp::Le => ord != &Greater,
        CompOp::Ge => ord != &Less,
        CompOp::In => false, // IN 走 List 分支
    }
}

/// 算术运算（UPDATE 赋值表达式与 SUM 使用）。
///
/// 提升规则：Int⊕Int → Int（溢出报错不回绕）；含 Long → Long；
/// 含浮点 → Double；日期/字符串不参与算术。
pub fn arith(op: ArithOp, a: &Value, b: &Value) -> Result<Value, EasyDbError> {
    use Value::*;
    let overflow = || EasyDbError::Internal("integer arithmetic overflow".to_string());
    let incompatible = || {
        Err(EasyDbError::IncompatibleType(
            a.col_type().as_str().to_string(),
            b.col_type().as_str().to_string(),
        ))
    };
    let float_arith = || {
        let (x, y) = (as_f64(a), as_f64(b));
        let r = match op {
            ArithOp::Add => x + y,
            ArithOp::Sub => x - y,
            ArithOp::Mul => x * y,
            ArithOp::Div => {
                if y == 0.0 {
                    return Err(EasyDbError::Internal("division by zero".to_string()));
                }
                x / y
            }
        };
        Ok(Double(r))
    };
    match (a, b) {
        (Int(x), Int(y)) => {
            let (x, y) = (*x, *y);
            match op {
                ArithOp::Add => Ok(Int(x.checked_add(y).ok_or_else(overflow)?)),
                ArithOp::Sub => Ok(Int(x.checked_sub(y).ok_or_else(overflow)?)),
                ArithOp::Mul => Ok(Int(x.checked_mul(y).ok_or_else(overflow)?)),
                ArithOp::Div => {
                    if y == 0 {
                        return Err(EasyDbError::Internal("division by zero".to_string()));
                    }
                    Ok(Int(x / y))
                }
            }
        }
        (Long(x), Long(y)) => {
            let (x, y) = (*x, *y);
            match op {
                ArithOp::Add => Ok(Long(x.checked_add(y).ok_or_else(overflow)?)),
                ArithOp::Sub => Ok(Long(x.checked_sub(y).ok_or_else(overflow)?)),
                ArithOp::Mul => Ok(Long(x.checked_mul(y).ok_or_else(overflow)?)),
                ArithOp::Div => {
                    if y == 0 {
                        return Err(EasyDbError::Internal("division by zero".to_string()));
                    }
                    Ok(Long(x / y))
                }
            }
        }
        (Int(_), Long(_)) | (Long(_), Int(_)) => {
            let (x, y) = (to_i64(a), to_i64(b));
            match op {
                ArithOp::Add => Ok(Long(x.checked_add(y).ok_or_else(overflow)?)),
                ArithOp::Sub => Ok(Long(x.checked_sub(y).ok_or_else(overflow)?)),
                ArithOp::Mul => Ok(Long(x.checked_mul(y).ok_or_else(overflow)?)),
                ArithOp::Div => {
                    if y == 0 {
                        return Err(EasyDbError::Internal("division by zero".to_string()));
                    }
                    Ok(Long(x / y))
                }
            }
        }
        (Float(_) | Double(_), Float(_) | Double(_))
        | (Float(_) | Double(_), Int(_) | Long(_))
        | (Int(_) | Long(_), Float(_) | Double(_)) => float_arith(),
        _ => incompatible(),
    }
}

fn to_i64(v: &Value) -> i64 {
    match v {
        Value::Int(x) => i64::from(*x),
        Value::Long(x) => *x,
        _ => 0,
    }
}

/// 把字面值转换为列类型的值（索引键编码与 UPDATE 回写使用）。
pub fn coerce_value_to(col_type: crate::types::ColType, v: &Value) -> Result<Value, EasyDbError> {
    use crate::types::ColType;
    Ok(match (col_type, v) {
        (ColType::Int, Value::Int(n)) => Value::Int(*n),
        (ColType::Int, Value::Long(n)) => Value::Int(
            i32::try_from(*n)
                .map_err(|_| EasyDbError::Internal("long value out of int range".to_string()))?,
        ),
        (ColType::Long, Value::Long(n)) => Value::Long(*n),
        (ColType::Long, Value::Int(n)) => Value::Long(i64::from(*n)),
        (ColType::Float, Value::Float(_) | Value::Double(_))
        | (ColType::Double, Value::Float(_) | Value::Double(_)) => v.clone(),
        (ColType::Date, Value::Date(n)) => Value::Date(*n),
        (ColType::Char, Value::Str(s)) | (ColType::VarChar, Value::Str(s)) => Value::Str(s.clone()),
        (t, other) => {
            return Err(EasyDbError::IncompatibleType(
                t.as_str().to_string(),
                other.col_type().as_str().to_string(),
            ));
        }
    })
}
