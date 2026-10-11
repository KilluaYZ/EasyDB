//! SQL 类型系统。
//!
//! C++ 版 `src-cpp/type/` 中 ~2.4k 行的 `Value`/`Type` 双继承体系在此
//! 收敛为一个枚举（`Value`）+ 两个小枚举（`ColType`/`AggregationType`）。

mod col_type;
mod value;

pub use col_type::{AggregationType, ColType};
pub use value::Value;
