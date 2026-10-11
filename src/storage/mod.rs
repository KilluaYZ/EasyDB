//! 存储层：磁盘管理与页标识。
//!
//! 对应 C++ 版 `src-cpp/storage/`（缓冲池位于 [`crate::buffer`]）。

pub mod disk;
pub mod page;
