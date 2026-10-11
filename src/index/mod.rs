//! 索引：B+ 树索引。
//!
//! 对应 C++ 版 `src-cpp/storage/index/`（可扩展哈希索引按决议不做）。

pub mod extendible_hash;
pub mod ix_defs;
pub mod ix_index_handle;
pub mod ix_manager;
pub mod ix_scan;
