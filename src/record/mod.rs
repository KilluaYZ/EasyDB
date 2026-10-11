//! 记录管理：表数据文件的页面布局与记录 CRUD。
//!
//! 对应 C++ 版 `src-cpp/record/`。

pub mod bitmap;
pub mod rm_defs;
pub mod rm_file_handle;
pub mod rm_manager;
pub mod rm_scan;
pub mod tuple;
