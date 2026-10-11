//! EasyDB —— C++ 版关系型数据库的 Rust 重写。
//!
//! 模块结构与 C++ 版（`src-cpp/`）一一对应，迁移计划见
//! `doc/rust-migration-plan.md`。各阶段进度：
//!
//! - P1 公共基础层：`common`（错误/配置/RID）、`types`（类型与值）、`catalog`（列/模式）
//! - P2 存储层起：磁盘管理、缓冲池、记录管理（待实现）

pub mod catalog;
pub mod common;
pub mod types;
