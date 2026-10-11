//! EasyDB —— C++ 版关系型数据库的 Rust 重写。
//!
//! 模块结构与 C++ 版（`src-cpp/`）一一对应，迁移计划见
//! `doc/rust-migration-plan.md`。各阶段进度：
//!
//! - P1 公共基础层：`common`（错误/配置/RID）、`types`（类型与值）、`catalog`（列/模式）
//! - P2 存储层：`storage`（磁盘/页）、`buffer`（LRU 与缓冲池 PageGuard）、
//!   `record`（slotted page 记录层与元组编解码）
//! - P3 起：B+ 树索引、SQL 前端（待实现）

pub mod analyze;
pub mod buffer;
pub mod catalog;
pub mod common;
pub mod execution;
pub mod index;
pub mod parser;
pub mod planner;
pub mod record;
pub mod storage;
pub mod system;
pub mod types;
