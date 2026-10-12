//! 存储引擎抽象层。
//!
//! 计算层只应依赖这里定义的逻辑描述，而不应依赖具体的页、槽目录、
//! B+Tree 或可扩展哈希实现。当前 `Database` 仍然承载默认的 Heap/页式
//! 引擎实现，后续可以把它替换为 InnoDB 风格的聚簇索引引擎。

use crate::catalog::Column;
use crate::common::errors::EasyDbError;

/// 存储引擎中的索引类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexKind {
    BTree,
    ExtendibleHash,
    Clustered,
}

/// 逻辑索引描述。计算层只关心索引列和能力，不关心索引页布局。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexDescriptor {
    pub columns: Vec<String>,
    pub kind: IndexKind,
}

/// 逻辑表描述。它是 planner/analyzer 与存储引擎之间的稳定边界。
#[derive(Debug, Clone, PartialEq)]
pub struct TableDescriptor {
    pub name: String,
    pub columns: Vec<Column>,
    pub indexes: Vec<IndexDescriptor>,
}

/// 存储引擎的最小逻辑接口。
///
/// 计算层通过该 trait 获取 schema 和索引能力。物理扫描、版本读取、
/// 插入、更新和删除接口会在下一阶段继续加入，避免一次性把现有执行器
/// 改成难以验证的大改动。
pub trait StorageEngine {
    fn table_descriptor(&self, table: &str) -> Result<TableDescriptor, EasyDbError>;
}
