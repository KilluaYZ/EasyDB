//! 存储引擎抽象层。
//!
//! 计算层只应依赖这里定义的逻辑描述，而不应依赖具体的页、槽目录、
//! B+Tree 或可扩展哈希实现。当前 `Database` 仍然承载默认的 Heap/页式
//! 引擎实现，后续可以把它替换为 InnoDB 风格的聚簇索引引擎。

use crate::catalog::Column;
use crate::common::rid::Rid;
use crate::common::errors::EasyDbError;
use crate::concurrency::LockManager;
use crate::recovery::LogManager;
use crate::transaction::Transaction;
use crate::transaction::WriteRecord;
use crate::types::Value;
use std::sync::{Arc, Mutex};

pub mod heap;

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

/// 存储层返回的行，不携带任何计算层的执行状态。
#[derive(Debug, Clone)]
pub struct StorageRow {
    pub rid: Rid,
    pub values: Vec<Value>,
}

/// 扫描请求。索引访问路径由存储引擎自行决定；当前 Heap 引擎先使用
/// 顺序扫描，后续可在不修改 executor 的前提下接入聚簇索引/二级索引。
#[derive(Debug, Clone, Default)]
pub struct StorageScanRequest {
    pub index: Option<IndexLookup>,
}

#[derive(Debug, Clone)]
pub struct IndexLookup {
    pub columns: Vec<String>,
    pub lower: Option<Vec<Value>>,
    pub upper: Option<Vec<Value>>,
    pub hash: bool,
}

/// 存储事务上下文。计算层只负责传递它，不接触 WAL 和锁的具体实现。
#[derive(Clone)]
pub struct StorageContext {
    pub txn: Arc<Transaction>,
    pub locks: Arc<LockManager>,
    pub log: Arc<Mutex<LogManager>>,
}

/// 存储扫描迭代器。
pub trait StorageScan: Send {
    fn next(&mut self) -> Result<Option<StorageRow>, EasyDbError>;
}

/// 单表物理访问接口。
///
/// Executor 只依赖这一层，不知道记录页、槽目录、索引句柄或版本链
/// 的具体布局。不同存储引擎只需要实现这个 trait。
pub trait StorageTable: Send + Sync {
    fn descriptor(&self) -> &TableDescriptor;
    fn scan(
        &self,
        request: StorageScanRequest,
        ctx: &StorageContext,
    ) -> Result<Box<dyn StorageScan>, EasyDbError>;
    fn insert(
        &self,
        values: &[Value],
        ctx: &StorageContext,
    ) -> Result<StorageRow, EasyDbError>;
    fn delete(&self, row: &StorageRow, ctx: &StorageContext) -> Result<(), EasyDbError>;
    fn update(
        &self,
        row: &StorageRow,
        values: &[Value],
        ctx: &StorageContext,
    ) -> Result<StorageRow, EasyDbError>;
    fn rollback(&self, record: &WriteRecord) -> Result<(), EasyDbError>;
}

/// 存储引擎的最小逻辑接口。
///
/// 计算层通过该 trait 获取 schema 和索引能力。物理扫描、版本读取、
/// 插入、更新和删除接口会在下一阶段继续加入，避免一次性把现有执行器
/// 改成难以验证的大改动。
pub trait StorageEngine {
    fn table_descriptor(&self, table: &str) -> Result<TableDescriptor, EasyDbError>;
    fn open_table(&self, table: &str) -> Result<Arc<dyn StorageTable>, EasyDbError>;
}
