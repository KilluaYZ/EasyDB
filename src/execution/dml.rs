//! DML 执行器：INSERT / DELETE / UPDATE。
//!
//! 对应 C++ 版 `executor_insert.cpp` / `executor_delete.cpp` /
//! `executor_update.cpp`。差异：
//! - 索引维护（插入/删除条目）在执行器内完成；
//! - UPDATE 沿用 C++ 的原地更新语义（新数据不得超过旧长度）。

use std::sync::Arc;

use super::{ExecCtx, Executor, Row, RowSchema, arith, coerce_value_to};
use crate::analyze::BoundSetClause;
use crate::catalog::Column;
use crate::common::errors::EasyDbError;
use crate::index::extendible_hash::ExtendibleHashIndex;
use crate::index::ix_index_handle::IxIndexHandle;
use crate::record::rm_defs::TupleMeta;
use crate::record::rm_file_handle::RmFileHandle;
use crate::record::tuple::encode_tuple;
use crate::recovery::LogRecord;
use crate::transaction::{WType, WriteRecord};
use crate::types::{ColType, Value};

/// 一张表索引的维护信息：句柄 + 索引列在表行中的下标/类型/宽度。
#[derive(Clone)]
pub struct IndexMaintenance {
    pub handle: IndexMaintenanceHandle,
    pub col_idx: Vec<usize>,
    pub col_types: Vec<ColType>,
    pub col_lens: Vec<usize>,
}

#[derive(Clone)]
pub enum IndexMaintenanceHandle {
    BTree(Arc<IxIndexHandle>),
    Hash(Arc<ExtendibleHashIndex>),
}

impl IndexMaintenanceHandle {
    pub fn insert_entry(
        &self,
        key: &[u8],
        rid: crate::common::rid::Rid,
    ) -> Result<(), EasyDbError> {
        match self {
            Self::BTree(h) => h.insert_entry(key, rid).map(|_| ()),
            Self::Hash(h) => h.insert_entry(key, rid),
        }
    }
    pub fn delete_entry(
        &self,
        key: &[u8],
        rid: crate::common::rid::Rid,
    ) -> Result<(), EasyDbError> {
        match self {
            Self::BTree(h) => h.delete_entry(key).map(|_| ()),
            Self::Hash(h) => h.delete_entry(key, rid),
        }
    }
}

impl IndexMaintenance {
    pub fn key(&self, row: &[Value]) -> Result<Vec<u8>, EasyDbError> {
        let vals: Vec<Value> = self.col_idx.iter().map(|&i| row[i].clone()).collect();
        crate::index::ix_defs::encode_key(&self.col_types, &self.col_lens, &vals)
    }
}

/// INSERT：每次 `next()` 写入一行，返回写入的行。
pub struct InsertExec {
    fh: Arc<RmFileHandle>,
    table: String,
    table_cols: Vec<Column>,
    rows: Vec<Vec<Value>>,
    next_row: usize,
    indexes: Vec<IndexMaintenance>,
    ctx: ExecCtx,
}

impl InsertExec {
    pub fn new(
        fh: Arc<RmFileHandle>,
        table: &str,
        table_cols: Vec<Column>,
        rows: Vec<Vec<Value>>,
        indexes: Vec<IndexMaintenance>,
        ctx: &ExecCtx,
    ) -> Self {
        Self {
            fh,
            table: table.to_string(),
            table_cols,
            rows,
            next_row: 0,
            indexes,
            ctx: ctx.clone(),
        }
    }
}

impl Executor for InsertExec {
    fn schema(&self) -> &RowSchema {
        static EMPTY: std::sync::OnceLock<RowSchema> = std::sync::OnceLock::new();
        EMPTY.get_or_init(RowSchema::default)
    }

    fn next(&mut self) -> Result<Option<Row>, EasyDbError> {
        if self.next_row >= self.rows.len() {
            return Ok(None);
        }
        let values = self.rows[self.next_row].clone();
        self.next_row += 1;
        let data = encode_tuple(
            &crate::catalog::Schema::new(self.table_cols.clone()),
            &values,
        )?;
        let rid = self.fh.insert_tuple(
            TupleMeta {
                xmin: self.ctx.txn.id,
                ..TupleMeta::default()
            },
            &data,
        )?;
        // 写锁 + 写集 + WAL
        self.ctx
            .locks
            .lock_exclusive_on_record(&self.ctx.txn, self.fh.fd(), rid)?;
        self.ctx.txn.append_write(WriteRecord {
            wtype: WType::Insert,
            table: self.table.clone(),
            rid,
            new_rid: None,
            old_meta: TupleMeta::default(),
            data: data.clone(),
        });
        {
            let mut log = self.ctx.log.lock().unwrap_or_else(|e| e.into_inner());
            let lsn = log.append(&LogRecord::Insert {
                txn: self.ctx.txn.id,
                table: self.table.clone(),
                rid,
                data: data.clone(),
            });
            self.ctx.txn.set_prev_lsn(lsn);
        }
        for ix in &self.indexes {
            let key = ix.key(&values)?;
            ix.handle.insert_entry(&key, rid)?;
        }
        Ok(Some(Row { rid, values }))
    }
}

/// DELETE：删除扫描产出的全部行（含索引条目维护）。
pub struct DeleteExec {
    scan: Box<dyn Executor>,
    fh: Arc<RmFileHandle>,
    table: String,
    indexes: Vec<IndexMaintenance>,
    ctx: ExecCtx,
    done: bool,
}

impl DeleteExec {
    pub fn new(
        scan: Box<dyn Executor>,
        fh: Arc<RmFileHandle>,
        table: &str,
        indexes: Vec<IndexMaintenance>,
        ctx: &ExecCtx,
    ) -> Self {
        Self {
            scan,
            fh,
            table: table.to_string(),
            indexes,
            ctx: ctx.clone(),
            done: false,
        }
    }
}

impl Executor for DeleteExec {
    fn schema(&self) -> &RowSchema {
        static EMPTY: std::sync::OnceLock<RowSchema> = std::sync::OnceLock::new();
        EMPTY.get_or_init(RowSchema::default)
    }

    fn next(&mut self) -> Result<Option<Row>, EasyDbError> {
        if self.done {
            return Ok(None);
        }
        let Some(row) = self.scan.next()? else {
            self.done = true;
            return Ok(None);
        };
        // 写锁 + 写集 + WAL（旧数据随写集/日志留存以支持撤销）
        self.ctx
            .locks
            .lock_exclusive_on_record(&self.ctx.txn, self.fh.fd(), row.rid)?;
        let (old_meta, old_data) = self.fh.get_tuple(row.rid)?;
        self.ctx.txn.append_write(WriteRecord {
            wtype: WType::Delete,
            table: self.table.clone(),
            rid: row.rid,
            new_rid: None,
            old_meta,
            data: old_data.clone(),
        });
        {
            let mut log = self.ctx.log.lock().unwrap_or_else(|e| e.into_inner());
            let lsn = log.append(&LogRecord::Delete {
                txn: self.ctx.txn.id,
                table: self.table.clone(),
                rid: row.rid,
                old_data: old_data.clone(),
            });
            self.ctx.txn.set_prev_lsn(lsn);
        }
        // 先删索引条目再删记录
        for ix in &self.indexes {
            ix.handle.delete_entry(&ix.key(&row.values)?, row.rid)?;
        }
        self.fh.mark_deleted(row.rid, self.ctx.txn.id)?;
        Ok(Some(row))
    }
}

/// UPDATE：对扫描产出的每行计算新值并原地更新（含索引条目维护）。
pub struct UpdateExec {
    scan: Box<dyn Executor>,
    fh: Arc<RmFileHandle>,
    table: String,
    table_cols: Vec<Column>,
    clauses: Vec<(usize, BoundSetClause)>, // 目标列下标 + 子句
    indexes: Vec<IndexMaintenance>,
    schema: RowSchema,
    ctx: ExecCtx,
    done: bool,
}

impl UpdateExec {
    pub fn new(
        scan: Box<dyn Executor>,
        fh: Arc<RmFileHandle>,
        table: &str,
        table_cols: Vec<Column>,
        clauses: Vec<(usize, BoundSetClause)>,
        indexes: Vec<IndexMaintenance>,
        ctx: &ExecCtx,
    ) -> Self {
        let schema = RowSchema::new(
            table_cols
                .iter()
                .map(|c| (c.tab_name().to_string(), c.name().to_string()))
                .collect(),
        );
        Self {
            scan,
            fh,
            table: table.to_string(),
            table_cols,
            clauses,
            indexes,
            schema,
            ctx: ctx.clone(),
            done: false,
        }
    }
}

impl Executor for UpdateExec {
    fn schema(&self) -> &RowSchema {
        &self.schema
    }

    fn next(&mut self) -> Result<Option<Row>, EasyDbError> {
        if self.done {
            return Ok(None);
        }
        let Some(old) = self.scan.next()? else {
            self.done = true;
            return Ok(None);
        };
        // 写锁 + 旧数据留存（写集/WAL）
        self.ctx
            .locks
            .lock_exclusive_on_record(&self.ctx.txn, self.fh.fd(), old.rid)?;
        let (old_meta, old_data) = self.fh.get_tuple(old.rid)?;
        let mut new_values = old.values.clone();
        for (target_idx, clause) in &self.clauses {
            let v = match &clause.rhs {
                crate::analyze::BoundSetRhs::Value(v) => v.clone(),
                crate::analyze::BoundSetRhs::Arith { col, op, value } => {
                    let src = self
                        .schema
                        .index_of(&col.tab, &col.col)
                        .ok_or_else(|| EasyDbError::ColumnNotFound(col.col.clone()))?;
                    arith(*op, &old.values[src], value)?
                }
            };
            // 回写时对齐列类型（Int 字面量写入 Long 列等）
            let coerced = coerce_value_to(self.table_cols[*target_idx].col_type(), &v)?;
            new_values[*target_idx] = coerced;
        }

        let data = encode_tuple(
            &crate::catalog::Schema::new(self.table_cols.clone()),
            &new_values,
        )?;

        // UPDATE 采用追加版本：旧版本进入链尾，新版本通过 prev 指向旧
        // 版本。旧版本不再被当前扫描返回，但仍保留在磁盘上供快照读取。
        let new_rid = self.fh.insert_tuple(
            TupleMeta {
                xmin: self.ctx.txn.id,
                prev: Some(old.rid),
                ..TupleMeta::default()
            },
            &data,
        )?;
        self.fh.mark_deleted(old.rid, self.ctx.txn.id)?;

        // 写集 + WAL（旧数据留存以支持撤销与恢复）
        self.ctx.txn.append_write(WriteRecord {
            wtype: WType::Update,
            table: self.table.clone(),
            rid: old.rid,
            new_rid: Some(new_rid),
            old_meta,
            data: old_data.clone(),
        });
        {
            let mut log = self.ctx.log.lock().unwrap_or_else(|e| e.into_inner());
            let lsn = log.append(&LogRecord::Update {
                txn: self.ctx.txn.id,
                table: self.table.clone(),
                rid: old.rid,
                new_rid,
                old_data: old_data.clone(),
                new_data: data.clone(),
            });
            self.ctx.txn.set_prev_lsn(lsn);
        }

        // 索引维护：键变化则删旧插新
        for ix in &self.indexes {
            let old_key = ix.key(&old.values)?;
            let new_key = ix.key(&new_values)?;
            if old_key != new_key {
                ix.handle.delete_entry(&old_key, old.rid)?;
                ix.handle.insert_entry(&new_key, new_rid)?;
            } else {
                ix.handle.delete_entry(&old_key, old.rid)?;
                ix.handle.insert_entry(&old_key, new_rid)?;
            }
        }
        Ok(Some(Row {
            rid: new_rid,
            values: new_values,
        }))
    }
}
