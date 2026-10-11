//! DML 执行器：INSERT / DELETE / UPDATE。
//!
//! 对应 C++ 版 `executor_insert.cpp` / `executor_delete.cpp` /
//! `executor_update.cpp`。差异：
//! - 索引维护（插入/删除条目）在执行器内完成；
//! - UPDATE 沿用 C++ 的原地更新语义（新数据不得超过旧长度）。

use std::sync::Arc;

use super::{Executor, Row, RowSchema, arith, coerce_value_to};
use crate::analyze::BoundSetClause;
use crate::catalog::Column;
use crate::common::errors::EasyDbError;
use crate::index::ix_index_handle::IxIndexHandle;
use crate::record::rm_defs::TupleMeta;
use crate::record::rm_file_handle::RmFileHandle;
use crate::record::tuple::encode_tuple;
use crate::types::{ColType, Value};

/// 一张表索引的维护信息：句柄 + 索引列在表行中的下标/类型/宽度。
#[derive(Clone)]
pub struct IndexMaintenance {
    pub handle: Arc<IxIndexHandle>,
    pub col_idx: Vec<usize>,
    pub col_types: Vec<ColType>,
    pub col_lens: Vec<usize>,
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
    table_cols: Vec<Column>,
    rows: Vec<Vec<Value>>,
    next_row: usize,
    indexes: Vec<IndexMaintenance>,
}

impl InsertExec {
    pub fn new(
        fh: Arc<RmFileHandle>,
        table_cols: Vec<Column>,
        rows: Vec<Vec<Value>>,
        indexes: Vec<IndexMaintenance>,
    ) -> Self {
        Self {
            fh,
            table_cols,
            rows,
            next_row: 0,
            indexes,
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
        let rid = self.fh.insert_tuple(TupleMeta::default(), &data)?;
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
    indexes: Vec<IndexMaintenance>,
    done: bool,
}

impl DeleteExec {
    pub fn new(
        scan: Box<dyn Executor>,
        fh: Arc<RmFileHandle>,
        indexes: Vec<IndexMaintenance>,
    ) -> Self {
        Self {
            scan,
            fh,
            indexes,
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
        // 先删索引条目再删记录
        for ix in &self.indexes {
            ix.handle.delete_entry(&ix.key(&row.values)?)?;
        }
        self.fh.delete_tuple(row.rid)?;
        Ok(Some(row))
    }
}

/// UPDATE：对扫描产出的每行计算新值并原地更新（含索引条目维护）。
pub struct UpdateExec {
    scan: Box<dyn Executor>,
    fh: Arc<RmFileHandle>,
    table_cols: Vec<Column>,
    clauses: Vec<(usize, BoundSetClause)>, // 目标列下标 + 子句
    indexes: Vec<IndexMaintenance>,
    schema: RowSchema,
    done: bool,
}

impl UpdateExec {
    pub fn new(
        scan: Box<dyn Executor>,
        fh: Arc<RmFileHandle>,
        table_cols: Vec<Column>,
        clauses: Vec<(usize, BoundSetClause)>,
        indexes: Vec<IndexMaintenance>,
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
            table_cols,
            clauses,
            indexes,
            schema,
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

        // 索引维护：键变化则删旧插新
        for ix in &self.indexes {
            let old_key = ix.key(&old.values)?;
            let new_key = ix.key(&new_values)?;
            if old_key != new_key {
                ix.handle.delete_entry(&old_key)?;
                ix.handle.insert_entry(&new_key, old.rid)?;
            }
        }

        let data = encode_tuple(
            &crate::catalog::Schema::new(self.table_cols.clone()),
            &new_values,
        )?;
        self.fh
            .update_tuple_in_place(old.rid, TupleMeta::default(), &data)?;
        Ok(Some(Row {
            rid: old.rid,
            values: new_values,
        }))
    }
}
