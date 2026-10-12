//! 存储无关的 DML 执行器。
//!
//! INSERT/UPDATE/DELETE 的页写入、版本链维护、索引维护和 WAL 都由
//! [`StorageTable`] 完成；这里仅负责 SQL 值表达式和执行器生命周期。

use std::sync::Arc;

use super::{arith, coerce_value_to, ExecCtx, Executor, Row, RowSchema};
use crate::analyze::BoundSetClause;
use crate::catalog::Column;
use crate::common::errors::EasyDbError;
use crate::storage_engine::{StorageRow, StorageTable};
use crate::types::Value;

pub struct InsertExec {
    table: Arc<dyn StorageTable>,
    rows: Vec<Vec<Value>>,
    next_row: usize,
    ctx: ExecCtx,
}

impl InsertExec {
    pub fn new(
        table: Arc<dyn StorageTable>,
        rows: Vec<Vec<Value>>,
        ctx: &ExecCtx,
    ) -> Self {
        Self {
            table,
            rows,
            next_row: 0,
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
        let Some(values) = self.rows.get(self.next_row).cloned() else {
            return Ok(None);
        };
        self.next_row += 1;
        let row = self.table.insert(&values, &self.ctx)?;
        Ok(Some(to_row(row)))
    }
}

pub struct DeleteExec {
    scan: Box<dyn Executor>,
    table: Arc<dyn StorageTable>,
    ctx: ExecCtx,
    done: bool,
}

impl DeleteExec {
    pub fn new(
        scan: Box<dyn Executor>,
        table: Arc<dyn StorageTable>,
        ctx: &ExecCtx,
    ) -> Self {
        Self {
            scan,
            table,
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
        self.table.delete(
            &StorageRow {
                rid: row.rid,
                values: row.values.clone(),
            },
            &self.ctx,
        )?;
        Ok(Some(row))
    }
}

pub struct UpdateExec {
    scan: Box<dyn Executor>,
    table: Arc<dyn StorageTable>,
    table_cols: Vec<Column>,
    clauses: Vec<(usize, BoundSetClause)>,
    schema: RowSchema,
    ctx: ExecCtx,
    done: bool,
}

impl UpdateExec {
    pub fn new(
        scan: Box<dyn Executor>,
        table: Arc<dyn StorageTable>,
        table_cols: Vec<Column>,
        clauses: Vec<(usize, BoundSetClause)>,
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
            table,
            table_cols,
            clauses,
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
        let mut new_values = old.values.clone();
        for (target_idx, clause) in &self.clauses {
            let value = match &clause.rhs {
                crate::analyze::BoundSetRhs::Value(value) => value.clone(),
                crate::analyze::BoundSetRhs::Arith { col, op, value } => {
                    let src = self
                        .schema
                        .index_of(&col.tab, &col.col)
                        .ok_or_else(|| EasyDbError::ColumnNotFound(col.col.clone()))?;
                    arith(*op, &old.values[src], value)?
                }
            };
            new_values[*target_idx] = coerce_value_to(
                self.table_cols[*target_idx].col_type(),
                &value,
            )?;
        }
        let updated = self.table.update(
            &StorageRow {
                rid: old.rid,
                values: old.values,
            },
            &new_values,
            &self.ctx,
        )?;
        Ok(Some(to_row(updated)))
    }
}

fn to_row(row: StorageRow) -> Row {
    Row {
        rid: row.rid,
        values: row.values,
    }
}
