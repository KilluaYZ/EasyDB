//! 存储无关的扫描执行器。

use std::sync::Arc;

use super::{eval_cond, ExecCtx, Executor, ResolvedCond, Row, RowSchema};
use crate::common::errors::EasyDbError;
use crate::storage_engine::{IndexLookup, StorageScanRequest, StorageTable};

pub struct StorageScanExec {
    scan: Box<dyn crate::storage_engine::StorageScan>,
    schema: RowSchema,
    conds: Vec<ResolvedCond>,
}

impl StorageScanExec {
    pub fn new(
        table: Arc<dyn StorageTable>,
        request: StorageScanRequest,
        conds: Vec<ResolvedCond>,
        ctx: &ExecCtx,
    ) -> Result<Self, EasyDbError> {
        let columns = table.descriptor().columns.clone();
        let schema = RowSchema::new(
            columns
                .iter()
                .map(|c| (c.tab_name().to_string(), c.name().to_string()))
                .collect(),
        );
        let scan = table.scan(request, ctx)?;
        Ok(Self { scan, schema, conds })
    }

    pub fn from_scan(
        scan: Box<dyn crate::storage_engine::StorageScan>,
        columns: Vec<crate::catalog::Column>,
        conds: Vec<ResolvedCond>,
    ) -> Self {
        let schema = RowSchema::new(
            columns
                .iter()
                .map(|c| (c.tab_name().to_string(), c.name().to_string()))
                .collect(),
        );
        Self { scan, schema, conds }
    }

    pub fn index_request(
        columns: Vec<String>,
        lower: Option<Vec<crate::types::Value>>,
        upper: Option<Vec<crate::types::Value>>,
        hash: bool,
    ) -> StorageScanRequest {
        StorageScanRequest {
            index: Some(IndexLookup { columns, lower, upper, hash }),
        }
    }
}

impl Executor for StorageScanExec {
    fn schema(&self) -> &RowSchema {
        &self.schema
    }

    fn next(&mut self) -> Result<Option<Row>, EasyDbError> {
        while let Some(row) = self.scan.next()? {
            let candidate = Row {
                rid: row.rid,
                values: row.values,
            };
            if self
                .conds
                .iter()
                .all(|cond| eval_cond(cond, &candidate).is_ok_and(|v| v))
            {
                return Ok(Some(candidate));
            }
        }
        Ok(None)
    }
}

/// 兼容旧 planner 名称；物理实现已经不再暴露给 executor。
pub type SeqScanExec = StorageScanExec;
pub type IndexScanExec = StorageScanExec;
pub type HashIndexScanExec = StorageScanExec;
