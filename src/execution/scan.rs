//! 扫描执行器：顺序扫描与索引扫描。
//!
//! 对应 C++ 版 `executor_seq_scan.cpp` / `executor_index_scan.cpp`。
//! C++ 版依赖手工 pin/unpin 与全局缓冲锁，Rust 版直接使用 P2/P3 的
//! RAII 接口（`RmFileHandle` / `IxIndexHandle`），迭代状态由执行器自身
//! 持有（页号 + 槽号 / 索引 Iid）。

use std::sync::Arc;

use super::{ExecCtx, Executor, ResolvedCond, Row, RowSchema, eval_cond};
use crate::catalog::Column;
use crate::catalog::Schema;
use crate::common::errors::EasyDbError;
use crate::common::rid::Rid;
use crate::index::extendible_hash::ExtendibleHashIndex;
use crate::index::ix_defs::Iid;
use crate::index::ix_index_handle::IxIndexHandle;
use crate::record::rm_file_handle::RmFileHandle;
use crate::record::tuple::decode_tuple;

/// 顺序扫描：按页 × 槽遍历表文件，跳过已删除记录。
pub struct SeqScanExec {
    fh: Arc<RmFileHandle>,
    tuple_schema: Schema,
    schema: RowSchema,
    conds: Vec<ResolvedCond>,
    page: i32,
    slot: u32,
    num_pages: i32,
    done: bool,
}

impl SeqScanExec {
    /// 事务内扫描：先取表级共享锁（对应 C++ LockSharedOnTable）。
    pub fn new(
        fh: Arc<RmFileHandle>,
        table_cols: Vec<Column>,
        conds: Vec<ResolvedCond>,
        ctx: &ExecCtx,
        _table: &str,
    ) -> Self {
        // 锁失败（wait-die 中止）：标记事务状态，错误经 next() 传播
        if let Err(e) = ctx.locks.lock_shared_on_table(&ctx.txn, fh.fd()) {
            ctx.txn
                .set_state(crate::transaction::TransactionState::Aborted);
            let _ = e;
        }
        Self::new_unlocked(fh, table_cols, conds)
    }

    /// 无锁构造：建索引回填 / 恢复重建等 DDL 语境使用。
    pub fn new_unlocked(
        fh: Arc<RmFileHandle>,
        table_cols: Vec<Column>,
        conds: Vec<ResolvedCond>,
    ) -> Self {
        let num_pages = fh.file_hdr().num_pages;
        let tuple_schema = Schema::new(table_cols.clone());
        let schema = RowSchema::new(
            table_cols
                .iter()
                .map(|c| (c.tab_name().to_string(), c.name().to_string()))
                .collect(),
        );
        Self {
            fh,
            tuple_schema,
            schema,
            conds,
            page: crate::record::rm_defs::RM_FIRST_RECORD_PAGE,
            slot: 0,
            num_pages,
            done: false,
        }
    }

    fn next_rid(&mut self) -> Option<Rid> {
        loop {
            if self.done || self.page >= self.num_pages {
                return None;
            }
            let num_records = self.fh.page_num_tuples(self.page).unwrap_or(0);
            if (self.slot as usize) < num_records as usize {
                let rid = Rid::new(self.page, self.slot);
                self.slot += 1;
                return Some(rid);
            }
            self.page += 1;
            self.slot = 0;
        }
    }

    fn fetch(&self, rid: Rid) -> Result<Row, EasyDbError> {
        let (_, data) = self.fh.get_tuple(rid)?;
        let values = decode_tuple(&self.tuple_schema, &data)?;
        Ok(Row { rid, values })
    }
}

impl Executor for SeqScanExec {
    fn schema(&self) -> &RowSchema {
        &self.schema
    }

    fn next(&mut self) -> Result<Option<Row>, EasyDbError> {
        loop {
            let Some(rid) = self.next_rid() else {
                self.done = true;
                return Ok(None);
            };
            // 已删除槽位跳过（逻辑删除仍在页内）
            if self
                .fh
                .get_tuple_meta(rid)
                .map(|m| m.is_deleted)
                .unwrap_or(true)
            {
                continue;
            }
            let Ok(row) = self.fetch(rid) else {
                continue;
            };
            let mut pass = true;
            for cond in &self.conds {
                if !eval_cond(cond, &row)? {
                    pass = false;
                    break;
                }
            }
            if pass {
                return Ok(Some(row));
            }
        }
    }
}

/// 索引扫描：按索引定位 RID 范围，再回表取行。
pub struct IndexScanExec {
    ih: Arc<IxIndexHandle>,
    fh: Arc<RmFileHandle>,
    tuple_schema: Schema,
    schema: RowSchema,
    conds: Vec<ResolvedCond>,
    cur: Iid,
    end: Iid,
    last_leaf: i32,
    done: bool,
}

/// 等值哈希索引扫描：哈希索引只负责定位 RID，结果仍回表并执行残余条件。
pub struct HashIndexScanExec {
    fh: Arc<RmFileHandle>,
    tuple_schema: Schema,
    schema: RowSchema,
    conds: Vec<ResolvedCond>,
    rids: Vec<Rid>,
    pos: usize,
}

impl HashIndexScanExec {
    pub fn new(
        ih: Arc<ExtendibleHashIndex>,
        fh: Arc<RmFileHandle>,
        table_cols: Vec<Column>,
        key: Vec<u8>,
        conds: Vec<ResolvedCond>,
        ctx: &ExecCtx,
    ) -> Result<Self, EasyDbError> {
        ctx.locks.lock_shared_on_table(&ctx.txn, fh.fd())?;
        let rids = ih.get_equal(&key)?;
        let tuple_schema = Schema::new(table_cols.clone());
        let schema = RowSchema::new(
            table_cols
                .iter()
                .map(|c| (c.tab_name().to_string(), c.name().to_string()))
                .collect(),
        );
        Ok(Self {
            fh,
            tuple_schema,
            schema,
            conds,
            rids,
            pos: 0,
        })
    }
}

impl Executor for HashIndexScanExec {
    fn schema(&self) -> &RowSchema {
        &self.schema
    }

    fn next(&mut self) -> Result<Option<Row>, EasyDbError> {
        while self.pos < self.rids.len() {
            let rid = self.rids[self.pos];
            self.pos += 1;
            let Ok((_, data)) = self.fh.get_tuple(rid) else {
                continue;
            };
            let values = decode_tuple(&self.tuple_schema, &data)?;
            let row = Row { rid, values };
            if self
                .conds
                .iter()
                .all(|c| eval_cond(c, &row).unwrap_or(false))
            {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }
}

impl IndexScanExec {
    /// `lower`/`upper` 为编码好的索引键（None 表示该侧无界）。
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ih: Arc<IxIndexHandle>,
        fh: Arc<RmFileHandle>,
        table_cols: Vec<Column>,
        lower: Option<Vec<u8>>,
        upper: Option<Vec<u8>>,
        conds: Vec<ResolvedCond>,
        ctx: &ExecCtx,
        _table: &str,
    ) -> Result<Self, EasyDbError> {
        if let Err(e) = ctx.locks.lock_shared_on_table(&ctx.txn, fh.fd()) {
            ctx.txn
                .set_state(crate::transaction::TransactionState::Aborted);
            let _ = e;
        }
        let (cur, end) = match (lower, upper) {
            (Some(lo), Some(hi)) => (ih.lower_bound(&lo)?, ih.upper_bound(&hi)?),
            (Some(lo), None) => (ih.lower_bound(&lo)?, ih.leaf_end()?),
            (None, Some(hi)) => (ih.leaf_begin(), ih.upper_bound(&hi)?),
            (None, None) => (ih.leaf_begin(), ih.leaf_end()?),
        };
        let last_leaf = ih.file_hdr().last_leaf;
        let tuple_schema = Schema::new(table_cols.clone());
        let schema = RowSchema::new(
            table_cols
                .iter()
                .map(|c| (c.tab_name().to_string(), c.name().to_string()))
                .collect(),
        );
        Ok(Self {
            ih,
            fh,
            tuple_schema,
            schema,
            conds,
            cur,
            end,
            last_leaf,
            done: false,
        })
    }

    fn advance(&mut self) {
        // 与 IxScan 推进逻辑一致：越过本页（且非最后一个叶子）跳后继叶子
        if self.cur.page_no != self.last_leaf
            && let Ok(size) = self.ih.page_num_keys(self.cur.page_no)
            && self.cur.slot as usize + 1 >= size
            && let Ok(next) = self.ih.page_next_leaf(self.cur.page_no)
        {
            self.cur = Iid::new(next, 0);
            return;
        }
        self.cur.slot += 1;
    }

    fn fetch(&self, rid: Rid) -> Result<Row, EasyDbError> {
        let (_, data) = self.fh.get_tuple(rid)?;
        let values = decode_tuple(&self.tuple_schema, &data)?;
        Ok(Row { rid, values })
    }
}

impl Executor for IndexScanExec {
    fn schema(&self) -> &RowSchema {
        &self.schema
    }

    fn next(&mut self) -> Result<Option<Row>, EasyDbError> {
        loop {
            if self.done || self.cur == self.end {
                self.done = true;
                return Ok(None);
            }
            let rid = match self.ih.get_rid(self.cur) {
                Ok(r) => r,
                Err(_) => {
                    // 索引项缺失（不应发生）：推进避免死循环
                    self.advance();
                    continue;
                }
            };
            self.advance();
            let row = self.fetch(rid)?;
            let mut pass = true;
            for cond in &self.conds {
                if !eval_cond(cond, &row)? {
                    pass = false;
                    break;
                }
            }
            if pass {
                return Ok(Some(row));
            }
        }
    }
}
