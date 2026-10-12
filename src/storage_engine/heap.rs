//! 当前默认的 Heap/页式存储引擎适配器。

use super::{StorageContext, StorageRow, StorageScan, StorageScanRequest, StorageTable, TableDescriptor};
use crate::catalog::Schema;
use crate::common::errors::EasyDbError;
use crate::record::rm_defs::TupleMeta;
use crate::record::rm_file_handle::RmFileHandle;
use crate::record::tuple::{decode_tuple, encode_tuple};
use crate::recovery::LogRecord;
use crate::transaction::{WType, WriteRecord};
use crate::types::Value;
use std::sync::Arc;

/// 现有 `.data` 文件 + 索引文件组成的表访问对象。
pub struct HeapTable {
    descriptor: TableDescriptor,
    pub(crate) fh: Arc<RmFileHandle>,
    pub(crate) indexes: Vec<HeapIndex>,
}

pub enum HeapIndex {
    BTree {
        columns: Vec<String>,
        handle: Arc<crate::index::ix_index_handle::IxIndexHandle>,
    },
    Hash {
        columns: Vec<String>,
        handle: Arc<crate::index::extendible_hash::ExtendibleHashIndex>,
    },
}

impl HeapTable {
    pub fn new(
        descriptor: TableDescriptor,
        fh: Arc<RmFileHandle>,
        indexes: Vec<HeapIndex>,
    ) -> Self {
        Self { descriptor, fh, indexes }
    }

    fn schema(&self) -> Schema {
        Schema::new(self.descriptor.columns.clone())
    }

    fn index_key(&self, index: &HeapIndex, values: &[Value]) -> Result<Vec<u8>, EasyDbError> {
        let columns = match index {
            HeapIndex::BTree { columns, .. } | HeapIndex::Hash { columns, .. } => columns,
        };
        let mut types = Vec::with_capacity(columns.len());
        let mut lens = Vec::with_capacity(columns.len());
        let mut selected = Vec::with_capacity(columns.len());
        for name in columns {
            let pos = self
                .descriptor
                .columns
                .iter()
                .position(|c| c.name() == name)
                .ok_or_else(|| EasyDbError::ColumnNotFound(name.clone()))?;
            let col = &self.descriptor.columns[pos];
            types.push(col.col_type());
            lens.push(col.storage_size() as usize);
            selected.push(values[pos].clone());
        }
        crate::index::ix_defs::encode_key(&types, &lens, &selected)
    }

    fn add_write(&self, rec: WriteRecord, ctx: &StorageContext) {
        ctx.txn.append_write(rec);
    }

    fn append_log(&self, record: LogRecord, ctx: &StorageContext) {
        let mut log = ctx.log.lock().unwrap_or_else(|e| e.into_inner());
        let lsn = log.append(&record);
        ctx.txn.set_prev_lsn(lsn);
    }

    pub fn scan_unlocked(&self) -> Box<dyn StorageScan> {
        Box::new(HeapScan {
            fh: self.fh.clone(),
            schema: self.schema(),
            page: crate::record::rm_defs::RM_FIRST_RECORD_PAGE,
            slot: 0,
            num_pages: self.fh.file_hdr().num_pages,
            page_limits: (0..self.fh.file_hdr().num_pages)
                .map(|page| self.fh.page_num_tuples(page).unwrap_or(0))
                .collect(),
        })
    }
}

impl StorageTable for HeapTable {
    fn descriptor(&self) -> &TableDescriptor {
        &self.descriptor
    }

    fn scan(
        &self,
        _request: StorageScanRequest,
        ctx: &StorageContext,
    ) -> Result<Box<dyn StorageScan>, EasyDbError> {
        ctx.locks.lock_shared_on_table(&ctx.txn, self.fh.fd())?;
        Ok(self.scan_unlocked())
    }

    fn insert(&self, values: &[Value], ctx: &StorageContext) -> Result<StorageRow, EasyDbError> {
        let data = encode_tuple(&self.schema(), values)?;
        let rid = self.fh.insert_tuple(
            TupleMeta { xmin: ctx.txn.id, ..TupleMeta::default() },
            &data,
        )?;
        ctx.locks.lock_exclusive_on_record(&ctx.txn, self.fh.fd(), rid)?;
        self.add_write(WriteRecord {
            wtype: WType::Insert,
            table: self.descriptor.name.clone(),
            rid,
            new_rid: None,
            old_meta: TupleMeta::default(),
            data: data.clone(),
        }, ctx);
        self.append_log(LogRecord::Insert {
            txn: ctx.txn.id,
            table: self.descriptor.name.clone(),
            rid,
            data,
        }, ctx);
        for index in &self.indexes {
            let key = self.index_key(index, values)?;
            match index {
                HeapIndex::BTree { handle, .. } => { handle.insert_entry(&key, rid)?; }
                HeapIndex::Hash { handle, .. } => { handle.insert_entry(&key, rid)?; }
            }
        }
        Ok(StorageRow { rid, values: values.to_vec() })
    }

    fn delete(&self, row: &StorageRow, ctx: &StorageContext) -> Result<(), EasyDbError> {
        ctx.locks.lock_exclusive_on_record(&ctx.txn, self.fh.fd(), row.rid)?;
        let (old_meta, old_data) = self.fh.get_tuple(row.rid)?;
        self.add_write(WriteRecord {
            wtype: WType::Delete,
            table: self.descriptor.name.clone(),
            rid: row.rid,
            new_rid: None,
            old_meta,
            data: old_data.clone(),
        }, ctx);
        self.append_log(LogRecord::Delete {
            txn: ctx.txn.id,
            table: self.descriptor.name.clone(),
            rid: row.rid,
            old_data,
        }, ctx);
        for index in &self.indexes {
            let key = self.index_key(index, &row.values)?;
            match index {
                HeapIndex::BTree { handle, .. } => { handle.delete_entry(&key)?; }
                HeapIndex::Hash { handle, .. } => { handle.delete_entry(&key, row.rid)?; }
            }
        }
        self.fh.mark_deleted(row.rid, ctx.txn.id)
    }

    fn update(
        &self,
        row: &StorageRow,
        values: &[Value],
        ctx: &StorageContext,
    ) -> Result<StorageRow, EasyDbError> {
        ctx.locks.lock_exclusive_on_record(&ctx.txn, self.fh.fd(), row.rid)?;
        let (old_meta, old_data) = self.fh.get_tuple(row.rid)?;
        let data = encode_tuple(&self.schema(), values)?;
        let new_rid = self.fh.insert_tuple(
            TupleMeta { xmin: ctx.txn.id, prev: Some(row.rid), ..TupleMeta::default() },
            &data,
        )?;
        self.fh.mark_deleted(row.rid, ctx.txn.id)?;
        self.add_write(WriteRecord {
            wtype: WType::Update,
            table: self.descriptor.name.clone(),
            rid: row.rid,
            new_rid: Some(new_rid),
            old_meta,
            data: old_data.clone(),
        }, ctx);
        self.append_log(LogRecord::Update {
            txn: ctx.txn.id,
            table: self.descriptor.name.clone(),
            rid: row.rid,
            new_rid,
            old_data,
            new_data: data,
        }, ctx);
        for index in &self.indexes {
            let old_key = self.index_key(index, &row.values)?;
            let new_key = self.index_key(index, values)?;
            match index {
                HeapIndex::BTree { handle, .. } => {
                    handle.delete_entry(&old_key)?;
                    handle.insert_entry(&new_key, new_rid)?;
                }
                HeapIndex::Hash { handle, .. } => {
                    handle.delete_entry(&old_key, row.rid)?;
                    handle.insert_entry(&new_key, new_rid)?;
                }
            }
        }
        Ok(StorageRow { rid: new_rid, values: values.to_vec() })
    }

    fn rollback(&self, record: &WriteRecord) -> Result<(), EasyDbError> {
        match record.wtype {
            WType::Insert => {
                let (_, data) = self.fh.get_tuple(record.rid)?;
                let values = decode_tuple(&self.schema(), &data)?;
                for index in &self.indexes {
                    let key = self.index_key(index, &values)?;
                    match index {
                        HeapIndex::BTree { handle, .. } => { handle.delete_entry(&key)?; }
                        HeapIndex::Hash { handle, .. } => { handle.delete_entry(&key, record.rid)?; }
                    }
                }
                self.fh.delete_tuple(record.rid)
            }
            WType::Delete => {
                self.fh.rewrite_tuple(record.rid, &record.data)?;
                self.fh.undelete_tuple(record.rid, record.old_meta)?;
                let values = decode_tuple(&self.schema(), &record.data)?;
                for index in &self.indexes {
                    let key = self.index_key(index, &values)?;
                    match index {
                        HeapIndex::BTree { handle, .. } => { handle.insert_entry(&key, record.rid)?; }
                        HeapIndex::Hash { handle, .. } => { handle.insert_entry(&key, record.rid)?; }
                    }
                }
                Ok(())
            }
            WType::Update => {
                let new_rid = record.new_rid.ok_or_else(|| {
                    EasyDbError::Internal("update write record missing new version RID".into())
                })?;
                let (_, new_data) = self.fh.get_tuple(new_rid)?;
                let new_values = decode_tuple(&self.schema(), &new_data)?;
                for index in &self.indexes {
                    let key = self.index_key(index, &new_values)?;
                    match index {
                        HeapIndex::BTree { handle, .. } => { handle.delete_entry(&key)?; }
                        HeapIndex::Hash { handle, .. } => { handle.delete_entry(&key, new_rid)?; }
                    }
                }
                self.fh.delete_tuple(new_rid)?;
                self.fh.rewrite_tuple(record.rid, &record.data)?;
                self.fh.undelete_tuple(record.rid, record.old_meta)?;
                let old_values = decode_tuple(&self.schema(), &record.data)?;
                for index in &self.indexes {
                    let key = self.index_key(index, &old_values)?;
                    match index {
                        HeapIndex::BTree { handle, .. } => { handle.insert_entry(&key, record.rid)?; }
                        HeapIndex::Hash { handle, .. } => { handle.insert_entry(&key, record.rid)?; }
                    }
                }
                Ok(())
            }
        }
    }
}

struct HeapScan {
    fh: Arc<RmFileHandle>,
    schema: Schema,
    page: i32,
    slot: u32,
    num_pages: i32,
    page_limits: Vec<u16>,
}

impl StorageScan for HeapScan {
    fn next(&mut self) -> Result<Option<StorageRow>, EasyDbError> {
        loop {
            if self.page >= self.num_pages {
                return Ok(None);
            }
            let limit = self.page_limits.get(self.page as usize).copied().unwrap_or(0);
            if self.slot >= limit as u32 {
                self.page += 1;
                self.slot = 0;
                continue;
            }
            let rid = crate::common::rid::Rid::new(self.page, self.slot);
            self.slot += 1;
            let meta = self.fh.get_tuple_meta(rid).ok();
            let Some(meta) = meta else { continue };
            if meta.is_deleted { continue; }
            let Ok((_, data)) = self.fh.get_tuple(rid) else { continue };
            let values = decode_tuple(&self.schema, &data)?;
            return Ok(Some(StorageRow { rid, values }));
        }
    }
}
