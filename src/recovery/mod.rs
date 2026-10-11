//! 恢复：WAL 日志与重启恢复。
//!
//! 对应 C++ 版 `src-cpp/recovery/`。日志格式为自持设计（不兼容 C++ 版）：
//! 每条记录 `| type(u32) | len(u32) | lsn(i32) | txn(i64) | payload |`，
//! 小端编码，len 为整条记录字节数。
//!
//! 恢复策略（简化 ARIES，正确性基于本引擎的两条不变式）：
//! 1. 记录分配是只增的（槽号不回收），且重放按原始顺序进行；
//! 2. 日志在事务提交时刷盘。
//!
//! 因此恢复 = 清空数据文件后按序重放全部操作（重复历史），再对崩溃时
//! 未提交的事务按操作逆序撤销；索引随后由数据回填重建。
//! 页级 LSN/脏页表等物理 ARIES 机制在此不需要。

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::buffer::buffer_pool::BufferPool;
use crate::catalog::Column;
use crate::common::config::{LOG_FILE_NAME, Lsn, TxnId};
use crate::common::errors::EasyDbError;
use crate::common::rid::Rid;
use crate::execution::Executor;
use crate::index::ix_index_handle::IxIndexHandle;
use crate::index::ix_manager::IxManager;
use crate::record::rm_defs::TupleMeta;
use crate::record::rm_file_handle::RmFileHandle;
use crate::record::rm_manager::RmManager;
use crate::storage::disk::DiskManager;
use crate::transaction::WType;

/// 日志记录类型（对应 C++ `LogType`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogType {
    Update = 0,
    Insert = 1,
    Delete = 2,
    Begin = 3,
    Commit = 4,
    Abort = 5,
    Checkpoint = 6,
}

/// 日志记录。
#[derive(Debug, Clone)]
pub enum LogRecord {
    Begin {
        txn: TxnId,
    },
    Commit {
        txn: TxnId,
    },
    Abort {
        txn: TxnId,
    },
    Checkpoint,
    Insert {
        txn: TxnId,
        table: String,
        rid: Rid,
        data: Vec<u8>,
    },
    Delete {
        txn: TxnId,
        table: String,
        rid: Rid,
        old_data: Vec<u8>,
    },
    Update {
        txn: TxnId,
        table: String,
        rid: Rid,
        old_data: Vec<u8>,
        new_data: Vec<u8>,
    },
}

const HDR: usize = 4 + 4 + 4 + 8; // type + len + lsn + txn

fn put_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}
fn put_i32(buf: &mut Vec<u8>, v: i32) {
    buf.extend_from_slice(&v.to_le_bytes());
}
fn put_i64(buf: &mut Vec<u8>, v: i64) {
    buf.extend_from_slice(&v.to_le_bytes());
}
fn put_str(buf: &mut Vec<u8>, s: &str) {
    put_u32(buf, s.len() as u32);
    buf.extend_from_slice(s.as_bytes());
}
fn put_bytes(buf: &mut Vec<u8>, d: &[u8]) {
    put_u32(buf, d.len() as u32);
    buf.extend_from_slice(d);
}

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn u32(&mut self) -> Result<u32, EasyDbError> {
        let b: [u8; 4] = self
            .buf
            .get(self.pos..self.pos + 4)
            .ok_or_else(truncated)?
            .try_into()
            .unwrap();
        self.pos += 4;
        Ok(u32::from_le_bytes(b))
    }
    fn i32v(&mut self) -> Result<i32, EasyDbError> {
        Ok(self.u32()? as i32)
    }
    fn i64v(&mut self) -> Result<i64, EasyDbError> {
        let b: [u8; 8] = self
            .buf
            .get(self.pos..self.pos + 8)
            .ok_or_else(truncated)?
            .try_into()
            .unwrap();
        self.pos += 8;
        Ok(i64::from_le_bytes(b))
    }
    fn strv(&mut self) -> Result<String, EasyDbError> {
        let len = self.u32()? as usize;
        let b = self
            .buf
            .get(self.pos..self.pos + len)
            .ok_or_else(truncated)?
            .to_vec();
        self.pos += len;
        String::from_utf8(b).map_err(|_| EasyDbError::Internal("log string not utf-8".into()))
    }
    fn bytesv(&mut self) -> Result<Vec<u8>, EasyDbError> {
        let len = self.u32()? as usize;
        let b = self
            .buf
            .get(self.pos..self.pos + len)
            .ok_or_else(truncated)?
            .to_vec();
        self.pos += len;
        Ok(b)
    }
}

fn truncated() -> EasyDbError {
    EasyDbError::Internal("log record truncated".to_string())
}

impl LogRecord {
    pub fn txn_id(&self) -> TxnId {
        match self {
            LogRecord::Begin { txn }
            | LogRecord::Commit { txn }
            | LogRecord::Abort { txn }
            | LogRecord::Insert { txn, .. }
            | LogRecord::Delete { txn, .. }
            | LogRecord::Update { txn, .. } => *txn,
            LogRecord::Checkpoint => 0,
        }
    }

    pub fn log_type(&self) -> LogType {
        match self {
            LogRecord::Update { .. } => LogType::Update,
            LogRecord::Insert { .. } => LogType::Insert,
            LogRecord::Delete { .. } => LogType::Delete,
            LogRecord::Begin { .. } => LogType::Begin,
            LogRecord::Commit { .. } => LogType::Commit,
            LogRecord::Abort { .. } => LogType::Abort,
            LogRecord::Checkpoint => LogType::Checkpoint,
        }
    }

    fn encode(&self, lsn: Lsn) -> Vec<u8> {
        let mut buf = Vec::new();
        put_u32(&mut buf, self.log_type() as u32);
        put_u32(&mut buf, 0); // len 占位
        put_i32(&mut buf, lsn);
        put_i64(&mut buf, self.txn_id());
        match self {
            LogRecord::Begin { .. }
            | LogRecord::Commit { .. }
            | LogRecord::Abort { .. }
            | LogRecord::Checkpoint => {}
            LogRecord::Insert {
                table, rid, data, ..
            } => {
                put_str(&mut buf, table);
                put_i64(&mut buf, rid.encoded());
                put_bytes(&mut buf, data);
            }
            LogRecord::Delete {
                table,
                rid,
                old_data,
                ..
            } => {
                put_str(&mut buf, table);
                put_i64(&mut buf, rid.encoded());
                put_bytes(&mut buf, old_data);
            }
            LogRecord::Update {
                table,
                rid,
                old_data,
                new_data,
                ..
            } => {
                put_str(&mut buf, table);
                put_i64(&mut buf, rid.encoded());
                put_bytes(&mut buf, old_data);
                put_bytes(&mut buf, new_data);
            }
        }
        let len = buf.len() as u32;
        buf[4..8].copy_from_slice(&len.to_le_bytes());
        buf
    }

    fn decode(src: &[u8]) -> Result<LogRecord, EasyDbError> {
        let mut r = Reader { buf: src, pos: 0 };
        let ty = r.u32()?;
        let _len = r.u32()?;
        let _lsn = r.i32v()?;
        let txn = r.i64v()?;
        Ok(match ty {
            t if t == LogType::Begin as u32 => LogRecord::Begin { txn },
            t if t == LogType::Commit as u32 => LogRecord::Commit { txn },
            t if t == LogType::Abort as u32 => LogRecord::Abort { txn },
            t if t == LogType::Checkpoint as u32 => LogRecord::Checkpoint,
            t if t == LogType::Insert as u32 => {
                let table = r.strv()?;
                let rid = Rid::from_encoded(r.i64v()?);
                let data = r.bytesv()?;
                LogRecord::Insert {
                    txn,
                    table,
                    rid,
                    data,
                }
            }
            t if t == LogType::Delete as u32 => {
                let table = r.strv()?;
                let rid = Rid::from_encoded(r.i64v()?);
                let old_data = r.bytesv()?;
                LogRecord::Delete {
                    txn,
                    table,
                    rid,
                    old_data,
                }
            }
            t if t == LogType::Update as u32 => {
                let table = r.strv()?;
                let rid = Rid::from_encoded(r.i64v()?);
                let old_data = r.bytesv()?;
                let new_data = r.bytesv()?;
                LogRecord::Update {
                    txn,
                    table,
                    rid,
                    old_data,
                    new_data,
                }
            }
            _ => return Err(EasyDbError::Internal("unknown log type".to_string())),
        })
    }
}

/// WAL 日志管理器：LSN 分配、缓冲与刷盘（提交时刷盘）。
pub struct LogManager {
    file: PathBuf,
    buffer: Vec<u8>,
    next_lsn: Lsn,
}

impl LogManager {
    /// 打开日志文件；已存在的日志用于恢复 next_lsn（内容留给 RecoveryManager）。
    pub fn open(dir: &Path) -> Result<Self, EasyDbError> {
        let file = dir.join(LOG_FILE_NAME);
        let next_lsn = if file.exists() {
            // 扫描既有记录确定下一个 LSN
            let data = std::fs::read(&file)?;
            let mut max_lsn = 0;
            let mut pos = 0usize;
            while pos + HDR <= data.len() {
                let len = u32::from_le_bytes(data[pos + 4..pos + 8].try_into().unwrap()) as usize;
                if len < HDR || pos + len > data.len() {
                    break; // 尾部残缺记录（崩溃时缓冲未刷完整），丢弃
                }
                let lsn = i32::from_le_bytes(data[pos + 8..pos + 12].try_into().unwrap());
                max_lsn = max_lsn.max(lsn);
                pos += len;
            }
            max_lsn + 1
        } else {
            1
        };
        Ok(Self {
            file,
            buffer: Vec::new(),
            next_lsn,
        })
    }

    /// 追加记录到缓冲，返回 LSN。
    pub fn append(&mut self, record: &LogRecord) -> Lsn {
        let lsn = self.next_lsn;
        self.next_lsn += 1;
        self.buffer.extend_from_slice(&record.encode(lsn));
        lsn
    }

    /// 把缓冲写入磁盘（追加）。
    pub fn flush(&mut self) -> Result<(), EasyDbError> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.file)?;
        f.write_all(&self.buffer)?;
        self.buffer.clear();
        Ok(())
    }
}

/// 表的恢复元数据（P7 由持久化的 db.meta 提供）。
pub struct TableRecoverMeta {
    pub name: String,
    pub cols: Vec<Column>,
    pub indexes: Vec<Vec<String>>,
}

/// 重启恢复：按日志重放全部操作，撤销崩溃时未完成的事务，并回填索引。
pub fn recover(
    dir: &Path,
    disk: Arc<DiskManager>,
    bpm: Arc<BufferPool>,
    rm: &RmManager,
    ix: &IxManager,
    metas: &[TableRecoverMeta],
) -> Result<(), EasyDbError> {
    // 1. 读取日志
    let log_path = dir.join(LOG_FILE_NAME);
    let records = if log_path.exists() {
        let data = std::fs::read(&log_path)?;
        let mut out = Vec::new();
        let mut pos = 0usize;
        while pos + HDR <= data.len() {
            let len = u32::from_le_bytes(data[pos + 4..pos + 8].try_into().unwrap()) as usize;
            if len < HDR || pos + len > data.len() {
                break;
            }
            out.push(LogRecord::decode(&data[pos..pos + len])?);
            pos += len;
        }
        out
    } else {
        Vec::new()
    };

    // 2. 提交集：有 COMMIT 记录的事务；其余出现过的事务视为失败者
    let committed: std::collections::HashSet<TxnId> = records
        .iter()
        .filter(|r| matches!(r, LogRecord::Commit { .. }))
        .map(|r| r.txn_id())
        .collect();

    // 3. 重建各表数据文件（清空后重复历史）
    let mut handles: Vec<(String, Arc<RmFileHandle>)> = Vec::new();
    for meta in metas {
        let path = dir.join(format!("{}.data", meta.name));
        if path.exists() {
            // 关闭旧文件句柄不可能（刚启动）；直接删除重建
            std::fs::remove_file(&path)?;
        }
        rm.create_file(&path)?;
        let fd = disk.open_file(&path)?;
        let fh = Arc::new(RmFileHandle::open(disk.clone(), bpm.clone(), fd)?);
        handles.push((meta.name.clone(), fh));
    }

    // 4. 重放全部操作（重复历史，未提交事务的修改随后撤销）
    for record in &records {
        match record {
            LogRecord::Insert {
                table, rid, data, ..
            } => {
                if let Some((_, fh)) = handles.iter().find(|(n, _)| n == table) {
                    fh.recover_insert(*rid, data)?;
                }
            }
            LogRecord::Delete { table, rid, .. } => {
                if let Some((_, fh)) = handles.iter().find(|(n, _)| n == table) {
                    let _ = fh.delete_tuple(*rid);
                }
            }
            LogRecord::Update {
                table,
                rid,
                new_data,
                ..
            } => {
                if let Some((_, fh)) = handles.iter().find(|(n, _)| n == table) {
                    let _ = fh.update_tuple_in_place(*rid, TupleMeta::default(), new_data);
                }
            }
            _ => {}
        }
    }

    // 5. 撤销失败者：按事务分组、组内逆序
    let mut failed_ops: HashMap<TxnId, Vec<&LogRecord>> = HashMap::new();
    for r in &records {
        let t = r.txn_id();
        if !committed.contains(&t) && !matches!(r, LogRecord::Begin { .. }) {
            failed_ops.entry(t).or_default().push(r);
        }
    }
    for (_, ops) in failed_ops {
        for record in ops.iter().rev() {
            match record {
                LogRecord::Insert { table, rid, .. } => {
                    if let Some((_, fh)) = handles.iter().find(|(n, _)| n == table) {
                        let _ = fh.delete_tuple(*rid);
                    }
                }
                LogRecord::Delete {
                    table,
                    rid,
                    old_data,
                    ..
                } => {
                    if let Some((_, fh)) = handles.iter().find(|(n, _)| n == table) {
                        let _ = fh.rewrite_tuple(*rid, old_data);
                        let _ = fh.undelete_tuple(*rid, TupleMeta::default());
                    }
                }
                LogRecord::Update {
                    table,
                    rid,
                    old_data,
                    ..
                } => {
                    if let Some((_, fh)) = handles.iter().find(|(n, _)| n == table) {
                        let _ = fh.rewrite_tuple(*rid, old_data);
                    }
                }
                _ => {}
            }
        }
    }

    // 5.5 刷盘：重放产生的脏页与文件头落盘（缓冲池丢弃不保证刷写）
    for (_, fh) in &handles {
        fh.flush_all()?;
    }

    // 6. 重建索引（回填恢复后的数据）
    for meta in metas {
        let Some((_, fh)) = handles.iter().find(|(n, _)| n == &meta.name) else {
            continue;
        };
        for cols in &meta.indexes {
            let types: Vec<crate::types::ColType> = cols
                .iter()
                .map(|n| {
                    meta.cols
                        .iter()
                        .find(|c| c.name() == *n)
                        .map(|c| c.col_type())
                        .unwrap_or(crate::types::ColType::Int)
                })
                .collect();
            let lens: Vec<usize> = cols
                .iter()
                .map(|n| {
                    meta.cols
                        .iter()
                        .find(|c| c.name() == *n)
                        .map(|c| c.storage_size() as usize)
                        .unwrap_or(0)
                })
                .collect();
            let path = dir.join(IxManager::index_name(&format!("{}.data", meta.name), cols));
            if path.exists() {
                std::fs::remove_file(&path)?;
            }
            let specs: Vec<(crate::types::ColType, i32)> = types
                .iter()
                .zip(lens.iter())
                .map(|(t, l)| (*t, *l as i32))
                .collect();
            ix.create_index(&path, &specs)?;
            let fd = disk.open_file(&path)?;
            let handle = Arc::new(IxIndexHandle::open(disk.clone(), bpm.clone(), fd)?);
            let mut scan = crate::execution::scan::SeqScanExec::new_unlocked(
                fh.clone(),
                meta.cols.clone(),
                Vec::new(),
            );
            while let Some(row) = scan.next()? {
                let key: Vec<crate::types::Value> = cols
                    .iter()
                    .map(|n| row.values[meta.col_pos(n)].clone())
                    .collect();
                let key = crate::index::ix_defs::encode_key(&types, &lens, &key)?;
                handle.insert_entry(&key, row.rid)?;
            }
            // 回填产生的树页落盘（缓冲池丢弃不保证刷写）
            bpm.flush_file_pages(handle.fd())?;
        }
    }
    Ok(())
}

/// 表元数据的列位置查询（恢复回填使用）。
impl TableRecoverMeta {
    pub fn col_pos(&self, name: &str) -> usize {
        self.cols.iter().position(|c| c.name() == name).unwrap_or(0)
    }
}

// WType 由写集引用；此处保留类型关联（撤销语义与写集一致）
#[allow(dead_code)]
fn _wtype_check(_: WType) {}

// Mutex 未直接使用（LogManager 的并发保护由持有方负责）
#[allow(dead_code)]
type _UnusedMutex = Mutex<()>;

// Path 类型占位引用
#[allow(dead_code)]
fn _path_check(_: &Path) {}
