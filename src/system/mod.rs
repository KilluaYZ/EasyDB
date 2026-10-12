//! 系统管理：数据库目录与服务门面。
//!
//! 对应 C++ 版 `src-cpp/system/sm_manager.cpp`。P5 为内存目录
//! （M2 里程碑：单用户、无事务），P7 接入元数据持久化与 TCP 服务。
//!
//! C++ 的 SmManager/DbFile 混用裸指针与全局 map，Rust 版收敛为
//! `Database`：每表一个 [`TableMeta`]（schema + 记录文件句柄 + 索引）。

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::analyze::{Analyzer, CatalogView, Query};
use crate::buffer::buffer_pool::BufferPool;
use crate::catalog::Column;
use crate::common::config::DB_META_NAME;
use crate::common::errors::EasyDbError;
use crate::concurrency::LockManager;
use crate::execution::ExecCtx;
use crate::execution::Executor;
use crate::index::extendible_hash::ExtendibleHashIndex;
use crate::index::ix_index_handle::IxIndexHandle;
use crate::index::ix_manager::IxManager;
use crate::parser::ast;
use crate::parser::parse_sql;
use crate::planner::{self, Plan};
use crate::record::rm_defs::TupleMeta;
use crate::record::rm_file_handle::RmFileHandle;
use crate::record::rm_manager::RmManager;
use crate::recovery::{LogManager, LogRecord};
use crate::storage::disk::DiskManager;
use crate::transaction::{IsolationLevel, Transaction, TransactionState, WType, WriteRecord};
use crate::types::ColType;

pub mod format;

pub use format::format_output;

/// db.meta 中的单表持久化条目。
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct PersistTable {
    name: String,
    cols: Vec<Column>,
    indexes: Vec<Vec<String>>,
    #[serde(default)]
    hash_indexes: Vec<Vec<String>>,
}

/// 语句执行结果。
#[derive(Debug, Clone, PartialEq)]
pub enum Output {
    /// SELECT / SHOW / DESC 的行集
    Rows {
        columns: Vec<String>,
        rows: Vec<Vec<crate::types::Value>>,
    },
    /// DML 影响行数
    Count(u64),
    /// 其它语句成功
    Ok,
}

/// 表的索引元数据。
pub struct IndexMeta {
    pub cols: Vec<String>,
    pub file: PathBuf,
    pub handle: Arc<IxIndexHandle>,
}

pub struct HashIndexMeta {
    pub cols: Vec<String>,
    pub file: PathBuf,
    pub handle: Arc<ExtendibleHashIndex>,
}

/// 表元数据：schema + 记录文件句柄 + 索引。
pub struct TableMeta {
    pub name: String,
    pub cols: Vec<Column>,
    pub fh: Arc<RmFileHandle>,
    pub indexes: Vec<IndexMeta>,
    pub hash_indexes: Vec<HashIndexMeta>,
}

/// 内存数据库：表目录 + 存储设施 + SQL 执行门面。
pub struct Database {
    dir: PathBuf,
    disk: Arc<DiskManager>,
    rm: RmManager,
    ix: IxManager,
    tables: HashMap<String, TableMeta>,
    knobs: HashMap<ast::Knob, bool>,
    // ---- 事务设施（P6）----
    lock_mgr: Arc<LockManager>,
    log_mgr: Arc<Mutex<LogManager>>,
    txn_counter: std::sync::atomic::AtomicI64,
    /// 显式事务（BEGIN 后存在；自动提交语句的事务不入此字段）
    current_txn: Option<Arc<Transaction>>,
    /// `run_sql` 的兼容入口使用的默认隔离级别。TCP 服务端使用独立 Session。
    default_isolation: IsolationLevel,
}

/// 客户端连接级会话。
///
/// 事务属于连接，而不是数据库实例。`Database::run_sql` 仍保留用于单线程
/// 测试和嵌入式调用；TCP 服务端必须使用 [`Database::run_sql_in_session`]。
pub struct Session {
    current_txn: Option<Arc<Transaction>>,
    isolation_level: IsolationLevel,
}

impl Session {
    pub fn new() -> Self {
        Self {
            current_txn: None,
            isolation_level: IsolationLevel::Serializable,
        }
    }

    pub fn isolation_level(&self) -> IsolationLevel {
        self.isolation_level
    }

    /// 只允许在事务外设置隔离级别，和 SQL 标准的事务边界语义一致。
    pub fn set_isolation_level(&mut self, level: IsolationLevel) -> Result<(), EasyDbError> {
        if self.current_txn.is_some() {
            return Err(EasyDbError::Internal(
                "cannot change isolation level during a transaction".to_string(),
            ));
        }
        self.isolation_level = level;
        Ok(())
    }

    pub fn in_transaction(&self) -> bool {
        self.current_txn.is_some()
    }
}

impl Default for Session {
    fn default() -> Self {
        Self::new()
    }
}

impl Database {
    /// 打开（不存在则创建）数据库目录。
    pub fn open(dir: impl Into<PathBuf>) -> Result<Self, EasyDbError> {
        let dir = dir.into();
        let disk = Arc::new(DiskManager::new(&dir)?);
        let bpm = Arc::new(BufferPool::new(
            crate::common::config::BUFFER_POOL_SIZE,
            disk.clone(),
        ));
        let rm = RmManager::new(disk.clone(), bpm.clone());
        let ix = IxManager::new(disk.clone(), bpm.clone());
        let log_mgr = Arc::new(Mutex::new(LogManager::open(&dir)?));
        let mut db = Self {
            dir,
            disk,
            rm,
            ix,
            tables: HashMap::new(),
            knobs: HashMap::new(),
            lock_mgr: LockManager::new(),
            log_mgr,
            txn_counter: std::sync::atomic::AtomicI64::new(1),
            current_txn: None,
            default_isolation: IsolationLevel::Serializable,
        };
        db.load_persisted()?;
        Ok(db)
    }

    /// 加载 db.meta（若存在）：先按元数据执行崩溃恢复（重放 + 撤销 +
    /// 索引重建），再把表挂载进目录。
    fn load_persisted(&mut self) -> Result<(), EasyDbError> {
        let meta_path = self.dir.join(DB_META_NAME);
        if !meta_path.exists() {
            return Ok(());
        }
        let data = std::fs::read_to_string(&meta_path)?;
        let persisted: Vec<PersistTable> = serde_json::from_str(&data)
            .map_err(|e| EasyDbError::Internal(format!("db.meta corrupt: {e}")))?;
        let metas: Vec<crate::recovery::TableRecoverMeta> = persisted
            .iter()
            .map(|t| crate::recovery::TableRecoverMeta {
                name: t.name.clone(),
                cols: t.cols.clone(),
                indexes: t.indexes.clone(),
                hash_indexes: t.hash_indexes.clone(),
            })
            .collect();
        crate::recovery::recover(
            &self.dir,
            self.disk.clone(),
            self.rm.bpm().clone(),
            &self.rm,
            &self.ix,
            &metas,
        )?;
        for t in &persisted {
            self.attach_table(
                &t.name,
                t.cols.clone(),
                t.indexes.clone(),
                t.hash_indexes.clone(),
            )?;
        }
        Ok(())
    }

    /// 把目录元数据写入 db.meta（DDL 之后调用）。
    fn save_meta(&self) -> Result<(), EasyDbError> {
        let persisted: Vec<PersistTable> = self
            .tables
            .values()
            .map(|t| PersistTable {
                name: t.name.clone(),
                cols: t.cols.clone(),
                indexes: t.indexes.iter().map(|ix| ix.cols.clone()).collect(),
                hash_indexes: t.hash_indexes.iter().map(|ix| ix.cols.clone()).collect(),
            })
            .collect();
        let data = serde_json::to_string_pretty(&persisted)
            .map_err(|e| EasyDbError::Internal(format!("serialize db.meta: {e}")))?;
        std::fs::write(self.dir.join(DB_META_NAME), data)?;
        Ok(())
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// 创建一个连接级会话。每个 TCP 客户端都应持有自己的 Session。
    pub fn new_session(&self) -> Session {
        Session::new()
    }

    pub fn disk(&self) -> &Arc<DiskManager> {
        &self.disk
    }

    pub fn knob(&self, k: ast::Knob) -> Option<bool> {
        self.knobs.get(&k).copied()
    }

    pub fn table(&self, name: &str) -> Result<&TableMeta, EasyDbError> {
        self.tables
            .get(name)
            .ok_or_else(|| EasyDbError::TableNotFound(name.to_string()))
    }

    fn table_mut(&mut self, name: &str) -> Result<&mut TableMeta, EasyDbError> {
        self.tables
            .get_mut(name)
            .ok_or_else(|| EasyDbError::TableNotFound(name.to_string()))
    }

    fn table_file(&self, name: &str) -> PathBuf {
        self.dir.join(format!("{name}.data"))
    }

    fn index_file(&self, table: &str, cols: &[String]) -> PathBuf {
        let file = IxManager::index_name(&format!("{table}.data"), cols);
        self.dir.join(file)
    }

    fn hash_index_file(&self, table: &str, cols: &[String]) -> PathBuf {
        let file = IxManager::hash_index_name(&format!("{table}.data"), cols);
        self.dir.join(file)
    }

    /// 执行一条 SQL 文本：解析 → 分析 → 计划 → 执行。
    pub fn run_sql(&mut self, sql: &str) -> Result<Output, EasyDbError> {
        let stmt = parse_sql(sql)?;
        let query = Analyzer::new(self).analyze(stmt)?;
        self.execute(query)
    }

    /// 在指定连接会话中执行 SQL。
    pub fn run_sql_in_session(
        &mut self,
        session: &mut Session,
        sql: &str,
    ) -> Result<Output, EasyDbError> {
        let stmt = parse_sql(sql)?;
        let query = Analyzer::new(self).analyze(stmt)?;

        if let Query::SetIsolationLevel(level) = &query {
            session.set_isolation_level(*level)?;
            return Ok(Output::Ok);
        }

        // 复用现有事务执行路径，同时把事务槽位的所有权限定在本次
        // Database 锁保护的调用中。各连接的真实状态保存在 Session。
        let old_txn = self.current_txn.take();
        debug_assert!(old_txn.is_none(), "database session slot is already in use");
        self.current_txn = session.current_txn.take();
        self.default_isolation = session.isolation_level;
        let result = self.execute(query);
        session.current_txn = self.current_txn.take();
        self.current_txn = old_txn;
        result
    }

    /// 连接断开时回滚其未提交事务并释放锁。
    pub fn close_session(&mut self, session: &mut Session) {
        let old_txn = self.current_txn.take();
        self.current_txn = session.current_txn.take();
        if let Some(txn) = self.current_txn.take() {
            self.rollback_writes(&txn, 0);
            self.abort_txn(&txn);
        }
        self.current_txn = old_txn;
    }

    /// 执行绑定后的查询。
    pub fn execute(&mut self, query: Query) -> Result<Output, EasyDbError> {
        // 事务控制语句先于常规生命周期处理
        match query {
            Query::SetIsolationLevel(level) => {
                if self.current_txn.is_some() {
                    return Err(EasyDbError::Internal(
                        "cannot change isolation level during a transaction".to_string(),
                    ));
                }
                self.default_isolation = level;
                return Ok(Output::Ok);
            }
            Query::TxnBegin => {
                if self.current_txn.is_some() {
                    return Err(EasyDbError::Internal(
                        "transaction already in progress".to_string(),
                    ));
                }
                self.begin_txn();
                return Ok(Output::Ok);
            }
            Query::TxnCommit => {
                let Some(txn) = self.current_txn.take() else {
                    return Err(EasyDbError::Internal("no active transaction".to_string()));
                };
                self.commit_txn(&txn)?;
                return Ok(Output::Ok);
            }
            Query::TxnAbort | Query::TxnRollback => {
                let Some(txn) = self.current_txn.take() else {
                    return Err(EasyDbError::Internal("no active transaction".to_string()));
                };
                // 撤销全部未提交写入后标记中止并释放锁
                self.rollback_writes(&txn, 0);
                self.abort_txn(&txn);
                return Ok(Output::Ok);
            }
            _ => {}
        }

        // 无显式事务 → 自动提交（每语句一个事务）
        let autocommit = self.current_txn.is_none();
        if autocommit {
            self.begin_txn();
        }
        let txn = self.current_txn.as_ref().unwrap().clone();
        let ctx = ExecCtx {
            txn: txn.clone(),
            locks: self.lock_mgr.clone(),
            log: self.log_mgr.clone(),
        };
        let mark = txn.write_set_len();
        let result = self.execute_in_txn(query, &ctx);
        match result {
            Ok(out) => {
                if autocommit {
                    self.commit_txn(&txn)?;
                    self.current_txn = None;
                }
                Ok(out)
            }
            Err(EasyDbError::TransactionAbort(id)) => {
                // wait-die 中止 / 状态违规：回滚整个事务
                self.rollback_writes(&txn, 0);
                self.abort_txn(&txn);
                self.current_txn = None;
                Err(EasyDbError::TransactionAbort(id))
            }
            Err(e) => {
                // 其它错误：回滚本语句的写入（保存点语义）
                self.rollback_writes(&txn, mark);
                if autocommit {
                    self.abort_txn(&txn);
                    self.current_txn = None;
                }
                Err(e)
            }
        }
    }

    /// 事务体内的实际执行（子查询经 ctx 求解）。
    fn execute_in_txn(&mut self, query: Query, ctx: &ExecCtx) -> Result<Output, EasyDbError> {
        match query {
            Query::Help => Ok(Output::Ok),
            Query::Exit => Ok(Output::Ok),
            Query::ShowTables => {
                let mut names: Vec<String> = self.tables.keys().cloned().collect();
                names.sort();
                Ok(Output::Rows {
                    columns: vec!["tables".to_string()],
                    rows: names
                        .into_iter()
                        .map(|n| vec![crate::types::Value::Str(n)])
                        .collect(),
                })
            }
            Query::ShowIndex { table } => {
                let meta = self.table(&table)?;
                let rows = meta
                    .indexes
                    .iter()
                    .map(|ix| {
                        vec![
                            crate::types::Value::Str(ix.cols.join(",")),
                            crate::types::Value::Str("B+Tree".into()),
                        ]
                    })
                    .chain(meta.hash_indexes.iter().map(|ix| {
                        vec![
                            crate::types::Value::Str(ix.cols.join(",")),
                            crate::types::Value::Str("ExtendibleHash".into()),
                        ]
                    }))
                    .collect();
                Ok(Output::Rows {
                    columns: vec!["index_cols".to_string(), "type".to_string()],
                    rows,
                })
            }
            Query::TxnBegin
            | Query::TxnCommit
            | Query::TxnAbort
            | Query::TxnRollback
            | Query::SetIsolationLevel(_) => {
                // 事务在 P6 接入；此处按成功处理
                Ok(Output::Ok)
            }
            Query::SetKnob { knob, value } => {
                self.knobs.insert(knob, value);
                Ok(Output::Ok)
            }
            Query::CreateTable { table, columns } => self.create_table(&table, columns),
            Query::DropTable { table } => {
                let fd = self.table(&table)?.fh.fd();
                ctx.locks.lock_exclusive_on_table(&ctx.txn, fd)?;
                self.drop_table(&table)
            }
            Query::DescTable { table } => self.desc_table(&table),
            Query::CreateIndex { table, cols } => {
                let fd = self.table(&table)?.fh.fd();
                ctx.locks.lock_exclusive_on_table(&ctx.txn, fd)?;
                self.create_index(&table, &cols)
            }
            Query::CreateHashIndex { table, cols } => {
                let fd = self.table(&table)?.fh.fd();
                ctx.locks.lock_exclusive_on_table(&ctx.txn, fd)?;
                self.create_hash_index(&table, &cols)
            }
            Query::DropIndex { table, cols } => {
                let fd = self.table(&table)?.fh.fd();
                ctx.locks.lock_exclusive_on_table(&ctx.txn, fd)?;
                self.drop_index(&table, &cols)
            }
            Query::DropHashIndex { table, cols } => {
                let fd = self.table(&table)?.fh.fd();
                ctx.locks.lock_exclusive_on_table(&ctx.txn, fd)?;
                self.drop_hash_index(&table, &cols)
            }
            Query::CreateStaticCheckpoint => {
                // 检查点随 P6 WAL 落地
                Ok(Output::Ok)
            }
            Query::LoadData { .. } => Err(EasyDbError::Internal(
                "LOAD will be supported in P7".to_string(),
            )),
            Query::Insert { table, values } => {
                let plan = planner::plan(Query::Insert { table, values }, self, ctx)?;
                let Plan::Insert { table, rows } = plan else {
                    return Err(EasyDbError::Internal("plan mismatch".to_string()));
                };
                let mut exec = planner::build_insert(self, &table, rows, ctx)?;
                let mut count = 0u64;
                while exec.next()?.is_some() {
                    count += 1;
                }
                Ok(Output::Count(count))
            }
            Query::Delete { table, wheres } => {
                let plan = planner::plan(Query::Delete { table, wheres }, self, ctx)?;
                let Plan::Delete { table, conds } = plan else {
                    return Err(EasyDbError::Internal("plan mismatch".to_string()));
                };
                let mut exec = planner::build_delete(self, &table, &conds, ctx)?;
                let mut count = 0u64;
                while exec.next()?.is_some() {
                    count += 1;
                }
                Ok(Output::Count(count))
            }
            Query::Update {
                table,
                clauses,
                wheres,
            } => {
                let plan = planner::plan(
                    Query::Update {
                        table,
                        clauses,
                        wheres,
                    },
                    self,
                    ctx,
                )?;
                let Plan::Update {
                    table,
                    clauses,
                    conds,
                } = plan
                else {
                    return Err(EasyDbError::Internal("plan mismatch".to_string()));
                };
                let mut exec = planner::build_update(self, &table, &clauses, &conds, ctx)?;
                let mut count = 0u64;
                while exec.next()?.is_some() {
                    count += 1;
                }
                Ok(Output::Count(count))
            }
            Query::Select(s) => {
                let plan = planner::plan(Query::Select(s), self, ctx)?;
                let Plan::Select { root, columns } = plan else {
                    return Err(EasyDbError::Internal("plan mismatch".to_string()));
                };
                let mut exec = planner::build_executor(&root, self, ctx)?;
                let mut rows = Vec::new();
                while let Some(row) = exec.next()? {
                    rows.push(row.values);
                }
                Ok(Output::Rows { columns, rows })
            }
        }
    }

    // ---- 事务生命周期 ----

    fn begin_txn(&mut self) {
        let id = self
            .txn_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let txn = Arc::new(Transaction::with_isolation(id, self.default_isolation));
        {
            let mut log = self.log_mgr.lock().unwrap_or_else(|e| e.into_inner());
            let lsn = log.append(&LogRecord::Begin { txn: id });
            txn.set_prev_lsn(lsn);
        }
        txn.set_state(TransactionState::Growing);
        self.current_txn = Some(txn);
    }

    fn commit_txn(&mut self, txn: &Arc<Transaction>) -> Result<(), EasyDbError> {
        {
            let mut log = self.log_mgr.lock().unwrap_or_else(|e| e.into_inner());
            let lsn = log.append(&LogRecord::Commit { txn: txn.id });
            txn.set_prev_lsn(lsn);
            log.flush()?; // 提交即刷盘（WAL）
        }
        txn.set_state(TransactionState::Committed);
        txn.clear_writes();
        self.lock_mgr.unlock_all(txn);
        Ok(())
    }

    fn abort_txn(&mut self, txn: &Arc<Transaction>) {
        let _ = (|| -> Result<(), EasyDbError> {
            let mut log = self.log_mgr.lock().unwrap_or_else(|e| e.into_inner());
            let lsn = log.append(&LogRecord::Abort { txn: txn.id });
            txn.set_prev_lsn(lsn);
            log.flush()?;
            Ok(())
        })();
        txn.set_state(TransactionState::Aborted);
        txn.clear_writes();
        self.lock_mgr.unlock_all(txn);
    }

    /// 从写集第 `from` 条起（含）按逆序撤销（对应 C++
    /// `TransactionManager::Abort` 的写集回滚 + 索引恢复）。
    fn rollback_writes(&self, txn: &Arc<Transaction>, from: usize) {
        for rec in txn.pop_writes_from(from) {
            let _ = self.undo_write(&rec);
        }
    }

    fn undo_write(&self, rec: &WriteRecord) -> Result<(), EasyDbError> {
        let Some(meta) = self.tables.get(&rec.table) else {
            return Ok(()); // 表已删除：无可撤销
        };

        let indexes = planner::index_maintenance(self, &rec.table)?;
        let schema = crate::catalog::Schema::new(meta.cols.clone());
        match rec.wtype {
            WType::Insert => {
                // 撤销插入：删索引条目 + 删记录
                let row = crate::record::tuple::decode_tuple(&schema, &rec.data)?;
                for ix in &indexes {
                    ix.handle.delete_entry(&ix.key(&row)?, rec.rid)?;
                }
                meta.fh.delete_tuple(rec.rid)?;
            }
            WType::Delete => {
                // 撤销删除：回写旧数据 + 复活 + 补索引条目
                meta.fh.rewrite_tuple(rec.rid, &rec.data)?;
                meta.fh.undelete_tuple(rec.rid, TupleMeta::default())?;
                let row = crate::record::tuple::decode_tuple(&schema, &rec.data)?;
                for ix in &indexes {
                    ix.handle.insert_entry(&ix.key(&row)?, rec.rid)?;
                }
            }
            WType::Update => {
                // 撤销更新：回写旧数据 + 索引键换回（当前键取自现值）
                let cur = meta.fh.get_tuple(rec.rid)?.1;
                let cur_row = crate::record::tuple::decode_tuple(&schema, &cur)?;
                let old_row = crate::record::tuple::decode_tuple(&schema, &rec.data)?;
                meta.fh.rewrite_tuple(rec.rid, &rec.data)?;
                for ix in &indexes {
                    let old_key = ix.key(&cur_row)?;
                    let restored = ix.key(&old_row)?;
                    if old_key != restored {
                        ix.handle.delete_entry(&old_key, rec.rid)?;
                        ix.handle.insert_entry(&restored, rec.rid)?;
                    }
                }
            }
        }
        Ok(())
    }

    // ---- 恢复后挂载（P7 持久化目录就绪前的过渡入口）----

    /// 把已存在（通常经 [`crate::recovery::recover`] 重建）的表文件挂载进
    /// 目录：打开数据文件与索引文件，并回填校验索引。
    pub fn attach_table(
        &mut self,
        name: &str,
        cols: Vec<Column>,
        indexes: Vec<Vec<String>>,
        hash_indexes: Vec<Vec<String>>,
    ) -> Result<(), EasyDbError> {
        if self.tables.contains_key(name) {
            return Err(EasyDbError::TableAlreadyExists(name.to_string()));
        }
        let mut cols = cols;
        for c in cols.iter_mut() {
            c.set_tab_name(name);
        }
        let path = self.table_file(name);
        let fd = self.disk.open_file(&path)?;
        let fh = Arc::new(RmFileHandle::open(
            self.disk.clone(),
            self.rm.bpm().clone(),
            fd,
        )?);
        self.tables.insert(
            name.to_string(),
            TableMeta {
                name: name.to_string(),
                cols: cols.clone(),
                fh,
                indexes: Vec::new(),
                hash_indexes: Vec::new(),
            },
        );
        self.save_meta()?;
        for col_group in indexes {
            let ipath = self.index_file(name, &col_group);
            if self.ix.exists(&ipath) {
                // 恢复流程已重建索引文件：直接打开并注册
                let fd = self.disk.open_file(&ipath)?;
                let handle = Arc::new(IxIndexHandle::open(
                    self.disk.clone(),
                    self.rm.bpm().clone(),
                    fd,
                )?);
                self.table_mut(name)?.indexes.push(IndexMeta {
                    cols: col_group,
                    file: ipath,
                    handle,
                });
            } else {
                self.create_index(name, &col_group)?;
            }
        }
        for col_group in hash_indexes {
            let hpath = self.hash_index_file(name, &col_group);
            if self.ix.exists(&hpath) {
                let handle = ExtendibleHashIndex::open(&hpath, &self.disk)?;
                self.table_mut(name)?.hash_indexes.push(HashIndexMeta {
                    cols: col_group,
                    file: hpath,
                    handle,
                });
            } else {
                self.create_hash_index(name, &col_group)?;
            }
        }
        // Persist the now-attached index metadata. Saving only before opening
        // the index files would overwrite the index list with an empty one
        // during restart.
        self.save_meta()?;
        Ok(())
    }

    // ---- 子查询求值（planner 调用）----

    /// 执行已绑定的 SELECT 并返回第一列的值列表（子查询 IN/标量使用）。
    pub fn eval_select_column(
        &mut self,
        s: &crate::analyze::BoundSelect,
        ctx: &ExecCtx,
    ) -> Result<Vec<crate::types::Value>, EasyDbError> {
        let plan = planner::plan(Query::Select(Box::new(s.clone())), self, ctx)?;
        let Plan::Select { root, .. } = plan else {
            return Err(EasyDbError::Internal("plan mismatch".to_string()));
        };
        let mut exec = planner::build_executor(&root, self, ctx)?;
        let mut out = Vec::new();
        while let Some(row) = exec.next()? {
            let Some(v) = row.values.into_iter().next() else {
                return Err(EasyDbError::SubqueryIllegal(
                    "subquery should only return one column".to_string(),
                ));
            };
            out.push(v);
        }
        Ok(out)
    }

    // ---- DDL ----

    fn create_table(&mut self, table: &str, mut cols: Vec<Column>) -> Result<Output, EasyDbError> {
        if self.tables.contains_key(table) {
            return Err(EasyDbError::TableAlreadyExists(table.to_string()));
        }
        if cols.iter().any(|c| c.tab_name().is_empty()) {
            for c in cols.iter_mut() {
                c.set_tab_name(table);
            }
        }
        let path = self.table_file(table);
        self.rm.create_file(&path)?;
        let fd = self.disk.open_file(&path)?;
        let fh = Arc::new(RmFileHandle::open(
            self.disk.clone(),
            self.bpm_shared(),
            fd,
        )?);
        self.tables.insert(
            table.to_string(),
            TableMeta {
                name: table.to_string(),
                cols,
                fh,
                indexes: Vec::new(),
                hash_indexes: Vec::new(),
            },
        );
        self.save_meta()?;
        Ok(Output::Ok)
    }

    fn drop_table(&mut self, table: &str) -> Result<Output, EasyDbError> {
        let meta = self.table(table)?;
        let fd = meta.fh.fd();
        let index_files: Vec<PathBuf> = meta.indexes.iter().map(|ix| ix.file.clone()).collect();
        let index_fds: Vec<i32> = meta.indexes.iter().map(|ix| ix.handle.fd()).collect();
        let hash_index_files: Vec<PathBuf> =
            meta.hash_indexes.iter().map(|ix| ix.file.clone()).collect();
        let hash_index_fds: Vec<i32> = meta.hash_indexes.iter().map(|ix| ix.handle.fd()).collect();
        let table_path = self.table_file(table);
        self.tables.remove(table);

        // 关闭索引文件并删除
        for (path, ifd) in index_files.into_iter().zip(index_fds) {
            self.disk.close_file(ifd)?;
            self.ix.destroy_index(&path)?;
        }
        for (path, ifd) in hash_index_files.into_iter().zip(hash_index_fds) {
            self.disk.close_file(ifd)?;
            self.ix.destroy_index(&path)?;
        }
        // 关闭表文件并删除
        self.disk.close_file(fd)?;
        self.rm.destroy_file(&table_path)?;
        self.save_meta()?;
        Ok(Output::Ok)
    }

    fn create_hash_index(&mut self, table: &str, cols: &[String]) -> Result<Output, EasyDbError> {
        let meta = self.table(table)?;
        let mut types = Vec::with_capacity(cols.len());
        let mut lens = Vec::with_capacity(cols.len());
        for name in cols {
            let col = meta
                .cols
                .iter()
                .find(|c| c.name() == *name)
                .ok_or_else(|| EasyDbError::ColumnNotFound(name.clone()))?;
            types.push(col.col_type());
            lens.push(col.storage_size() as usize);
        }
        let path = self.hash_index_file(table, cols);
        if self.ix.exists(&path) {
            return Err(EasyDbError::IndexAlreadyExists {
                table: table.into(),
                cols: cols.to_vec(),
            });
        }
        let handle = ExtendibleHashIndex::create(&path, &self.disk)?;
        let fh = meta.fh.clone();
        let table_cols = meta.cols.clone();
        let mut scan =
            crate::execution::scan::SeqScanExec::new_unlocked(fh, table_cols, Vec::new());
        while let Some(row) = scan.next()? {
            let values: Vec<crate::types::Value> = cols
                .iter()
                .map(|n| row.values[meta.col_pos(n)].clone())
                .collect();
            let key = crate::index::ix_defs::encode_key(&types, &lens, &values)?;
            handle.insert_entry(&key, row.rid)?;
        }
        self.table_mut(table)?.hash_indexes.push(HashIndexMeta {
            cols: cols.to_vec(),
            file: path,
            handle,
        });
        self.save_meta()?;
        Ok(Output::Ok)
    }

    fn desc_table(&self, table: &str) -> Result<Output, EasyDbError> {
        let meta = self.table(table)?;
        let rows = meta
            .cols
            .iter()
            .map(|c| {
                let indexed = meta
                    .indexes
                    .iter()
                    .any(|ix| ix.cols.iter().any(|n| n == c.name()));
                vec![
                    crate::types::Value::Str(c.name().to_string()),
                    crate::types::Value::Str(c.col_type().as_str().to_string()),
                    crate::types::Value::Str(if indexed { "YES" } else { "" }.to_string()),
                ]
            })
            .collect();
        Ok(Output::Rows {
            columns: vec!["field".to_string(), "type".to_string(), "index".to_string()],
            rows,
        })
    }

    fn create_index(&mut self, table: &str, cols: &[String]) -> Result<Output, EasyDbError> {
        let meta = self.table(table)?;
        // 列存在性
        let mut types = Vec::with_capacity(cols.len());
        let mut lens = Vec::with_capacity(cols.len());
        for name in cols {
            let col = meta
                .cols
                .iter()
                .find(|c| c.name() == *name)
                .ok_or_else(|| EasyDbError::ColumnNotFound(name.clone()))?;
            types.push(col.col_type());
            lens.push(col.storage_size() as usize);
        }
        let path = self.index_file(table, cols);
        if self.ix.exists(&path) {
            return Err(EasyDbError::IndexAlreadyExists {
                table: table.to_string(),
                cols: cols.to_vec(),
            });
        }
        let col_specs: Vec<(ColType, i32)> = types
            .iter()
            .zip(lens.iter())
            .map(|(t, l)| (*t, *l as i32))
            .collect();
        self.ix.create_index(&path, &col_specs)?;
        let fd = self.disk.open_file(&path)?;
        let handle = Arc::new(IxIndexHandle::open(
            self.disk.clone(),
            self.bpm_shared(),
            fd,
        )?);

        // 回填既有数据
        {
            let fh = meta.fh.clone();
            let table_cols = meta.cols.clone();
            let mut scan = crate::execution::scan::SeqScanExec::new_unlocked(
                fh,
                table_cols.clone(),
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
        }

        self.table_mut(table)?.indexes.push(IndexMeta {
            cols: cols.to_vec(),
            file: path,
            handle,
        });
        self.save_meta()?;
        Ok(Output::Ok)
    }

    fn drop_index(&mut self, table: &str, cols: &[String]) -> Result<Output, EasyDbError> {
        let pos = self
            .table(table)?
            .indexes
            .iter()
            .position(|ix| ix.cols == cols)
            .ok_or_else(|| EasyDbError::IndexNotFound {
                table: table.to_string(),
                cols: cols.to_vec(),
            })?;
        let meta = self.table_mut(table)?;
        let ix = meta.indexes.remove(pos);
        let fd = ix.handle.fd();
        self.disk.close_file(fd)?;
        self.ix.destroy_index(&ix.file)?;
        self.save_meta()?;
        Ok(Output::Ok)
    }

    fn drop_hash_index(&mut self, table: &str, cols: &[String]) -> Result<Output, EasyDbError> {
        let pos = self
            .table(table)?
            .hash_indexes
            .iter()
            .position(|ix| ix.cols == cols)
            .ok_or_else(|| EasyDbError::IndexNotFound {
                table: table.into(),
                cols: cols.to_vec(),
            })?;
        let meta = self.table_mut(table)?;
        let ix = meta.hash_indexes.remove(pos);
        self.disk.close_file(ix.handle.fd())?;
        self.ix.destroy_index(&ix.file)?;
        self.save_meta()?;
        Ok(Output::Ok)
    }

    fn bpm_shared(&self) -> Arc<BufferPool> {
        // rm 与 ix 共用同一缓冲池实例
        self.rm.bpm().clone()
    }
}

impl TableMeta {
    /// 列名 → 表内下标。
    pub fn col_pos(&self, name: &str) -> usize {
        self.cols.iter().position(|c| c.name() == name).unwrap_or(0)
    }

    /// 列类型。
    pub fn col_type(&self, name: &str) -> Option<ColType> {
        self.cols
            .iter()
            .find(|c| c.name() == name)
            .map(|c| c.col_type())
    }
}

impl CatalogView for Database {
    fn table_exists(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    fn table_columns(&self, name: &str) -> Option<Vec<Column>> {
        self.tables.get(name).map(|t| t.cols.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Value;

    fn setup(tag: &str) -> (tempfile::TempDir, Database) {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(dir.path().join(tag)).unwrap();
        (dir, db)
    }

    fn rows(out: Output) -> Vec<Vec<Value>> {
        match out {
            Output::Rows { rows, .. } => rows,
            other => panic!("expected rows, got {other:?}"),
        }
    }

    fn ints(out: Output) -> Vec<i32> {
        rows(out)
            .into_iter()
            .map(|r| match &r[0] {
                Value::Int(n) => *n,
                other => panic!("want int, got {other:?}"),
            })
            .collect()
    }

    const SETUP: &[&str] = &[
        "CREATE TABLE student (id INT, name VARCHAR(16), score FLOAT);",
        "CREATE TABLE course (sid INT, grade INT);",
        "INSERT INTO student VALUES (1, 'alice', 90.5);",
        "INSERT INTO student VALUES (2, 'bob', 75.0);",
        "INSERT INTO student VALUES (3, 'carol', 88.0);",
        "INSERT INTO course VALUES (1, 95);",
        "INSERT INTO course VALUES (2, 60);",
        "INSERT INTO course VALUES (1, 88);",
    ];

    fn seeded(tag: &str) -> (tempfile::TempDir, Database) {
        let (dir, mut db) = setup(tag);
        for sql in SETUP {
            db.run_sql(sql).unwrap();
        }
        (dir, db)
    }

    #[test]
    fn ddl_and_crud_end_to_end() {
        let (_dir, mut db) = seeded("crud");

        // 全表扫描
        let out = db.run_sql("SELECT * FROM student;").unwrap();
        assert_eq!(rows(out).len(), 3);

        // WHERE 过滤（数值 + 字符串）
        let out = db
            .run_sql("SELECT id FROM student WHERE score >= 88;")
            .unwrap();
        assert_eq!(ints(out), vec![1, 3]);

        let out = db
            .run_sql("SELECT id FROM student WHERE name = 'bob';")
            .unwrap();
        assert_eq!(ints(out), vec![2]);

        // 限定表名 + 条件列
        let out = db
            .run_sql("SELECT student.id FROM student WHERE student.name <> 'bob';")
            .unwrap();
        assert_eq!(ints(out), vec![1, 3]);

        // UPDATE（含算术）
        let n = db
            .run_sql("UPDATE student SET score = score + 5 WHERE id = 2;")
            .unwrap();
        assert_eq!(n, Output::Count(1));
        let out = db
            .run_sql("SELECT score FROM student WHERE id = 2;")
            .unwrap();
        match &rows(out)[0][0] {
            Value::Double(v) => assert!((v - 80.0).abs() < 1e-9),
            other => panic!("want double, got {other:?}"),
        }

        // DELETE
        let n = db.run_sql("DELETE FROM student WHERE id = 3;").unwrap();
        assert_eq!(n, Output::Count(1));
        let out = db.run_sql("SELECT id FROM student;").unwrap();
        assert_eq!(ints(out), vec![1, 2]);
    }

    #[test]
    fn join_group_aggregate_order() {
        let (_dir, mut db) = seeded("agg");

        // 两表连接（等值条件跨表）
        let out = db
            .run_sql("SELECT student.name, course.grade FROM student, course WHERE student.id = course.sid;")
            .unwrap();
        let r = rows(out);
        assert_eq!(r.len(), 3);
        // alice 两条、bob 一条
        let names: Vec<String> = r
            .iter()
            .map(|row| match &row[0] {
                Value::Str(s) => s.clone(),
                other => panic!("want str, got {other:?}"),
            })
            .collect();
        assert!(names.contains(&"alice".to_string()));
        assert!(names.contains(&"bob".to_string()));

        // 聚合：GROUP BY + COUNT/SUM/MAX
        let out = db
            .run_sql("SELECT sid, COUNT(*) AS cnt, SUM(grade) AS total, MAX(grade) AS best FROM course GROUP BY sid;")
            .unwrap();
        let r = rows(out);
        assert_eq!(r.len(), 2);
        // 按首现顺序：sid=1 (2 门课, 183, 95)，sid=2 (1 门课, 60, 60)
        let get = |sid: i32| r.iter().find(|row| row[0] == Value::Int(sid)).unwrap();
        assert_eq!(get(1)[1], Value::Int(2));
        assert_eq!(get(1)[2], Value::Int(183));
        assert_eq!(get(1)[3], Value::Int(95));
        assert_eq!(get(2)[1], Value::Int(1));
        assert_eq!(get(2)[2], Value::Int(60));

        // HAVING
        let out = db
            .run_sql("SELECT sid, COUNT(*) FROM course GROUP BY sid HAVING COUNT(*) > 1;")
            .unwrap();
        assert_eq!(ints(out), vec![1]);

        // 全表聚合（无分组）
        let out = db
            .run_sql("SELECT COUNT(*), MAX(score) FROM student;")
            .unwrap();
        let r = rows(out);
        assert_eq!(r[0][0], Value::Int(3));
        assert_eq!(r[0][1], Value::Double(90.5));

        // ORDER BY（降序）+ UNIQUE
        let out = db
            .run_sql("SELECT id FROM student ORDER BY id DESC;")
            .unwrap();
        assert_eq!(ints(out), vec![3, 2, 1]);
        let out = db
            .run_sql("SELECT UNIQUE sid FROM course ORDER BY sid;")
            .unwrap();
        assert_eq!(ints(out), vec![1, 2]);

        // 范围条件 + AND
        let out = db
            .run_sql("SELECT id FROM student WHERE score > 70 AND score < 90;")
            .unwrap();
        assert_eq!(ints(out), vec![2, 3]);
    }

    #[test]
    fn index_point_and_range_lookup() {
        let (_dir, mut db) = seeded("ix");

        // 建索引（回填既有数据）
        db.run_sql("CREATE INDEX student (id);").unwrap();
        // 回填后新插入的行也要进索引
        db.run_sql("INSERT INTO student VALUES (10, 'dave', 60.0);")
            .unwrap();

        // 等值点查走索引
        let out = db
            .run_sql("SELECT name FROM student WHERE id = 10;")
            .unwrap();
        let r = rows(out);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0][0], Value::Str("dave".into()));

        // 范围查询
        let out = db.run_sql("SELECT id FROM student WHERE id >= 2;").unwrap();
        assert_eq!(ints(out), vec![2, 3, 10]);

        // 索引列更新：键迁移
        db.run_sql("UPDATE student SET id = 20 WHERE id = 10;")
            .unwrap();
        assert!(rows(db.run_sql("SELECT * FROM student WHERE id = 10;").unwrap()).is_empty());
        assert_eq!(
            ints(db.run_sql("SELECT id FROM student WHERE id = 20;").unwrap()),
            vec![20]
        );

        // 索引列删除
        db.run_sql("DELETE FROM student WHERE id = 20;").unwrap();
        assert!(rows(db.run_sql("SELECT * FROM student WHERE id = 20;").unwrap()).is_empty());

        // SHOW INDEX / DROP INDEX
        let out = db.run_sql("SHOW INDEX FROM student;").unwrap();
        assert_eq!(rows(out).len(), 1);
        db.run_sql("DROP INDEX student (id);").unwrap();
        // 删除索引后查询仍正确（顺序扫描）
        assert_eq!(
            ints(db.run_sql("SELECT id FROM student;").unwrap()),
            vec![1, 2, 3]
        );
    }

    #[test]
    fn show_and_desc() {
        let (_dir, mut db) = seeded("meta");
        let out = db.run_sql("SHOW TABLES;").unwrap();
        let r = rows(out);
        assert_eq!(r.len(), 2);
        assert_eq!(r[0][0], Value::Str("course".into()));
        assert_eq!(r[1][0], Value::Str("student".into()));

        let out = db.run_sql("DESC student;").unwrap();
        let r = rows(out);
        assert_eq!(r.len(), 3);
        assert_eq!(r[0][0], Value::Str("id".into()));
        assert_eq!(r[0][1], Value::Str("INT".into()));
    }

    #[test]
    fn drop_table_removes_everything() {
        let (_dir, mut db) = seeded("drop");
        db.run_sql("CREATE INDEX student (id);").unwrap();
        db.run_sql("DROP TABLE student;").unwrap();
        assert!(matches!(
            db.run_sql("SELECT * FROM student;"),
            Err(EasyDbError::TableNotFound(_))
        ));
        // 重建同名表可用
        db.run_sql("CREATE TABLE student (id INT);").unwrap();
        assert!(db.run_sql("SELECT * FROM student;").is_ok());
    }

    #[test]
    fn subquery_in() {
        let (_dir, mut db) = seeded("sub");
        let out = db
            .run_sql(
                "SELECT name FROM student WHERE id IN (SELECT sid FROM course WHERE grade >= 88);",
            )
            .unwrap();
        let r = rows(out);
        let names: Vec<String> = r
            .iter()
            .map(|row| match &row[0] {
                Value::Str(s) => s.clone(),
                other => panic!("want str, got {other:?}"),
            })
            .collect();
        // grade>=88 的 sid：1(alice)、1(alice)
        assert_eq!(names, vec!["alice".to_string()]);
    }

    #[test]
    fn txn_and_knob_statements() {
        let (_dir, mut db) = seeded("knob");
        assert!(matches!(db.run_sql("BEGIN;").unwrap(), Output::Ok));
        assert!(matches!(db.run_sql("COMMIT;").unwrap(), Output::Ok));
        assert!(matches!(
            db.run_sql("SET ENABLE_NESTLOOP = ON;").unwrap(),
            Output::Ok
        ));
        assert_eq!(db.knob(ast::Knob::EnableNestLoop), Some(true));
        assert!(matches!(db.run_sql("help").unwrap(), Output::Ok));
    }

    #[test]
    fn in_list_and_arith_update() {
        let (_dir, mut db) = seeded("inlist");
        let out = db
            .run_sql("SELECT id FROM student WHERE id IN (1, 3);")
            .unwrap();
        assert_eq!(ints(out), vec![1, 3]);

        db.run_sql("UPDATE course SET grade = grade * 2 WHERE sid = 2;")
            .unwrap();
        let out = db
            .run_sql("SELECT grade FROM course WHERE sid = 2;")
            .unwrap();
        assert_eq!(rows(out)[0][0], Value::Int(120));
    }
}

#[cfg(test)]
mod txn_tests {
    use super::*;
    use crate::common::rid::Rid;
    use crate::types::Value;

    fn setup(tag: &str) -> (tempfile::TempDir, Database) {
        let dir = tempfile::tempdir().unwrap();
        let mut db = Database::open(dir.path().join(tag)).unwrap();
        db.run_sql("CREATE TABLE t (id INT, name VARCHAR(16));")
            .unwrap();
        db.run_sql("INSERT INTO t VALUES (1, 'one');").unwrap();
        db.run_sql("INSERT INTO t VALUES (2, 'two');").unwrap();
        (dir, db)
    }

    fn ids(db: &mut Database) -> Vec<i32> {
        match db.run_sql("SELECT id FROM t;").unwrap() {
            Output::Rows { rows, .. } => rows
                .into_iter()
                .map(|r| match &r[0] {
                    Value::Int(n) => *n,
                    other => panic!("want int, got {other:?}"),
                })
                .collect(),
            other => panic!("want rows, got {other:?}"),
        }
    }

    #[test]
    fn rollback_on_abort_statement() {
        let (_dir, mut db) = setup("rb");

        // INSERT 后回滚
        db.run_sql("BEGIN;").unwrap();
        db.run_sql("INSERT INTO t VALUES (3, 'three');").unwrap();
        assert_eq!(ids(&mut db), vec![1, 2, 3]); // 事务内可见（同事务读）
        db.run_sql("ABORT;").unwrap();
        assert_eq!(ids(&mut db), vec![1, 2]);

        // DELETE 后回滚
        db.run_sql("BEGIN;").unwrap();
        db.run_sql("DELETE FROM t WHERE id = 1;").unwrap();
        assert_eq!(ids(&mut db), vec![2]);
        db.run_sql("ROLLBACK;").unwrap();
        assert_eq!(ids(&mut db), vec![1, 2]);

        // UPDATE 后回滚
        db.run_sql("BEGIN;").unwrap();
        db.run_sql("UPDATE t SET name = 'six' WHERE id = 1;")
            .unwrap();
        db.run_sql("ABORT;").unwrap();
        match db.run_sql("SELECT name FROM t WHERE id = 1;").unwrap() {
            Output::Rows { rows, .. } => assert_eq!(rows[0][0], Value::Str("one".into())),
            other => panic!("want rows, got {other:?}"),
        }
    }

    #[test]
    fn commit_persists_within_and_across() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = Database::open(dir.path().join("c")).unwrap();
        db.run_sql("CREATE TABLE t (id INT);").unwrap();
        db.run_sql("BEGIN;").unwrap();
        db.run_sql("INSERT INTO t VALUES (1);").unwrap();
        db.run_sql("INSERT INTO t VALUES (2);").unwrap();
        db.run_sql("COMMIT;").unwrap();
        // 重开数据库（模拟重启后目录仍在）：显式重建目录 + 恢复由 recover 测试覆盖，
        // 这里验证提交后语句可读
        assert_eq!(
            match db.run_sql("SELECT COUNT(*) FROM t;").unwrap() {
                Output::Rows { rows, .. } => rows[0][0].clone(),
                _ => panic!(),
            },
            Value::Int(2)
        );
    }

    #[test]
    fn explicit_txn_holds_locks_until_commit() {
        let (_dir, mut db) = setup("wd");

        // 单连接语义：显式事务内的写持续持有记录 X 锁，同事务的后续
        // 语句可重入；跨连接的 wait-die（年轻者中止/年长者等待）由
        // concurrency 模块的单测覆盖（P7 多连接接入后在此补端到端用例）
        db.run_sql("BEGIN;").unwrap();
        db.run_sql("UPDATE t SET name = 't1' WHERE id = 1;")
            .unwrap();
        // 同事务内读到自己未提交的写
        match db.run_sql("SELECT name FROM t WHERE id = 1;").unwrap() {
            Output::Rows { rows, .. } => assert_eq!(rows[0][0], Value::Str("t1".into())),
            other => panic!("want rows, got {other:?}"),
        }
        db.run_sql("COMMIT;").unwrap();
        match db.run_sql("SELECT name FROM t WHERE id = 1;").unwrap() {
            Output::Rows { rows, .. } => assert_eq!(rows[0][0], Value::Str("t1".into())),
            other => panic!("want rows, got {other:?}"),
        }
    }

    #[test]
    fn statement_error_rolls_back_statement_only() {
        let (_dir, mut db) = setup("savepoint");

        // 显式事务里第二条语句失败（列不存在）：第一条的写入保留，
        // 失败语句的写入回滚
        db.run_sql("BEGIN;").unwrap();
        db.run_sql("INSERT INTO t VALUES (3, 'three');").unwrap();
        assert!(db.run_sql("INSERT INTO t VALUES (4, nosuchcol);").is_err());
        db.run_sql("COMMIT;").unwrap();
        assert_eq!(ids(&mut db), vec![1, 2, 3]);
    }

    #[test]
    fn crash_recovery_redo_and_undo() {
        use crate::record::tuple::encode_tuple;
        use crate::recovery::{LogManager, LogRecord};

        let dir = tempfile::tempdir().unwrap();
        let db_dir = dir.path().join("db");

        // 阶段一：已提交数据（WAL 随提交刷盘）
        {
            let mut db = Database::open(&db_dir).unwrap();
            db.run_sql("CREATE TABLE t (id INT, name VARCHAR(16));")
                .unwrap();
            db.run_sql("CREATE INDEX t (id);").unwrap();
            db.run_sql("INSERT INTO t VALUES (1, 'one');").unwrap();
            db.run_sql("INSERT INTO t VALUES (2, 'two');").unwrap();
            let _ = db.table("t").unwrap().cols.len();
        }

        // 阶段二：模拟"已刷盘但未提交"的事务 99——直接向 db.log 追加
        // （正常路径中，后续事务提交刷盘时会把缓冲里的未提交记录一并落盘）
        {
            let cols = vec![
                Column::with_table("id", "t", ColType::Int).unwrap(),
                Column::with_table_varlen("name", "t", ColType::VarChar, 16).unwrap(),
            ];
            let schema = crate::catalog::Schema::new(cols);
            let enc = |vals: &[Value]| encode_tuple(&schema, vals).unwrap();
            let mut log = LogManager::open(&db_dir).unwrap();
            log.append(&LogRecord::Begin { txn: 99 });
            // 插入 ghost（slot 2，追加语义与原执行一致）
            log.append(&LogRecord::Insert {
                txn: 99,
                table: "t".into(),
                rid: Rid::new(1, 2),
                data: enc(&[Value::Int(99), Value::Str("ghost".into())]),
            });
            // 删除 id=1
            log.append(&LogRecord::Delete {
                txn: 99,
                table: "t".into(),
                rid: Rid::new(1, 0),
                old_data: enc(&[Value::Int(1), Value::Str("one".into())]),
            });
            // 更新 id=2：two → six（等长，符合原地更新限制）
            log.append(&LogRecord::Update {
                txn: 99,
                table: "t".into(),
                rid: Rid::new(1, 1),
                old_data: enc(&[Value::Int(2), Value::Str("two".into())]),
                new_data: enc(&[Value::Int(2), Value::Str("six".into())]),
            });
            log.flush().unwrap();
        }

        // 阶段三：重启即自动恢复（db.meta 提供元数据 → 重放 + 撤销 +
        // 索引重建 + 表挂载）
        let mut db = Database::open(&db_dir).unwrap();

        // 提交数据完好；未提交事务的三条修改全部撤销
        match db.run_sql("SELECT id, name FROM t ORDER BY id;").unwrap() {
            Output::Rows { rows, .. } => {
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0][0], Value::Int(1));
                assert_eq!(rows[0][1], Value::Str("one".into())); // DELETE 被撤销
                assert_eq!(rows[1][0], Value::Int(2));
                assert_eq!(rows[1][1], Value::Str("two".into())); // UPDATE 被撤销
            }
            other => panic!("want rows, got {other:?}"),
        }
        // 索引重建后点查可用；ghost 不存在
        match db.run_sql("SELECT name FROM t WHERE id = 2;").unwrap() {
            Output::Rows { rows, .. } => assert_eq!(rows[0][0], Value::Str("two".into())),
            other => panic!("want rows, got {other:?}"),
        }
        match db.run_sql("SELECT name FROM t WHERE id = 99;").unwrap() {
            Output::Rows { rows, .. } => assert!(rows.is_empty()),
            other => panic!("want rows, got {other:?}"),
        }
    }
}

#[cfg(test)]
mod server_tests {
    use super::{Database, format_output};
    use crate::types::Value;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::{Arc, Mutex};

    /// 起一个监听随机端口的服务端线程；数据库目录由调用方指定，
    /// 返回 (端口, 目录句柄)。
    fn spawn_server(db_dir: &std::path::Path) -> (u16, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(Mutex::new(Database::open(db_dir).unwrap()));
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let db2 = db.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { continue };
                let db = db2.clone();
                std::thread::spawn(move || {
                    let mut stream = stream;
                    let mut buf = [0u8; 8192];
                    let mut acc: Vec<u8> = Vec::new();
                    loop {
                        let n = match stream.read(&mut buf) {
                            Ok(0) | Err(_) => return,
                            Ok(n) => n,
                        };
                        acc.extend_from_slice(&buf[..n]);
                        // 协议要求 NUL 结尾；未完整则继续读
                        let Some(end) = acc.iter().position(|&b| b == 0) else {
                            continue;
                        };
                        let sql = String::from_utf8_lossy(&acc[..end]).to_string();
                        acc.drain(..=end);
                        eprintln!("[srv] recv: {sql:?}");
                        if sql.trim() == "exit" {
                            return;
                        }
                        let resp = {
                            let mut db = db.lock().unwrap();
                            db.run_sql(sql.trim())
                                .map(|o| format_output(&o))
                                .map_err(|e| format!("{e}\n"))
                        };
                        let text = match resp {
                            Ok(t) | Err(t) => t,
                        };
                        eprintln!("[srv] resp: {text:?}");
                        let _ = stream.write_all(text.as_bytes());
                        let _ = stream.write_all(b"\0");
                    }
                });
            }
        });
        (port, dir)
    }

    fn exchange(stream: &mut TcpStream, sql: &str) -> String {
        stream.write_all(sql.as_bytes()).unwrap();
        stream.write_all(b"\0").unwrap();
        // 协议：读到 NUL 为止（TCP 可能分段，必须循环读）
        let mut acc = Vec::new();
        let mut chunk = [0u8; 8192];
        loop {
            let n = stream.read(&mut chunk).unwrap();
            acc.extend_from_slice(&chunk[..n]);
            if acc.contains(&0) || n == 0 {
                break;
            }
        }
        let end = acc.iter().position(|&b| b == 0).unwrap_or(acc.len());
        String::from_utf8_lossy(&acc[..end]).to_string()
    }

    #[test]
    fn server_end_to_end_text_protocol() {
        let dir = tempfile::tempdir().unwrap();
        let (port, _guard) = spawn_server(&dir.path().join("db"));
        let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();

        // DDL / DML：成功为空响应
        assert_eq!(
            exchange(&mut stream, "CREATE TABLE t (id INT, name VARCHAR(16));"),
            ""
        );
        assert_eq!(
            exchange(&mut stream, "INSERT INTO t VALUES (1, 'alice');"),
            ""
        );

        // SELECT：RecordPrinter 风格表格
        let resp = exchange(&mut stream, "SELECT * FROM t;");
        assert!(
            resp.contains("|               id |             name |"),
            "got: {resp}"
        );
        assert!(
            resp.contains("|                1 |            alice |"),
            "got: {resp}"
        );
        assert!(resp.contains("Total record(s): 1"));

        // 空结果 → empty set
        assert_eq!(
            exchange(&mut stream, "SELECT * FROM t WHERE id = 99;"),
            "empty set\n"
        );

        // 错误 → 错误文本 + 换行
        let resp = exchange(&mut stream, "SELECT * FROM nosuch;");
        assert!(resp.contains("Table not found"), "got: {resp}");

        // 语法错误 → Internal/parse 报错文本
        let resp = exchange(&mut stream, "SELEC;");
        assert!(!resp.is_empty(), "syntax error must produce text");
    }

    #[test]
    fn server_persists_across_restart() {
        let dir = tempfile::tempdir().unwrap();
        let db_dir = dir.path().join("db");
        let (port, _guard) = spawn_server(&db_dir);

        // 实例一：建表插数据后断开
        {
            let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
            exchange(&mut stream, "CREATE TABLE t (id INT, name VARCHAR(16));");
            exchange(&mut stream, "INSERT INTO t VALUES (1, 'one');");
            exchange(&mut stream, "INSERT INTO t VALUES (2, 'two');");
            let _ = stream.shutdown(std::net::Shutdown::Both);
        }

        // 实例二：同目录重启——db.meta 自动加载 + WAL 恢复 + 表挂载
        let mut db = Database::open(&db_dir).unwrap();
        match db.run_sql("SELECT id FROM t ORDER BY id;").unwrap() {
            crate::system::Output::Rows { rows, .. } => {
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0][0], Value::Int(1));
                assert_eq!(rows[1][0], Value::Int(2));
            }
            other => panic!("want rows, got {other:?}"),
        }
    }
}
