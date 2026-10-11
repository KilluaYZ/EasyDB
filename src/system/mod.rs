//! 系统管理：数据库目录与服务门面。
//!
//! 对应 C++ 版 `src-cpp/system/sm_manager.cpp`。P5 为内存目录
//! （M2 里程碑：单用户、无事务），P7 接入元数据持久化与 TCP 服务。
//!
//! C++ 的 SmManager/DbFile 混用裸指针与全局 map，Rust 版收敛为
//! `Database`：每表一个 [`TableMeta`]（schema + 记录文件句柄 + 索引）。

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::analyze::{Analyzer, CatalogView, Query};
use crate::buffer::buffer_pool::BufferPool;
use crate::catalog::Column;
use crate::common::errors::EasyDbError;
use crate::execution::Executor;
use crate::index::ix_index_handle::IxIndexHandle;
use crate::index::ix_manager::IxManager;
use crate::parser::ast;
use crate::parser::parse_sql;
use crate::planner::{self, Plan};
use crate::record::rm_file_handle::RmFileHandle;
use crate::record::rm_manager::RmManager;
use crate::storage::disk::DiskManager;
use crate::types::ColType;

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

/// 表元数据：schema + 记录文件句柄 + 索引。
pub struct TableMeta {
    pub name: String,
    pub cols: Vec<Column>,
    pub fh: Arc<RmFileHandle>,
    pub indexes: Vec<IndexMeta>,
}

/// 内存数据库：表目录 + 存储设施 + SQL 执行门面。
pub struct Database {
    dir: PathBuf,
    disk: Arc<DiskManager>,
    rm: RmManager,
    ix: IxManager,
    tables: HashMap<String, TableMeta>,
    knobs: HashMap<ast::Knob, bool>,
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
        Ok(Self {
            dir,
            disk,
            rm,
            ix,
            tables: HashMap::new(),
            knobs: HashMap::new(),
        })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
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

    /// 执行一条 SQL 文本：解析 → 分析 → 计划 → 执行。
    pub fn run_sql(&mut self, sql: &str) -> Result<Output, EasyDbError> {
        let stmt = parse_sql(sql)?;
        let query = Analyzer::new(self).analyze(stmt)?;
        self.execute(query)
    }

    /// 执行绑定后的查询。
    pub fn execute(&mut self, query: Query) -> Result<Output, EasyDbError> {
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
                    .map(|ix| vec![crate::types::Value::Str(ix.cols.join(","))])
                    .collect();
                Ok(Output::Rows {
                    columns: vec!["index_cols".to_string()],
                    rows,
                })
            }
            Query::TxnBegin | Query::TxnCommit | Query::TxnAbort | Query::TxnRollback => {
                // 事务在 P6 接入；此处按成功处理
                Ok(Output::Ok)
            }
            Query::SetKnob { knob, value } => {
                self.knobs.insert(knob, value);
                Ok(Output::Ok)
            }
            Query::CreateTable { table, columns } => self.create_table(&table, columns),
            Query::DropTable { table } => self.drop_table(&table),
            Query::DescTable { table } => self.desc_table(&table),
            Query::CreateIndex { table, cols } => self.create_index(&table, &cols),
            Query::DropIndex { table, cols } => self.drop_index(&table, &cols),
            Query::CreateStaticCheckpoint => {
                // 检查点随 P6 WAL 落地
                Ok(Output::Ok)
            }
            Query::LoadData { .. } => Err(EasyDbError::Internal(
                "LOAD will be supported in P7".to_string(),
            )),
            Query::Insert { table, values } => {
                let plan = planner::plan(Query::Insert { table, values }, self)?;
                let Plan::Insert { table, rows } = plan else {
                    return Err(EasyDbError::Internal("plan mismatch".to_string()));
                };
                let mut exec = planner::build_insert(self, &table, rows)?;
                let mut count = 0u64;
                while exec.next()?.is_some() {
                    count += 1;
                }
                Ok(Output::Count(count))
            }
            Query::Delete { table, wheres } => {
                let plan = planner::plan(Query::Delete { table, wheres }, self)?;
                let Plan::Delete { table, conds } = plan else {
                    return Err(EasyDbError::Internal("plan mismatch".to_string()));
                };
                let mut exec = planner::build_delete(self, &table, &conds)?;
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
                )?;
                let Plan::Update {
                    table,
                    clauses,
                    conds,
                } = plan
                else {
                    return Err(EasyDbError::Internal("plan mismatch".to_string()));
                };
                let mut exec = planner::build_update(self, &table, &clauses, &conds)?;
                let mut count = 0u64;
                while exec.next()?.is_some() {
                    count += 1;
                }
                Ok(Output::Count(count))
            }
            Query::Select(s) => {
                let plan = planner::plan(Query::Select(s), self)?;
                let Plan::Select { root, columns } = plan else {
                    return Err(EasyDbError::Internal("plan mismatch".to_string()));
                };
                let mut exec = planner::build_executor(&root, self)?;
                let mut rows = Vec::new();
                while let Some(row) = exec.next()? {
                    rows.push(row.values);
                }
                Ok(Output::Rows { columns, rows })
            }
        }
    }

    // ---- 子查询求值（planner 调用）----

    /// 执行已绑定的 SELECT 并返回第一列的值列表（子查询 IN/标量使用）。
    pub fn eval_select_column(
        &mut self,
        s: &crate::analyze::BoundSelect,
    ) -> Result<Vec<crate::types::Value>, EasyDbError> {
        let plan = planner::plan(Query::Select(Box::new(s.clone())), self)?;
        let Plan::Select { root, .. } = plan else {
            return Err(EasyDbError::Internal("plan mismatch".to_string()));
        };
        let mut exec = planner::build_executor(&root, self)?;
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
            },
        );
        Ok(Output::Ok)
    }

    fn drop_table(&mut self, table: &str) -> Result<Output, EasyDbError> {
        let meta = self.table(table)?;
        let fd = meta.fh.fd();
        let index_files: Vec<PathBuf> = meta.indexes.iter().map(|ix| ix.file.clone()).collect();
        let index_fds: Vec<i32> = meta.indexes.iter().map(|ix| ix.handle.fd()).collect();
        let table_path = self.table_file(table);
        self.tables.remove(table);

        // 关闭索引文件并删除
        for (path, ifd) in index_files.into_iter().zip(index_fds) {
            self.disk.close_file(ifd)?;
            self.ix.destroy_index(&path)?;
        }
        // 关闭表文件并删除
        self.disk.close_file(fd)?;
        self.rm.destroy_file(&table_path)?;
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
            let mut scan =
                crate::execution::scan::SeqScanExec::new(fh, table_cols.clone(), Vec::new());
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
