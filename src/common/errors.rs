//! 统一错误类型。
//!
//! 对应 C++ 版 `src-cpp/include/common/errors.h` 中的异常类层次：
//! C++ 按 PF/RM/IX/SM/QL 错误域定义了 20 余个异常类，Rust 版收敛为单个
//! [`EasyDbError`] 枚举，全部错误经 `Result<T, EasyDbError>` 传递。

use std::io;

/// EasyDB 全局错误类型。
#[derive(Debug, thiserror::Error)]
pub enum EasyDbError {
    /// 内部错误（逻辑缺陷，不应暴露给用户）
    #[error("Internal error: {0}")]
    Internal(String),

    // ---- 文件/存储（对应 C++ PF errors）----
    /// 底层 IO 错误（对应 C++ `UnixError`）
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("Invalid file descriptor: {0}")]
    FileNotOpen(i32),
    #[error("File is opened: {0}")]
    FileNotClosed(String),
    #[error("File already exists: {0}")]
    FileExists(String),
    #[error("File not found: {0}")]
    FileNotFound(String),

    // ---- 记录（对应 C++ RM errors）----
    #[error("Record not found: ({page_no},{slot_no})")]
    RecordNotFound { page_no: i32, slot_no: u32 },
    #[error("Invalid record size: {0}")]
    InvalidRecordSize(i32),

    // ---- 索引（对应 C++ IX errors）----
    #[error("Invalid column length: {0}")]
    InvalidColLength(u32),
    #[error("Index entry not found")]
    IndexEntryNotFound,

    // ---- 系统元数据（对应 C++ SM errors）----
    #[error("Database not found: {0}")]
    DatabaseNotFound(String),
    #[error("Database already exists: {0}")]
    DatabaseAlreadyExists(String),
    #[error("Table not found: {0}")]
    TableNotFound(String),
    #[error("Table already exists: {0}")]
    TableAlreadyExists(String),
    #[error("Column not found: {0}")]
    ColumnNotFound(String),
    #[error("Index not found: {table}.({})", cols.join(", "))]
    IndexNotFound { table: String, cols: Vec<String> },
    #[error("Index already exists: {table}.({})", cols.join(", "))]
    IndexAlreadyExists { table: String, cols: Vec<String> },

    // ---- 查询（对应 C++ QL errors）----
    #[error("Invalid value count")]
    InvalidValueCount,
    #[error("String is too long")]
    StringOverflow,
    #[error("Incompatible type error: lhs {0}, rhs {1}")]
    IncompatibleType(String, String),
    #[error("Ambiguous column: {0}")]
    AmbiguousColumn(String),
    #[error("Page {page_no} in table {table} not exists")]
    PageNotExist { table: String, page_no: i32 },
    #[error("Select query with illegal aggregation conditions")]
    AggregationIllegal,
    #[error("Subquery illegal: {0}")]
    SubqueryIllegal(String),

    // ---- 其他 ----
    #[error("Unexpected null access")]
    NullAccess,
    #[error("Buffer pool is full: all frames are pinned")]
    BufferPoolFull,
    #[error("syntax error: {0}")]
    Syntax(#[from] crate::parser::ParseError),
    #[error("transaction {0} aborted (wait-die deadlock prevention or state violation)")]
    TransactionAbort(crate::common::config::TxnId),
    // 事务中止（TransactionAbort）将随 P6 事务/锁阶段补充。
}
