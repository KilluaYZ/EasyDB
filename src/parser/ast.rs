//! SQL 语法树（解析器输出，未绑定 schema）。
//!
//! 对应 C++ 版 `src-cpp/include/parser/ast.h`：继承体系收敛为 enum，
//! 字符串全部持有（无生命周期标注）。

/// 列引用：`t.c` / `c` / 聚合形式。
#[derive(Debug, Clone, PartialEq)]
pub struct Col {
    /// 限定表名（未限定为空）
    pub table: String,
    /// 列名（COUNT(*) 时为空串，语义分析阶段改写为 "*"，与 C++ 一致）
    pub name: String,
    /// 聚合函数（None = 普通列）
    pub agg: Option<AggFunc>,
    /// AS 别名
    pub alias: Option<String>,
}

impl Col {
    /// 普通列引用。
    pub fn plain(table: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            name: name.into(),
            agg: None,
            alias: None,
        }
    }

    /// 聚合列引用。
    pub fn agg(func: AggFunc, name: impl Into<String>, alias: Option<String>) -> Self {
        Self {
            table: String::new(),
            name: name.into(),
            agg: Some(func),
            alias,
        }
    }
}

/// 聚合函数（对应 `AggregationType` 的聚合成员）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFunc {
    Count,
    Max,
    Min,
    Sum,
}

/// 比较操作符。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompOp {
    Eq,
    Ne,
    Lt,
    Gt,
    Le,
    Ge,
    In,
}

/// 算术操作符（UPDATE 赋值右侧表达式）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArithOp {
    Add,
    Sub,
    Mul,
    Div,
}

/// 字面量值。
#[derive(Debug, Clone, PartialEq)]
pub enum Lit {
    Int(i32),
    Float(f64),
    Str(String),
    Bool(bool),
}

/// 条件右部。
#[derive(Debug, Clone, PartialEq)]
pub enum CondRhs {
    Lit(Lit),
    Col(Col),
    /// `IN (v1, v2, ...)`
    LitList(Vec<Lit>),
    /// 子查询（`IN (SELECT ...)` / `op (SELECT ...)`）
    SubQuery(Box<SelectStmt>),
}

/// WHERE / HAVING 条件：`col op rhs`。
#[derive(Debug, Clone, PartialEq)]
pub struct Condition {
    pub lhs: Col,
    pub op: CompOp,
    pub rhs: CondRhs,
}

/// UPDATE 赋值右部。
#[derive(Debug, Clone, PartialEq)]
pub enum SetRhs {
    Lit(Lit),
    /// `col = other_col op value`
    Arith {
        col: String,
        op: ArithOp,
        value: Lit,
    },
}

/// UPDATE 赋值子句。
#[derive(Debug, Clone, PartialEq)]
pub struct SetClause {
    pub col: String,
    pub rhs: SetRhs,
}

/// CREATE TABLE 列定义。
#[derive(Debug, Clone, PartialEq)]
pub struct ColDef {
    pub name: String,
    pub ty: DeclType,
    /// NOT NULL（当前引擎不支持 NULL 语义，仅保留语法位）
    pub not_null: bool,
}

/// 声明的列类型（对应 C++ `SvType` + 长度）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeclType {
    Int,
    Float,
    /// CHAR(n) / VARCHAR(n)
    String(u32),
    /// DATETIME（按 C++ 口径为 19 字节字符串）
    Datetime,
}

/// 排序方向。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderDir {
    Default,
    Asc,
    Desc,
}

/// 系统开关（SET 语句）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Knob {
    EnableNestLoop,
    EnableSortMerge,
    EnableHashJoin,
    EnableOptimizer,
    OutputFile,
}

/// SELECT 语句。
#[derive(Debug, Clone, PartialEq)]
pub struct SelectStmt {
    /// UNIQUE（去重）
    pub distinct: bool,
    /// 选择列；空 = `*`
    pub cols: Vec<Col>,
    pub tables: Vec<String>,
    /// WHERE 子句（方言允许 WHERE 出现在 FROM 前后两处，两处同时给
    /// 出时按 AND 合并）
    pub wheres: Vec<Condition>,
    pub group_by: Vec<Col>,
    pub having: Vec<Condition>,
    pub order_by: Option<(Col, OrderDir)>,
}

/// 顶层语句。
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Help,
    Exit,
    ShowTables,
    ShowIndex {
        table: String,
    },
    TxnBegin,
    TxnCommit,
    TxnAbort,
    TxnRollback,
    SetKnob {
        knob: Knob,
        value: bool,
    },
    CreateTable {
        table: String,
        fields: Vec<ColDef>,
    },
    DropTable {
        table: String,
    },
    DescTable {
        table: String,
    },
    CreateIndex {
        table: String,
        cols: Vec<String>,
    },
    DropIndex {
        table: String,
        cols: Vec<String>,
    },
    CreateStaticCheckpoint,
    LoadData {
        file: String,
        table: String,
    },
    Insert {
        table: String,
        values: Vec<Lit>,
    },
    Delete {
        table: String,
        wheres: Vec<Condition>,
    },
    Update {
        table: String,
        clauses: Vec<SetClause>,
        wheres: Vec<Condition>,
    },
    Select(Box<SelectStmt>),
}
