//! 语义分析：把解析树绑定到 schema，产出类型化的 [`Query`]。
//!
//! 对应 C++ 版 `src-cpp/analyze/analyze.cpp`。
//!
//! 与 C++ 版的差异：
//! - ORDER BY 在分析阶段一并完成列解析（C++ 版把它留给 planner 回读
//!   原始语法树）；
//! - 子查询无条件递归绑定（C++ 版仅在部分路径处理）；
//! - 布尔字面量出现在值位置时报 `IncompatibleType`（C++ 版抛 Internal）。

use crate::catalog::Column;
use crate::common::errors::EasyDbError;
use crate::parser::ast::{self, CompOp, Condition, Lit};
use crate::types::{AggregationType, ColType, Value};

/// 分析器所需的目录只读视图（P7 由 SmManager 实现）。
pub trait CatalogView {
    fn table_exists(&self, name: &str) -> bool;
    /// 表的全部列（含表名回填）。
    fn table_columns(&self, name: &str) -> Option<Vec<Column>>;
}

/// 限定列：表名 + 列名 + 聚合 + 别名（对应 C++ `TabCol`）。
#[derive(Debug, Clone, PartialEq)]
pub struct TabCol {
    pub tab: String,
    pub col: String,
    pub agg: AggregationType,
    pub alias: String,
}

impl TabCol {
    fn plain(tab: impl Into<String>, col: impl Into<String>) -> Self {
        Self {
            tab: tab.into(),
            col: col.into(),
            agg: AggregationType::None,
            alias: String::new(),
        }
    }
}

/// 绑定后的条件右部。
#[derive(Debug, Clone, PartialEq)]
pub enum BoundRhs {
    Value(Value),
    Col(TabCol),
    /// `IN (v1, v2, ...)`
    LitList(Vec<Value>),
    /// 子查询（已递归绑定）
    SubQuery(Box<Query>),
}

/// 绑定后的条件。
#[derive(Debug, Clone, PartialEq)]
pub struct BoundCond {
    pub lhs: TabCol,
    pub op: CompOp,
    pub rhs: BoundRhs,
}

/// UPDATE 赋值右部。
#[derive(Debug, Clone, PartialEq)]
pub enum BoundSetRhs {
    Value(Value),
    Arith {
        col: TabCol,
        op: ast::ArithOp,
        value: Value,
    },
}

/// 绑定后的 UPDATE 赋值。
#[derive(Debug, Clone, PartialEq)]
pub struct BoundSetClause {
    pub col: TabCol,
    pub rhs: BoundSetRhs,
}

/// 绑定后的 SELECT。
#[derive(Debug, Clone, PartialEq)]
pub struct BoundSelect {
    pub distinct: bool,
    /// 选择列；`*` 展开后的完整列清单
    pub items: Vec<TabCol>,
    pub tables: Vec<String>,
    pub wheres: Vec<BoundCond>,
    pub group_by: Vec<TabCol>,
    pub having: Vec<BoundCond>,
    pub order_by: Option<(TabCol, ast::OrderDir)>,
}

/// 语义分析产出：按语句类别完全类型化。
#[derive(Debug, Clone, PartialEq)]
pub enum Query {
    // ---- 直接透传（无需绑定）----
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
        knob: ast::Knob,
        value: bool,
    },
    CreateTable {
        table: String,
        columns: Vec<Column>,
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
    // ---- DML（已绑定）----
    Insert {
        table: String,
        values: Vec<Value>,
    },
    Delete {
        table: String,
        wheres: Vec<BoundCond>,
    },
    Update {
        table: String,
        clauses: Vec<BoundSetClause>,
        wheres: Vec<BoundCond>,
    },
    Select(Box<BoundSelect>),
}

/// 语义分析器。
pub struct Analyzer<'a> {
    catalog: &'a dyn CatalogView,
}

impl<'a> Analyzer<'a> {
    pub fn new(catalog: &'a dyn CatalogView) -> Self {
        Self { catalog }
    }

    pub fn analyze(&self, stmt: ast::Statement) -> Result<Query, EasyDbError> {
        match stmt {
            ast::Statement::Help => Ok(Query::Help),
            ast::Statement::Exit => Ok(Query::Exit),
            ast::Statement::ShowTables => Ok(Query::ShowTables),
            ast::Statement::ShowIndex { table } => Ok(Query::ShowIndex { table }),
            ast::Statement::TxnBegin => Ok(Query::TxnBegin),
            ast::Statement::TxnCommit => Ok(Query::TxnCommit),
            ast::Statement::TxnAbort => Ok(Query::TxnAbort),
            ast::Statement::TxnRollback => Ok(Query::TxnRollback),
            ast::Statement::SetKnob { knob, value } => Ok(Query::SetKnob { knob, value }),
            ast::Statement::CreateTable { table, fields } => {
                let columns = fields
                    .into_iter()
                    .map(|f| {
                        // DATETIME 按 C++ 口径映射为 19 字节变长字符串
                        match f.ty {
                            ast::DeclType::Int => Column::new(f.name, ColType::Int),
                            ast::DeclType::Float => Column::new(f.name, ColType::Float),
                            ast::DeclType::String(n) => {
                                Column::new_varlen(f.name, ColType::VarChar, n)
                            }
                            ast::DeclType::Datetime => {
                                Column::new_varlen(f.name, ColType::VarChar, 19)
                            }
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Query::CreateTable { table, columns })
            }
            ast::Statement::DropTable { table } => Ok(Query::DropTable { table }),
            ast::Statement::DescTable { table } => Ok(Query::DescTable { table }),
            ast::Statement::CreateIndex { table, cols } => Ok(Query::CreateIndex { table, cols }),
            ast::Statement::DropIndex { table, cols } => Ok(Query::DropIndex { table, cols }),
            ast::Statement::CreateStaticCheckpoint => Ok(Query::CreateStaticCheckpoint),
            ast::Statement::LoadData { file, table } => Ok(Query::LoadData { file, table }),
            ast::Statement::Insert { table, values } => {
                let values = values
                    .into_iter()
                    .map(lit_to_value)
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Query::Insert { table, values })
            }
            ast::Statement::Delete { table, wheres } => {
                let cols = self.require_table(&table)?;
                let wheres = self.bind_conds(&cols, wheres)?;
                Ok(Query::Delete { table, wheres })
            }
            ast::Statement::Update {
                table,
                clauses,
                wheres,
            } => {
                let cols = self.require_table(&table)?;
                let mut bound_clauses = Vec::with_capacity(clauses.len());
                for c in clauses {
                    let col = TabCol::plain(table.clone(), c.col);
                    self.check_column(&cols, &col)?;
                    let rhs = match c.rhs {
                        ast::SetRhs::Lit(l) => BoundSetRhs::Value(lit_to_value(l)?),
                        ast::SetRhs::Arith {
                            col: rhs_col,
                            op,
                            value,
                        } => {
                            let rhs_col = TabCol::plain(table.clone(), rhs_col);
                            self.check_column(&cols, &rhs_col)?;
                            BoundSetRhs::Arith {
                                col: rhs_col,
                                op,
                                value: lit_to_value(value)?,
                            }
                        }
                    };
                    bound_clauses.push(BoundSetClause { col, rhs });
                }
                let wheres = self.bind_conds(&cols, wheres)?;
                Ok(Query::Update {
                    table,
                    clauses: bound_clauses,
                    wheres,
                })
            }
            ast::Statement::Select(s) => Ok(Query::Select(Box::new(self.bind_select(&s)?))),
        }
    }

    // ---- SELECT 绑定 ----

    fn bind_select(&self, s: &ast::SelectStmt) -> Result<BoundSelect, EasyDbError> {
        // 表存在性
        for t in &s.tables {
            if !self.catalog.table_exists(t) {
                return Err(EasyDbError::TableNotFound(t.clone()));
            }
        }

        // 全部列（按表顺序展开）
        let mut all_cols: Vec<Column> = Vec::new();
        for t in &s.tables {
            all_cols.extend(self.catalog.table_columns(t).unwrap_or_default());
        }

        // 选择列：`*` 展开；COUNT(*) 改写 col="*"；其余做解析
        let mut items = Vec::with_capacity(s.cols.len());
        if s.cols.is_empty() {
            for col in &all_cols {
                items.push(TabCol::plain(col.tab_name(), col.name()));
            }
        } else {
            for c in &s.cols {
                if c.name == "*" && c.agg == Some(ast::AggFunc::Count) {
                    items.push(TabCol {
                        tab: String::new(),
                        col: "*".to_string(),
                        agg: AggregationType::Count,
                        alias: c.alias.clone().unwrap_or_default(),
                    });
                } else {
                    items.push(self.check_column(&all_cols, &to_tab_col(c))?);
                }
            }
        }

        let wheres = self.bind_conds(&all_cols, s.wheres.clone())?;

        let mut group_by = Vec::with_capacity(s.group_by.len());
        for c in &s.group_by {
            group_by.push(self.check_column(&all_cols, &to_tab_col(c))?);
        }

        let having = self.bind_conds(&all_cols, s.having.clone())?;

        let order_by = match &s.order_by {
            Some((c, dir)) => {
                let col = self.check_column(&all_cols, &to_tab_col(c))?;
                Some((col, *dir))
            }
            None => None,
        };

        // 聚合合法性（对应 C++ check_aggregation_legality）
        if !s.group_by.is_empty() {
            for item in &items {
                if item.agg == AggregationType::None && !group_by.iter().any(|g| g.col == item.col)
                {
                    return Err(EasyDbError::AggregationIllegal);
                }
            }
        }
        if wheres.iter().any(|c| c.lhs.agg != AggregationType::None) {
            return Err(EasyDbError::AggregationIllegal);
        }

        Ok(BoundSelect {
            distinct: s.distinct,
            items,
            tables: s.tables.clone(),
            wheres,
            group_by,
            having,
            order_by,
        })
    }

    // ---- 条件绑定 ----

    fn bind_conds(
        &self,
        all_cols: &[Column],
        conds: Vec<Condition>,
    ) -> Result<Vec<BoundCond>, EasyDbError> {
        let mut out = Vec::with_capacity(conds.len());
        for cond in conds {
            // COUNT(*)（文法中列名为 "*"）走特判，不做普通列解析
            // （与 C++ 顺序一致：先判 COUNT(*) 再 check_column）
            let lhs_is_count_star =
                cond.lhs.name == "*" && cond.lhs.agg == Some(ast::AggFunc::Count);
            let lhs = if lhs_is_count_star {
                TabCol {
                    tab: String::new(),
                    col: "*".to_string(),
                    agg: AggregationType::Count,
                    alias: String::new(),
                }
            } else {
                self.check_column(all_cols, &to_tab_col(&cond.lhs))?
            };
            if lhs_is_count_star {
                // COUNT(*) 出现在条件左部：仅允许与值比较（对应 C++ 逻辑）
                let rhs = match cond.rhs {
                    ast::CondRhs::Lit(l) => BoundRhs::Value(lit_to_value(l)?),
                    _ => return Err(EasyDbError::AggregationIllegal),
                };
                out.push(BoundCond {
                    lhs: TabCol {
                        tab: String::new(),
                        col: "*".to_string(),
                        agg: AggregationType::Count,
                        alias: String::new(),
                    },
                    op: cond.op,
                    rhs,
                });
                continue;
            }

            let rhs = match cond.rhs {
                ast::CondRhs::Lit(l) => {
                    let v = lit_to_value(l)?;
                    self.check_type_compat(all_cols, &lhs, Some(&v), None)?;
                    BoundRhs::Value(v)
                }
                ast::CondRhs::Col(c) => {
                    let rhs_col = self.check_column(all_cols, &to_tab_col(&c))?;
                    self.check_type_compat(all_cols, &lhs, None, Some(&rhs_col))?;
                    BoundRhs::Col(rhs_col)
                }
                ast::CondRhs::LitList(ls) => {
                    let vals = ls
                        .into_iter()
                        .map(lit_to_value)
                        .collect::<Result<Vec<_>, _>>()?;
                    if let Some(v) = vals.first() {
                        self.check_type_compat(all_cols, &lhs, Some(v), None)?;
                    }
                    BoundRhs::LitList(vals)
                }
                ast::CondRhs::SubQuery(sq) => {
                    // 子查询：递归绑定；只允许单列（对应 C++ SubqueryIllegal）
                    if sq.cols.len() != 1 {
                        return Err(EasyDbError::SubqueryIllegal(
                            "subquery should only return one column".to_string(),
                        ));
                    }
                    if sq.tables.is_empty() {
                        return Err(EasyDbError::SubqueryIllegal(
                            "subquery should specify table name".to_string(),
                        ));
                    }
                    let bound = self.bind_select(&sq)?;
                    BoundRhs::SubQuery(Box::new(Query::Select(Box::new(bound))))
                }
            };
            out.push(BoundCond {
                lhs,
                op: cond.op,
                rhs,
            });
        }
        Ok(out)
    }

    // ---- 列解析与类型检查 ----

    /// 解析列引用（对应 C++ `check_column`）。
    fn check_column(&self, all_cols: &[Column], target: &TabCol) -> Result<TabCol, EasyDbError> {
        if target.tab.is_empty() {
            let mut tab = String::new();
            for col in all_cols {
                if col.name() == target.col {
                    if !tab.is_empty() {
                        return Err(EasyDbError::AmbiguousColumn(target.col.clone()));
                    }
                    tab = col.tab_name().to_string();
                }
            }
            if tab.is_empty() {
                return Err(EasyDbError::ColumnNotFound(target.col.clone()));
            }
            Ok(TabCol {
                tab,
                col: target.col.clone(),
                agg: target.agg,
                alias: target.alias.clone(),
            })
        } else {
            if !self.catalog.table_exists(&target.tab) {
                return Err(EasyDbError::TableNotFound(target.tab.clone()));
            }
            if all_cols
                .iter()
                .any(|c| c.name() == target.col && c.tab_name() == target.tab)
            {
                Ok(target.clone())
            } else {
                Err(EasyDbError::ColumnNotFound(target.col.clone()))
            }
        }
    }

    /// 字符串类型 vs 非字符串类型不兼容（对应 C++ check_clause 的类型检查）。
    fn check_type_compat(
        &self,
        all_cols: &[Column],
        lhs: &TabCol,
        rhs_val: Option<&Value>,
        rhs_col: Option<&TabCol>,
    ) -> Result<(), EasyDbError> {
        let lhs_type = all_cols
            .iter()
            .find(|c| c.name() == lhs.col && c.tab_name() == lhs.tab)
            .map(|c| c.col_type());
        let rhs_type = match (rhs_val, rhs_col) {
            (Some(v), _) => Some(v.col_type()),
            (None, Some(c)) => all_cols
                .iter()
                .find(|col| col.name() == c.col && col.tab_name() == c.tab)
                .map(|col| col.col_type()),
            _ => None,
        };
        if let (Some(l), Some(r)) = (lhs_type, rhs_type) {
            let str_involved = matches!(l, ColType::Char | ColType::VarChar)
                || matches!(r, ColType::Char | ColType::VarChar);
            if l != r && str_involved {
                return Err(EasyDbError::IncompatibleType(
                    l.as_str().to_string(),
                    r.as_str().to_string(),
                ));
            }
        }
        Ok(())
    }

    fn require_table(&self, table: &str) -> Result<Vec<Column>, EasyDbError> {
        self.catalog
            .table_columns(table)
            .ok_or_else(|| EasyDbError::TableNotFound(table.to_string()))
    }
}

// ==================== 辅助 ====================

/// AST 列引用 → 绑定列（聚合/别名随行）。
fn to_tab_col(c: &ast::Col) -> TabCol {
    TabCol {
        tab: c.table.clone(),
        col: c.name.clone(),
        agg: match c.agg {
            Some(ast::AggFunc::Count) => AggregationType::Count,
            Some(ast::AggFunc::Max) => AggregationType::Max,
            Some(ast::AggFunc::Min) => AggregationType::Min,
            Some(ast::AggFunc::Sum) => AggregationType::Sum,
            None => AggregationType::None,
        },
        alias: c.alias.clone().unwrap_or_default(),
    }
}

/// 字面量 → 值（对应 C++ `convert_sv_value`；浮点统一为 f64）。
fn lit_to_value(l: Lit) -> Result<Value, EasyDbError> {
    Ok(match l {
        Lit::Int(v) => Value::Int(v),
        Lit::Float(v) => Value::Double(v),
        Lit::Str(s) => Value::Str(s),
        Lit::Bool(_) => {
            return Err(EasyDbError::IncompatibleType(
                "value".to_string(),
                "BOOL".to_string(),
            ));
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::parse_sql;
    use std::collections::HashMap;

    struct FakeCatalog {
        tables: HashMap<String, Vec<Column>>,
    }

    impl FakeCatalog {
        fn std_two_tables() -> Self {
            let mut tables = HashMap::new();
            tables.insert(
                "t".to_string(),
                vec![
                    Column::with_table("id", "t", ColType::Int).unwrap(),
                    Column::with_table_varlen("name", "t", ColType::VarChar, 16).unwrap(),
                ],
            );
            tables.insert(
                "s".to_string(),
                vec![
                    Column::with_table("id", "s", ColType::Int).unwrap(),
                    Column::with_table("score", "s", ColType::Float).unwrap(),
                ],
            );
            Self { tables }
        }
    }

    impl CatalogView for FakeCatalog {
        fn table_exists(&self, name: &str) -> bool {
            self.tables.contains_key(name)
        }
        fn table_columns(&self, name: &str) -> Option<Vec<Column>> {
            self.tables.get(name).cloned()
        }
    }

    fn analyze(sql: &str) -> Result<Query, EasyDbError> {
        let cat = FakeCatalog::std_two_tables();
        let az = Analyzer::new(&cat);
        az.analyze(parse_sql(sql).unwrap())
    }

    fn analyze_with(sql: &str, cat: &FakeCatalog) -> Result<Query, EasyDbError> {
        Analyzer::new(cat).analyze(parse_sql(sql).unwrap())
    }

    #[test]
    fn star_expansion_and_column_resolution() {
        let q = analyze("SELECT * FROM t;").unwrap();
        let Query::Select(s) = q else {
            panic!("not select")
        };
        assert_eq!(s.items.len(), 2);
        assert_eq!(s.items[0], TabCol::plain("t", "id"));
        assert_eq!(s.items[1], TabCol::plain("t", "name"));

        // 未限定列自动补表名（name 仅 t 表有，无歧义）
        let q = analyze("SELECT name FROM t, s;").unwrap();
        let Query::Select(s) = q else {
            panic!("not select")
        };
        assert_eq!(s.items[0], TabCol::plain("t", "name"));

        // 限定列
        let q = analyze("SELECT s.id FROM t, s;").unwrap();
        let Query::Select(s) = q else {
            panic!("not select")
        };
        assert_eq!(s.items[0], TabCol::plain("s", "id"));
    }

    #[test]
    fn error_cases_match_cpp() {
        assert!(matches!(
            analyze("SELECT * FROM nosuch;"),
            Err(EasyDbError::TableNotFound(_))
        ));
        assert!(matches!(
            analyze("SELECT nosuchcol FROM t;"),
            Err(EasyDbError::ColumnNotFound(_))
        ));
        // 两表同名列未限定 → 歧义（与 C++ 一致）
        assert!(matches!(
            analyze("SELECT * FROM t, s WHERE id = 1;")
                .err()
                .unwrap_or_else(|| panic!("want ambiguous")),
            EasyDbError::AmbiguousColumn(_)
        ));
        // UPDATE 指定表后限定名解析
        assert!(matches!(
            analyze("UPDATE t SET id = 1;"),
            Ok(Query::Update { .. })
        ));
    }

    #[test]
    fn type_compat_string_vs_number() {
        assert!(matches!(
            analyze("SELECT * FROM t WHERE id = 'x';"),
            Err(EasyDbError::IncompatibleType(_, _))
        ));
        // 数值列 vs 字符串列——同为字符串才检查
        assert!(analyze("SELECT * FROM t WHERE name = 'alice';").is_ok());
        assert!(matches!(
            analyze("SELECT * FROM t WHERE name = 1;"),
            Err(EasyDbError::IncompatibleType(_, _))
        ));
    }

    #[test]
    fn aggregation_legality() {
        // 有 GROUP BY：非聚合选择列必须在分组列中
        assert!(analyze("SELECT name, COUNT(*) FROM t GROUP BY name;").is_ok());
        assert!(matches!(
            analyze("SELECT id, COUNT(*) FROM t GROUP BY name;"),
            Err(EasyDbError::AggregationIllegal)
        ));
        // WHERE 条件里不允许聚合
        assert!(matches!(
            analyze("SELECT COUNT(*) FROM t WHERE COUNT(*) > 1;"),
            Err(EasyDbError::AggregationIllegal)
        ));
    }

    #[test]
    fn insert_and_update_binding() {
        let q = analyze("INSERT INTO t VALUES (1, 'alice');").unwrap();
        match q {
            Query::Insert { table, values } => {
                assert_eq!(table, "t");
                assert_eq!(values, vec![Value::Int(1), Value::Str("alice".into())]);
            }
            other => panic!("unexpected {other:?}"),
        }

        let q = analyze("UPDATE t SET id = id + 5 WHERE name = 'bob';").unwrap();
        match q {
            Query::Update {
                clauses, wheres, ..
            } => {
                assert_eq!(clauses[0].col, TabCol::plain("t", "id"));
                match &clauses[0].rhs {
                    BoundSetRhs::Arith {
                        col,
                        op: ast::ArithOp::Add,
                        value,
                    } => {
                        assert_eq!(*col, TabCol::plain("t", "id"));
                        assert_eq!(*value, Value::Int(5));
                    }
                    other => panic!("unexpected rhs {other:?}"),
                }
                assert_eq!(wheres.len(), 1);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn subquery_binding() {
        let q =
            analyze("SELECT * FROM t WHERE id IN (SELECT id FROM s WHERE score > 3.5);").unwrap();
        let Query::Select(s) = q else {
            panic!("not select")
        };
        assert_eq!(s.wheres.len(), 1);
        let BoundRhs::SubQuery(sub) = &s.wheres[0].rhs else {
            panic!("want subquery");
        };
        let Query::Select(inner) = sub.as_ref() else {
            panic!("want bound select");
        };
        assert_eq!(inner.tables, vec!["s"]);
        assert_eq!(inner.wheres.len(), 1);
        // 子查询列已解析出表名
        assert_eq!(inner.wheres[0].lhs.tab, "s");
    }

    #[test]
    fn create_table_maps_types() {
        let q = analyze("CREATE TABLE x (a INT, b FLOAT, c VARCHAR(8), d DATETIME);").unwrap();
        match q {
            Query::CreateTable { table, columns } => {
                assert_eq!(table, "x");
                assert_eq!(columns[0].col_type(), ColType::Int);
                assert_eq!(columns[1].col_type(), ColType::Float);
                assert_eq!(columns[2].col_type(), ColType::VarChar);
                assert_eq!(columns[2].storage_size(), 8);
                assert_eq!(columns[3].storage_size(), 19);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn empty_catalog_table_not_found() {
        let cat = FakeCatalog {
            tables: HashMap::new(),
        };
        assert!(matches!(
            analyze_with("SELECT * FROM t;", &cat),
            Err(EasyDbError::TableNotFound(_))
        ));
    }
}
