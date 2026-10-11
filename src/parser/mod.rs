//! SQL 解析：词法器 + LALRPOP 文法。
//!
//! 对应 C++ 版 `src-cpp/parser/`（flex/bison → 手写词法器 + LALRPOP）。

pub mod ast;
pub mod lexer;

// 由 build.rs 经 LALRPOP 生成到本目录（OUT_DIR 之外的约定位置）
lalrpop_mod!(pub sql, "/parser/sql.rs");

use lalrpop_util::lalrpop_mod;

use crate::parser::lexer::{LexError, tokenize};
use crate::parser::sql::TopParser;

/// 解析错误：词法或语法。
#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("{0}")]
    Lex(#[from] LexError),
    #[error("syntax error: {0}")]
    Syntax(String),
}

/// 解析一条 SQL 语句（不含结尾 `;` 也可以，尾部分号可选处理在文法内）。
pub fn parse_sql(src: &str) -> Result<ast::Statement, ParseError> {
    let mut tokens = tokenize(src)?;
    // LALRPOP 以迭代器耗尽为输入结束，显式的 Eof 单元不再进入解析器
    debug_assert!(matches!(tokens.last(), Some((_, lexer::Tok::Eof, _))));
    tokens.pop();
    TopParser::new()
        .parse(tokens)
        .map_err(|e| ParseError::Syntax(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ast::*;

    fn sel(src: &str) -> SelectStmt {
        match parse_sql(src).unwrap() {
            Statement::Select(s) => *s,
            other => panic!("expected select, got {other:?}"),
        }
    }

    #[test]
    fn create_table() {
        let s = parse_sql("CREATE TABLE student (id INT NOT NULL, name VARCHAR(50), score FLOAT);")
            .unwrap();
        match s {
            Statement::CreateTable { table, fields } => {
                assert_eq!(table, "student");
                assert_eq!(fields.len(), 3);
                assert_eq!(fields[0].name, "id");
                assert_eq!(fields[0].ty, DeclType::Int);
                assert!(fields[0].not_null);
                assert_eq!(fields[1].ty, DeclType::String(50));
                assert_eq!(fields[2].ty, DeclType::Float);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn insert_values() {
        let s = parse_sql("INSERT INTO t VALUES (1, 'alice', 3.5, -2);").unwrap();
        match s {
            Statement::Insert { table, values } => {
                assert_eq!(table, "t");
                assert_eq!(
                    values,
                    vec![
                        Lit::Int(1),
                        Lit::Str("alice".into()),
                        Lit::Float(3.5),
                        Lit::Int(-2),
                    ]
                );
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn select_variants() {
        // 基础
        let s = sel("SELECT * FROM t;");
        assert!(s.cols.is_empty());
        assert_eq!(s.tables, vec!["t"]);
        assert!(!s.distinct);

        // UNIQUE + WHERE + ORDER BY
        let s =
            sel("SELECT UNIQUE id, name FROM t, s WHERE id = 5 AND name <> 'x' ORDER BY id DESC;");
        assert!(s.distinct);
        assert_eq!(s.cols.len(), 2);
        assert_eq!(s.tables, vec!["t", "s"]);
        assert_eq!(s.wheres.len(), 2);
        assert!(matches!(s.wheres[1].op, CompOp::Ne));
        assert_eq!(s.order_by.as_ref().unwrap().0.name, "id");
        assert_eq!(s.order_by.as_ref().unwrap().1, OrderDir::Desc);

        // 方言：WHERE 在 FROM 之前
        let s = sel("SELECT id WHERE id > 3 FROM t;");
        assert_eq!(s.wheres.len(), 1);

        // JOIN 语法
        let s = sel("SELECT * FROM t JOIN s WHERE t.id = s.id;");
        assert_eq!(s.tables, vec!["t", "s"]);
    }

    #[test]
    fn aggregates_with_alias() {
        let s = sel(
            "SELECT COUNT(*) AS total, MAX(score), MIN(score) AS lo FROM t GROUP BY name HAVING COUNT(*) > 2;",
        );
        assert_eq!(s.cols.len(), 3);
        assert_eq!(s.cols[0].agg, Some(AggFunc::Count));
        assert_eq!(s.cols[0].alias.as_deref(), Some("total"));
        assert_eq!(s.cols[1].agg, Some(AggFunc::Max));
        assert_eq!(s.cols[1].alias, None);
        assert_eq!(s.cols[2].alias.as_deref(), Some("lo"));
        assert_eq!(s.group_by.len(), 1);
        assert_eq!(s.having.len(), 1);
        assert!(matches!(s.having[0].lhs.agg, Some(AggFunc::Count)));
    }

    #[test]
    fn update_delete_and_misc() {
        let s = parse_sql("UPDATE t SET score = score + 5, name = 'x' WHERE id = 1;").unwrap();
        match s {
            Statement::Update { clauses, .. } => {
                assert_eq!(clauses.len(), 2);
                assert!(matches!(
                    clauses[0].rhs,
                    SetRhs::Arith {
                        op: ArithOp::Add,
                        ..
                    }
                ));
            }
            other => panic!("unexpected {other:?}"),
        }

        assert!(matches!(
            parse_sql("DELETE FROM t WHERE id IN (1, 2, 3);").unwrap(),
            Statement::Delete { .. }
        ));
        assert!(matches!(
            parse_sql("DESC t;").unwrap(),
            Statement::DescTable { .. }
        ));
        assert!(matches!(
            parse_sql("CREATE INDEX t (id, name);").unwrap(),
            Statement::CreateIndex { .. }
        ));
        assert!(matches!(
            parse_sql("LOAD test_data/t1.csv INTO t1;").unwrap(),
            Statement::LoadData { .. }
        ));
        assert!(matches!(parse_sql("BEGIN;").unwrap(), Statement::TxnBegin));
        assert!(matches!(
            parse_sql("SET ENABLE_NESTLOOP = ON;").unwrap(),
            Statement::SetKnob {
                knob: Knob::EnableNestLoop,
                value: true
            }
        ));
        assert!(matches!(
            parse_sql("SET ENABLE_OPTIMIZER OFF;").unwrap(),
            Statement::SetKnob { value: false, .. }
        ));
        assert!(matches!(parse_sql("help").unwrap(), Statement::Help));
        assert!(matches!(parse_sql("exit").unwrap(), Statement::Exit));
    }

    #[test]
    fn subquery_condition() {
        let s = sel("SELECT * FROM t WHERE id IN (SELECT id FROM s);");
        assert!(matches!(s.wheres[0].rhs, CondRhs::SubQuery(_)));
    }

    #[test]
    fn syntax_errors() {
        assert!(parse_sql("SELECT FROM t;").is_err()); // 缺 selector
        assert!(parse_sql("CREATE TABLE t;").is_err()); // 缺字段列表
        assert!(parse_sql("INSERT INTO t VALUES (1);").is_ok());
    }
}
