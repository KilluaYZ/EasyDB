//! 结果集文本格式化（协议兼容层）。
//!
//! 对应 C++ 版 `record_printer.h` + `execution_manager.cpp` 的输出：
//! 表格列宽 16、值右对齐、`+---+` 分隔、结尾 `Total record(s): N` /
//! `empty set`；浮点按 C++ `std::to_string(double)` 输出 6 位小数。

use super::Output;
use crate::types::Value;

const COL_WIDTH: usize = 16;

/// 把执行结果格式化为发往客户端的文本（不含结尾 NUL）。
pub fn format_output(out: &Output) -> String {
    match out {
        Output::Rows { columns, rows } => format_rows(columns, rows),
        // C++ 版 DML/DDL 成功时不产生文本输出
        Output::Count(_) | Output::Ok => String::new(),
    }
}

fn value_to_str(v: &Value) -> String {
    match v {
        // 与 C++ std::to_string(double) 对齐：固定 6 位小数
        Value::Float(f) | Value::Double(f) => format!("{f:.6}"),
        other => other.to_string(),
    }
}

fn format_rows(columns: &[String], rows: &[Vec<Value>]) -> String {
    if rows.is_empty() {
        // C++ print_record_count：空集时整个响应被覆写为 "empty set"
        return "empty set\n".to_string();
    }
    let num_cols = columns.len();
    let mut sep = String::new();
    for _ in 0..num_cols {
        sep.push('+');
        sep.push_str(&"-".repeat(COL_WIDTH + 2));
    }
    sep.push('+');
    sep.push('\n');
    let sep = sep;
    let mut out = String::new();
    out.push_str(&sep);
    for c in columns {
        let mut col = c.clone();
        if col.len() > COL_WIDTH {
            col.truncate(COL_WIDTH - 3);
            col.push_str("...");
        }
        out.push_str(&format!("| {col:>COL_WIDTH$} "));
    }
    out.push_str("|\n");
    out.push_str(&sep);
    for row in rows {
        for v in row {
            let mut col = value_to_str(v);
            if col.len() > COL_WIDTH {
                col.truncate(COL_WIDTH - 3);
                col.push_str("...");
            }
            out.push_str(&format!("| {col:>COL_WIDTH$} "));
        }
        out.push_str("|\n");
    }
    out.push_str(&sep);
    out.push_str(&format!("Total record(s): {}\n", rows.len()));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn select_table_format() {
        let out = Output::Rows {
            columns: vec!["id".into(), "name".into()],
            rows: vec![
                vec![Value::Int(1), Value::Str("alice".into())],
                vec![Value::Int(2), Value::Str("bob".into())],
            ],
        };
        let text = format_output(&out);
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines[0], "+------------------+------------------+");
        assert_eq!(lines[1], "|               id |             name |");
        assert_eq!(lines[2], "+------------------+------------------+");
        assert_eq!(lines[3], "|                1 |            alice |");
        assert_eq!(lines[4], "|                2 |              bob |");
        assert_eq!(lines[5], "+------------------+------------------+");
        assert_eq!(lines.last().unwrap(), &"Total record(s): 2");
    }

    #[test]
    fn empty_set_overrides_everything() {
        let out = Output::Rows {
            columns: vec!["id".into()],
            rows: Vec::new(),
        };
        assert_eq!(format_output(&out), "empty set\n");
    }

    #[test]
    fn floats_use_cpp_format() {
        let out = Output::Rows {
            columns: vec!["score".into()],
            rows: vec![vec![Value::Double(90.5)]],
        };
        assert!(format_output(&out).contains("|        90.500000 |"));
    }

    #[test]
    fn long_values_truncated_with_ellipsis() {
        let out = Output::Rows {
            columns: vec!["name".into()],
            rows: vec![vec![Value::Str("a".repeat(40))]],
        };
        let text = format_output(&out);
        assert!(text.contains("..."));
        assert!(text.contains("Total record(s): 1"));
    }

    #[test]
    fn dml_output_is_empty_text() {
        assert_eq!(format_output(&Output::Count(3)), "");
        assert_eq!(format_output(&Output::Ok), "");
    }
}
