//! 模式：一组列及其元组布局。
//!
//! 对应 C++ 版 `src-cpp/catalog/schema.cpp`。

use std::fmt;

use crate::catalog::Column;
use crate::common::errors::EasyDbError;

/// 变长列在元组内占用的“指针位”宽度（与 C++ 版布局一致：元组内只存
/// 4 字节的长度/偏移信息，实际数据追加在元组尾部）。
const VARLEN_SLOT_SIZE: u32 = 4;

/// 表或查询结果的列结构。
///
/// 构造时统一计算各列在元组内的偏移：内联列推进其存储宽度，变长列只占
/// 4 字节指针位；追加列（`append`）后会重新布局。
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Schema {
    columns: Vec<Column>,
    /// 内联部分的字节数
    length: u32,
    /// 非内联（变长）列的下标
    unlined_columns: Vec<u32>,
}

impl Schema {
    /// 由列集合构造模式，并统一计算各列偏移。
    pub fn new(columns: Vec<Column>) -> Self {
        let mut schema = Self {
            columns,
            length: 0,
            unlined_columns: Vec::new(),
        };
        schema.relayout();
        schema
    }

    /// 从源模式中按列下标投影出新模式（对应 C++ `CopySchema`），
    /// 用于查询计划中的投影操作。偏移按新布局重新计算。
    pub fn copy_projection(from: &Schema, attrs: &[u32]) -> Schema {
        let columns = attrs
            .iter()
            .map(|&i| from.columns[i as usize].clone())
            .collect();
        Schema::new(columns)
    }

    /// 重新计算所有列的偏移、内联长度与变长列下标。
    fn relayout(&mut self) {
        let mut offset = 0;
        self.unlined_columns.clear();
        for (idx, col) in self.columns.iter_mut().enumerate() {
            col.set_offset(offset);
            if col.is_inlined() {
                offset += col.storage_size();
            } else {
                self.unlined_columns.push(idx as u32);
                offset += VARLEN_SLOT_SIZE;
            }
        }
        self.length = offset;
    }

    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    /// 按下标取列（越界 panic，属于内部不变量破坏）。
    pub fn column(&self, idx: u32) -> &Column {
        &self.columns[idx as usize]
    }

    /// 按列名查找第一处匹配的下标；不存在返回 `None`（多列同名时取第一个，
    /// 与 C++ 版一致；歧义检查由语义分析阶段的 `AmbiguousColumn` 负责）。
    pub fn try_col_idx(&self, name: &str) -> Option<u32> {
        self.columns
            .iter()
            .position(|col| col.name() == name)
            .map(|i| i as u32)
    }

    /// 按列名查找下标，不存在返回 [`EasyDbError::ColumnNotFound`]。
    pub fn col_idx(&self, name: &str) -> Result<u32, EasyDbError> {
        self.try_col_idx(name)
            .ok_or_else(|| EasyDbError::ColumnNotFound(name.to_string()))
    }

    pub fn column_count(&self) -> u32 {
        self.columns.len() as u32
    }

    /// 元组内联部分的字节数（不含变长列的实际数据）。
    pub fn inlined_storage_size(&self) -> u32 {
        self.length
    }

    /// 是否所有列均内联。
    pub fn is_inlined(&self) -> bool {
        self.unlined_columns.is_empty()
    }

    /// 非内联（变长）列的下标列表。
    pub fn unlined_columns(&self) -> &[u32] {
        &self.unlined_columns
    }

    /// 元组的物理大小：内联部分 + 变长列的实际数据（按声明上限估算），
    /// 计算口径与 C++ 版 `SetPhysicalSize` 一致。
    pub fn physical_size(&self) -> u32 {
        let mut size = self.length + VARLEN_SLOT_SIZE;
        for col in &self.columns {
            if !col.is_inlined() {
                size += col.storage_size() + VARLEN_SLOT_SIZE;
            }
        }
        size
    }

    /// 追加列并重新布局（用于 JOIN 合并两侧模式）。
    ///
    /// 注：C++ 版 `Schema::Append` 追加后不重算偏移（沿用各列在自己元组中
    /// 的旧偏移，属于潜在 bug）；此处追加后统一重算。
    pub fn append(&mut self, app: impl IntoIterator<Item = Column>) {
        self.columns.extend(app);
        self.relayout();
    }
}

impl fmt::Display for Schema {
    /// 与 C++ 版 `ToString(simplified=true)` 一致：`(id:INT, name:VARCHAR(50))`。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("(")?;
        for (i, col) in self.columns.iter().enumerate() {
            if i > 0 {
                f.write_str(", ")?;
            }
            write!(f, "{col}")?;
        }
        f.write_str(")")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ColType;

    fn sample_columns() -> Vec<Column> {
        vec![
            Column::new("id", ColType::Int).unwrap(),
            Column::new_varlen("name", ColType::VarChar, 50).unwrap(),
            Column::new("age", ColType::Long).unwrap(),
        ]
    }

    #[test]
    fn offsets_follow_cpp_layout() {
        let schema = Schema::new(sample_columns());
        // Int@0（4字节），VarChar@4（4字节指针位），Long@8（8字节）
        assert_eq!(schema.column(0).offset(), 0);
        assert_eq!(schema.column(1).offset(), 4);
        assert_eq!(schema.column(2).offset(), 8);
        assert_eq!(schema.inlined_storage_size(), 16);
        assert_eq!(schema.unlined_columns(), &[1]);
        assert!(!schema.is_inlined());
    }

    #[test]
    fn all_inlined_schema() {
        let schema = Schema::new(vec![
            Column::new("id", ColType::Int).unwrap(),
            Column::new("ts", ColType::Date).unwrap(),
        ]);
        assert_eq!(schema.column(1).offset(), 4);
        assert_eq!(schema.inlined_storage_size(), 12);
        assert!(schema.is_inlined());
        assert!(schema.unlined_columns().is_empty());
    }

    #[test]
    fn col_lookup() {
        let schema = Schema::new(sample_columns());
        assert_eq!(schema.try_col_idx("name"), Some(1));
        assert_eq!(schema.col_idx("age").unwrap(), 2);
        assert!(matches!(
            schema.col_idx("nope"),
            Err(EasyDbError::ColumnNotFound(_))
        ));
    }

    #[test]
    fn projection_recomputes_offsets() {
        let schema = Schema::new(sample_columns());
        let proj = Schema::copy_projection(&schema, &[2, 0]);
        assert_eq!(proj.column_count(), 2);
        assert_eq!(proj.column(0).name(), "age");
        assert_eq!(proj.column(0).offset(), 0);
        assert_eq!(proj.column(1).name(), "id");
        assert_eq!(proj.column(1).offset(), 8);
        assert_eq!(proj.inlined_storage_size(), 12);
    }

    #[test]
    fn append_relayouts() {
        let mut schema = Schema::new(vec![Column::new("id", ColType::Int).unwrap()]);
        schema.append([Column::new("score", ColType::Double).unwrap()]);
        assert_eq!(schema.column_count(), 2);
        assert_eq!(schema.column(1).name(), "score");
        assert_eq!(schema.column(1).offset(), 4);
        assert_eq!(schema.inlined_storage_size(), 12);
    }

    #[test]
    fn physical_size_includes_varlen_data() {
        let schema = Schema::new(sample_columns());
        // 内联 16 + 4 + 变长数据 50 + 4 = 74
        assert_eq!(schema.physical_size(), 74);
    }

    #[test]
    fn display_simplified_format() {
        let schema = Schema::new(sample_columns());
        assert_eq!(schema.to_string(), "(id:INT, name:VARCHAR(50), age:LONG)");
    }
}
