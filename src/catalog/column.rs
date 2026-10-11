//! 列元数据。
//!
//! 对应 C++ 版 `src-cpp/include/catalog/column.h`。

use std::fmt;

use crate::common::errors::EasyDbError;
use crate::types::{AggregationType, ColType};

/// 表中的一列。
///
/// 与 C++ 版的差异：
/// - 五个重载构造函数收敛为 [`Column::new`] / [`Column::new_varlen`]
///   （及对应的带表名变体），定长/变长错配在构造时报错而非 assert；
/// - C++ 中参数赋值写反的 `SetStorageSize`（`length = length_`）为已知
///   bug，不再保留。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Column {
    name: String,
    tab_name: String,
    col_type: ColType,
    /// 存储字节数：定长列为类型宽度，变长列为声明的最大长度
    length: u32,
    /// 元组内字节偏移，由 [`Schema`](crate::catalog::Schema) 构造时统一计算
    offset: u32,
    agg_type: AggregationType,
}

impl Column {
    /// 定长类型列（INT/LONG/FLOAT/DOUBLE/DATE）。
    pub fn new(name: impl Into<String>, col_type: ColType) -> Result<Self, EasyDbError> {
        Self::with_table(name, "", col_type)
    }

    /// 变长类型列（CHAR/VARCHAR），`length` 为最大字节数。
    pub fn new_varlen(
        name: impl Into<String>,
        col_type: ColType,
        length: u32,
    ) -> Result<Self, EasyDbError> {
        Self::with_table_varlen(name, "", col_type, length)
    }

    /// 带表名的定长类型列。
    pub fn with_table(
        name: impl Into<String>,
        tab_name: impl Into<String>,
        col_type: ColType,
    ) -> Result<Self, EasyDbError> {
        let size = col_type.storage_size().ok_or_else(|| {
            EasyDbError::Internal(format!(
                "fixed-size constructor used for non-fixed type {}",
                col_type.as_str()
            ))
        })?;
        Ok(Self {
            name: name.into(),
            tab_name: tab_name.into(),
            col_type,
            length: size,
            offset: 0,
            agg_type: AggregationType::None,
        })
    }

    /// 带表名的变长类型列。
    pub fn with_table_varlen(
        name: impl Into<String>,
        tab_name: impl Into<String>,
        col_type: ColType,
        length: u32,
    ) -> Result<Self, EasyDbError> {
        if col_type.is_inlined() {
            return Err(EasyDbError::Internal(format!(
                "varlen constructor used for fixed-size type {}",
                col_type.as_str()
            )));
        }
        Ok(Self {
            name: name.into(),
            tab_name: tab_name.into(),
            col_type,
            length,
            offset: 0,
            agg_type: AggregationType::None,
        })
    }

    /// 复制本列并改名（对应 C++ `WithColumnName`，用于投影/聚合列重命名）。
    pub fn with_name(&self, name: impl Into<String>) -> Self {
        let mut col = self.clone();
        col.name = name.into();
        col
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn set_name(&mut self, name: impl Into<String>) {
        self.name = name.into();
    }

    pub fn tab_name(&self) -> &str {
        &self.tab_name
    }

    pub fn set_tab_name(&mut self, tab_name: impl Into<String>) {
        self.tab_name = tab_name.into();
    }

    pub fn col_type(&self) -> ColType {
        self.col_type
    }

    /// 存储字节数（定长为类型宽度，变长为声明长度）。
    pub fn storage_size(&self) -> u32 {
        self.length
    }

    /// 元组内字节偏移。
    pub fn offset(&self) -> u32 {
        self.offset
    }

    /// 设置元组内字节偏移（通常只由 [`Schema`](crate::catalog::Schema) 调用）。
    pub fn set_offset(&mut self, offset: u32) {
        self.offset = offset;
    }

    /// 偏移量增减（可负），溢出报错。
    pub fn add_offset(&mut self, off: i32) -> Result<(), EasyDbError> {
        self.offset = self
            .offset
            .checked_add_signed(off)
            .ok_or_else(|| EasyDbError::Internal("column offset overflow".to_string()))?;
        Ok(())
    }

    pub fn agg_type(&self) -> AggregationType {
        self.agg_type
    }

    pub fn set_agg_type(&mut self, agg_type: AggregationType) {
        self.agg_type = agg_type;
    }

    /// 是否内联存储（非变长类型）。
    pub fn is_inlined(&self) -> bool {
        self.col_type.is_inlined()
    }
}

impl fmt::Display for Column {
    /// 与 C++ 版 `ToString(simplified=true)` 一致：`name:TYPE`，变长带长度。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.col_type.is_inlined() {
            write!(f, "{}:{}", self.name, self.col_type.as_str())
        } else {
            write!(
                f,
                "{}:{}({})",
                self.name,
                self.col_type.as_str(),
                self.length
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_column_storage_size() {
        let col = Column::new("id", ColType::Int).unwrap();
        assert_eq!(col.storage_size(), 4);
        assert!(col.is_inlined());
        assert_eq!(col.agg_type(), AggregationType::None);
    }

    #[test]
    fn varlen_column_keeps_declared_length() {
        let col = Column::new_varlen("name", ColType::VarChar, 50).unwrap();
        assert_eq!(col.storage_size(), 50);
        assert!(!col.is_inlined());
    }

    #[test]
    fn constructor_type_mismatch_errors() {
        // 变长类型不能用定长构造
        assert!(Column::new("name", ColType::VarChar).is_err());
        // 定长类型不能用变长构造
        assert!(Column::new_varlen("id", ColType::Int, 4).is_err());
        assert!(Column::new("e", ColType::Empty).is_err());
    }

    #[test]
    fn with_name_copies_metadata() {
        let mut col = Column::with_table("id", "student", ColType::Long).unwrap();
        col.set_offset(8);
        let renamed = col.with_name("student.id");
        assert_eq!(renamed.name(), "student.id");
        assert_eq!(renamed.tab_name(), "student");
        assert_eq!(renamed.col_type(), ColType::Long);
        assert_eq!(renamed.offset(), 8);
    }

    #[test]
    fn add_offset_rejects_overflow() {
        let mut col = Column::new("id", ColType::Int).unwrap();
        col.set_offset(u32::MAX);
        assert!(col.add_offset(1).is_err());
    }

    #[test]
    fn display_simplified_format() {
        assert_eq!(
            Column::new("id", ColType::Int).unwrap().to_string(),
            "id:INT"
        );
        assert_eq!(
            Column::new_varlen("name", ColType::VarChar, 50)
                .unwrap()
                .to_string(),
            "name:VARCHAR(50)"
        );
    }
}
