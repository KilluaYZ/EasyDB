//! 列类型与聚合类型。
//!
//! C++ 中 `ColType` 在 `type/type_id.h` 与 `defs.h` 里定义了两份，此处收敛为
//! 唯一定义；存储宽度与 C++ 版 `Column::TypeSize` 保持一致。

/// SQL 列数据类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum ColType {
    /// 空类型（对应 C++ TYPE_EMPTY）
    Empty,
    /// 32 位整数
    Int,
    /// 64 位整数
    Long,
    /// 单精度浮点（存储为 f64，与 C++ 版一致）
    Float,
    /// 双精度浮点
    Double,
    /// 定长字符串
    Char,
    /// 变长字符串
    VarChar,
    /// 日期（i64 编码的时间戳）
    Date,
}

impl ColType {
    /// 定长类型的存储字节数；变长类型（CHAR/VARCHAR）与空类型返回 `None`，
    /// 变长列的实际宽度由列定义给出。
    pub const fn storage_size(self) -> Option<u32> {
        match self {
            ColType::Int => Some(4),
            ColType::Long | ColType::Float | ColType::Double | ColType::Date => Some(8),
            ColType::Char | ColType::VarChar | ColType::Empty => None,
        }
    }

    /// 是否内联存储在元组中（非变长类型）。
    pub const fn is_inlined(self) -> bool {
        !matches!(self, ColType::Char | ColType::VarChar)
    }

    /// 类型名称，与 C++ 版 `coltype2str` 一致。
    pub const fn as_str(self) -> &'static str {
        match self {
            ColType::Empty => "EMPTY",
            ColType::Int => "INT",
            ColType::Long => "LONG",
            ColType::Float => "FLOAT",
            ColType::Double => "DOUBLE",
            ColType::Char => "CHAR",
            ColType::VarChar => "VARCHAR",
            ColType::Date => "DATE",
        }
    }
}

/// 聚合类型，用于 SELECT 聚合查询。
///
/// 对应 C++ `defs.h` 的 `AggregationType`。
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Default, serde::Serialize, serde::Deserialize,
)]
pub enum AggregationType {
    Max,
    Min,
    Count,
    Sum,
    /// 非聚合列
    #[default]
    None,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_sizes_match_cpp() {
        assert_eq!(ColType::Int.storage_size(), Some(4));
        assert_eq!(ColType::Long.storage_size(), Some(8));
        assert_eq!(ColType::Float.storage_size(), Some(8));
        assert_eq!(ColType::Double.storage_size(), Some(8));
        assert_eq!(ColType::Date.storage_size(), Some(8));
        assert_eq!(ColType::VarChar.storage_size(), None);
        assert_eq!(ColType::Char.storage_size(), None);
    }

    #[test]
    fn varlen_types_are_not_inlined() {
        assert!(ColType::Int.is_inlined());
        assert!(ColType::Date.is_inlined());
        assert!(!ColType::VarChar.is_inlined());
        assert!(!ColType::Char.is_inlined());
    }

    #[test]
    fn type_names_match_cpp() {
        assert_eq!(ColType::Int.as_str(), "INT");
        assert_eq!(ColType::VarChar.as_str(), "VARCHAR");
        assert_eq!(ColType::Empty.as_str(), "EMPTY");
    }
}
