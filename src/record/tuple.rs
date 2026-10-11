//! 元组编码/解码：`Vec<Value>` 与页内字节流的互转。
//!
//! 对应 C++ 版 `src-cpp/storage/table/tuple.cpp` 的
//! `Tuple(values, schema)` 构造与 `GetValue/SerializeTo` 体系。
//!
//! 布局：内联列按 [`Schema`] 计算的偏移直接内嵌；变长列（CHAR/VARCHAR）
//! 在列偏移处写 4 字节的相对偏移，实际数据以 `u32 长度 + 字节` 追加在
//! 内联区之后（与 C++ 版一致）。

use crate::catalog::Schema;
use crate::common::errors::EasyDbError;
use crate::types::Value;

/// 按模式把一组值编码为元组字节流。
pub fn encode_tuple(schema: &Schema, values: &[Value]) -> Result<Vec<u8>, EasyDbError> {
    if values.len() != schema.column_count() as usize {
        return Err(EasyDbError::InvalidValueCount);
    }

    // 变长列的数据长度，用于计算总大小
    fn str_bytes(v: &Value) -> Result<&[u8], EasyDbError> {
        match v {
            Value::Str(s) => Ok(s.as_bytes()),
            other => Err(EasyDbError::IncompatibleType(
                "VARCHAR".to_string(),
                other.col_type().as_str().to_string(),
            )),
        }
    }

    let inlined = schema.inlined_storage_size() as usize;
    let mut total = inlined;
    for &i in schema.unlined_columns() {
        total += 4 + str_bytes(&values[i as usize])?.len();
    }
    let mut buf = vec![0u8; total];

    for (i, col) in schema.columns().iter().enumerate() {
        let v = &values[i];
        let off = col.offset() as usize;
        match col.col_type() {
            crate::types::ColType::Int => {
                let Value::Int(n) = v else {
                    return Err(incompatible(col.name(), v));
                };
                buf[off..off + 4].copy_from_slice(&n.to_le_bytes());
            }
            crate::types::ColType::Long => {
                let Value::Long(n) = v else {
                    return Err(incompatible(col.name(), v));
                };
                buf[off..off + 8].copy_from_slice(&n.to_le_bytes());
            }
            crate::types::ColType::Float | crate::types::ColType::Double => {
                let n = match v {
                    Value::Float(x) | Value::Double(x) => *x,
                    other => return Err(incompatible(col.name(), other)),
                };
                buf[off..off + 8].copy_from_slice(&n.to_le_bytes());
            }
            crate::types::ColType::Date => {
                let Value::Date(n) = v else {
                    return Err(incompatible(col.name(), v));
                };
                buf[off..off + 8].copy_from_slice(&n.to_le_bytes());
            }
            crate::types::ColType::Char | crate::types::ColType::VarChar => {
                let bytes = str_bytes(v)?;
                let tail = varlen_region_offset(schema, values, i)?;
                buf[off..off + 4].copy_from_slice(&(tail as u32).to_le_bytes());
                buf[tail..tail + 4].copy_from_slice(&(bytes.len() as u32).to_le_bytes());
                buf[tail + 4..tail + 4 + bytes.len()].copy_from_slice(bytes);
            }
            crate::types::ColType::Empty => {
                return Err(EasyDbError::Internal(format!(
                    "column {} has EMPTY type",
                    col.name()
                )));
            }
        }
    }
    Ok(buf)
}

fn incompatible(col: &str, v: &Value) -> EasyDbError {
    EasyDbError::IncompatibleType(col.to_string(), v.col_type().as_str().to_string())
}

/// 第 `idx` 个列的变长数据区在元组内的起始偏移：
/// 内联区 + 之前所有变长列的 (4 + len) 之和。
fn varlen_region_offset(
    schema: &Schema,
    values: &[Value],
    idx: usize,
) -> Result<usize, EasyDbError> {
    let mut off = schema.inlined_storage_size() as usize;
    for &c in schema.unlined_columns() {
        let c = c as usize;
        let len = match &values[c] {
            Value::Str(s) => s.len(),
            _ => {
                return Err(EasyDbError::Internal(
                    "non-string value in varlen column".to_string(),
                ));
            }
        };
        if c == idx {
            return Ok(off);
        }
        off += 4 + len;
    }
    Err(EasyDbError::Internal("column is not varlen".to_string()))
}

/// 按模式把元组字节流解码为一组值。
pub fn decode_tuple(schema: &Schema, data: &[u8]) -> Result<Vec<Value>, EasyDbError> {
    let inlined = schema.inlined_storage_size() as usize;
    if data.len() < inlined {
        return Err(EasyDbError::Internal(format!(
            "tuple data too short: {} < inlined {}",
            data.len(),
            inlined
        )));
    }

    let mut values = Vec::with_capacity(schema.column_count() as usize);
    for col in schema.columns() {
        let off = col.offset() as usize;
        let v = match col.col_type() {
            crate::types::ColType::Int => {
                Value::Int(i32::from_le_bytes(data[off..off + 4].try_into().unwrap()))
            }
            crate::types::ColType::Long => {
                Value::Long(i64::from_le_bytes(data[off..off + 8].try_into().unwrap()))
            }
            crate::types::ColType::Float | crate::types::ColType::Double => {
                Value::Double(f64::from_le_bytes(data[off..off + 8].try_into().unwrap()))
            }
            crate::types::ColType::Date => {
                Value::Date(i64::from_le_bytes(data[off..off + 8].try_into().unwrap()))
            }
            crate::types::ColType::Char | crate::types::ColType::VarChar => {
                let rel = u32::from_le_bytes(data[off..off + 4].try_into().unwrap()) as usize;
                if rel + 4 > data.len() {
                    return Err(EasyDbError::Internal(
                        "varlen offset out of range".to_string(),
                    ));
                }
                let len = u32::from_le_bytes(data[rel..rel + 4].try_into().unwrap()) as usize;
                if rel + 4 + len > data.len() {
                    return Err(EasyDbError::Internal(
                        "varlen length out of range".to_string(),
                    ));
                }
                let s = std::str::from_utf8(&data[rel + 4..rel + 4 + len])
                    .map_err(|e| EasyDbError::Internal(format!("invalid utf-8 in varchar: {e}")))?;
                Value::Str(s.to_string())
            }
            crate::types::ColType::Empty => {
                return Err(EasyDbError::Internal("column has EMPTY type".to_string()));
            }
        };
        values.push(v);
    }
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Column;
    use crate::types::ColType;

    fn sample_schema() -> Schema {
        Schema::new(vec![
            Column::new("id", ColType::Int).unwrap(),
            Column::new_varlen("name", ColType::VarChar, 50).unwrap(),
            Column::new("score", ColType::Double).unwrap(),
            Column::new("ts", ColType::Date).unwrap(),
        ])
    }

    #[test]
    fn encode_decode_roundtrip() {
        let schema = sample_schema();
        let values = vec![
            Value::Int(7),
            Value::Str("alice".to_string()),
            Value::Double(3.25),
            Value::Date(20240101),
        ];
        let bytes = encode_tuple(&schema, &values).unwrap();
        // 内联：4 + 4 + 8 + 8 = 24，变长：4 + 5 = 9 → 33
        assert_eq!(bytes.len(), 33);
        let decoded = decode_tuple(&schema, &bytes).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn multiple_varlen_columns() {
        let schema = Schema::new(vec![
            Column::new_varlen("a", ColType::VarChar, 10).unwrap(),
            Column::new_varlen("b", ColType::VarChar, 10).unwrap(),
        ]);
        let values = vec![Value::Str("hello".into()), Value::Str("hi".into())];
        let bytes = encode_tuple(&schema, &values).unwrap();
        // 8 内联 + (4+5) + (4+2) = 23
        assert_eq!(bytes.len(), 23);
        assert_eq!(decode_tuple(&schema, &bytes).unwrap(), values);
    }

    #[test]
    fn wrong_value_count_and_type_error() {
        let schema = sample_schema();
        assert!(matches!(
            encode_tuple(&schema, &[Value::Int(1)]),
            Err(EasyDbError::InvalidValueCount)
        ));
        let bad = vec![
            Value::Str("x".into()),
            Value::Str("x".into()),
            Value::Double(1.0),
            Value::Date(0),
        ];
        assert!(matches!(
            encode_tuple(&schema, &bad),
            Err(EasyDbError::IncompatibleType(_, _))
        ));
    }

    #[test]
    fn empty_string_roundtrip() {
        let schema = Schema::new(vec![Column::new_varlen("s", ColType::VarChar, 5).unwrap()]);
        let values = vec![Value::Str(String::new())];
        let bytes = encode_tuple(&schema, &values).unwrap();
        assert_eq!(bytes.len(), 8); // 4 指针位 + 4 长度
        assert_eq!(decode_tuple(&schema, &bytes).unwrap(), values);
    }
}
