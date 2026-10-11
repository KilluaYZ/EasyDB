//! 索引相关定义：常量、Iid、文件头、键比较器与键编码。
//!
//! 对应 C++ 版 `src-cpp/include/storage/index/ix_defs.h`。
//!
//! 与 C++ 版的差异：
//! - 没有"虚拟叶子链表头页"：叶子双向链表以 `IX_NO_PAGE` 为边界锚点，
//!   文件第 1 页即初始根节点（C++ 占了第 1 页做链表头）；
//! - FLOAT/DOUBLE 键统一按 f64 编码（C++ 的 FLOAT 键是 4 字节 f32）；
//! - 文件头增加空闲页链表头 `first_free`，被删除的节点页可复用
//!   （C++ 版释放的页永不复用）。

use std::cmp::Ordering;

use crate::common::config::PAGE_SIZE;
use crate::common::errors::EasyDbError;
use crate::types::ColType;

/// 无效页号
pub const IX_NO_PAGE: i32 = -1;
/// 索引文件头所在页号
pub const IX_FILE_HDR_PAGE: i32 = 0;
/// 初始根节点页号（文件头之后第一页）
pub const IX_INIT_ROOT_PAGE: i32 = 1;
/// 初始页数（文件头 + 根节点）
pub const IX_INIT_NUM_PAGES: i32 = 2;
/// 索引列的最大总长度（字节）
pub const IX_MAX_COL_LEN: usize = 512;

/// 节点页头大小：
/// `| next_free(4) | lsn(4) | is_leaf(1)+pad(3) | num_key(2)+pad(2) | parent(4) | prev_leaf(4) | next_leaf(4) |`
pub const IX_NODE_HDR_SIZE: usize = 28;
/// 节点内 RID 的编码宽度（Rid::encoded 的 i64）
pub const IX_RID_SIZE: usize = 8;

// ---- 节点页头内部偏移 ----
pub(super) const OFF_NEXT_FREE: usize = 0;
pub(super) const OFF_IS_LEAF: usize = 8;
pub(super) const OFF_NUM_KEYS: usize = 12;
pub(super) const OFF_PARENT: usize = 16;
pub(super) const OFF_PREV_LEAF: usize = 20;
pub(super) const OFF_NEXT_LEAF: usize = 24;

/// 由键总长推导节点最大键数（= C++ 的 btree_order + 1）。
pub const fn max_keys_for(col_tot_len: usize) -> usize {
    (PAGE_SIZE - IX_NODE_HDR_SIZE) / (col_tot_len + IX_RID_SIZE)
}

/// 索引内部位置标识：节点页号 + 槽位（对应 C++ `Iid`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Iid {
    pub page_no: i32,
    pub slot: u32,
}

impl Iid {
    pub const fn new(page_no: i32, slot: u32) -> Self {
        Self { page_no, slot }
    }
}

/// B+ 树索引文件头（存储在第 0 页开头）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IxFileHdr {
    /// 已分配页数（高水位，含文件头页；删除的页进入空闲链表复用）
    pub num_pages: i32,
    /// 根节点页号
    pub root_page: i32,
    /// 第一个叶子页号
    pub first_leaf: i32,
    /// 最后一个叶子页号
    pub last_leaf: i32,
    /// 空闲页链表头（[`IX_NO_PAGE`] 表示无空闲页）
    pub first_free: i32,
    /// 索引列类型
    pub col_types: Vec<ColType>,
    /// 索引列宽度（字节）
    pub col_lens: Vec<i32>,
}

impl IxFileHdr {
    /// 键总长（各列宽度之和）。
    pub fn col_tot_len(&self) -> usize {
        self.col_lens.iter().map(|&l| l as usize).sum()
    }

    /// 节点最大键数（分裂阈值 = C++ `btree_order_ + 1`）。
    pub fn max_keys(&self) -> usize {
        max_keys_for(self.col_tot_len())
    }

    /// 节点最小键数（非根节点的下限 = C++ `GetMinSize`）。
    pub fn min_keys(&self) -> usize {
        self.max_keys() / 2
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(24 + self.col_types.len() * 8);
        b.extend_from_slice(&self.num_pages.to_le_bytes());
        b.extend_from_slice(&self.root_page.to_le_bytes());
        b.extend_from_slice(&self.first_leaf.to_le_bytes());
        b.extend_from_slice(&self.last_leaf.to_le_bytes());
        b.extend_from_slice(&self.first_free.to_le_bytes());
        b.extend_from_slice(&(self.col_types.len() as i32).to_le_bytes());
        for t in &self.col_types {
            // ColType 按 as_str 之外的稳定序号编码：用 std::mem::discriminant
            // 不可序列化，这里直接使用定义顺序的序号
            b.extend_from_slice(&(col_type_ord(*t) as i32).to_le_bytes());
        }
        for l in &self.col_lens {
            b.extend_from_slice(&l.to_le_bytes());
        }
        b
    }

    pub fn from_bytes(src: &[u8]) -> Result<Self, EasyDbError> {
        if src.len() < 24 {
            return Err(EasyDbError::Internal("index file header too short".into()));
        }
        let i32_at = |off: usize| i32::from_le_bytes(src[off..off + 4].try_into().unwrap());
        let num_pages = i32_at(0);
        let root_page = i32_at(4);
        let first_leaf = i32_at(8);
        let last_leaf = i32_at(12);
        let first_free = i32_at(16);
        let col_num = i32_at(20) as usize;
        let need = 24 + col_num * 8;
        if src.len() < need {
            return Err(EasyDbError::Internal("index file header truncated".into()));
        }
        let mut col_types = Vec::with_capacity(col_num);
        for i in 0..col_num {
            let ord = i32_at(24 + i * 4);
            col_types.push(col_type_from_ord(ord));
        }
        let mut col_lens = Vec::with_capacity(col_num);
        for i in 0..col_num {
            col_lens.push(i32_at(24 + col_num * 4 + i * 4));
        }
        Ok(Self {
            num_pages,
            root_page,
            first_leaf,
            last_leaf,
            first_free,
            col_types,
            col_lens,
        })
    }
}

/// ColType 的稳定文件序号（跨版本持久化用）。
fn col_type_ord(t: ColType) -> u8 {
    match t {
        ColType::Empty => 0,
        ColType::Int => 1,
        ColType::Long => 2,
        ColType::Float => 3,
        ColType::Double => 4,
        ColType::Char => 5,
        ColType::VarChar => 6,
        ColType::Date => 7,
    }
}

fn col_type_from_ord(ord: i32) -> ColType {
    match ord {
        1 => ColType::Int,
        2 => ColType::Long,
        3 => ColType::Float,
        4 => ColType::Double,
        5 => ColType::Char,
        6 => ColType::VarChar,
        7 => ColType::Date,
        _ => ColType::Empty,
    }
}

/// 键比较器：按键列的 类型/宽度 逐列比较（对应 C++ `ix_compare`）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyComparator {
    col_types: Vec<ColType>,
    col_lens: Vec<usize>,
}

impl KeyComparator {
    pub fn new(col_types: Vec<ColType>, col_lens: Vec<usize>) -> Self {
        debug_assert_eq!(col_types.len(), col_lens.len());
        Self {
            col_types,
            col_lens,
        }
    }

    pub fn from_hdr(hdr: &IxFileHdr) -> Self {
        Self::new(
            hdr.col_types.clone(),
            hdr.col_lens.iter().map(|&l| l as usize).collect(),
        )
    }

    pub fn col_tot_len(&self) -> usize {
        self.col_lens.iter().sum()
    }

    /// 比较两个定宽键（长度必须等于 col_tot_len）。
    pub fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        let mut off = 0usize;
        for (t, &len) in self.col_types.iter().zip(&self.col_lens) {
            let (ca, cb) = (&a[off..off + len], &b[off..off + len]);
            let ord = match t {
                ColType::Int => i32::from_le_bytes(ca.try_into().unwrap())
                    .cmp(&i32::from_le_bytes(cb.try_into().unwrap())),
                ColType::Long | ColType::Date => i64::from_le_bytes(ca.try_into().unwrap())
                    .cmp(&i64::from_le_bytes(cb.try_into().unwrap())),
                ColType::Float | ColType::Double => {
                    let fa = f64::from_le_bytes(ca.try_into().unwrap());
                    let fb = f64::from_le_bytes(cb.try_into().unwrap());
                    // NaN 归约为相等（与 Value::compare 口径一致）
                    fa.partial_cmp(&fb).unwrap_or(Ordering::Equal)
                }
                ColType::Char | ColType::VarChar => ca.cmp(cb),
                ColType::Empty => Ordering::Equal,
            };
            if ord != Ordering::Equal {
                return ord;
            }
            off += len;
        }
        Ordering::Equal
    }
}

/// 把一个 [`Value`] 按列定义编码进键缓冲的指定偏移
/// （数值 LE 定宽；字符串右补 0 到列宽，超长报错）。
pub fn encode_key_column(
    buf: &mut [u8],
    off: usize,
    col_type: ColType,
    col_len: usize,
    v: &crate::types::Value,
) -> Result<(), EasyDbError> {
    use crate::types::Value;
    fn take(buf: &mut [u8], off: usize, n: usize) -> Result<&mut [u8], EasyDbError> {
        if off + n > buf.len() {
            return Err(EasyDbError::Internal("key buffer too small".into()));
        }
        Ok(&mut buf[off..off + n])
    }
    match (col_type, v) {
        (ColType::Int, Value::Int(n)) => take(buf, off, 4)?.copy_from_slice(&n.to_le_bytes()),
        (ColType::Long, Value::Long(n)) | (ColType::Date, Value::Date(n)) => {
            take(buf, off, 8)?.copy_from_slice(&n.to_le_bytes())
        }
        (ColType::Float, Value::Float(n)) | (ColType::Double, Value::Double(n)) => {
            take(buf, off, 8)?.copy_from_slice(&n.to_le_bytes())
        }
        (ColType::Char, Value::Str(s)) | (ColType::VarChar, Value::Str(s)) => {
            let bytes = s.as_bytes();
            if bytes.len() > col_len {
                return Err(EasyDbError::StringOverflow);
            }
            let slot = take(buf, off, col_len)?;
            slot[..bytes.len()].copy_from_slice(bytes);
            slot[bytes.len()..].fill(0);
        }
        (t, other) => {
            return Err(EasyDbError::IncompatibleType(
                t.as_str().to_string(),
                other.col_type().as_str().to_string(),
            ));
        }
    }
    Ok(())
}

/// 按 `col_types/col_lens` 把一组值编码为完整索引键。
pub fn encode_key(
    col_types: &[ColType],
    col_lens: &[usize],
    values: &[crate::types::Value],
) -> Result<Vec<u8>, EasyDbError> {
    if values.len() != col_types.len() {
        return Err(EasyDbError::InvalidValueCount);
    }
    let total: usize = col_lens.iter().sum();
    let mut buf = vec![0u8; total];
    let mut off = 0usize;
    for (i, v) in values.iter().enumerate() {
        encode_key_column(&mut buf, off, col_types[i], col_lens[i], v)?;
        off += col_lens[i];
    }
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_hdr_roundtrip() {
        let hdr = IxFileHdr {
            num_pages: 9,
            root_page: 3,
            first_leaf: 2,
            last_leaf: 8,
            first_free: IX_NO_PAGE,
            col_types: vec![ColType::Int, ColType::VarChar],
            col_lens: vec![4, 32],
        };
        let bytes = hdr.to_bytes();
        assert_eq!(IxFileHdr::from_bytes(&bytes).unwrap(), hdr);
    }

    #[test]
    fn max_keys_formula() {
        // 与 C++ 公式同构：(PAGE_SIZE - 头) / (键长 + RID) 取整
        assert_eq!(max_keys_for(4), (4096 - 28) / (4 + 8));
        assert!(max_keys_for(4) > 100);
        assert!(max_keys_for(508) < 10); // 超宽键 → 小阶数，便于压测分裂/合并
    }

    #[test]
    fn comparator_orders_by_type() {
        let cmp = KeyComparator::new(vec![ColType::Int], vec![4]);
        let neg1 = (-1_i32).to_le_bytes();
        let one = 1_i32.to_le_bytes();
        assert_eq!(cmp.compare(&neg1, &one), Ordering::Less);

        let cmp = KeyComparator::new(vec![ColType::VarChar], vec![4]);
        assert_eq!(cmp.compare(b"abc\0", b"abd\0"), Ordering::Less);
        assert_eq!(cmp.compare(b"abd\0", b"abc\0"), Ordering::Greater);
        assert_eq!(cmp.compare(b"abc\0", b"abc\0"), Ordering::Equal);

        // 复合键：先比第一列
        let cmp = KeyComparator::new(vec![ColType::Int, ColType::Int], vec![4, 4]);
        let a = [1_i32.to_le_bytes(), 5_i32.to_le_bytes()].concat();
        let b = [2_i32.to_le_bytes(), 0_i32.to_le_bytes()].concat();
        assert_eq!(cmp.compare(&a, &b), Ordering::Less);
        assert_eq!(cmp.compare(&a, &a), Ordering::Equal);
    }

    #[test]
    fn encode_key_layout() {
        let key = encode_key(
            &[ColType::Int, ColType::VarChar],
            &[4, 8],
            &[
                crate::types::Value::Int(-2),
                crate::types::Value::Str("ab".into()),
            ],
        )
        .unwrap();
        assert_eq!(key.len(), 12);
        assert_eq!(&key[..4], &(-2_i32).to_le_bytes());
        assert_eq!(&key[4..6], b"ab");
        assert!(key[6..].iter().all(|&b| b == 0));
        // 超长字符串报 StringOverflow
        assert!(matches!(
            encode_key(
                &[ColType::VarChar],
                &[2],
                &[crate::types::Value::Str("abc".into())]
            ),
            Err(EasyDbError::StringOverflow)
        ));
    }
}
