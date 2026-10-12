//! 记录管理的数据布局定义。
//!
//! 对应 C++ 版 `src-cpp/include/record/rm_defs.h` 与
//! `src-cpp/include/record/rm_file_handle.h` 中的页头/槽目录结构。
//!
//! # 页面布局（slotted page，元组从页尾向前生长）
//!
//! ```text
//! | 通用页头 8B | Rm 页头 8B | 槽目录 TupleInfo × N | ... 空闲 ... | 元组数据 |
//! ```
//!
//! 与 C++ 版的 C 结构体布局逐字节一致（`TupleInfo` 为 24 字节对齐布局），
//! 便于移植比对；磁盘格式本身不承诺与 C++ 版兼容。

use crate::storage::page::{OFFSET_PAGE_HDR, get_lsn, set_lsn};

/// 无页号
pub const RM_NO_PAGE: i32 = -1;
/// 文件头所在页号（第 0 页）
pub const RM_FILE_HDR_PAGE: i32 = 0;
/// 第一个数据页的页号
pub const RM_FIRST_RECORD_PAGE: i32 = 1;

// ---- 页内偏移（基于通用页头之后的区域）----

/// Rm 页头 @8：下一空闲/链接页号（i32）
pub const OFFSET_NEXT_PAGE_ID: usize = OFFSET_PAGE_HDR; // 8
/// Rm 页头 @12：当前页记录数（u16，只增不减）
pub const OFFSET_NUM_RECORDS: usize = OFFSET_PAGE_HDR + 4; // 12
/// Rm 页头 @14：当前页已删除记录数（u16）
pub const OFFSET_NUM_DELETED: usize = OFFSET_PAGE_HDR + 6; // 14
/// Rm 页头大小：4 + 2 + 2 = 8 字节
pub const RM_PAGE_HDR_SIZE: usize = 8;
/// 通用页头 + Rm 页头
pub const TABLE_PAGE_HEADER_SIZE: usize = OFFSET_PAGE_HDR + RM_PAGE_HDR_SIZE; // 16

// ---- 槽目录（TupleInfo）布局 ----

/// 槽目录区起始偏移
pub const TUPLE_INFO_AREA: usize = TABLE_PAGE_HEADER_SIZE;
/// 每个槽目录条目的大小。
///
/// 版本链需要保存 xmin、xmax 和前一版本 RID，因此这里从旧的 24
/// 字节扩展为 32 字节。新建的数据文件使用新布局；旧数据文件需要
/// 通过迁移工具重建，不能直接按旧槽目录解释。
pub const TUPLE_INFO_SIZE: usize = 32;
/// 条目内偏移：元组数据在页内的起始偏移（u16）
const INFO_OFF: usize = 0;
/// 条目内偏移：元组数据长度（u16）
const INFO_SIZE: usize = 2;
const INFO_XMIN: usize = 4;
const INFO_XMAX: usize = 12;
const INFO_PREV: usize = 20;
const INFO_DELETED: usize = 28;

/// 元组版本元数据。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TupleMeta {
    /// 创建该版本的事务。
    pub xmin: crate::common::config::TxnId,
    /// 删除/替换该版本的事务；为空表示当前仍未被替换。
    pub xmax: Option<crate::common::config::TxnId>,
    /// 同一逻辑记录的前一个物理版本。
    pub prev: Option<crate::common::rid::Rid>,
    /// 是否已被（逻辑）删除
    pub is_deleted: bool,
}

impl Default for TupleMeta {
    fn default() -> Self {
        Self {
            xmin: crate::common::config::INVALID_TXN_ID,
            xmax: None,
            prev: None,
            is_deleted: false,
        }
    }
}

/// 表数据文件的文件头（存储在第 0 页开头）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RmFileHdr {
    /// 文件中已分配的页面数（含第 0 页文件头页）
    pub num_pages: i32,
    /// 第一个可能有空闲空间的页号；[`RM_NO_PAGE`] 表示没有
    pub first_free_page_no: i32,
}

impl RmFileHdr {
    pub const SIZE: usize = 8;

    /// 初始文件头：只有文件头页自身，无空闲页。
    pub fn init() -> Self {
        Self {
            num_pages: 1,
            first_free_page_no: RM_NO_PAGE,
        }
    }

    pub fn to_bytes(self) -> [u8; Self::SIZE] {
        let mut b = [0u8; Self::SIZE];
        b[0..4].copy_from_slice(&self.num_pages.to_le_bytes());
        b[4..8].copy_from_slice(&self.first_free_page_no.to_le_bytes());
        b
    }

    pub fn from_bytes(b: &[u8]) -> Self {
        Self {
            num_pages: i32::from_le_bytes(b[0..4].try_into().unwrap()),
            first_free_page_no: i32::from_le_bytes(b[4..8].try_into().unwrap()),
        }
    }
}

// ---- 页内字段访问（对页数据字节切片的编解码视图）----

/// 页 LSN。
pub fn page_lsn(data: &[u8]) -> i32 {
    get_lsn(data)
}

/// 设置页 LSN。
pub fn set_page_lsn(data: &mut [u8], lsn: i32) {
    set_lsn(data, lsn);
}

/// 页的链接下一页号（插入时页满后新页链接在此）。
pub fn page_next_page_id(data: &[u8]) -> i32 {
    i32::from_le_bytes(
        data[OFFSET_NEXT_PAGE_ID..OFFSET_NEXT_PAGE_ID + 4]
            .try_into()
            .unwrap(),
    )
}

pub fn set_page_next_page_id(data: &mut [u8], next: i32) {
    data[OFFSET_NEXT_PAGE_ID..OFFSET_NEXT_PAGE_ID + 4].copy_from_slice(&next.to_le_bytes());
}

/// 页内记录数（只增不减，删除仅置位标记）。
pub fn page_num_records(data: &[u8]) -> u16 {
    u16::from_le_bytes(
        data[OFFSET_NUM_RECORDS..OFFSET_NUM_RECORDS + 2]
            .try_into()
            .unwrap(),
    )
}

pub fn set_page_num_records(data: &mut [u8], n: u16) {
    data[OFFSET_NUM_RECORDS..OFFSET_NUM_RECORDS + 2].copy_from_slice(&n.to_le_bytes());
}

/// 页内已删除记录数。
pub fn page_num_deleted(data: &[u8]) -> u16 {
    u16::from_le_bytes(
        data[OFFSET_NUM_DELETED..OFFSET_NUM_DELETED + 2]
            .try_into()
            .unwrap(),
    )
}

pub fn set_page_num_deleted(data: &mut [u8], n: u16) {
    data[OFFSET_NUM_DELETED..OFFSET_NUM_DELETED + 2].copy_from_slice(&n.to_le_bytes());
}

fn info_at(slot: usize) -> usize {
    TUPLE_INFO_AREA + slot * TUPLE_INFO_SIZE
}

/// 读取槽目录：返回 (数据偏移, 数据长度, 元数据)。
pub fn read_tuple_info(data: &[u8], slot: usize) -> (u16, u16, TupleMeta) {
    let base = info_at(slot);
    let offset = u16::from_le_bytes(
        data[base + INFO_OFF..base + INFO_OFF + 2]
            .try_into()
            .unwrap(),
    );
    let size = u16::from_le_bytes(
        data[base + INFO_SIZE..base + INFO_SIZE + 2]
            .try_into()
            .unwrap(),
    );
    let xmin = i64::from_le_bytes(
        data[base + INFO_XMIN..base + INFO_XMIN + 8]
            .try_into()
            .unwrap(),
    );
    let xmax_raw = i64::from_le_bytes(
        data[base + INFO_XMAX..base + INFO_XMAX + 8]
            .try_into()
            .unwrap(),
    );
    let prev_raw = i64::from_le_bytes(
        data[base + INFO_PREV..base + INFO_PREV + 8]
            .try_into()
            .unwrap(),
    );
    let is_deleted = data[base + INFO_DELETED] != 0;
    (
        offset,
        size,
        TupleMeta {
            xmin,
            xmax: (xmax_raw != crate::common::config::INVALID_TXN_ID).then_some(xmax_raw),
            prev: (prev_raw != crate::common::rid::Rid::INVALID.encoded())
                .then_some(crate::common::rid::Rid::from_encoded(prev_raw)),
            is_deleted,
        },
    )
}

/// 写入槽目录。
pub fn write_tuple_info(data: &mut [u8], slot: usize, offset: u16, size: u16, meta: TupleMeta) {
    let base = info_at(slot);
    data[base + INFO_OFF..base + INFO_OFF + 2].copy_from_slice(&offset.to_le_bytes());
    data[base + INFO_SIZE..base + INFO_SIZE + 2].copy_from_slice(&size.to_le_bytes());
    data[base + INFO_XMIN..base + INFO_XMIN + 8].copy_from_slice(&meta.xmin.to_le_bytes());
    data[base + INFO_XMAX..base + INFO_XMAX + 8].copy_from_slice(
        &meta
            .xmax
            .unwrap_or(crate::common::config::INVALID_TXN_ID)
            .to_le_bytes(),
    );
    data[base + INFO_PREV..base + INFO_PREV + 8].copy_from_slice(
        &meta
            .prev
            .unwrap_or(crate::common::rid::Rid::INVALID)
            .encoded()
            .to_le_bytes(),
    );
    data[base + INFO_DELETED] = u8::from(meta.is_deleted);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_hdr_roundtrip() {
        let hdr = RmFileHdr {
            num_pages: 7,
            first_free_page_no: 3,
        };
        assert_eq!(RmFileHdr::from_bytes(&hdr.to_bytes()), hdr);
        assert_eq!(
            RmFileHdr::init(),
            RmFileHdr {
                num_pages: 1,
                first_free_page_no: RM_NO_PAGE
            }
        );
    }

    #[test]
    fn page_header_and_tuple_info_layout() {
        let mut data = vec![0u8; 4096];
        set_page_next_page_id(&mut data, -1);
        set_page_num_records(&mut data, 2);
        set_page_num_deleted(&mut data, 1);
        set_page_lsn(&mut data, 99);

        assert_eq!(page_next_page_id(&data), -1);
        assert_eq!(page_num_records(&data), 2);
        assert_eq!(page_num_deleted(&data), 1);
        assert_eq!(page_lsn(&data), 99);

        write_tuple_info(
            &mut data,
            0,
            4000,
            64,
            TupleMeta {
                xmin: 5,
                xmax: None,
                prev: None,
                is_deleted: false,
            },
        );
        write_tuple_info(
            &mut data,
            1,
            3900,
            100,
            TupleMeta {
                xmin: 6,
                xmax: None,
                prev: None,
                is_deleted: true,
            },
        );
        assert_eq!(
            read_tuple_info(&data, 0),
            (
                4000,
                64,
                TupleMeta {
                    xmin: 5,
                    xmax: None,
                    prev: None,
                    is_deleted: false
                }
            )
        );
        assert_eq!(
            read_tuple_info(&data, 1),
            (
                3900,
                100,
                TupleMeta {
                    xmin: 6,
                    xmax: None,
                    prev: None,
                    is_deleted: true
                }
            )
        );
    }
}
