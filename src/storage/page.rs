//! 页标识与通用页头布局。
//!
//! 对应 C++ 版 `src-cpp/include/storage/page/page.h` 中的 `PageId` 与页头常量。

use std::fmt;

use crate::common::config::INVALID_PAGE_ID;

/// 页面标识：文件句柄 + 页内编号，二者组合唯一确定一个物理页。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PageId {
    /// 文件句柄（[`crate::storage::disk::DiskManager`] 分配的内部编号）
    pub fd: i32,
    /// 页面在文件中的编号
    pub page_no: i32,
}

impl PageId {
    /// 无效页标识（对应 C++ `PageId{-1, INVALID_PAGE_ID}`）。
    pub const INVALID: PageId = PageId {
        fd: -1,
        page_no: INVALID_PAGE_ID,
    };

    pub const fn new(fd: i32, page_no: i32) -> Self {
        Self { fd, page_no }
    }

    /// 编码为 i64：高 32 位 fd，低 32 位 page_no（与 C++ `PageId::Get` 一致）。
    pub const fn encoded(self) -> i64 {
        ((self.fd as i64) << 32) | (self.page_no as u32 as i64)
    }

    /// 从 i64 编码恢复。
    pub const fn from_encoded(encoded: i64) -> Self {
        Self {
            fd: (encoded >> 32) as i32,
            page_no: encoded as i32,
        }
    }
}

impl fmt::Display for PageId {
    /// 输出格式与 C++ 版 `toString` 一致：`"{fd: X page_no: Y}"`。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{{fd: {} page_no: {}}}", self.fd, self.page_no)
    }
}

/// 通用页头布局（所有页面共享）：
/// `| page_lsn (4B) | ...(页面特定头) |`
pub const SIZE_PAGE_HEADER: usize = 8;
/// LSN 在页内的字节偏移
pub const OFFSET_LSN: usize = 4;
/// 页面特定头（各子系统自己的头部）的起始偏移
pub const OFFSET_PAGE_HDR: usize = 8;

/// 读取页头中的 LSN（恢复系统使用）。
pub fn get_lsn(data: &[u8]) -> i32 {
    let b: [u8; 4] = data[OFFSET_LSN..OFFSET_LSN + 4].try_into().unwrap();
    i32::from_le_bytes(b)
}

/// 写入页头中的 LSN。
pub fn set_lsn(data: &mut [u8], lsn: i32) {
    data[OFFSET_LSN..OFFSET_LSN + 4].copy_from_slice(&lsn.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lsn_roundtrip() {
        let mut data = vec![0u8; PAGE_HEAD_LEN];
        set_lsn(&mut data, 42);
        assert_eq!(get_lsn(&data), 42);
        set_lsn(&mut data, -7);
        assert_eq!(get_lsn(&data), -7);
    }

    const PAGE_HEAD_LEN: usize = 16;

    #[test]
    fn page_id_display() {
        assert_eq!(
            PageId { fd: 3, page_no: 9 }.to_string(),
            "{fd: 3 page_no: 9}"
        );
    }
}
