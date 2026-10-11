//! 记录标识符（Record Identifier）。
//!
//! 对应 C++ 版 `src-cpp/include/common/rid.h`。

use std::fmt;

use super::config::{INVALID_PAGE_ID, PageId, SlotId};

/// 记录标识符：由 (page_id, slot_num) 唯一定位表中的一条记录。
///
/// 可编码为 i64：高 32 位为 page_id，低 32 位为 slot_num
/// （编码格式与 C++ 版一致，供索引键与日志记录使用）。
///
/// 排序先按 page_id 再按 slot_num，可直接用作 B+ 树键的一部分。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Rid {
    page_id: PageId,
    slot_num: SlotId,
}

impl Rid {
    /// 无效 RID（页ID为 [`INVALID_PAGE_ID`]），对应 C++ 默认构造。
    pub const INVALID: Rid = Rid {
        page_id: INVALID_PAGE_ID,
        slot_num: 0,
    };

    pub const fn new(page_id: PageId, slot_num: SlotId) -> Self {
        Self { page_id, slot_num }
    }

    /// 从 i64 编码恢复（高 32 位 page_id，低 32 位 slot_num）。
    pub const fn from_encoded(encoded: i64) -> Self {
        Self {
            page_id: (encoded >> 32) as PageId,
            slot_num: encoded as SlotId,
        }
    }

    /// 编码为 i64（高 32 位 page_id，低 32 位 slot_num）。
    pub const fn encoded(&self) -> i64 {
        ((self.page_id as i64) << 32) | (self.slot_num as i64)
    }

    pub const fn page_id(&self) -> PageId {
        self.page_id
    }

    pub const fn slot_num(&self) -> SlotId {
        self.slot_num
    }

    pub fn set(&mut self, page_id: PageId, slot_num: SlotId) {
        self.page_id = page_id;
        self.slot_num = slot_num;
    }
}

impl Default for Rid {
    fn default() -> Self {
        Self::INVALID
    }
}

impl fmt::Display for Rid {
    /// 输出格式与 C++ 版一致：`"(page_id,slot_num)"`。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({},{})", self.page_id, self.slot_num)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_roundtrip() {
        let rid = Rid::new(3, 7);
        assert_eq!(Rid::from_encoded(rid.encoded()), rid);

        // 负数页ID也要能往返（与 C++ 的算术移位语义一致）
        let rid = Rid::new(-1, 1023);
        assert_eq!(Rid::from_encoded(rid.encoded()), rid);
    }

    #[test]
    fn encoded_layout_is_high_page_low_slot() {
        let rid = Rid::new(2, 5);
        assert_eq!(rid.encoded(), (2_i64 << 32) | 5);
    }

    #[test]
    fn display_matches_cpp_format() {
        assert_eq!(Rid::new(12, 34).to_string(), "(12,34)");
    }

    #[test]
    fn default_is_invalid() {
        let rid = Rid::default();
        assert_eq!(rid.page_id(), INVALID_PAGE_ID);
        assert_eq!(rid, Rid::INVALID);
    }

    #[test]
    fn orders_by_page_then_slot() {
        let mut rids = vec![Rid::new(1, 9), Rid::new(0, 100), Rid::new(1, 2)];
        rids.sort();
        assert_eq!(rids, vec![Rid::new(0, 100), Rid::new(1, 2), Rid::new(1, 9)]);
    }
}
