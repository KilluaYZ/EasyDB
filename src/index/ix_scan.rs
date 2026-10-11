//! B+ 树范围扫描迭代器。
//!
//! 对应 C++ 版 `src-cpp/storage/index/ix_scan.cpp` 的 `IxScan`，改为
//! `Iterator<Item = Rid>` 形式。

use crate::common::rid::Rid;
use crate::index::ix_defs::Iid;
use crate::index::ix_index_handle::IxIndexHandle;

/// 在 `[lower, upper)` 范围内按序产出索引项的 RID。
///
/// 扫描沿叶子双向链表推进（与 C++ 版一致）；并发修改下的行为说明：
/// 每次推进都重新取页（经缓冲池），树结构在同一语句内不会被并发修改
/// （上层每条语句单线程执行），P6 事务阶段再收紧该约定。
pub struct IxScan<'a> {
    ih: &'a IxIndexHandle,
    iid: Iid,
    end: Iid,
}

impl<'a> IxScan<'a> {
    pub fn new(ih: &'a IxIndexHandle, lower: Iid, upper: Iid) -> Self {
        Self {
            ih,
            iid: lower,
            end: upper,
        }
    }

    pub fn get_iid(&self) -> Iid {
        self.iid
    }
}

impl Iterator for IxScan<'_> {
    type Item = Rid;

    fn next(&mut self) -> Option<Rid> {
        if self.iid == self.end {
            return None;
        }
        let rid = self.ih.get_rid(self.iid).ok()?;
        // 推进：槽 +1；越过本页（且不是最后一个叶子）则跳到后继叶子
        let hdr = self.ih.file_hdr();
        if self.iid.page_no != hdr.last_leaf
            && let Ok(size) = self.ih.page_num_keys(self.iid.page_no)
            && self.iid.slot as usize + 1 >= size
            && let Ok(next) = self.ih.page_next_leaf(self.iid.page_no)
        {
            self.iid = Iid::new(next, 0);
            return Some(rid);
        }
        self.iid.slot += 1;
        Some(rid)
    }
}
