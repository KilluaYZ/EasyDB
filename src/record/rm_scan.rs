//! 全表顺序扫描迭代器。
//!
//! 对应 C++ 版 `src-cpp/record/rm_scan.cpp` 的 `RmScan`，改为 Rust 惯用的
//! `Iterator<Item = Rid>` 形式：构造与每次推进都跳过已删除槽位
//! （修复 C++ 版构造时不检查首槽有效性的问题）。

use crate::common::rid::Rid;
use crate::record::rm_defs::RM_FIRST_RECORD_PAGE;
use crate::record::rm_file_handle::RmFileHandle;

/// 顺序扫描一个表文件，产出所有未删除记录的 RID。
pub struct RmScan<'a> {
    fh: &'a RmFileHandle,
    rid: Rid,
    finished: bool,
}

impl<'a> RmScan<'a> {
    /// 从第一个数据页开始扫描，自动定位到第一条有效记录。
    pub fn new(fh: &'a RmFileHandle) -> Self {
        let mut scan = Self {
            fh,
            rid: Rid::new(RM_FIRST_RECORD_PAGE, 0),
            finished: false,
        };
        scan.advance();
        scan
    }

    /// 从当前位置向后找第一条未删除记录；越过末页则标记结束。
    fn advance(&mut self) {
        let num_pages = self.fh.file_hdr().num_pages;
        let mut page_no = self.rid.page_id();
        let mut slot = self.rid.slot_num() as i64;

        loop {
            if page_no >= num_pages {
                self.finished = true;
                return;
            }
            let num_records = self.fh.page_num_tuples(page_no).unwrap_or(0);
            while (slot as i32) < num_records as i32 {
                let deleted = self
                    .fh
                    .is_slot_deleted(page_no, slot as u32)
                    .unwrap_or(true);
                if !deleted {
                    self.rid = Rid::new(page_no, slot as u32);
                    return;
                }
                slot += 1;
            }
            page_no += 1;
            slot = 0;
        }
    }
}

impl Iterator for RmScan<'_> {
    type Item = Rid;

    fn next(&mut self) -> Option<Rid> {
        if self.finished {
            return None;
        }
        let current = self.rid;
        self.rid = Rid::new(self.rid.page_id(), self.rid.slot_num() + 1);
        self.advance();
        Some(current)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::buffer_pool::BufferPool;
    use crate::record::rm_defs::TupleMeta;
    use crate::record::rm_manager::RmManager;
    use crate::storage::disk::DiskManager;
    use std::sync::Arc;

    fn setup(dir: &std::path::Path) -> (Arc<DiskManager>, Arc<BufferPool>, RmManager) {
        let disk = Arc::new(DiskManager::new(dir).unwrap());
        let bpm = Arc::new(BufferPool::new(16, disk.clone()));
        let rm = RmManager::new(disk.clone(), bpm.clone());
        (disk, bpm, rm)
    }

    #[test]
    fn scan_skips_deleted_records() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("db");
        let (_disk, _bpm, rm) = setup(&db);
        rm.create_file(dir.path().join("t")).unwrap();
        let fh = rm.open_file(dir.path().join("t")).unwrap();

        let meta = TupleMeta::default();
        let mut rids = Vec::new();
        for i in 0..10 {
            rids.push(
                fh.insert_tuple(meta, format!("rec-{i}").as_bytes())
                    .unwrap(),
            );
        }
        // 删除 2、5、7
        for &i in &[2usize, 5, 7] {
            fh.delete_tuple(rids[i]).unwrap();
        }

        let got: Vec<Rid> = RmScan::new(&fh).collect();
        assert_eq!(got.len(), 7);
        for (k, rid) in got.iter().enumerate() {
            let (_, data) = fh.get_tuple(*rid).unwrap();
            let expect = format!("rec-{}", [0, 1, 3, 4, 6, 8, 9][k]);
            assert_eq!(data, expect.into_bytes());
        }
    }

    #[test]
    fn scan_empty_file_yields_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let (_disk, _bpm, rm) = setup(&dir.path().join("db"));
        rm.create_file(dir.path().join("t")).unwrap();
        let fh = rm.open_file(dir.path().join("t")).unwrap();
        assert_eq!(RmScan::new(&fh).count(), 0);
    }
}
