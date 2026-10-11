//! LRU 替换器：管理所有 pin_count 为 0、可被替换的帧。
//!
//! 对应 C++ 版 `src-cpp/buffer/lru_replacer.cpp`（双向链表 + 哈希表）。
//!
//! Rust 实现用「帧ID -> 最后 unpin 序号」的哈希表表达同样的 LRU 语义：
//! victim 取序号最小的帧。语义与 C++ 版逐点一致：
//! - `unpin` 已在表中的帧是**空操作**（保持原 LRU 位置，不刷新）；
//! - `pin` 不在表中的帧是空操作；
//! - victim 返回最久 unpin 的帧并移出。
//!
//! 并发约定：本结构自身不加锁，由 [`crate::buffer::buffer_pool::BufferPool`]
//! 的池级 latch 保护（与 C++ 版一致）。

use std::collections::HashMap;

#[derive(Debug, Default)]
pub struct LruReplacer {
    /// 可替换帧的最后 unpin 序号
    last_unpinned: HashMap<usize, u64>,
    /// 单调递增时钟，用于给 unpin 排序
    clock: u64,
}

impl LruReplacer {
    pub fn new(_num_frames: usize) -> Self {
        Self::default()
    }

    /// 挑选最久未使用的帧并移出替换器；为空返回 `None`（对应 `Victim`）。
    pub fn victim(&mut self) -> Option<usize> {
        let victim = self
            .last_unpinned
            .iter()
            .min_by_key(|&(_, stamp)| stamp)
            .map(|(&fid, _)| fid);
        if let Some(fid) = victim {
            self.last_unpinned.remove(&fid);
        }
        victim
    }

    /// 帧被固定，移出替换器；不在表中则空操作（对应 `Pin`）。
    pub fn pin(&mut self, frame_id: usize) {
        self.last_unpinned.remove(&frame_id);
    }

    /// 帧解除固定进入替换器；已在表中则空操作（对应 `Unpin`）。
    pub fn unpin(&mut self, frame_id: usize) {
        if !self.last_unpinned.contains_key(&frame_id) {
            self.last_unpinned.insert(frame_id, self.clock);
            self.clock += 1;
        }
    }

    /// 可替换帧数量（对应 `Size`）。
    pub fn size(&self) -> usize {
        self.last_unpinned.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 移植自 C++ 版 test/buffer/lru_replacer_test.cpp 的 TestFunctionality。
    #[test]
    fn functionality_matches_cpp_test() {
        let mut lru = LruReplacer::new(7);

        // unpin 六个元素；重复 unpin 1 是空操作
        lru.unpin(1);
        lru.unpin(2);
        lru.unpin(3);
        lru.unpin(4);
        lru.unpin(5);
        lru.unpin(6);
        lru.unpin(1);
        assert_eq!(6, lru.size());

        // 依次取出三个受害者：1、2、3
        assert_eq!(lru.victim(), Some(1));
        assert_eq!(lru.victim(), Some(2));
        assert_eq!(lru.victim(), Some(3));

        // pin 已被 victim 的 3 是空操作；pin 4 将其移出
        lru.pin(3);
        lru.pin(4);
        assert_eq!(2, lru.size());

        // 重新 unpin 4
        lru.unpin(4);

        // 继续取受害者：5、6、4
        assert_eq!(lru.victim(), Some(5));
        assert_eq!(lru.victim(), Some(6));
        assert_eq!(lru.victim(), Some(4));
        assert_eq!(lru.victim(), None);
    }

    #[test]
    fn pin_then_unpin_refreshes_recency() {
        let mut lru = LruReplacer::new(3);
        lru.unpin(1);
        lru.unpin(2);
        lru.pin(1); // 移出
        lru.unpin(1); // 重新加入，成为最近使用
        assert_eq!(lru.victim(), Some(2));
        assert_eq!(lru.victim(), Some(1));
    }
}
