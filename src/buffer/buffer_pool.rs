//! 缓冲池管理器：内存帧与磁盘页之间的调度。
//!
//! 对应 C++ 版 `src-cpp/buffer/buffer_pool_manager.cpp`。
//!
//! # 核心设计：RAII `PageGuard` 取代裸 `Page*` + 手工 pin/unpin
//!
//! C++ 版要求调用者对每次 `FetchPage/NewPage` 手工配对 `UnpinPage`——忘记
//! unpin 耗尽缓冲池、unpin 后继续使用就是 use-after-free。Rust 版的
//! [`PageGuard`] 在 `Drop` 时自动 unpin，pin 期间该帧不可能被替换，因此
//! 通过 guard 拿到的页数据引用在生命周期上不可能悬垂：
//!
//! ```ignore
//! let page = bpm.fetch_page(page_id)?;   // pin_count + 1
//! page.write()[0] = 42;                  // 拿 &mut [u8; PAGE_SIZE]，自动标脏
//! drop(page);                            // 自动 unpin，pin_count - 1
//! ```
//!
//! # 并发模型
//!
//! - 池级 latch（`Mutex<PoolState>`）保护页表 / 空闲帧表 / LRU 替换器 / pin 计数；
//! - 每帧一把 `RwLock` 保护页数据，保证多个 guard 可并发读、互斥写；
//! - **锁序固定为 池 latch → 帧数据锁**，且绝不在持有帧数据锁时调用缓冲池
//!   方法（fetch/new/flush 都要拿池 latch）。调用方（B+ 树、记录层）遵守：
//!   先取齐所有 guard，再逐个 `read()/write()`。
//!
//! # 与 C++ 版的行为差异
//!
//! - 失败路径用 `Result` 而非 `nullptr`（池满时返回 [`EasyDbError::BufferPoolFull`]）；
//! - 写访问（`write()`）立即标脏，不再需要 `UnpinPage(pid, is_dirty)` 的脏参数；
//! - `delete_page` 不再把脏页写回（删除页写回属于 C++ 版的可疑行为）；
//! - `remove_file_pages` 把释放的帧归还空闲表（C++ 版直接丢弃，属于帧泄漏）；
//! - `recover_page` 用读 IO 加载页（C++ 版误用 WritePage，见其源码注释）。

use std::collections::{HashMap, VecDeque};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicI32, AtomicI64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::buffer::lru_replacer::LruReplacer;
use crate::common::config::PAGE_SIZE;
use crate::common::errors::EasyDbError;
use crate::storage::disk::DiskManager;
use crate::storage::page::PageId;

/// 页数据类型：定长 4KB 字节数组。
pub type PageBytes = [u8; PAGE_SIZE];

/// 帧数据：页内容 + 脏标记（由帧自己的 RwLock 保护）。
struct FrameData {
    bytes: Box<PageBytes>,
    dirty: bool,
}

/// 帧：缓冲池中的物理槽位。
struct Frame {
    /// 页数据（每帧独立锁；多个 guard 可并发读，写互斥）
    data: RwLock<FrameData>,
    /// 当前承载的页标识（i64 编码，[`PageId::INVALID`] 表示未承载）。
    /// 逻辑上仅在外层 state 锁内变更，原子类型只为通过 `&self` 检查
    page_id: AtomicI64,
    /// 固定计数。逻辑上仅在外层 state 锁内变更（用原子类型只为通过
    /// `&self` 可变性检查，不承担同步职责）
    pin_count: AtomicI32,
}

/// 池级状态，由一把 latch 保护。
struct PoolState {
    page_table: HashMap<PageId, usize>,
    free_frames: VecDeque<usize>,
    replacer: LruReplacer,
}

/// 缓冲池管理器。
pub struct BufferPool {
    frames: Vec<Frame>,
    state: Mutex<PoolState>,
    disk: Arc<DiskManager>,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    // 个别线程 panic 后不连锁毒化其余线程
    m.lock().unwrap_or_else(|e| e.into_inner())
}

fn read_lock<T>(m: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    m.read().unwrap_or_else(|e| e.into_inner())
}

fn write_lock<T>(m: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    m.write().unwrap_or_else(|e| e.into_inner())
}

impl BufferPool {
    /// 创建缓冲池：一次性分配 `num_frames` 个帧，初始全部空闲。
    pub fn new(num_frames: usize, disk: Arc<DiskManager>) -> Self {
        let frames = (0..num_frames)
            .map(|_| Frame {
                data: RwLock::new(FrameData {
                    bytes: Box::new([0; PAGE_SIZE]),
                    dirty: false,
                }),
                page_id: AtomicI64::new(PageId::INVALID.encoded()),
                pin_count: AtomicI32::new(0),
            })
            .collect();
        Self {
            frames,
            state: Mutex::new(PoolState {
                page_table: HashMap::new(),
                free_frames: (0..num_frames).collect(),
                replacer: LruReplacer::new(num_frames),
            }),
            disk,
        }
    }

    pub fn size(&self) -> usize {
        self.frames.len()
    }

    /// 获取页面并固定（pin_count + 1）；不在池中则从磁盘加载。
    ///
    /// 返回的 [`PageGuard`] 在 Drop 时自动 unpin。
    pub fn fetch_page(&self, page_id: PageId) -> Result<PageGuard<'_>, EasyDbError> {
        let mut state = lock(&self.state);

        let cached = state.page_table.get(&page_id).copied();
        let frame_idx = match cached {
            Some(idx) => {
                self.frames[idx].pin_count.fetch_add(1, Ordering::Relaxed);
                idx
            }
            None => {
                let idx = self
                    .find_victim(&mut state)
                    .ok_or(EasyDbError::BufferPoolFull)?;
                // 淘汰旧页（脏则写回），从磁盘载入新页
                self.evict_frame(&mut state, idx);
                {
                    let mut data = write_lock(&self.frames[idx].data);
                    self.disk
                        .read_page(page_id.fd, page_id.page_no, &mut data.bytes[..])?;
                    data.dirty = false;
                }
                let frame = &self.frames[idx];
                frame.page_id.store(page_id.encoded(), Ordering::Relaxed);
                frame.pin_count.store(1, Ordering::Relaxed);
                state.page_table.insert(page_id, idx);
                idx
            }
        };
        state.replacer.pin(frame_idx);
        Ok(PageGuard {
            pool: self,
            frame_idx,
            page_id,
        })
    }

    /// 分配新页（磁盘页号 +1）并固定，页数据清零。
    /// 返回 guard 与新页号（对应 C++ `NewPage`）。
    pub fn new_page(&self, fd: i32) -> Result<(PageGuard<'_>, i32), EasyDbError> {
        let page_no = self.disk.allocate_page(fd);
        let page_id = PageId { fd, page_no };
        let mut state = lock(&self.state);

        let frame_idx = self
            .find_victim(&mut state)
            .ok_or(EasyDbError::BufferPoolFull)?;
        self.evict_frame(&mut state, frame_idx);
        {
            let mut data = write_lock(&self.frames[frame_idx].data);
            data.bytes.fill(0);
            data.dirty = false;
        }
        let frame = &self.frames[frame_idx];
        frame.page_id.store(page_id.encoded(), Ordering::Relaxed);
        frame.pin_count.store(1, Ordering::Relaxed);
        state.page_table.insert(page_id, frame_idx);
        state.replacer.pin(frame_idx);

        Ok((
            PageGuard {
                pool: self,
                frame_idx,
                page_id,
            },
            page_no,
        ))
    }

    /// 从缓冲池删除页面（对应 C++ `DeletePage`）。
    ///
    /// 返回 `false` 表示页面被固定无法删除；页面不在池中视为已删除。
    /// 注：不把脏页写回（见模块文档「行为差异」）。
    pub fn delete_page(&self, page_id: PageId) -> bool {
        let mut state = lock(&self.state);
        let Some(frame_idx) = state.page_table.remove(&page_id) else {
            return true; // 不在池中，视为成功
        };
        let frame = &self.frames[frame_idx];
        if frame.pin_count.load(Ordering::Relaxed) != 0 {
            // 已固定：恢复映射
            state.page_table.insert(page_id, frame_idx);
            return false;
        }
        {
            let mut data = write_lock(&frame.data);
            data.bytes.fill(0);
            data.dirty = false;
        }
        frame
            .page_id
            .store(PageId::INVALID.encoded(), Ordering::Relaxed);
        state.free_frames.push_back(frame_idx);
        true
    }

    /// 将页面写回磁盘（无论是否脏），并清除脏标记。
    /// 返回 `false` 表示页面不在缓冲池中。
    pub fn flush_page(&self, page_id: PageId) -> Result<bool, EasyDbError> {
        let state = lock(&self.state);
        let Some(&frame_idx) = state.page_table.get(&page_id) else {
            return Ok(false);
        };
        let mut data = write_lock(&self.frames[frame_idx].data);
        self.disk
            .write_page(page_id.fd, page_id.page_no, &data.bytes[..])?;
        data.dirty = false;
        Ok(true)
    }

    /// 将属于 `fd` 文件的所有缓冲页写回磁盘（对应 C++ `FlushAllPages`）。
    pub fn flush_file_pages(&self, fd: i32) -> Result<(), EasyDbError> {
        let page_ids: Vec<PageId> = {
            let state = lock(&self.state);
            state
                .page_table
                .keys()
                .filter(|p| p.fd == fd)
                .copied()
                .collect()
        };
        for page_id in page_ids {
            self.flush_page(page_id)?;
        }
        Ok(())
    }

    /// 将缓冲池中所有脏页写回磁盘（对应 C++ `FlushAllDirtyPages`）。
    pub fn flush_all_dirty_pages(&self) -> Result<(), EasyDbError> {
        let dirty: Vec<PageId> = {
            let state = lock(&self.state);
            let mut dirty = Vec::new();
            for (&pid, &idx) in state.page_table.iter() {
                if read_lock(&self.frames[idx].data).dirty {
                    dirty.push(pid);
                }
            }
            dirty
        };
        for page_id in dirty {
            self.flush_page(page_id)?;
        }
        Ok(())
    }

    /// 丢弃缓冲池中属于 `fd` 文件的所有页（drop table/index 后调用，
    /// 避免 fd 复用读到陈旧数据）。
    ///
    /// 注：释放的帧归还空闲表（修复 C++ 版帧泄漏，见模块文档）。
    pub fn remove_file_pages(&self, fd: i32) {
        let mut state = lock(&self.state);
        let mut freed = Vec::new();
        state.page_table.retain(|page_id, &mut frame_idx| {
            if page_id.fd == fd {
                freed.push(frame_idx);
                false
            } else {
                true
            }
        });
        for frame_idx in freed {
            let frame = &self.frames[frame_idx];
            if frame.pin_count.load(Ordering::Relaxed) == 0 {
                let mut data = write_lock(&frame.data);
                data.bytes.fill(0);
                data.dirty = false;
                drop(data);
                frame
                    .page_id
                    .store(PageId::INVALID.encoded(), Ordering::Relaxed);
                state.free_frames.push_back(frame_idx);
            }
            // pin_count != 0 的帧不该出现在 drop 场景：保留数据原样，
            // 使既有 guard 仍能工作，但不归还空闲表（与 C++ 行为对齐的保守处理）。
        }
    }

    /// 从磁盘把已知页恢复进缓冲池并固定（对应 C++ `RecoverPage`，
    /// 修复 C++ 版误用 WritePage 的问题，此处为真正的读 IO）。
    pub fn recover_page(&self, page_id: PageId) -> Result<PageGuard<'_>, EasyDbError> {
        // 已在池中：fetch 语义即可
        if lock(&self.state).page_table.contains_key(&page_id) {
            return self.fetch_page(page_id);
        }
        let mut state = lock(&self.state);
        let frame_idx = self
            .find_victim(&mut state)
            .ok_or_else(|| EasyDbError::Internal("recover_page: no victim frame".to_string()))?;
        self.evict_frame(&mut state, frame_idx);
        {
            let mut data = write_lock(&self.frames[frame_idx].data);
            self.disk
                .read_page(page_id.fd, page_id.page_no, &mut data.bytes[..])?;
            data.dirty = false;
        }
        let frame = &self.frames[frame_idx];
        frame.page_id.store(page_id.encoded(), Ordering::Relaxed);
        frame.pin_count.store(1, Ordering::Relaxed);
        state.page_table.insert(page_id, frame_idx);
        state.replacer.pin(frame_idx);
        drop(state);
        Ok(PageGuard {
            pool: self,
            frame_idx,
            page_id,
        })
    }

    // ---- 内部方法（调用方必须已持有 state 锁）----

    /// 从空闲帧表或 LRU 替换器中取一个可用帧。
    fn find_victim(&self, state: &mut PoolState) -> Option<usize> {
        if let Some(idx) = state.free_frames.pop_front() {
            return Some(idx);
        }
        state.replacer.victim()
    }

    /// 让帧让位：脏则写回其当前页，并解除页表映射。
    /// 调用方必须已持有 `state` 锁（作为参数传入以避免重入加锁）。
    fn evict_frame(&self, state: &mut PoolState, frame_idx: usize) {
        let frame = &self.frames[frame_idx];
        let old_page_id = PageId::from_encoded(frame.page_id.load(Ordering::Relaxed));
        if old_page_id != PageId::INVALID {
            let dirty = read_lock(&frame.data).dirty;
            if dirty {
                let data = read_lock(&frame.data);
                // 写回失败只能忽略：淘汰路径无法回退
                let _ = self
                    .disk
                    .write_page(old_page_id.fd, old_page_id.page_no, &data.bytes[..]);
            }
            state.page_table.remove(&old_page_id);
        }
    }

    /// guard Drop 时调用：pin_count - 1，减到 0 时交还替换器。
    fn unpin_frame(&self, frame_idx: usize) {
        let mut state = lock(&self.state);
        let prev = self.frames[frame_idx]
            .pin_count
            .fetch_sub(1, Ordering::Relaxed);
        debug_assert!(prev > 0);
        if prev == 1 {
            state.replacer.unpin(frame_idx);
        }
    }

    /// 页面当前 pin 计数（诊断用）。
    pub fn pin_count(&self, page_id: PageId) -> Option<i32> {
        let state = lock(&self.state);
        state
            .page_table
            .get(&page_id)
            .map(|&idx| self.frames[idx].pin_count.load(Ordering::Relaxed))
    }
}

/// 固定页面的 RAII 句柄：Drop 时自动 unpin。
///
/// 通过 [`PageGuard::read`] / [`PageGuard::write`] 访问页数据，所得引用的
/// 生命周期被限制在本 guard 之内——数据访问不可能越过 pin 存活。
///
/// # 锁序约定
///
/// 不要在持有 `read()/write()` 返回的数据句柄时调用任何 `BufferPool` 方法。
pub struct PageGuard<'a> {
    pool: &'a BufferPool,
    frame_idx: usize,
    page_id: PageId,
}

impl PageGuard<'_> {
    /// 本页标识。
    pub fn page_id(&self) -> PageId {
        self.page_id
    }

    /// 共享读访问页数据。
    pub fn read(&self) -> PageRead<'_> {
        PageRead {
            inner: read_lock(&self.pool.frames[self.frame_idx].data),
        }
    }

    /// 独占写访问页数据；访问即标脏。
    pub fn write(&self) -> PageWrite<'_> {
        let mut inner = write_lock(&self.pool.frames[self.frame_idx].data);
        inner.dirty = true;
        PageWrite { inner }
    }

    /// 页头 LSN。
    pub fn lsn(&self) -> i32 {
        crate::storage::page::get_lsn(&self.read()[..])
    }

    /// 设置页头 LSN。
    pub fn set_lsn(&self, lsn: i32) {
        crate::storage::page::set_lsn(&mut self.write()[..], lsn);
    }
}

impl Drop for PageGuard<'_> {
    fn drop(&mut self) {
        self.pool.unpin_frame(self.frame_idx);
    }
}

impl std::fmt::Debug for PageGuard<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PageGuard")
            .field("page_id", &self.page_id)
            .field("frame_idx", &self.frame_idx)
            .finish()
    }
}

/// 页数据的共享读句柄，`Deref` 到 `[u8; PAGE_SIZE]`。
pub struct PageRead<'a> {
    inner: RwLockReadGuard<'a, FrameData>,
}

impl Deref for PageRead<'_> {
    type Target = PageBytes;
    fn deref(&self) -> &PageBytes {
        &self.inner.bytes
    }
}

/// 页数据的独占写句柄，`Deref`/`DerefMut` 到 `[u8; PAGE_SIZE]`。
pub struct PageWrite<'a> {
    inner: RwLockWriteGuard<'a, FrameData>,
}

impl Deref for PageWrite<'_> {
    type Target = PageBytes;
    fn deref(&self) -> &PageBytes {
        &self.inner.bytes
    }
}

impl DerefMut for PageWrite<'_> {
    fn deref_mut(&mut self) -> &mut PageBytes {
        &mut self.inner.bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 建一个临时数据库目录，返回 (磁盘管理器, 缓冲池)。
    fn setup(frames: usize) -> (tempfile::TempDir, Arc<DiskManager>, Arc<BufferPool>) {
        let dir = tempfile::tempdir().unwrap();
        let db_dir = dir.path().join("db");
        let disk = Arc::new(DiskManager::new(&db_dir).unwrap());
        let bpm = Arc::new(BufferPool::new(frames, disk.clone()));
        (dir, disk, bpm)
    }

    #[test]
    fn fetch_new_roundtrip_with_dirty_writeback() {
        let (_dir, disk, bpm) = setup(4);
        disk.create_file(disk.db_dir().join("t")).unwrap();
        let fd = disk.open_file(disk.db_dir().join("t")).unwrap();

        // 新页写入数据
        let (page, page_no) = bpm.new_page(fd).unwrap();
        assert_eq!(page_no, 0);
        page.write()[0] = 0xAA;
        drop(page); // 自动 unpin

        // 池中命中读回
        let pid = PageId { fd, page_no };
        let page = bpm.fetch_page(pid).unwrap();
        assert_eq!(page.read()[0], 0xAA);
        assert_eq!(bpm.pin_count(pid), Some(1));
        drop(page);
        assert_eq!(bpm.pin_count(pid), Some(0));

        // 逐出（占满缓冲池迫使写回），再从磁盘加载验证
        for i in 1..=4u32 {
            let (p, _n) = bpm.new_page(fd).unwrap();
            p.write()[0] = i as u8;
            drop(p);
        }
        // 此时 pid=0 的帧已被淘汰且脏页写回磁盘
        let page = bpm.fetch_page(pid).unwrap();
        assert_eq!(
            page.read()[0],
            0xAA,
            "dirty page must be written back on eviction"
        );
    }

    #[test]
    fn pool_full_error_when_all_pinned() {
        let (_dir, disk, bpm) = setup(2);
        disk.create_file(disk.db_dir().join("t")).unwrap();
        let fd = disk.open_file(disk.db_dir().join("t")).unwrap();

        let _p0 = bpm.new_page(fd).unwrap();
        let _p1 = bpm.new_page(fd).unwrap();
        assert!(matches!(bpm.new_page(fd), Err(EasyDbError::BufferPoolFull)));
    }

    #[test]
    fn guard_drop_unpins_automatically() {
        let (_dir, disk, bpm) = setup(2);
        disk.create_file(disk.db_dir().join("t")).unwrap();
        let fd = disk.open_file(disk.db_dir().join("t")).unwrap();

        let (pid, _) = {
            let (page, page_no) = bpm.new_page(fd).unwrap();
            (page.page_id(), page_no)
        }; // guard 在此 drop
        assert_eq!(bpm.pin_count(pid), Some(0));

        // 全部帧都已释放 → 还能继续分配
        let _p = bpm.new_page(fd).unwrap();
        let _q = bpm.new_page(fd).unwrap();
    }

    #[test]
    fn delete_page_semantics() {
        let (_dir, disk, bpm) = setup(2);
        disk.create_file(disk.db_dir().join("t")).unwrap();
        let fd = disk.open_file(disk.db_dir().join("t")).unwrap();

        let (page, _) = bpm.new_page(fd).unwrap();
        let pid = page.page_id();
        // 被固定时不可删除
        assert!(!bpm.delete_page(pid));
        drop(page);
        assert!(bpm.delete_page(pid));
        // 删除后帧归还空闲表：又能分配
        bpm.new_page(fd).unwrap();
    }

    #[test]
    fn concurrent_readers_share_frame() {
        use std::sync::Barrier;
        let (_dir, disk, bpm) = setup(2);
        disk.create_file(disk.db_dir().join("t")).unwrap();
        let fd = disk.open_file(disk.db_dir().join("t")).unwrap();

        let (page, _) = bpm.new_page(fd).unwrap();
        page.write()[1] = 7;
        let pid = page.page_id();
        drop(page);

        let barrier = Arc::new(Barrier::new(4));
        let mut handles = Vec::new();
        for _ in 0..4 {
            let bpm = bpm.clone();
            let barrier = barrier.clone();
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                let page = bpm.fetch_page(pid).unwrap();
                assert_eq!(page.read()[1], 7);
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(bpm.pin_count(pid), Some(0));
    }
}
