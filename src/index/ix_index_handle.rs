//! B+ 树索引句柄。
//!
//! 对应 C++ 版 `src-cpp/storage/index/ix_index_handle.cpp`。
//!
//! # 设计
//!
//! - **节点 = 页**：节点操作通过 [`PageGuard`] 的 `read()/write()` 以字节
//!   视图函数（本文件 `node` 模块）完成，带边界检查、无结构体强转；
//! - **RAII pin**：C++ 版每个函数都要手工配对 UnpinPage，Rust 版 guard
//!   离开作用域自动 unpin，全树操作不存在手工 pin/unpin；
//! - **锁**：树级 `RwLock` 保护结构——读操作（查找/扫描）共享，写操作
//!   （插入/删除）独占。C++ 版写用一把 root_latch 串行化、读完全无锁
//!   （存在数据竞争）；P6 事务阶段再升级为 latch crabbing；
//! - 文件头常驻内存（`Mutex<IxFileHdr>`），结构变更后在顶层操作结束时
//!   持久化第 0 页（C++ 版从不持久化根指针，重启即丢树）。
//!
//! # 键的约定（与 C++ 版一致）
//!
//! 父节点中 `key[i]` = 第 i 棵子树的**第一个键**；子树 i 覆盖
//! `[key[i], key[i+1])`。`num_pages` 为页号高水位（删除的页进空闲链表
//! 复用，不再像 C++ 版那样泄漏）。

use std::cmp::Ordering;
use std::sync::atomic::{AtomicI64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};

use crate::buffer::buffer_pool::{BufferPool, PageGuard};
use crate::common::config::PAGE_SIZE;
use crate::common::errors::EasyDbError;
use crate::common::rid::Rid;
use crate::index::ix_defs::{
    IX_FILE_HDR_PAGE, IX_NO_PAGE, IX_NODE_HDR_SIZE, Iid, IxFileHdr, KeyComparator, OFF_IS_LEAF,
    OFF_NEXT_FREE, OFF_NEXT_LEAF, OFF_NUM_KEYS, OFF_PARENT, OFF_PREV_LEAF,
};
use crate::storage::disk::DiskManager;
use crate::storage::page::PageId;

pub struct IxIndexHandle {
    disk: Arc<DiskManager>,
    bpm: Arc<BufferPool>,
    fd: i32,
    hdr: Mutex<IxFileHdr>,
    /// 树级结构锁：读共享、写独占
    tree_lock: RwLock<()>,
    /// 键总长缓存（快速校验入参键长度）
    key_len: AtomicI64,
}

fn mutex_lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

fn write_lock<T>(m: &RwLock<T>) -> std::sync::RwLockWriteGuard<'_, T> {
    m.write().unwrap_or_else(|e| e.into_inner())
}

fn read_lock<T>(m: &RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    m.read().unwrap_or_else(|e| e.into_inner())
}

// ==================== 节点字节视图 ====================

/// 节点页内字段的编解码视图。所有函数带 `hdr` 定位键区/RID 区，
/// 禁止结构体强转，键/RID 读写均显式定宽拷贝。
pub(crate) mod node {
    use super::*;

    pub(crate) fn next_free(d: &[u8]) -> i32 {
        i32::from_le_bytes(d[OFF_NEXT_FREE..OFF_NEXT_FREE + 4].try_into().unwrap())
    }

    pub(crate) fn set_next_free(d: &mut [u8], page_no: i32) {
        d[OFF_NEXT_FREE..OFF_NEXT_FREE + 4].copy_from_slice(&page_no.to_le_bytes());
    }

    pub(crate) fn is_leaf(d: &[u8]) -> bool {
        d[OFF_IS_LEAF] != 0
    }

    pub(crate) fn set_is_leaf(d: &mut [u8], v: bool) {
        d[OFF_IS_LEAF] = u8::from(v);
    }

    pub(crate) fn num_keys(d: &[u8]) -> usize {
        u16::from_le_bytes(d[OFF_NUM_KEYS..OFF_NUM_KEYS + 2].try_into().unwrap()) as usize
    }

    pub(crate) fn set_num_keys(d: &mut [u8], n: usize) {
        assert!(n <= u16::MAX as usize, "node key count overflow");
        d[OFF_NUM_KEYS..OFF_NUM_KEYS + 2].copy_from_slice(&(n as u16).to_le_bytes());
    }

    pub(crate) fn parent(d: &[u8]) -> i32 {
        i32::from_le_bytes(d[OFF_PARENT..OFF_PARENT + 4].try_into().unwrap())
    }

    pub(crate) fn set_parent(d: &mut [u8], page_no: i32) {
        d[OFF_PARENT..OFF_PARENT + 4].copy_from_slice(&page_no.to_le_bytes());
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn prev_leaf(d: &[u8]) -> i32 {
        i32::from_le_bytes(d[OFF_PREV_LEAF..OFF_PREV_LEAF + 4].try_into().unwrap())
    }

    pub(crate) fn set_prev_leaf(d: &mut [u8], page_no: i32) {
        d[OFF_PREV_LEAF..OFF_PREV_LEAF + 4].copy_from_slice(&page_no.to_le_bytes());
    }

    pub(crate) fn next_leaf(d: &[u8]) -> i32 {
        i32::from_le_bytes(d[OFF_NEXT_LEAF..OFF_NEXT_LEAF + 4].try_into().unwrap())
    }

    pub(crate) fn set_next_leaf(d: &mut [u8], page_no: i32) {
        d[OFF_NEXT_LEAF..OFF_NEXT_LEAF + 4].copy_from_slice(&page_no.to_le_bytes());
    }

    fn key_at(hdr: &IxFileHdr, i: usize) -> usize {
        IX_NODE_HDR_SIZE + i * hdr.col_tot_len()
    }

    fn rid_at(hdr: &IxFileHdr, i: usize) -> usize {
        IX_NODE_HDR_SIZE + hdr.max_keys() * hdr.col_tot_len() + i * 8
    }

    /// 第 i 个键的字节切片。
    pub(crate) fn key_slice<'a>(hdr: &IxFileHdr, d: &'a [u8], i: usize) -> &'a [u8] {
        let off = key_at(hdr, i);
        &d[off..off + hdr.col_tot_len()]
    }

    pub(crate) fn set_key(hdr: &IxFileHdr, d: &mut [u8], i: usize, key: &[u8]) {
        debug_assert_eq!(key.len(), hdr.col_tot_len());
        let off = key_at(hdr, i);
        d[off..off + hdr.col_tot_len()].copy_from_slice(key);
    }

    /// 第 i 个位置存储的 RID（内部节点即孩子页号，叶子即记录位置）。
    pub(crate) fn get_rid(hdr: &IxFileHdr, d: &[u8], i: usize) -> Rid {
        let off = rid_at(hdr, i);
        Rid::from_encoded(i64::from_le_bytes(d[off..off + 8].try_into().unwrap()))
    }

    pub(crate) fn set_rid(hdr: &IxFileHdr, d: &mut [u8], i: usize, rid: Rid) {
        let off = rid_at(hdr, i);
        d[off..off + 8].copy_from_slice(&rid.encoded().to_le_bytes());
    }

    /// 内部节点第 i 个孩子页号。
    pub(crate) fn value_at(hdr: &IxFileHdr, d: &[u8], i: usize) -> i32 {
        get_rid(hdr, d, i).page_id()
    }

    /// 节点内二分查找：第一个 >= key 的下标（范围 [0, num_keys]）。
    pub(crate) fn lower_bound(cmp: &KeyComparator, hdr: &IxFileHdr, d: &[u8], key: &[u8]) -> usize {
        let mut lo = 0usize;
        let mut hi = num_keys(d);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if cmp.compare(key_slice(hdr, d, mid), key) == Ordering::Less {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    /// 节点内二分查找：第一个 > key 的下标。
    pub(crate) fn upper_bound(cmp: &KeyComparator, hdr: &IxFileHdr, d: &[u8], key: &[u8]) -> usize {
        let mut lo = 0usize;
        let mut hi = num_keys(d);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if cmp.compare(key_slice(hdr, d, mid), key) != Ordering::Greater {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    /// 在 pos 处插入一个 (key, rid)；要求节点未满。
    pub(crate) fn insert_pair(hdr: &IxFileHdr, d: &mut [u8], pos: usize, key: &[u8], rid: Rid) {
        let n = num_keys(d);
        debug_assert!(pos <= n && n < hdr.max_keys());
        let tot = hdr.col_tot_len();
        let keys_base = IX_NODE_HDR_SIZE;
        // 键区右移一格
        d.copy_within(
            keys_base + pos * tot..keys_base + n * tot,
            keys_base + (pos + 1) * tot,
        );
        d[keys_base + pos * tot..keys_base + (pos + 1) * tot].copy_from_slice(key);
        // RID 区右移一格
        let rids_base = keys_base + hdr.max_keys() * tot;
        d.copy_within(
            rids_base + pos * 8..rids_base + n * 8,
            rids_base + (pos + 1) * 8,
        );
        d[rids_base + pos * 8..rids_base + (pos + 1) * 8]
            .copy_from_slice(&rid.encoded().to_le_bytes());
        set_num_keys(d, n + 1);
    }

    /// 删除 pos 处的 (key, rid)；要求 pos < num_keys。
    pub(crate) fn erase_pair(hdr: &IxFileHdr, d: &mut [u8], pos: usize) {
        let n = num_keys(d);
        debug_assert!(pos < n);
        let tot = hdr.col_tot_len();
        let keys_base = IX_NODE_HDR_SIZE;
        d.copy_within(
            keys_base + (pos + 1) * tot..keys_base + n * tot,
            keys_base + pos * tot,
        );
        let rids_base = keys_base + hdr.max_keys() * tot;
        d.copy_within(
            rids_base + (pos + 1) * 8..rids_base + n * 8,
            rids_base + pos * 8,
        );
        set_num_keys(d, n - 1);
    }

    /// 查找孩子页号在父节点中的下标；不存在报内部错误。
    pub(crate) fn find_child(
        hdr: &IxFileHdr,
        d: &[u8],
        child_no: i32,
    ) -> Result<usize, EasyDbError> {
        for i in 0..num_keys(d) {
            if value_at(hdr, d, i) == child_no {
                return Ok(i);
            }
        }
        Err(EasyDbError::Internal(format!(
            "find_child: child page {child_no} not found in parent"
        )))
    }
}

// ==================== IxIndexHandle ====================

impl IxIndexHandle {
    /// 打开索引文件：读入文件头，页号分配起点对齐到页数高水位。
    pub fn open(
        disk: Arc<DiskManager>,
        bpm: Arc<BufferPool>,
        fd: i32,
    ) -> Result<Self, EasyDbError> {
        let mut page = vec![0u8; PAGE_SIZE];
        disk.read_page(fd, IX_FILE_HDR_PAGE, &mut page)?;
        let hdr = IxFileHdr::from_bytes(&page)?;
        let key_len = hdr.col_tot_len() as i64;
        disk.set_fd2pageno(fd, hdr.num_pages);
        Ok(Self {
            disk,
            bpm,
            fd,
            hdr: Mutex::new(hdr),
            tree_lock: RwLock::new(()),
            key_len: AtomicI64::new(key_len),
        })
    }

    pub fn fd(&self) -> i32 {
        self.fd
    }

    pub fn bpm(&self) -> &Arc<BufferPool> {
        &self.bpm
    }

    /// 文件头快照。
    pub fn file_hdr(&self) -> IxFileHdr {
        mutex_lock(&self.hdr).clone()
    }

    /// 把文件头写回第 0 页。
    pub fn flush_header(&self) -> Result<(), EasyDbError> {
        let hdr = mutex_lock(&self.hdr);
        self.persist_hdr(&hdr)
    }

    fn persist_hdr(&self, hdr: &IxFileHdr) -> Result<(), EasyDbError> {
        let mut page = vec![0u8; PAGE_SIZE];
        let bytes = hdr.to_bytes();
        page[..bytes.len()].copy_from_slice(&bytes);
        self.disk.write_page(self.fd, IX_FILE_HDR_PAGE, &page)
    }

    fn check_key(&self, key: &[u8]) -> Result<(), EasyDbError> {
        if key.len() as i64 != self.key_len.load(AtomicOrdering::Relaxed) {
            return Err(EasyDbError::Internal(format!(
                "index key length mismatch: expect {}, got {}",
                self.key_len.load(AtomicOrdering::Relaxed),
                key.len()
            )));
        }
        Ok(())
    }

    // ---- 查找 ----

    /// 查找 key 所在的叶子页号（从根下降；页 guard 不外借）。
    fn find_leaf(
        &self,
        hdr: &IxFileHdr,
        cmp: &KeyComparator,
        key: &[u8],
    ) -> Result<i32, EasyDbError> {
        let mut cur = hdr.root_page;
        if cur == IX_NO_PAGE {
            return Err(EasyDbError::Internal(
                "index is empty (no root)".to_string(),
            ));
        }
        loop {
            let node = self.fetch_node(cur)?;
            let (is_leaf, child) = {
                let d = node.read();
                if node::is_leaf(&d[..]) {
                    (true, IX_NO_PAGE)
                } else {
                    let pos = node::upper_bound(cmp, hdr, &d[..], key);
                    let pos = if pos > 0 { pos - 1 } else { 0 };
                    (false, node::value_at(hdr, &d[..], pos))
                }
            };
            drop(node);
            if is_leaf {
                return Ok(cur);
            }
            cur = child;
        }
    }

    /// 精确查找 key 对应的 RID。
    pub fn get_value(&self, key: &[u8]) -> Result<Option<Rid>, EasyDbError> {
        self.check_key(key)?;
        let _r = read_lock(&self.tree_lock);
        let hdr = self.file_hdr();
        let cmp = KeyComparator::from_hdr(&hdr);
        let leaf_no = self.find_leaf(&hdr, &cmp, key)?;
        let leaf = self.fetch_node(leaf_no)?;
        let d = leaf.read();
        let pos = node::lower_bound(&cmp, &hdr, &d[..], key);
        if pos < node::num_keys(&d[..])
            && cmp.compare(node::key_slice(&hdr, &d[..], pos), key) == Ordering::Equal
        {
            Ok(Some(node::get_rid(&hdr, &d[..], pos)))
        } else {
            Ok(None)
        }
    }

    /// 把内部位置标识转换为记录 RID（对应 C++ `GetRid(Iid)`）。
    pub fn get_rid(&self, iid: Iid) -> Result<Rid, EasyDbError> {
        let hdr = self.file_hdr();
        let node = self.fetch_node(iid.page_no)?;
        let d = node.read();
        if iid.slot as usize >= node::num_keys(&d[..]) {
            return Err(EasyDbError::IndexEntryNotFound);
        }
        Ok(node::get_rid(&hdr, &d[..], iid.slot as usize))
    }

    /// 第一个 >= key 的位置。
    pub fn lower_bound(&self, key: &[u8]) -> Result<Iid, EasyDbError> {
        self.check_key(key)?;
        let _r = read_lock(&self.tree_lock);
        let hdr = self.file_hdr();
        let cmp = KeyComparator::from_hdr(&hdr);
        let leaf_no = self.find_leaf(&hdr, &cmp, key)?;
        let leaf = self.fetch_node(leaf_no)?;
        let d = leaf.read();
        let pos = node::lower_bound(&cmp, &hdr, &d[..], key);
        let size = node::num_keys(&d[..]);
        let next = node::next_leaf(&d[..]);
        drop(d);
        Ok(self.bound_iid(&hdr, leaf_no, pos, size, next))
    }

    /// 第一个 > key 的位置。
    pub fn upper_bound(&self, key: &[u8]) -> Result<Iid, EasyDbError> {
        self.check_key(key)?;
        let _r = read_lock(&self.tree_lock);
        let hdr = self.file_hdr();
        let cmp = KeyComparator::from_hdr(&hdr);
        let leaf_no = self.find_leaf(&hdr, &cmp, key)?;
        let leaf = self.fetch_node(leaf_no)?;
        let d = leaf.read();
        let pos = node::upper_bound(&cmp, &hdr, &d[..], key);
        let size = node::num_keys(&d[..]);
        let next = node::next_leaf(&d[..]);
        drop(d);
        Ok(self.bound_iid(&hdr, leaf_no, pos, size, next))
    }

    /// pos 到达本页末尾时的越界推进（与 C++ LowerBound/UpperBound 一致）。
    fn bound_iid(&self, hdr: &IxFileHdr, page_no: i32, pos: usize, size: usize, next: i32) -> Iid {
        if pos == size {
            if page_no == hdr.last_leaf {
                Iid::new(page_no, size as u32)
            } else {
                Iid::new(next, 0)
            }
        } else {
            Iid::new(page_no, pos as u32)
        }
    }

    /// 全表扫描起点。
    pub fn leaf_begin(&self) -> Iid {
        Iid::new(self.file_hdr().first_leaf, 0)
    }

    /// 全表扫描终点（最后一个叶子的末尾）。
    pub fn leaf_end(&self) -> Result<Iid, EasyDbError> {
        let hdr = self.file_hdr();
        let node = self.fetch_node(hdr.last_leaf)?;
        let size = node::num_keys(&node.read()[..]);
        Ok(Iid::new(hdr.last_leaf, size as u32))
    }

    /// 构造范围扫描迭代器（`[lower, upper)`）。
    pub fn scan(&self, lower: Iid, upper: Iid) -> crate::index::ix_scan::IxScan<'_> {
        crate::index::ix_scan::IxScan::new(self, lower, upper)
    }

    // ---- 插入 ----

    /// 插入 (key, rid)。键已存在时不插入，返回 `false`（对应 C++ 重复键
    /// 返回 -1 的语义）。
    pub fn insert_entry(&self, key: &[u8], rid: Rid) -> Result<bool, EasyDbError> {
        self.check_key(key)?;
        let _w = write_lock(&self.tree_lock);
        let mut hdr = self.file_hdr();
        let cmp = KeyComparator::from_hdr(&hdr);

        let leaf_no = self.find_leaf(&hdr, &cmp, key)?;
        let (inserted, at_front) = {
            let leaf = self.fetch_node(leaf_no)?;
            let mut d = leaf.write();
            let pos = node::lower_bound(&cmp, &hdr, &d[..], key);
            if pos < node::num_keys(&d[..])
                && cmp.compare(node::key_slice(&hdr, &d[..], pos), key) == Ordering::Equal
            {
                (false, false)
            } else {
                node::insert_pair(&hdr, &mut d[..], pos, key, rid);
                (true, pos == 0)
            }
        };
        if !inserted {
            return Ok(false);
        }
        // 插入成为叶子首键时，父链分隔键需同步（修复 C++ 版遗留问题：
        // C++ 的 InsertEntry 不做该维护，父节点键乱序后二分查找会失准）
        if at_front {
            self.maintain_parent(&hdr, leaf_no)?;
        }

        // 满则分裂，并把新节点首键插入父节点（可能级联）
        let need_split = {
            let leaf = self.fetch_node(leaf_no)?;
            node::num_keys(&leaf.read()[..]) >= hdr.max_keys()
        };
        if need_split {
            let new_no = self.split_node(&mut hdr, leaf_no)?;
            let first_key = self.key_at(&hdr, new_no, 0)?;
            self.insert_into_parent(&mut hdr, leaf_no, &first_key, new_no)?;
        }

        self.persist_hdr(&hdr)?;
        mutex_lock(&self.hdr).clone_from(&hdr);
        Ok(true)
    }

    /// 分裂 node 页：右半移入新页并维护链接/孩子指针，返回新页号。
    fn split_node(&self, hdr: &mut IxFileHdr, page_no: i32) -> Result<i32, EasyDbError> {
        let node = self.fetch_node(page_no)?;
        let (is_leaf_v, old_next, old_parent) = {
            let d = node.read();
            (
                node::is_leaf(&d[..]),
                node::next_leaf(&d[..]),
                node::parent(&d[..]),
            )
        };

        let (new_guard, new_no) = self.create_node(hdr)?;
        {
            let mut d = node.write();
            let mut nd = new_guard.write();
            let total = node::num_keys(&d[..]);
            let split = total / 2;
            let new_num = total - split;

            node::set_is_leaf(&mut nd[..], is_leaf_v);
            node::set_parent(&mut nd[..], old_parent);
            // 右半键值对移入新节点
            for i in 0..new_num {
                let k = node::key_slice(hdr, &d[..], split + i).to_vec();
                let r = node::get_rid(hdr, &d[..], split + i);
                node::set_key(hdr, &mut nd[..], i, &k);
                node::set_rid(hdr, &mut nd[..], i, r);
            }
            node::set_num_keys(&mut nd[..], new_num);
            node::set_num_keys(&mut d[..], split);
        }
        drop(new_guard);

        if is_leaf_v {
            // 叶子：维护双向链表 node <-> new <-> old_next
            // （先取齐 guard 再加数据锁：不得持帧锁调用缓冲池）
            let new_leaf = self.fetch_node(new_no)?;
            {
                let mut d = node.write();
                let mut ndd = new_leaf.write();
                node::set_next_leaf(&mut ndd[..], old_next);
                node::set_prev_leaf(&mut ndd[..], page_no);
                node::set_next_leaf(&mut d[..], new_no);
            }
            drop(new_leaf);
            if old_next != IX_NO_PAGE {
                let next_node = self.fetch_node(old_next)?;
                node::set_prev_leaf(&mut next_node.write()[..], new_no);
            }
            if page_no == hdr.last_leaf {
                hdr.last_leaf = new_no;
            }
        } else {
            // 内部节点：修正新节点各孩子的父指针
            let children = {
                let nd = self.fetch_node(new_no)?;
                let d = nd.read();
                (0..node::num_keys(&d[..]))
                    .map(|i| node::value_at(hdr, &d[..], i))
                    .collect::<Vec<_>>()
            };
            for child_no in children {
                self.maintain_child(new_no, child_no)?;
            }
        }
        Ok(new_no)
    }

    /// 分裂后把 (key -> new_page) 插入 old_page 的父节点；父节点满则级联
    /// 分裂；old 为根则创建新根（对应 C++ `InsertIntoParent`）。
    fn insert_into_parent(
        &self,
        hdr: &mut IxFileHdr,
        old_no: i32,
        key: &[u8],
        new_no: i32,
    ) -> Result<(), EasyDbError> {
        let old = self.fetch_node(old_no)?;
        let old_parent = node::parent(&old.read()[..]);

        if old_parent == IX_NO_PAGE {
            // 创建新根：(old_first -> old), (key -> new)
            let (root_guard, root_no) = self.create_node(hdr)?;
            {
                let old_first = node::key_slice(hdr, &old.read()[..], 0).to_vec();
                let mut rd = root_guard.write();
                node::set_is_leaf(&mut rd[..], false);
                node::set_parent(&mut rd[..], IX_NO_PAGE);
                node::insert_pair(hdr, &mut rd[..], 0, &old_first, Rid::new(old_no, 0));
                node::insert_pair(hdr, &mut rd[..], 1, key, Rid::new(new_no, 0));
            }
            {
                let mut od = old.write();
                node::set_parent(&mut od[..], root_no);
            }
            let new = self.fetch_node(new_no)?;
            {
                let mut nd = new.write();
                node::set_parent(&mut nd[..], root_no);
            }
            hdr.root_page = root_no;
            return Ok(());
        }
        drop(old);

        let parent = self.fetch_node(old_parent)?;
        let need_split = {
            let mut pd = parent.write();
            let pos = node::find_child(hdr, &pd[..], old_no)? + 1;
            node::insert_pair(hdr, &mut pd[..], pos, key, Rid::new(new_no, 0));
            node::num_keys(&pd[..]) >= hdr.max_keys()
        };
        if need_split {
            let new_sibling = self.split_node(hdr, old_parent)?;
            let mid_key = self.key_at(hdr, new_sibling, 0)?;
            self.insert_into_parent(hdr, old_parent, &mid_key, new_sibling)?;
        }
        Ok(())
    }

    // ---- 删除 ----

    /// 删除 key 对应的键值对；不存在返回 `false`。
    pub fn delete_entry(&self, key: &[u8]) -> Result<bool, EasyDbError> {
        self.check_key(key)?;
        let _w = write_lock(&self.tree_lock);
        let mut hdr = self.file_hdr();
        let cmp = KeyComparator::from_hdr(&hdr);

        let leaf_no = self.find_leaf(&hdr, &cmp, key)?;
        let removed = {
            let leaf = self.fetch_node(leaf_no)?;
            let mut d = leaf.write();
            let pos = node::lower_bound(&cmp, &hdr, &d[..], key);
            if pos < node::num_keys(&d[..])
                && cmp.compare(node::key_slice(&hdr, &d[..], pos), key) == Ordering::Equal
            {
                node::erase_pair(&hdr, &mut d[..], pos);
                true
            } else {
                false
            }
        };
        if !removed {
            return Ok(false);
        }

        self.coalesce_or_redistribute(&mut hdr, leaf_no)?;

        self.persist_hdr(&hdr)?;
        mutex_lock(&self.hdr).clone_from(&hdr);
        Ok(true)
    }

    /// 删除后处理下溢：调整根 / 直接返回 / 重分配 / 合并（对应 C++
    /// `CoalesceOrRedistribute`）。返回该页是否应被删除。
    fn coalesce_or_redistribute(
        &self,
        hdr: &mut IxFileHdr,
        page_no: i32,
    ) -> Result<bool, EasyDbError> {
        let node = self.fetch_node(page_no)?;
        let (parent_no, size) = {
            let d = node.read();
            (node::parent(&d[..]), node::num_keys(&d[..]))
        };

        // 根节点：只做根调整（叶根允许为空，保持树头有效）
        if parent_no == IX_NO_PAGE {
            drop(node);
            return self.adjust_root(hdr, page_no);
        }
        // 未下溢：维护父节点首键即可
        if size >= hdr.min_keys() {
            drop(node);
            self.maintain_parent(hdr, page_no)?;
            return Ok(false);
        }
        drop(node);

        // 找兄弟（优先前驱）
        let parent = self.fetch_node(parent_no)?;
        let (node_index, sibling_no) = {
            let d = parent.read();
            let idx = node::find_child(hdr, &d[..], page_no)?;
            let sib_idx = if idx == 0 { 1 } else { idx - 1 };
            (idx, node::value_at(hdr, &d[..], sib_idx))
        };
        drop(parent);

        let sibling = self.fetch_node(sibling_no)?;
        let sib_size = node::num_keys(&sibling.read()[..]);
        drop(sibling);

        if size + sib_size >= 2 * hdr.min_keys() {
            // 重分配：从兄弟借一个键值对
            self.redistribute(hdr, sibling_no, page_no, node_index)?;
            Ok(false)
        } else {
            // 合并：node 并入左兄弟，右节点删除；父节点可能级联下溢
            let (left_no, right_no, sep_index) = if node_index == 0 {
                (page_no, sibling_no, 1)
            } else {
                (sibling_no, page_no, node_index)
            };
            self.coalesce_nodes(hdr, left_no, right_no, parent_no, sep_index)?;
            self.release_node(hdr, right_no)?;
            // 合并后左节点的首键可能已变（被删键恰为左节点首键），先同步父链
            // （修复 C++ 版同类遗留问题），再处理父节点的级联下溢
            self.maintain_parent(hdr, left_no)?;
            self.coalesce_or_redistribute(hdr, parent_no)
        }
    }

    /// 根调整：内部根仅剩一个孩子时孩子升为新根；叶根允许为空（保持
    /// 有效树头，修复 C++ 清空后 root 悬空的问题）。返回旧根是否删除。
    fn adjust_root(&self, hdr: &mut IxFileHdr, root_no: i32) -> Result<bool, EasyDbError> {
        let node = self.fetch_node(root_no)?;
        let (is_leaf_v, size, only_child) = {
            let d = node.read();
            (
                node::is_leaf(&d[..]),
                node::num_keys(&d[..]),
                node::value_at(hdr, &d[..], 0),
            )
        };
        if !is_leaf_v && size == 1 {
            let child = self.fetch_node(only_child)?;
            {
                let mut cd = child.write();
                node::set_parent(&mut cd[..], IX_NO_PAGE);
            }
            hdr.root_page = only_child;
            drop(node);
            self.release_node(hdr, root_no)?;
            return Ok(true);
        }
        Ok(false)
    }

    /// 从兄弟页借一个键值对（对应 C++ `Redistribute`）。
    /// `node_index == 0` 表示兄弟是后继，否则兄弟是前驱。
    fn redistribute(
        &self,
        hdr: &mut IxFileHdr,
        sibling_no: i32,
        node_no: i32,
        node_index: usize,
    ) -> Result<(), EasyDbError> {
        if node_index > 0 {
            // 前驱兄弟 -> node：前驱的最后一个对移到 node 首部
            let sibling = self.fetch_node(sibling_no)?;
            let (k, r, sib_last) = {
                let sd = sibling.read();
                let last = node::num_keys(&sd[..]) - 1;
                (
                    node::key_slice(hdr, &sd[..], last).to_vec(),
                    node::get_rid(hdr, &sd[..], last),
                    last,
                )
            };
            {
                let node = self.fetch_node(node_no)?;
                let mut nd = node.write();
                node::insert_pair(hdr, &mut nd[..], 0, &k, r);
            }
            {
                let mut sd = sibling.write();
                node::erase_pair(hdr, &mut sd[..], sib_last);
            }
            drop(sibling);
            self.maintain_parent(hdr, node_no)?;
            let is_leaf = self.is_leaf_page(node_no)?;
            if !is_leaf {
                let child = self.child_at(hdr, node_no, 0)?;
                self.maintain_child(node_no, child)?;
            }
        } else {
            // node -> 后继兄弟：后继的第一个对移到 node 末尾
            let sibling = self.fetch_node(sibling_no)?;
            let (k, r) = {
                let sd = sibling.read();
                (
                    node::key_slice(hdr, &sd[..], 0).to_vec(),
                    node::get_rid(hdr, &sd[..], 0),
                )
            };
            {
                let node = self.fetch_node(node_no)?;
                let mut nd = node.write();
                let pos = node::num_keys(&nd[..]);
                node::insert_pair(hdr, &mut nd[..], pos, &k, r);
            }
            {
                let mut sd = sibling.write();
                node::erase_pair(hdr, &mut sd[..], 0);
            }
            drop(sibling);
            // node 的首键可能因删除而变化（被删键恰为其首键），需一并同步
            // （修复 C++ 版遗留问题：C++ 的 Redistribute 只维护 neighbor）
            self.maintain_parent(hdr, node_no)?;
            self.maintain_parent(hdr, sibling_no)?;
            let is_leaf = self.is_leaf_page(node_no)?;
            if !is_leaf {
                let last = {
                    let n = self.fetch_node(node_no)?;
                    node::num_keys(&n.read()[..]) - 1
                };
                let child = self.child_at(hdr, node_no, last)?;
                self.maintain_child(node_no, child)?;
            }
        }
        Ok(())
    }

    /// 把 right 页的内容并入 left 页，并从父节点摘除分隔项
    /// （对应 C++ `Coalesce` 的主体；父节点级联由调用方处理）。
    fn coalesce_nodes(
        &self,
        hdr: &mut IxFileHdr,
        left_no: i32,
        right_no: i32,
        parent_no: i32,
        sep_index: usize,
    ) -> Result<(), EasyDbError> {
        let left = self.fetch_node(left_no)?;
        let right = self.fetch_node(right_no)?;
        let is_leaf_v = node::is_leaf(&left.read()[..]);
        let right_next;
        {
            let mut ld = left.write();
            let rd = right.read();
            let start = node::num_keys(&ld[..]);
            let n = node::num_keys(&rd[..]);
            for i in 0..n {
                let k = node::key_slice(hdr, &rd[..], i).to_vec();
                let r = node::get_rid(hdr, &rd[..], i);
                node::set_key(hdr, &mut ld[..], start + i, &k);
                node::set_rid(hdr, &mut ld[..], start + i, r);
            }
            node::set_num_keys(&mut ld[..], start + n);
            right_next = node::next_leaf(&rd[..]);
        }
        if is_leaf_v {
            // 摘除 right：left.next = right.next；right.next.prev = left
            {
                let mut ld = left.write();
                node::set_next_leaf(&mut ld[..], right_next);
            }
            if right_next != IX_NO_PAGE {
                let next_node = self.fetch_node(right_next)?;
                node::set_prev_leaf(&mut next_node.write()[..], left_no);
            }
            if right_no == hdr.last_leaf {
                hdr.last_leaf = left_no;
            }
            if right_no == hdr.first_leaf {
                hdr.first_leaf = right_next;
            }
        } else {
            // 修正搬移后孩子的父指针
            let children = {
                let ld = left.read();
                (0..node::num_keys(&ld[..]))
                    .map(|i| node::value_at(hdr, &ld[..], i))
                    .collect::<Vec<_>>()
            };
            for child_no in children {
                self.maintain_child(left_no, child_no)?;
            }
        }
        drop(left);
        drop(right);

        // 父节点摘除分隔项
        let parent = self.fetch_node(parent_no)?;
        {
            let mut pd = parent.write();
            node::erase_pair(hdr, &mut pd[..], sep_index);
        }
        Ok(())
    }

    // ---- 结构维护辅助 ----

    /// 从 node 起向上同步父节点的分隔键（key[i] = 子树首键）。
    fn maintain_parent(&self, hdr: &IxFileHdr, page_no: i32) -> Result<(), EasyDbError> {
        let mut cur = page_no;
        loop {
            let node = self.fetch_node(cur)?;
            let parent_no = node::parent(&node.read()[..]);
            if parent_no == IX_NO_PAGE {
                return Ok(());
            }
            let parent = self.fetch_node(parent_no)?;
            let changed = {
                let mut pd = parent.write();
                let cd = node.read();
                let rank = node::find_child(hdr, &pd[..], cur)?;
                let child_first = node::key_slice(hdr, &cd[..], 0);
                if node::key_slice(hdr, &pd[..], rank) == child_first {
                    false
                } else {
                    let k = child_first.to_vec();
                    node::set_key(hdr, &mut pd[..], rank, &k);
                    true
                }
            };
            if !changed {
                return Ok(());
            }
            cur = parent_no;
        }
    }

    /// 设置孩子页的父指针。
    fn maintain_child(&self, parent_no: i32, child_no: i32) -> Result<(), EasyDbError> {
        let child = self.fetch_node(child_no)?;
        {
            let mut cd = child.write();
            node::set_parent(&mut cd[..], parent_no);
        }
        Ok(())
    }

    fn is_leaf_page(&self, page_no: i32) -> Result<bool, EasyDbError> {
        let node = self.fetch_node(page_no)?;
        Ok(node::is_leaf(&node.read()[..]))
    }

    /// 读取页首键（拷贝）。
    fn key_at(&self, hdr: &IxFileHdr, page_no: i32, i: usize) -> Result<Vec<u8>, EasyDbError> {
        let node = self.fetch_node(page_no)?;
        Ok(node::key_slice(hdr, &node.read()[..], i).to_vec())
    }

    /// 读取内部节点第 i 个孩子页号。
    fn child_at(&self, hdr: &IxFileHdr, page_no: i32, i: usize) -> Result<i32, EasyDbError> {
        let node = self.fetch_node(page_no)?;
        Ok(node::value_at(hdr, &node.read()[..], i))
    }

    /// 页内键数（扫描推进用）。
    pub(crate) fn page_num_keys(&self, page_no: i32) -> Result<usize, EasyDbError> {
        let node = self.fetch_node(page_no)?;
        Ok(node::num_keys(&node.read()[..]))
    }

    /// 页的后继叶子页号（扫描推进用）。
    pub(crate) fn page_next_leaf(&self, page_no: i32) -> Result<i32, EasyDbError> {
        let node = self.fetch_node(page_no)?;
        Ok(node::next_leaf(&node.read()[..]))
    }

    // ---- 页管理 ----

    fn fetch_node(&self, page_no: i32) -> Result<PageGuard<'_>, EasyDbError> {
        self.bpm.fetch_page(PageId::new(self.fd, page_no))
    }

    /// 分配节点页：优先复用空闲链表页，否则新分配（页号高水位 +1）。
    /// 返回的页已清零。
    fn create_node(&self, hdr: &mut IxFileHdr) -> Result<(PageGuard<'_>, i32), EasyDbError> {
        if hdr.first_free != IX_NO_PAGE {
            let page_no = hdr.first_free;
            let guard = self.fetch_node(page_no)?;
            {
                let mut d = guard.write();
                let next = node::next_free(&d[..]);
                hdr.first_free = next;
                d[..].fill(0);
            }
            Ok((guard, page_no))
        } else {
            let (guard, page_no) = self.bpm.new_page(self.fd)?;
            hdr.num_pages += 1;
            Ok((guard, page_no))
        }
    }

    /// 释放节点页：链入空闲链表（内容保留以便调试，重用时会清零）。
    fn release_node(&self, hdr: &mut IxFileHdr, page_no: i32) -> Result<(), EasyDbError> {
        let guard = self.fetch_node(page_no)?;
        {
            let mut d = guard.write();
            node::set_next_free(&mut d[..], hdr.first_free);
            node::set_num_keys(&mut d[..], 0);
        }
        hdr.first_free = page_no;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::ix_manager::IxManager;
    use crate::types::ColType;

    pub(crate) fn setup(
        tag: &str,
        cols: &[(ColType, i32)],
    ) -> (tempfile::TempDir, Arc<IxIndexHandle>) {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join(tag);
        let disk = Arc::new(DiskManager::new(&db).unwrap());
        let bpm = Arc::new(BufferPool::new(64, disk.clone()));
        let ix = IxManager::new(disk, bpm);
        let path = db.join("t.idx");
        ix.create_index(&path, cols).unwrap();
        (dir, ix.open_index(&path).unwrap())
    }

    fn int_key(k: i32) -> Vec<u8> {
        k.to_le_bytes().to_vec()
    }

    #[test]
    fn insert_get_delete_basic() {
        let (_dir, ih) = setup("basic", &[(ColType::Int, 4)]);
        let n = 1000;
        for i in 0..n {
            assert!(
                ih.insert_entry(&int_key(i), Rid::new(i / 10, (i % 10) as u32))
                    .unwrap()
            );
        }
        // 重复插入拒绝
        assert!(!ih.insert_entry(&int_key(42), Rid::new(0, 0)).unwrap());
        // 精确查找
        for i in 0..n {
            let rid = ih.get_value(&int_key(i)).unwrap().expect("key must exist");
            assert_eq!(rid, Rid::new(i / 10, (i % 10) as u32));
        }
        assert!(ih.get_value(&int_key(-1)).unwrap().is_none());
        assert!(ih.get_value(&int_key(n)).unwrap().is_none());

        // 全表扫描
        let got: Vec<i32> = {
            let begin = ih.leaf_begin();
            let end = ih.leaf_end().unwrap();
            ih.scan(begin, end)
                .map(|r| r.page_id() * 10 + r.slot_num() as i32)
                .collect()
        };
        assert_eq!(got.len(), n as usize);
        assert_eq!(got, (0..n).collect::<Vec<_>>());

        // 删除一半，剩余校验
        for i in (0..n).step_by(2) {
            assert!(ih.delete_entry(&int_key(i)).unwrap());
        }
        assert!(!ih.delete_entry(&int_key(0)).unwrap());
        for i in 0..n {
            let hit = ih.get_value(&int_key(i)).unwrap().is_some();
            assert_eq!(hit, i % 2 == 1, "key {i}");
        }
    }

    #[test]
    fn lower_and_upper_bounds() {
        let (_dir, ih) = setup("bounds", &[(ColType::Int, 4)]);
        for &k in &[10, 20, 20, 30, 40, 50] {
            ih.insert_entry(&int_key(k), Rid::new(k, 0))
                .unwrap_or_else(|e| panic!("insert {k}: {e}"));
            // 第二个 20 是重复键，预期 false
            if k == 20 {
                assert!(!ih.insert_entry(&int_key(k), Rid::new(k, 1)).unwrap());
            }
        }
        assert_eq!(ih.lower_bound(&int_key(20)).unwrap().slot, 1);
        assert_eq!(ih.upper_bound(&int_key(20)).unwrap().slot, 2);
        // 范围 [20, 40)
        let lo = ih.lower_bound(&int_key(20)).unwrap();
        let up = ih.lower_bound(&int_key(40)).unwrap();
        let rids: Vec<i32> = ih.scan(lo, up).map(|r| r.page_id()).collect();
        assert_eq!(rids, vec![20, 30]);
        // 空范围
        let lo = ih.lower_bound(&int_key(21)).unwrap();
        let up = ih.upper_bound(&int_key(29)).unwrap();
        assert_eq!(ih.scan(lo, up).count(), 0);
    }

    #[test]
    fn reopen_persists_tree() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("reopen");
        let disk = Arc::new(DiskManager::new(&db).unwrap());
        let bpm = Arc::new(BufferPool::new(64, disk.clone()));
        let ix = IxManager::new(disk, bpm);
        let path = db.join("t.idx");
        ix.create_index(&path, &[(ColType::Int, 4)]).unwrap();
        {
            let ih = ix.open_index(&path).unwrap();
            for i in 0..500 {
                ih.insert_entry(&int_key(i), Rid::new(i, 0)).unwrap();
            }
            ih.flush_header().unwrap();
            // 刷盘所有脏页
            ih.bpm().flush_file_pages(ih.fd()).unwrap();
            ix.disk().close_file(ih.fd()).unwrap();
        }
        {
            let ih = ix.open_index(&path).unwrap();
            for i in 0..500 {
                assert!(ih.get_value(&int_key(i)).unwrap().is_some(), "key {i} lost");
            }
        }
    }
}

#[cfg(test)]
mod property_tests {
    use super::node;
    use super::tests::setup;
    use super::*;
    use crate::index::ix_defs::encode_key;
    use crate::types::{ColType, Value};
    use std::collections::{BTreeMap, HashMap, HashSet};

    /// 确定性 LCG 随机源（避免引入 rand 依赖）。
    struct Lcg(u64);
    impl Lcg {
        fn new(seed: u64) -> Self {
            Self(seed | 1)
        }
        fn below(&mut self, n: u64) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (self.0 >> 16) % n
        }
    }

    /// 小阶数索引：Int(4) + VarChar(504) = 508 字节键 → max_keys = 7，
    /// 频繁触发分裂/合并/重分配。
    fn stress_setup(tag: &str) -> (tempfile::TempDir, Arc<IxIndexHandle>) {
        setup(tag, &[(ColType::Int, 4), (ColType::VarChar, 504)])
    }

    fn stress_key(k: i32) -> Vec<u8> {
        encode_key(
            &[ColType::Int, ColType::VarChar],
            &[4, 504],
            &[Value::Int(k), Value::Str(String::new())],
        )
        .unwrap()
    }

    fn stress_rid(k: i32) -> Rid {
        // 无损编码：页号 = k，便于扫描结果与键直接比对
        Rid::new(k, (k % 13) as u32)
    }

    /// 校验 B+ 树全部结构不变量，并返回叶子层 (key -> rid) 快照。
    fn validate(ih: &IxIndexHandle) -> HashMap<i32, Rid> {
        let hdr = ih.file_hdr();
        let cmp = KeyComparator::from_hdr(&hdr);
        let max = hdr.max_keys();
        let min = hdr.min_keys();
        let mut visited = HashSet::new();
        let mut leaf_pages_in_order = Vec::new();
        let mut map = HashMap::new();

        // 递归下降：返回 (本子树键数, 首键)
        struct Ctx<'a> {
            ih: &'a IxIndexHandle,
            hdr: &'a IxFileHdr,
            cmp: &'a KeyComparator,
        }

        // 递归下降：返回本子树首键
        fn walk(
            c: &Ctx<'_>,
            page_no: i32,
            is_root: bool,
            visited: &mut HashSet<i32>,
            leaf_pages: &mut Vec<i32>,
            map: &mut HashMap<i32, Rid>,
        ) -> Vec<u8> {
            let (ih, hdr, cmp) = (c.ih, c.hdr, c.cmp);
            assert!(visited.insert(page_no), "page {page_no} visited twice");
            let node = ih.bpm().fetch_page(PageId::new(ih.fd(), page_no)).unwrap();
            let d = node.read();
            let n = node::num_keys(&d[..]);
            assert!(n <= hdr.max_keys(), "node {page_no} overfull: {n}");
            if !is_root {
                assert!(n >= hdr.min_keys(), "node {page_no} underfull: {n}");
            }
            for i in 1..n {
                assert!(
                    cmp.compare(
                        node::key_slice(hdr, &d[..], i - 1),
                        node::key_slice(hdr, &d[..], i)
                    ) == std::cmp::Ordering::Less,
                    "node {page_no} keys not strictly ascending at {i}"
                );
            }
            let first_key = if n > 0 {
                node::key_slice(hdr, &d[..], 0).to_vec()
            } else {
                Vec::new()
            };
            if node::is_leaf(&d[..]) {
                leaf_pages.push(page_no);
                for i in 0..n {
                    let k = node::key_slice(hdr, &d[..], i).to_vec();
                    let int_part = i32::from_le_bytes(k[..4].try_into().unwrap());
                    assert!(
                        map.insert(int_part, node::get_rid(hdr, &d[..], i))
                            .is_none()
                    );
                }
                drop(d);
                return first_key;
            }
            // 内部节点：key[i] == 孩子 i 的首键，孩子父指针指回本页
            for i in 0..n {
                let child_no = node::value_at(hdr, &d[..], i);
                let child_first = walk(c, child_no, false, visited, leaf_pages, map);
                assert_eq!(
                    node::key_slice(hdr, &d[..], i),
                    &child_first[..],
                    "separator {i} in page {page_no} != child {child_no} first key"
                );
            }
            drop(d);
            first_key
        }

        let ctx = Ctx {
            ih,
            hdr: &hdr,
            cmp: &cmp,
        };
        walk(
            &ctx,
            hdr.root_page,
            true,
            &mut visited,
            &mut leaf_pages_in_order,
            &mut map,
        );

        // 叶子链表：从 first_leaf 走 next 应与递归发现的叶子顺序一致
        let mut chain = Vec::new();
        let mut cur = hdr.first_leaf;
        let mut prev = IX_NO_PAGE;
        loop {
            assert_ne!(cur, IX_NO_PAGE, "leaf chain ended before last_leaf");
            let node = ih.bpm().fetch_page(PageId::new(ih.fd(), cur)).unwrap();
            let d = node.read();
            assert_eq!(node::prev_leaf(&d[..]), prev, "leaf {cur} prev broken");
            prev = cur;
            let next = node::next_leaf(&d[..]);
            drop(d);
            chain.push(cur);
            if cur == hdr.last_leaf {
                assert_eq!(next, IX_NO_PAGE);
                break;
            }
            cur = next;
        }
        assert_eq!(chain, leaf_pages_in_order, "leaf chain order != tree order");

        // 页数高水位 = 访问的节点页 + 空闲页 + 文件头页
        let mut free_cnt = 0;
        let mut cur = hdr.first_free;
        while cur != IX_NO_PAGE {
            assert!(visited.insert(cur), "page {cur} both live and free");
            free_cnt += 1;
            let node = ih.bpm().fetch_page(PageId::new(ih.fd(), cur)).unwrap();
            cur = node::next_free(&node.read()[..]);
        }
        assert_eq!(
            hdr.num_pages as usize,
            visited.len() + 1,
            "num_pages != live+free+hdr"
        );
        let _ = (max, min, free_cnt);
        map
    }

    #[test]
    fn property_test_against_btreemap() {
        let (_dir, ih) = stress_setup("prop");
        let mut model: BTreeMap<i32, Rid> = BTreeMap::new();
        let mut rng = Lcg::new(2026_1011);
        const KEY_SPACE: u64 = 800;
        const OPS: u64 = 6000;

        for op in 0..OPS {
            let k = rng.below(KEY_SPACE) as i32;
            match rng.below(100) {
                0..=44 => {
                    // 插入
                    let inserted = ih.insert_entry(&stress_key(k), stress_rid(k)).unwrap();
                    assert_eq!(inserted, !model.contains_key(&k), "op{op} insert {k}");
                    model.insert(k, stress_rid(k));
                }
                45..=79 => {
                    // 删除
                    let removed = ih.delete_entry(&stress_key(k)).unwrap();
                    assert_eq!(removed, model.remove(&k).is_some(), "op{op} delete {k}");
                }
                _ => {
                    // 精确查找
                    let got = ih.get_value(&stress_key(k)).unwrap();
                    assert_eq!(got, model.get(&k).copied(), "op{op} lookup {k}");
                }
            }
            // 每 500 步做一次全量校验
            if op % 500 == 499 {
                let snapshot = validate(&ih);
                assert_eq!(snapshot.len(), model.len(), "op{}", op + 1);
                for (k, v) in &model {
                    assert_eq!(snapshot.get(k), Some(v), "op{} key {k}", op + 1);
                }
            }
        }

        // 终态：全量不变量 + 全表扫描逐键比对
        let snapshot = validate(&ih);
        assert_eq!(snapshot.len(), model.len());
        for (k, v) in &model {
            assert_eq!(snapshot.get(k), Some(v));
        }
        let begin = ih.leaf_begin();
        let end = ih.leaf_end().unwrap();
        let scanned: Vec<i32> = ih.scan(begin, end).map(|r| r.page_id()).collect();
        let expect: Vec<i32> = model.keys().copied().collect();
        assert_eq!(scanned, expect);
        // 范围扫描若干组
        for _ in 0..50 {
            let a = rng.below(KEY_SPACE) as i32;
            let b = a + rng.below(100) as i32;
            let lo = ih.lower_bound(&stress_key(a)).unwrap();
            let up = ih.lower_bound(&stress_key(b)).unwrap();
            let got: Vec<i32> = ih.scan(lo, up).map(|r| r.page_id()).collect();
            let want: Vec<i32> = model.range(a..b).map(|(&k, _)| k).collect();
            assert_eq!(got, want, "range [{a},{b})");
        }
    }
}
