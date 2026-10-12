//! 表数据文件句柄：页面级与记录级操作。
//!
//! 对应 C++ 版 `src-cpp/record/rm_file_handle.cpp`。
//!
//! 与 C++ 版的差异：
//! - 页面的 pin/unpin 全部由 [`PageGuard`](crate::buffer::buffer_pool::PageGuard)
//!   RAII 管理，本文件不再出现任何手工 unpin（C++ 版每个方法都要配对 unpin）；
//! - C++ 通过 `Context*` 隐式调用锁管理器的钩子，事务/锁在 P6 接入，
//!   此处暂无该参数；
//! - 逻辑删除/恢复时同步维护 `num_deleted_records`（C++ 版只增不减，
//!   计数会漂移）；
//! - 原地更新在槽目录中记录**新**长度（C++ 版保留旧长度，读到的是
//!   新数据 + 旧长度，属于隐患）。

use std::sync::{Arc, Mutex};

use crate::buffer::buffer_pool::BufferPool;
use crate::common::config::PAGE_SIZE;
use crate::common::errors::EasyDbError;
use crate::common::rid::Rid;
use crate::record::rm_defs::{
    self, RM_FILE_HDR_PAGE, RM_NO_PAGE, RmFileHdr, TupleMeta, page_num_deleted, page_num_records,
    read_tuple_info, set_page_next_page_id, set_page_num_deleted, set_page_num_records,
    write_tuple_info,
};
use crate::storage::disk::DiskManager;
use crate::storage::page::PageId;

/// 表数据文件句柄：一个文件对应一张表的数据。
pub struct RmFileHandle {
    disk: Arc<DiskManager>,
    bpm: Arc<BufferPool>,
    fd: i32,
    /// 文件头缓存（页 0 的前 8 字节）
    hdr: Mutex<RmFileHdr>,
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl RmFileHandle {
    /// 打开已存在的表数据文件：读入文件头，并把磁盘管理器的页号分配
    /// 起点对齐到文件头记录的页数。
    pub fn open(
        disk: Arc<DiskManager>,
        bpm: Arc<BufferPool>,
        fd: i32,
    ) -> Result<Self, EasyDbError> {
        let mut page = vec![0u8; PAGE_SIZE];
        disk.read_page(fd, RM_FILE_HDR_PAGE, &mut page)?;
        let hdr = RmFileHdr::from_bytes(&page[..RmFileHdr::SIZE]);
        disk.set_fd2pageno(fd, hdr.num_pages);
        Ok(Self {
            disk,
            bpm,
            fd,
            hdr: Mutex::new(hdr),
        })
    }

    pub fn fd(&self) -> i32 {
        self.fd
    }

    /// 文件头快照。
    pub fn file_hdr(&self) -> RmFileHdr {
        *lock(&self.hdr)
    }

    /// 刷盘：本文件在缓冲池中的所有页 + 文件头。
    /// 关闭表（drop/迁移 fd 复用前）时必须调用（对应 C++ 侧
    /// `FlushAllPages(fd)` + 写文件头的流程）。
    pub fn flush_all(&self) -> Result<(), EasyDbError> {
        self.bpm.flush_file_pages(self.fd)?;
        self.flush_header()
    }

    /// 把文件头写回第 0 页。
    pub fn flush_header(&self) -> Result<(), EasyDbError> {
        let hdr = self.file_hdr();
        let mut page = vec![0u8; PAGE_SIZE];
        page[..RmFileHdr::SIZE].copy_from_slice(&hdr.to_bytes());
        self.disk.write_page(self.fd, RM_FILE_HDR_PAGE, &page)
    }

    // ---- 记录级操作 ----

    /// 插入一条记录：自动分配 RID，必要时链接新页。
    pub fn insert_tuple(&self, meta: TupleMeta, data: &[u8]) -> Result<Rid, EasyDbError> {
        let mut page_no = self.file_hdr().first_free_page_no;
        loop {
            if page_no == RM_NO_PAGE {
                page_no = self.create_new_page()?;
            }
            match self.try_insert_on_page(page_no, meta, data)? {
                Some(slot) => return Ok(Rid::new(page_no, slot as u32)),
                None => {
                    // 页满：空页仍放不下说明元组过大
                    if page_num_records(&self.read_page_data(page_no)?) == 0 {
                        return Err(EasyDbError::InvalidRecordSize(data.len() as i32));
                    }
                    let next = self.create_new_page()?;
                    self.set_next_page_id(page_no, next)?;
                    page_no = next;
                }
            }
        }
    }

    /// 恢复重放：在确切的 rid 处重做插入。
    /// 页按需创建（重放按原始顺序进行，页号/槽号与原执行完全一致）。
    pub fn recover_insert(&self, rid: Rid, data: &[u8]) -> Result<(), EasyDbError> {
        self.recover_insert_with_meta(rid, TupleMeta::default(), data)
    }

    /// 恢复重放：在确切 RID 处重做插入，并保留版本元数据。
    pub fn recover_insert_with_meta(
        &self,
        rid: Rid,
        meta: TupleMeta,
        data: &[u8],
    ) -> Result<(), EasyDbError> {
        while self.file_hdr().num_pages <= rid.page_id() {
            self.create_new_page()?;
        }
        let page = self
            .bpm
            .fetch_page(crate::storage::page::PageId::new(self.fd, rid.page_id()))?;
        let mut d = page.write();
        let slot = rid.slot_num() as usize;
        let num_records = page_num_records(&d[..]) as usize;
        let (offset, size, slot_meta) = if slot < num_records {
            // 槽位已存在（原执行中先插后删再重放的场合）：直接覆写
            read_tuple_info(&d[..], slot)
        } else {
            // 追加（与原执行相同的生长路径）
            let slot_end: usize = if num_records > 0 {
                read_tuple_info(&d[..], num_records - 1).0 as usize
            } else {
                PAGE_SIZE
            };
            use crate::record::rm_defs::{TABLE_PAGE_HEADER_SIZE, TUPLE_INFO_SIZE};
            let dir_end = TABLE_PAGE_HEADER_SIZE + TUPLE_INFO_SIZE * (num_records + 1);
            if slot_end < dir_end + data.len() {
                return Err(EasyDbError::Internal(
                    "recover_insert: page layout diverged from original execution".to_string(),
                ));
            }
            (
                (slot_end - data.len()) as u16,
                data.len() as u16,
                meta,
            )
        };
        let slot_meta = if slot < num_records { meta } else { slot_meta };
        write_tuple_info(&mut d[..], slot, offset, size, slot_meta);
        if slot >= num_records {
            set_page_num_records(&mut d[..], slot as u16 + 1);
        }
        let off = offset as usize;
        d[off..off + data.len()].copy_from_slice(data);
        Ok(())
    }

    /// 回滚重写：按原始内容覆写某槽位（长度可与当前不同——槽内布局
    /// 不会移动，扩展只写回自己原来的区域）。
    pub fn rewrite_tuple(&self, rid: Rid, data: &[u8]) -> Result<(), EasyDbError> {
        let page = self
            .bpm
            .fetch_page(crate::storage::page::PageId::new(self.fd, rid.page_id()))?;
        let mut d = page.write();
        let slot = rid.slot_num() as usize;
        let num_records = page_num_records(&d[..]) as usize;
        if slot >= num_records {
            return Err(EasyDbError::Internal(
                "rewrite_tuple: slot out of range".to_string(),
            ));
        }
        let (offset, _, meta) = read_tuple_info(&d[..], slot);
        let off = offset as usize;
        if off + data.len() > PAGE_SIZE {
            return Err(EasyDbError::Internal(
                "rewrite_tuple: out of page".to_string(),
            ));
        }
        write_tuple_info(&mut d[..], slot, offset, data.len() as u16, meta);
        d[off..off + data.len()].copy_from_slice(data);
        Ok(())
    }

    /// 在指定位置恢复一条已删除的记录（事务回滚/恢复场景）。
    pub fn undelete_tuple(&self, rid: Rid, meta: TupleMeta) -> Result<(), EasyDbError> {
        let page = self.bpm.fetch_page(PageId::new(self.fd, rid.page_id()))?;
        let mut data = page.write();
        let slot = rid.slot_num() as usize;
        let num_records = page_num_records(&data[..]) as usize;
        if slot >= num_records {
            return Err(EasyDbError::Internal("tuple id out of range".to_string()));
        }
        let (offset, size, old_meta) = read_tuple_info(&data[..], slot);
        if !old_meta.is_deleted {
            return Err(EasyDbError::Internal(
                "undelete_tuple: tuple already exists".to_string(),
            ));
        }
        let deleted = page_num_deleted(&data[..]).saturating_sub(1);
        set_page_num_deleted(&mut data[..], deleted);
        write_tuple_info(
            &mut data[..],
            slot,
            offset,
            size,
            TupleMeta {
                xmin: meta.xmin,
                xmax: meta.xmax,
                prev: meta.prev,
                is_deleted: false,
            },
        );
        Ok(())
    }

    /// 逻辑删除一条记录（置删除标记，不回收空间）。
    pub fn delete_tuple(&self, rid: Rid) -> Result<(), EasyDbError> {
        self.mark_deleted(rid, crate::common::config::INVALID_TXN_ID)
    }

    /// 将当前版本标记为被某事务删除/替换，并保留其在版本链中的位置。
    pub fn mark_deleted(
        &self,
        rid: Rid,
        txn: crate::common::config::TxnId,
    ) -> Result<(), EasyDbError> {
        let page = self.bpm.fetch_page(PageId::new(self.fd, rid.page_id()))?;
        let mut data = page.write();
        let slot = rid.slot_num() as usize;
        let num_records = page_num_records(&data[..]) as usize;
        if slot >= num_records {
            return Err(EasyDbError::Internal("tuple id out of range".to_string()));
        }
        let (offset, size, mut meta) = read_tuple_info(&data[..], slot);
        if meta.is_deleted {
            return Err(EasyDbError::Internal(
                "delete_tuple: tuple already deleted".to_string(),
            ));
        }
        meta.is_deleted = true;
        if txn != crate::common::config::INVALID_TXN_ID {
            meta.xmax = Some(txn);
        }
        let deleted = page_num_deleted(&data[..]) + 1;
        set_page_num_deleted(&mut data[..], deleted);
        write_tuple_info(&mut data[..], slot, offset, size, meta);
        Ok(())
    }

    /// 原地更新记录（新数据长度不得超过旧长度）。
    pub fn update_tuple_in_place(
        &self,
        rid: Rid,
        meta: TupleMeta,
        data: &[u8],
    ) -> Result<(), EasyDbError> {
        let page = self.bpm.fetch_page(PageId::new(self.fd, rid.page_id()))?;
        let mut page_data = page.write();
        let slot = rid.slot_num() as usize;
        let num_records = page_num_records(&page_data[..]) as usize;
        if slot >= num_records {
            return Err(EasyDbError::Internal("tuple id out of range".to_string()));
        }
        let (offset, old_size, old_meta) = read_tuple_info(&page_data[..], slot);
        if old_size < data.len() as u16 {
            return Err(EasyDbError::Internal("tuple size mismatch".to_string()));
        }
        let meta = TupleMeta {
            xmin: meta.xmin,
            xmax: meta.xmax,
            prev: meta.prev,
            is_deleted: meta.is_deleted,
        };
        if !old_meta.is_deleted && meta.is_deleted {
            let deleted = page_num_deleted(&page_data[..]) + 1;
            set_page_num_deleted(&mut page_data[..], deleted);
        }
        write_tuple_info(&mut page_data[..], slot, offset, data.len() as u16, meta);
        let off = offset as usize;
        page_data[off..off + data.len()].copy_from_slice(data);
        Ok(())
    }

    /// 只更新记录元数据（如删除标记），不动数据。
    pub fn update_tuple_meta(&self, rid: Rid, meta: TupleMeta) -> Result<(), EasyDbError> {
        let page = self.bpm.fetch_page(PageId::new(self.fd, rid.page_id()))?;
        let mut data = page.write();
        let slot = rid.slot_num() as usize;
        let num_records = page_num_records(&data[..]) as usize;
        if slot >= num_records {
            return Err(EasyDbError::Internal("tuple id out of range".to_string()));
        }
        let (offset, size, old_meta) = read_tuple_info(&data[..], slot);
        if !old_meta.is_deleted && meta.is_deleted {
            let deleted = page_num_deleted(&data[..]) + 1;
            set_page_num_deleted(&mut data[..], deleted);
        }
        write_tuple_info(&mut data[..], slot, offset, size, meta);
        Ok(())
    }

    /// 读取一条记录，返回 (元数据, 数据)。
    pub fn get_tuple(&self, rid: Rid) -> Result<(TupleMeta, Vec<u8>), EasyDbError> {
        let page = self.bpm.fetch_page(PageId::new(self.fd, rid.page_id()))?;
        let data = page.read();
        let slot = rid.slot_num() as usize;
        let num_records = page_num_records(&data[..]) as usize;
        if slot >= num_records {
            return Err(EasyDbError::Internal("tuple id out of range".to_string()));
        }
        let (offset, size, meta) = read_tuple_info(&data[..], slot);
        let (offset, size) = (offset as usize, size as usize);
        Ok((meta, data[offset..offset + size].to_vec()))
    }

    /// 只读记录元数据。
    pub fn get_tuple_meta(&self, rid: Rid) -> Result<TupleMeta, EasyDbError> {
        let page = self.bpm.fetch_page(PageId::new(self.fd, rid.page_id()))?;
        let data = page.read();
        let slot = rid.slot_num() as usize;
        let num_records = page_num_records(&data[..]) as usize;
        if slot >= num_records {
            return Err(EasyDbError::Internal("tuple id out of range".to_string()));
        }
        let (_, _, meta) = read_tuple_info(&data[..], slot);
        Ok(meta)
    }

    /// 从指定版本沿 `prev` 指针向旧版本遍历，返回“新到旧”的版本链。
    pub fn version_chain(
        &self,
        rid: Rid,
    ) -> Result<Vec<(Rid, TupleMeta, Vec<u8>)>, EasyDbError> {
        let mut chain = Vec::new();
        let mut current = Some(rid);
        while let Some(version_rid) = current {
            let (meta, data) = self.get_tuple(version_rid)?;
            current = meta.prev;
            chain.push((version_rid, meta, data));
        }
        Ok(chain)
    }

    /// 指定槽位是否为已删除的有效记录。
    pub fn is_slot_deleted(&self, page_no: i32, slot: u32) -> Result<bool, EasyDbError> {
        let page = self.bpm.fetch_page(PageId::new(self.fd, page_no))?;
        let data = page.read();
        if slot as usize >= page_num_records(&data[..]) as usize {
            return Err(EasyDbError::Internal("tuple id out of range".to_string()));
        }
        Ok(read_tuple_info(&data[..], slot as usize).2.is_deleted)
    }

    /// 页内当前记录数。
    pub fn page_num_tuples(&self, page_no: i32) -> Result<u16, EasyDbError> {
        Ok(page_num_records(&self.read_page_data(page_no)?))
    }

    /// 设置页 LSN（P6 恢复使用）。
    pub fn set_page_lsn(&self, page_no: i32, lsn: i32) -> Result<(), EasyDbError> {
        let page = self.bpm.fetch_page(PageId::new(self.fd, page_no))?;
        rm_defs::set_page_lsn(&mut page.write()[..], lsn);
        Ok(())
    }

    // ---- 页面辅助 ----

    /// 在指定页尝试插入；空间不足返回 `None`。
    fn try_insert_on_page(
        &self,
        page_no: i32,
        meta: TupleMeta,
        tuple: &[u8],
    ) -> Result<Option<u16>, EasyDbError> {
        use crate::record::rm_defs::TABLE_PAGE_HEADER_SIZE;
        use crate::record::rm_defs::TUPLE_INFO_SIZE;

        let page = self.bpm.fetch_page(PageId::new(self.fd, page_no))?;
        let mut data = page.write();
        let num_records = page_num_records(&data[..]) as usize;

        // 与 C++ 一致：元组从页尾向前生长
        let slot_end: usize = if num_records > 0 {
            read_tuple_info(&data[..], num_records - 1).0 as usize
        } else {
            PAGE_SIZE
        };
        let dir_end = TABLE_PAGE_HEADER_SIZE + TUPLE_INFO_SIZE * (num_records + 1);
        if slot_end < dir_end + tuple.len() {
            return Ok(None);
        }
        let tuple_offset = (slot_end - tuple.len()) as u16;
        let slot = num_records as u16;
        write_tuple_info(
            &mut data[..],
            slot as usize,
            tuple_offset,
            tuple.len() as u16,
            meta,
        );
        set_page_num_records(&mut data[..], slot + 1);
        let off = tuple_offset as usize;
        data[off..off + tuple.len()].copy_from_slice(tuple);
        Ok(Some(slot))
    }

    /// 创建新数据页：初始化页头、更新文件头并落盘，返回新页号。
    fn create_new_page(&self) -> Result<i32, EasyDbError> {
        let (page, page_no) = self.bpm.new_page(self.fd)?;
        {
            let mut data = page.write();
            set_page_next_page_id(&mut data[..], RM_NO_PAGE);
            set_page_num_records(&mut data[..], 0);
            set_page_num_deleted(&mut data[..], 0);
        }
        {
            let mut hdr = lock(&self.hdr);
            hdr.num_pages += 1;
            hdr.first_free_page_no = page_no;
        }
        self.flush_header()?;
        Ok(page_no)
    }

    /// 设置页链接（当前页满后指向新空闲页）。
    fn set_next_page_id(&self, page_no: i32, next: i32) -> Result<(), EasyDbError> {
        let page = self.bpm.fetch_page(PageId::new(self.fd, page_no))?;
        set_page_next_page_id(&mut page.write()[..], next);
        Ok(())
    }

    fn read_page_data(&self, page_no: i32) -> Result<Vec<u8>, EasyDbError> {
        let page = self.bpm.fetch_page(PageId::new(self.fd, page_no))?;
        Ok(page.read().to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::rm_manager::RmManager;

    fn setup() -> (tempfile::TempDir, RmManager, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let disk = Arc::new(DiskManager::new(dir.path().join("db")).unwrap());
        let bpm = Arc::new(BufferPool::new(8, disk.clone()));
        let rm = RmManager::new(disk, bpm);
        let path = dir.path().join("db").join("t.data");
        (dir, rm, path)
    }

    #[test]
    fn crud_across_pages_with_persistence() {
        let (_dir, rm, path) = setup();
        rm.create_file(&path).unwrap();
        let fh = rm.open_file(&path).unwrap();

        // 插入 200 条跨多页的记录
        let meta = TupleMeta::default();
        let mut rids = Vec::new();
        for i in 0..200 {
            let data = format!("record-{i:04}-payload").into_bytes();
            rids.push(fh.insert_tuple(meta, &data).unwrap());
        }
        // RID 连续分配：页号递增、槽号递增
        assert_eq!(rids[0], Rid::new(1, 0));
        assert_eq!(rids[1], Rid::new(1, 1));

        // 全部读回校验
        for (i, &rid) in rids.iter().enumerate() {
            let (m, data) = fh.get_tuple(rid).unwrap();
            assert!(!m.is_deleted);
            assert_eq!(data, format!("record-{i:04}-payload").into_bytes());
        }

        // 原地更新（等长）
        fh.update_tuple_in_place(rids[5], meta, b"record-XXXX-payload")
            .unwrap();
        let (_, data) = fh.get_tuple(rids[5]).unwrap();
        assert_eq!(data, b"record-XXXX-payload");

        // 更长数据不允许原地更新
        assert!(
            fh.update_tuple_in_place(rids[5], meta, b"0123456789abcdef0123456789abcdefXYZ")
                .is_err()
        );

        // 逻辑删除 + 元数据校验
        fh.delete_tuple(rids[5]).unwrap();
        assert!(fh.get_tuple_meta(rids[5]).unwrap().is_deleted);
        // 重复删除报错
        assert!(fh.delete_tuple(rids[5]).is_err());

        // 回滚恢复（undelete）
        fh.undelete_tuple(rids[5], meta).unwrap();
        let (m, _) = fh.get_tuple(rids[5]).unwrap();
        assert!(!m.is_deleted);

        // 文件头页数与跨页情况
        assert!(fh.file_hdr().num_pages > 1);
    }

    #[test]
    fn reopen_persists_header_and_data() {
        let (_dir, rm, path) = setup();
        rm.create_file(&path).unwrap();
        let fh = rm.open_file(&path).unwrap();
        let meta = TupleMeta::default();
        let rid = fh.insert_tuple(meta, b"persist-me").unwrap();
        let num_pages = fh.file_hdr().num_pages;

        // 刷盘、关闭旧句柄后重新打开：文件头从磁盘恢复，数据可读
        fh.flush_all().unwrap();
        rm.disk().close_file(fh.fd()).unwrap();
        let fd = rm.disk().open_file(&path).unwrap();
        let fh2 = RmFileHandle::open(rm.disk().clone(), rm.bpm().clone(), fd).unwrap();
        assert_eq!(fh2.file_hdr().num_pages, num_pages);
        let (_, data) = fh2.get_tuple(rid).unwrap();
        assert_eq!(data, b"persist-me");
    }

    #[test]
    fn oversized_tuple_rejected() {
        let (_dir, rm, path) = setup();
        rm.create_file(&path).unwrap();
        let fh = rm.open_file(&path).unwrap();
        // 空页放不下的大元组（> PAGE_SIZE - 头部与槽目录）
        let big = vec![0u8; PAGE_SIZE];
        assert!(matches!(
            fh.insert_tuple(TupleMeta::default(), &big),
            Err(EasyDbError::InvalidRecordSize(_))
        ));
    }

    #[test]
    fn version_chain_keeps_xmin_xmax_and_previous_rid() {
        let (_dir, rm, path) = setup();
        rm.create_file(&path).unwrap();
        let fh = rm.open_file(&path).unwrap();
        let first = fh
            .insert_tuple(
                TupleMeta {
                    xmin: 1,
                    ..TupleMeta::default()
                },
                b"v1",
            )
            .unwrap();
        let second = fh
            .insert_tuple(
                TupleMeta {
                    xmin: 2,
                    prev: Some(first),
                    ..TupleMeta::default()
                },
                b"v2",
            )
            .unwrap();
        fh.mark_deleted(first, 2).unwrap();

        let chain = fh.version_chain(second).unwrap();
        assert_eq!(chain.len(), 2);
        assert_eq!(chain[0].0, second);
        assert_eq!(chain[0].1.xmin, 2);
        assert_eq!(chain[0].1.prev, Some(first));
        assert_eq!(chain[1].0, first);
        assert_eq!(chain[1].1.xmax, Some(2));
        assert_eq!(chain[1].2, b"v1");
    }
}
