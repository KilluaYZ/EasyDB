//! 索引文件管理器：B+ 树索引文件的创建/删除/打开。
//!
//! 对应 C++ 版 `src-cpp/include/storage/index/ix_manager.h`。
//!
//! 与 C++ 版的差异：没有虚拟叶子链表头页（文件第 1 页即根节点），
//! 初始页数为 2。

use std::sync::Arc;

use crate::buffer::buffer_pool::BufferPool;
use crate::common::config::PAGE_SIZE;
use crate::common::errors::EasyDbError;
use crate::index::ix_defs::{
    IX_FILE_HDR_PAGE, IX_INIT_NUM_PAGES, IX_INIT_ROOT_PAGE, IX_MAX_COL_LEN, IX_NO_PAGE, IxFileHdr,
};
use crate::index::ix_index_handle::IxIndexHandle;
use crate::storage::disk::DiskManager;
use crate::types::ColType;

pub struct IxManager {
    disk: Arc<DiskManager>,
    bpm: Arc<BufferPool>,
}

impl IxManager {
    pub fn new(disk: Arc<DiskManager>, bpm: Arc<BufferPool>) -> Self {
        Self { disk, bpm }
    }

    pub fn disk(&self) -> &Arc<DiskManager> {
        &self.disk
    }

    pub fn bpm(&self) -> &Arc<BufferPool> {
        &self.bpm
    }

    /// 索引文件名：`{表文件}_{列1}_{列2}.idx`（与 C++ 版一致）。
    pub fn index_name(file: &str, cols: &[String]) -> String {
        let mut name = file.to_string();
        for c in cols {
            name.push('_');
            name.push_str(c);
        }
        name.push_str(".idx");
        name
    }

    pub fn hash_index_name(file: &str, cols: &[String]) -> String {
        let mut name = file.to_string();
        for c in cols {
            name.push('_');
            name.push_str(c);
        }
        name.push_str(".hash.idx");
        name
    }

    pub fn exists(&self, path: impl AsRef<std::path::Path>) -> bool {
        path.as_ref().is_file()
    }

    /// 创建索引文件：写入文件头页与空的根叶节点。
    /// `cols` 为索引列的 (类型, 宽度) 序列。
    pub fn create_index(
        &self,
        path: impl AsRef<std::path::Path>,
        cols: &[(ColType, i32)],
    ) -> Result<(), EasyDbError> {
        let col_tot_len: usize = cols.iter().map(|&(_, l)| l as usize).sum();
        if col_tot_len > IX_MAX_COL_LEN {
            return Err(EasyDbError::InvalidColLength(col_tot_len as u32));
        }
        // 至少要有 3 阶（与 C++ assert 对应），否则报列过长
        if crate::index::ix_defs::max_keys_for(col_tot_len) < 3 {
            return Err(EasyDbError::InvalidColLength(col_tot_len as u32));
        }

        self.disk.create_file(path.as_ref())?;
        let fd = self.disk.open_file(path.as_ref())?;
        let hdr = IxFileHdr {
            num_pages: IX_INIT_NUM_PAGES,
            root_page: IX_INIT_ROOT_PAGE,
            first_leaf: IX_INIT_ROOT_PAGE,
            last_leaf: IX_INIT_ROOT_PAGE,
            first_free: IX_NO_PAGE,
            col_types: cols.iter().map(|&(t, _)| t).collect(),
            col_lens: cols.iter().map(|&(_, l)| l).collect(),
        };

        let result = (|| -> Result<(), EasyDbError> {
            // 第 0 页：文件头
            let mut page = vec![0u8; PAGE_SIZE];
            let bytes = hdr.to_bytes();
            page[..bytes.len()].copy_from_slice(&bytes);
            self.disk.write_page(fd, IX_FILE_HDR_PAGE, &page)?;

            // 第 1 页：根节点（空叶节点，prev/next 均无）
            let mut page = vec![0u8; PAGE_SIZE];
            {
                use crate::index::ix_index_handle::node;
                node::set_is_leaf(&mut page, true);
                node::set_parent(&mut page, IX_NO_PAGE);
                node::set_prev_leaf(&mut page, IX_NO_PAGE);
                node::set_next_leaf(&mut page, IX_NO_PAGE);
                node::set_num_keys(&mut page, 0);
            }
            self.disk.write_page(fd, IX_INIT_ROOT_PAGE, &page)
        })();

        self.disk.set_fd2pageno(fd, IX_INIT_NUM_PAGES);
        self.disk.close_file(fd)?;
        result
    }

    /// 删除索引文件。
    pub fn destroy_index(&self, path: impl AsRef<std::path::Path>) -> Result<(), EasyDbError> {
        self.disk.destroy_file(path.as_ref())
    }

    /// 打开索引文件。
    pub fn open_index(
        &self,
        path: impl AsRef<std::path::Path>,
    ) -> Result<Arc<IxIndexHandle>, EasyDbError> {
        let fd = self.disk.open_file(path.as_ref())?;
        Ok(Arc::new(IxIndexHandle::open(
            self.disk.clone(),
            self.bpm.clone(),
            fd,
        )?))
    }
}
