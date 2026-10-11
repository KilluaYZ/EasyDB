//! 记录文件管理器：表数据文件的创建/删除/打开。
//!
//! 对应 C++ 版 `src-cpp/include/record/rm_manager.h`（头文件实现）。
//! C++ 版 `CreateFile` 的 `record_size` 参数是定长记录时代的遗留
//! （现行为变长元组），不再保留。

use std::sync::Arc;

use crate::buffer::buffer_pool::BufferPool;
use crate::common::errors::EasyDbError;
use crate::record::rm_defs::{RM_FILE_HDR_PAGE, RmFileHdr};
use crate::record::rm_file_handle::RmFileHandle;
use crate::storage::disk::DiskManager;

pub struct RmManager {
    disk: Arc<DiskManager>,
    bpm: Arc<BufferPool>,
}

impl RmManager {
    pub fn new(disk: Arc<DiskManager>, bpm: Arc<BufferPool>) -> Self {
        Self { disk, bpm }
    }

    pub fn disk(&self) -> &Arc<DiskManager> {
        &self.disk
    }

    pub fn bpm(&self) -> &Arc<BufferPool> {
        &self.bpm
    }

    /// 创建表数据文件并写入初始文件头（第 0 页：仅文件头页，无空闲页）。
    pub fn create_file(&self, path: impl AsRef<std::path::Path>) -> Result<(), EasyDbError> {
        self.disk.create_file(path.as_ref())?;
        let fd = self.disk.open_file(path.as_ref())?;
        let mut page = vec![0u8; crate::common::config::PAGE_SIZE];
        page[..RmFileHdr::SIZE].copy_from_slice(&RmFileHdr::init().to_bytes());
        let result = self.disk.write_page(fd, RM_FILE_HDR_PAGE, &page);
        self.disk.close_file(fd)?;
        result
    }

    /// 删除表数据文件。
    pub fn destroy_file(&self, path: impl AsRef<std::path::Path>) -> Result<(), EasyDbError> {
        self.disk.destroy_file(path.as_ref())
    }

    /// 打开表数据文件，返回文件句柄。
    pub fn open_file(
        &self,
        path: impl AsRef<std::path::Path>,
    ) -> Result<Arc<RmFileHandle>, EasyDbError> {
        let fd = self.disk.open_file(path.as_ref())?;
        Ok(Arc::new(RmFileHandle::open(
            self.disk.clone(),
            self.bpm.clone(),
            fd,
        )?))
    }
}
