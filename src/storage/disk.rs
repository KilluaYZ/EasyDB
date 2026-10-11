//! 磁盘管理器：文件与页面级 IO。
//!
//! 对应 C++ 版 `src-cpp/storage/disk/disk_manager.cpp`。
//!
//! 与 C++ 版的差异：
//! - 用 `std::fs::File` + 定位读写（`read_at/write_at`）替代 `lseek+read/write`，
//!   天然免除了 C++ 版共享文件游标在多线程下的竞态；
//! - "fd" 是 DiskManager 自己分配的内部句柄（自 0 递增），不直接暴露 OS fd；
//! - C++ 构造时打开的 `db.meta` 文件流未被任何代码消费，不再创建
//!   （库级元数据由 P7 的系统管理模块负责）。

use std::collections::HashMap;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicI32, Ordering};

use crate::common::config::PAGE_SIZE;
use crate::common::errors::EasyDbError;
use crate::common::errors::EasyDbError::{FileNotClosed, FileNotOpen, Internal};

struct Files {
    path2handle: HashMap<PathBuf, i32>,
    handle2path: HashMap<i32, PathBuf>,
    files: HashMap<i32, std::fs::File>,
    /// 每个文件的下一个待分配页号（对应 C++ `fd2pageno_`）
    fd2pageno: HashMap<i32, i32>,
}

/// 磁盘管理器：负责数据库目录、数据文件的打开/关闭与按页读写。
pub struct DiskManager {
    db_dir: PathBuf,
    inner: Mutex<Files>,
    next_handle: AtomicI32,
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    // 个别线程 panic 后不连锁毒化其余线程
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl DiskManager {
    /// 打开（不存在则创建）数据库目录。
    pub fn new(db_dir: impl Into<PathBuf>) -> Result<Self, EasyDbError> {
        let db_dir = db_dir.into();
        if !db_dir.exists() {
            std::fs::create_dir_all(&db_dir)?;
        }
        Ok(Self {
            db_dir,
            inner: Mutex::new(Files {
                path2handle: HashMap::new(),
                handle2path: HashMap::new(),
                files: HashMap::new(),
                fd2pageno: HashMap::new(),
            }),
            next_handle: AtomicI32::new(0),
        })
    }

    pub fn db_dir(&self) -> &Path {
        &self.db_dir
    }

    // ---- 按页读写 ----

    /// 读取一页（PAGE_SIZE 字节）。文件末端不足部分补零
    /// （与 C++ 版 `ReadPage` 行为一致：新页尚未落盘时读到全零）。
    pub fn read_page(&self, handle: i32, page_no: i32, buf: &mut [u8]) -> Result<(), EasyDbError> {
        assert_eq!(buf.len(), PAGE_SIZE, "page buffer must be PAGE_SIZE");
        let file = self.file(handle)?;
        let offset = (page_no as i64 * PAGE_SIZE as i64) as u64;
        let mut filled = 0usize;
        while filled < PAGE_SIZE {
            match file.read_at(&mut buf[filled..], offset + filled as u64)? {
                0 => break, // EOF
                n => filled += n,
            }
        }
        buf[filled..].fill(0);
        Ok(())
    }

    /// 写入一页（PAGE_SIZE 字节）。
    pub fn write_page(&self, handle: i32, page_no: i32, buf: &[u8]) -> Result<(), EasyDbError> {
        assert_eq!(buf.len(), PAGE_SIZE, "page buffer must be PAGE_SIZE");
        let file = self.file(handle)?;
        let offset = (page_no as i64 * PAGE_SIZE as i64) as u64;
        file.write_all_at(buf, offset)?;
        Ok(())
    }

    /// 在文件中分配一个新页号（每文件独立递增）。
    pub fn allocate_page(&self, handle: i32) -> i32 {
        let mut inner = lock(&self.inner);
        let counter = inner.fd2pageno.entry(handle).or_insert(0);
        let page_no = *counter;
        *counter += 1;
        page_no
    }

    /// 设置文件的下一个待分配页号（打开已有文件时由文件句柄调用，
    /// 对应 C++ `SetFd2Pageno`）。
    pub fn set_fd2pageno(&self, handle: i32, next_page_no: i32) {
        lock(&self.inner).fd2pageno.insert(handle, next_page_no);
    }

    /// 文件的下一个待分配页号。
    pub fn get_fd2pageno(&self, handle: i32) -> i32 {
        lock(&self.inner)
            .fd2pageno
            .get(&handle)
            .copied()
            .unwrap_or(0)
    }

    // ---- 文件管理 ----

    /// 创建文件；已存在则报错（对应 C++ `CreateFile`）。
    pub fn create_file(&self, path: impl AsRef<Path>) -> Result<(), EasyDbError> {
        let path = path.as_ref();
        if path.is_file() {
            return Err(EasyDbError::FileExists(path.display().to_string()));
        }
        std::fs::File::create(path)?;
        Ok(())
    }

    /// 删除文件；不存在或仍被打开则报错（对应 C++ `DestroyFile`）。
    pub fn destroy_file(&self, path: impl AsRef<Path>) -> Result<(), EasyDbError> {
        let path = path.as_ref();
        if !path.is_file() {
            return Err(EasyDbError::FileNotFound(path.display().to_string()));
        }
        {
            let inner = lock(&self.inner);
            if inner.path2handle.contains_key(path) {
                return Err(FileNotClosed(path.display().to_string()));
            }
        }
        std::fs::remove_file(path)?;
        Ok(())
    }

    /// 打开文件并返回内部句柄；重复打开报错（对应 C++ `OpenFile`）。
    pub fn open_file(&self, path: impl AsRef<Path>) -> Result<i32, EasyDbError> {
        let path: PathBuf = path.as_ref().to_path_buf();
        if !path.is_file() {
            return Err(EasyDbError::FileNotFound(path.display().to_string()));
        }
        let mut inner = lock(&self.inner);
        if inner.path2handle.contains_key(&path) {
            return Err(FileNotClosed(path.display().to_string()));
        }
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)?;
        let handle = self.next_handle.fetch_add(1, Ordering::Relaxed);
        inner.path2handle.insert(path.clone(), handle);
        inner.handle2path.insert(handle, path);
        inner.files.insert(handle, file);
        Ok(handle)
    }

    /// 关闭文件句柄（对应 C++ `CloseFile`）。
    pub fn close_file(&self, handle: i32) -> Result<(), EasyDbError> {
        let mut inner = lock(&self.inner);
        match inner.handle2path.remove(&handle) {
            Some(path) => {
                inner.path2handle.remove(&path);
                inner.files.remove(&handle);
                Ok(())
            }
            None => Err(FileNotOpen(handle)),
        }
    }

    /// 句柄对应的文件路径（对应 C++ `GetFileName`）。
    pub fn file_name(&self, handle: i32) -> Result<PathBuf, EasyDbError> {
        lock(&self.inner)
            .handle2path
            .get(&handle)
            .cloned()
            .ok_or(FileNotOpen(handle))
    }

    /// 取得文件的句柄；未打开则自动打开（对应 C++ `GetFileFd`）。
    pub fn get_file_handle(&self, path: impl AsRef<Path>) -> Result<i32, EasyDbError> {
        let path: PathBuf = path.as_ref().to_path_buf();
        let already_open = lock(&self.inner).path2handle.contains_key(&path);
        if already_open {
            Ok(lock(&self.inner).path2handle.get(&path).copied().unwrap())
        } else {
            self.open_file(path)
        }
    }

    /// 文件大小（字节）。
    pub fn file_size(&self, path: impl AsRef<Path>) -> Result<u64, EasyDbError> {
        Ok(std::fs::metadata(path)?.len())
    }

    fn file(&self, handle: i32) -> Result<std::fs::File, EasyDbError> {
        let inner = lock(&self.inner);
        let file = inner.files.get(&handle).ok_or(FileNotOpen(handle))?;
        file.try_clone()
            .map_err(|e| Internal(format!("dup file handle failed: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_db(tag: &str) -> (tempfile::TempDir, DiskManager) {
        let dir = tempfile::tempdir().unwrap();
        let dm = DiskManager::new(dir.path().join(tag)).unwrap();
        (dir, dm)
    }

    #[test]
    fn page_read_write_roundtrip() {
        let (_dir, dm) = temp_db("rw");
        dm.create_file(dm.db_dir().join("t1")).unwrap();
        let h = dm.open_file(dm.db_dir().join("t1")).unwrap();

        let mut page = [0u8; PAGE_SIZE];
        page[0] = 0xAB;
        page[PAGE_SIZE - 1] = 0xCD;
        dm.write_page(h, 2, &page).unwrap();

        let mut back = [0u8; PAGE_SIZE];
        dm.read_page(h, 2, &mut back).unwrap();
        assert_eq!(back, page);
    }

    #[test]
    fn read_beyond_eof_zero_fills() {
        let (_dir, dm) = temp_db("eof");
        dm.create_file(dm.db_dir().join("t1")).unwrap();
        let h = dm.open_file(dm.db_dir().join("t1")).unwrap();
        let mut buf = [0xFFu8; PAGE_SIZE];
        dm.read_page(h, 5, &mut buf).unwrap(); // 文件为空
        assert!(buf.iter().all(|&b| b == 0));
    }

    #[test]
    fn allocate_page_increments_per_file() {
        let (_dir, dm) = temp_db("alloc");
        dm.create_file(dm.db_dir().join("a")).unwrap();
        dm.create_file(dm.db_dir().join("b")).unwrap();
        let ha = dm.open_file(dm.db_dir().join("a")).unwrap();
        let hb = dm.open_file(dm.db_dir().join("b")).unwrap();
        assert_eq!(dm.allocate_page(ha), 0);
        assert_eq!(dm.allocate_page(ha), 1);
        assert_eq!(dm.allocate_page(hb), 0); // 各文件独立计数
        dm.set_fd2pageno(ha, 10);
        assert_eq!(dm.allocate_page(ha), 10);
    }

    #[test]
    fn open_twice_and_close_semantics() {
        let (_dir, dm) = temp_db("open");
        dm.create_file(dm.db_dir().join("t")).unwrap();
        let h = dm.open_file(dm.db_dir().join("t")).unwrap();
        // 重复打开报 FileNotClosed
        assert!(matches!(
            dm.open_file(dm.db_dir().join("t")),
            Err(EasyDbError::FileNotClosed(_))
        ));
        dm.close_file(h).unwrap();
        // 关闭后再读报 FileNotOpen
        let mut buf = [0u8; PAGE_SIZE];
        assert!(matches!(
            dm.read_page(h, 0, &mut buf),
            Err(EasyDbError::FileNotOpen(_))
        ));
        // 可重新打开
        dm.open_file(dm.db_dir().join("t")).unwrap();
    }

    #[test]
    fn destroy_rejects_open_file() {
        let (_dir, dm) = temp_db("destroy");
        dm.create_file(dm.db_dir().join("t")).unwrap();
        let _h = dm.open_file(dm.db_dir().join("t")).unwrap();
        assert!(matches!(
            dm.destroy_file(dm.db_dir().join("t")),
            Err(EasyDbError::FileNotClosed(_))
        ));
        dm.close_file(_h).unwrap();
        dm.destroy_file(dm.db_dir().join("t")).unwrap();
        assert!(!dm.db_dir().join("t").exists());
    }

    #[test]
    fn get_file_handle_opens_lazily() {
        let (_dir, dm) = temp_db("lazy");
        dm.create_file(dm.db_dir().join("t")).unwrap();
        let h1 = dm.get_file_handle(dm.db_dir().join("t")).unwrap();
        let h2 = dm.get_file_handle(dm.db_dir().join("t")).unwrap();
        assert_eq!(h1, h2);
    }
}
