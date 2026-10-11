//! 全局配置常量与基础类型别名。
//!
//! 常量取值与 C++ 版 `src-cpp/include/common/config.h` 一致。

/// 帧ID（缓冲池）
pub type FrameId = i32;
/// 页面ID
pub type PageId = i32;
/// 槽ID（记录管理）
pub type SlotId = u32;
/// 事务ID
pub type TxnId = i64;
/// 日志序列号（Log Sequence Number）
pub type Lsn = i32;

/// 无效帧ID
pub const INVALID_FRAME_ID: FrameId = -1;
/// 无效页面ID
pub const INVALID_PAGE_ID: PageId = -1;
/// 无效事务ID
pub const INVALID_TXN_ID: TxnId = -1;
/// 无效日志序列号
pub const INVALID_LSN: Lsn = -1;

/// 页大小（字节）
pub const PAGE_SIZE: usize = 4096;
/// 缓冲池帧数
pub const BUFFER_POOL_SIZE: usize = 1024;
/// 数据库文件在磁盘上的初始大小（页数）
pub const DEFAULT_DB_IO_SIZE: usize = 16;
/// 日志缓冲区大小（字节）
pub const LOG_BUFFER_SIZE: usize = (BUFFER_POOL_SIZE + 1) * PAGE_SIZE;
/// 第一个事务ID的起始值
pub const TXN_START_ID: TxnId = 1_i64 << 62;
/// VARCHAR 类型的默认长度（字节）
pub const VARCHAR_DEFAULT_LENGTH: u32 = 128;

/// 日志文件名
pub const LOG_FILE_NAME: &str = "db.log";
/// 重启文件名（用于恢复）
pub const RESTART_FILE_NAME: &str = "db.restart";
/// 数据库元数据文件名
pub const DB_META_NAME: &str = "db.meta";
/// 默认数据库目录名
pub const DEFAULT_DB_NAME: &str = "test.db";
/// 客户端交互缓冲长度（字节）
pub const BUFFER_LENGTH: usize = 8192;
