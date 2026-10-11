//! 事务：状态机、写集与锁集合。
//!
//! 对应 C++ 版 `src-cpp/include/transaction/`。C++ 的 WriteRecord 裸指针
//! deque 收敛为 Owned Vec；隔离级别当前固定（无 NULL/MVCC 语义）。

use std::collections::HashSet;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Mutex, OnceLock};

use crate::common::config::{INVALID_LSN, TxnId};
use crate::common::rid::Rid;

/// 事务状态（对应 C++ `TransactionState`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    /// 刚创建，尚未获取锁
    Default,
    /// 两阶段锁的增长阶段
    Growing,
    /// 收缩阶段（本实现为严格 2PL，正常路径不会进入）
    Shrinking,
    Committed,
    Aborted,
}

/// 写操作类型（对应 C++ `WType`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WType {
    Insert,
    Delete,
    Update,
}

/// 写集条目：撤销所需的最小信息（旧数据用于回滚重写）。
#[derive(Debug, Clone)]
pub struct WriteRecord {
    pub wtype: WType,
    pub table: String,
    pub rid: Rid,
    /// 元组编码字节：INSERT 存新数据（撤销删行+索引）、
    /// DELETE/UPDATE 存旧数据（撤销回写）
    pub data: Vec<u8>,
}

/// 事务对象：多线程共享（Arc）。
pub struct Transaction {
    pub id: TxnId,
    state: Mutex<TransactionState>,
    write_set: Mutex<Vec<WriteRecord>>,
    lock_set: Mutex<HashSet<crate::concurrency::LockDataId>>,
    prev_lsn: AtomicI32,
}

impl Transaction {
    pub fn new(id: TxnId) -> Self {
        Self {
            id,
            state: Mutex::new(TransactionState::Default),
            write_set: Mutex::new(Vec::new()),
            lock_set: Mutex::new(HashSet::new()),
            prev_lsn: AtomicI32::new(INVALID_LSN),
        }
    }

    pub fn state(&self) -> TransactionState {
        *self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn set_state(&self, state: TransactionState) {
        *self.state.lock().unwrap_or_else(|e| e.into_inner()) = state;
    }

    pub fn prev_lsn(&self) -> i32 {
        self.prev_lsn.load(Ordering::Relaxed)
    }

    pub fn set_prev_lsn(&self, lsn: i32) {
        self.prev_lsn.store(lsn, Ordering::Relaxed);
    }

    pub fn append_write(&self, rec: WriteRecord) {
        self.write_set
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(rec);
    }

    /// 写集长度（语句级保存点用）。
    pub fn write_set_len(&self) -> usize {
        self.write_set
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len()
    }

    /// 取出写集尾部（撤销使用）：`from` 之后的条目按逆序弹出。
    pub fn pop_writes_from(&self, from: usize) -> Vec<WriteRecord> {
        let mut ws = self.write_set.lock().unwrap_or_else(|e| e.into_inner());
        if from >= ws.len() {
            return Vec::new();
        }
        ws.drain(from..).rev().collect()
    }

    /// 清空写集（提交时）。
    pub fn clear_writes(&self) {
        self.write_set
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
    }

    pub fn lock_set(&self) -> &'_ Mutex<HashSet<crate::concurrency::LockDataId>> {
        &self.lock_set
    }
}

/// 全局事务 ID 分配（从 1 开始；0 保留给 INVALID 语义之外的占位）。
pub fn next_txn_id(counter: &OnceLock<std::sync::atomic::AtomicI64>) -> TxnId {
    counter
        .get_or_init(|| std::sync::atomic::AtomicI64::new(1))
        .fetch_add(1, Ordering::Relaxed)
}
