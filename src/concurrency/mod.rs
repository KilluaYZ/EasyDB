//! 并发控制：锁管理器（严格两阶段锁 + wait-die 死锁避免）。
//!
//! 对应 C++ 版 `src-cpp/concurrency/lock_manager.cpp`。简化（文档注明）：
//! - 锁点为表级 S/X 与记录级 S/X；当前仍未实现意向锁与间隙锁；
//! - wait-die：冲突时年长事务（id 更小）等待，年轻事务直接中止
//!   （等待边严格从低 id 指向高 id，等待图无环，天然无死锁）。

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Condvar, Mutex};

use crate::common::config::TxnId;
use crate::common::errors::EasyDbError;
use crate::common::rid::Rid;
use crate::transaction::{Transaction, TransactionState};

/// 锁对象类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LockType {
    Table,
    Record,
}

/// 锁对象标识：类型 + 文件句柄 + 键（记录为 RID 编码，表为 0）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LockDataId {
    pub typ: LockType,
    pub fd: i32,
    pub key: i64,
}

impl LockDataId {
    pub fn table(fd: i32) -> Self {
        Self {
            typ: LockType::Table,
            fd,
            key: 0,
        }
    }

    pub fn record(fd: i32, rid: Rid) -> Self {
        Self {
            typ: LockType::Record,
            fd,
            key: rid.encoded(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LockMode {
    Shared,
    Exclusive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GroupMode {
    NonLock,
    Shared,
    Exclusive,
}

#[derive(Debug)]
struct LockRequest {
    txn: TxnId,
    mode: LockMode,
}

#[derive(Debug)]
struct LockQueue {
    requests: VecDeque<LockRequest>,
    group: GroupMode,
}

impl Default for LockQueue {
    fn default() -> Self {
        Self {
            requests: VecDeque::new(),
            group: GroupMode::NonLock,
        }
    }
}

struct Inner {
    table: HashMap<LockDataId, LockQueue>,
}

/// 锁管理器。
pub struct LockManager {
    inner: Mutex<Inner>,
    cv: Condvar,
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl LockManager {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(Inner {
                table: HashMap::new(),
            }),
            cv: Condvar::new(),
        })
    }

    /// 表级共享锁（扫描读）。
    pub fn lock_shared_on_table(&self, txn: &Transaction, fd: i32) -> Result<(), EasyDbError> {
        self.acquire(txn, LockDataId::table(fd), LockMode::Shared)
    }

    /// 表级排他锁（DDL 或需要锁住整张表的操作）。
    pub fn lock_exclusive_on_table(&self, txn: &Transaction, fd: i32) -> Result<(), EasyDbError> {
        self.acquire(txn, LockDataId::table(fd), LockMode::Exclusive)
    }

    /// 记录级共享锁。Serializable/严格 2PL 读取路径使用；MVCC 接入后
    /// Read Committed 等级可按语句生命周期决定是否需要它。
    pub fn lock_shared_on_record(
        &self,
        txn: &Transaction,
        fd: i32,
        rid: Rid,
    ) -> Result<(), EasyDbError> {
        self.acquire(txn, LockDataId::record(fd, rid), LockMode::Shared)
    }

    /// 记录级排他锁（写行）。
    pub fn lock_exclusive_on_record(
        &self,
        txn: &Transaction,
        fd: i32,
        rid: Rid,
    ) -> Result<(), EasyDbError> {
        self.acquire(txn, LockDataId::record(fd, rid), LockMode::Exclusive)
    }

    /// 提交/中止时释放事务持有的全部锁（严格 2PL）。
    pub fn unlock_all(&self, txn: &Transaction) {
        let ids: Vec<LockDataId> = txn
            .lock_set()
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .drain()
            .collect();
        let mut inner = lock(&self.inner);
        for id in ids {
            if let Some(q) = inner.table.get_mut(&id) {
                q.requests.retain(|r| r.txn != txn.id);
                q.group = recompute_group(&q.requests);
            }
        }
        // 唤醒所有等待者重新判断（wait-die 等待者醒来后按谓词继续等待或获得）
        self.cv.notify_all();
        // 清理空队列
        inner.table.retain(|_, q| !q.requests.is_empty());
    }

    /// 两阶段锁状态检查（对应 C++ `CheckTxnStateLock`）。
    fn check_state(&self, txn: &Transaction) -> Result<(), EasyDbError> {
        match txn.state() {
            TransactionState::Committed | TransactionState::Aborted => {
                Err(EasyDbError::TransactionAbort(txn.id))
            }
            TransactionState::Default => {
                txn.set_state(TransactionState::Growing);
                Ok(())
            }
            TransactionState::Growing => Ok(()),
            TransactionState::Shrinking => Err(EasyDbError::TransactionAbort(txn.id)),
        }
    }

    fn acquire(
        &self,
        txn: &Transaction,
        id: LockDataId,
        mode: LockMode,
    ) -> Result<(), EasyDbError> {
        self.check_state(txn)?;
        let mut inner = lock(&self.inner);

        // 已持有同级锁直接成功。升级则继续走冲突检查，避免旧的
        // “队列长度为 1 即可升级”逻辑绕过层级锁冲突。
        if let Some(q) = inner.table.get(&id) {
            if q.requests.iter().any(|r| r.txn == txn.id && r.mode == mode) {
                return Ok(());
            }
        }

        let conflicts = |inner: &Inner| conflicting_txns(inner, id, mode, txn.id);

        // wait-die：与冲突持有者比较——请求者更年长（id 更小）→ 等待
        // 持有者释放；更年轻 → 立即中止（等待图无环，无死锁）
        let conflicting = conflicts(&inner);
        if !conflicting.is_empty() {
            let conflict = *conflicting.iter().min().unwrap();
            if txn.id > conflict {
                return Err(EasyDbError::TransactionAbort(txn.id));
            }
            // 年长：等待（Condvar 等待期间释放内部锁）；周期性醒来检查
            // 兼容性与自身状态（外部中止的等待者能及时退出）
            loop {
                let (g, _) = self
                    .cv
                    .wait_timeout(inner, std::time::Duration::from_millis(20))
                    .unwrap_or_else(|e| e.into_inner());
                inner = g;
                if conflicts(&inner).is_empty() {
                    break;
                }
                match txn.state() {
                    TransactionState::Aborted | TransactionState::Committed => {
                        return Err(EasyDbError::TransactionAbort(txn.id));
                    }
                    _ => {}
                }
            }
        }

        let q = inner.table.entry(id).or_default();
        if let Some(pos) = q.requests.iter().position(|r| r.txn == txn.id) {
            // 只有升级会到这里；此时层级范围内已无其它冲突者。
            q.requests[pos].mode = mode;
        } else {
            q.requests.push_back(LockRequest { txn: txn.id, mode });
        }
        q.group = recompute_group(&q.requests);
        txn.lock_set()
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(id);
        Ok(())
    }
}

/// 判断两个锁对象是否处于同一层级冲突范围。
fn lock_scope_overlaps(a: LockDataId, b: LockDataId) -> bool {
    if a.fd != b.fd {
        return false;
    }
    match (a.typ, b.typ) {
        (LockType::Table, LockType::Table) => true,
        (LockType::Record, LockType::Record) => a.key == b.key,
        _ => true,
    }
}

fn modes_conflict(requested: LockMode, held: LockMode) -> bool {
    requested == LockMode::Exclusive || held == LockMode::Exclusive
}

fn conflicting_txns(inner: &Inner, id: LockDataId, mode: LockMode, requester: TxnId) -> Vec<TxnId> {
    inner
        .table
        .iter()
        .filter(|(held_id, _)| lock_scope_overlaps(**held_id, id))
        .flat_map(|(_, q)| q.requests.iter())
        .filter(|r| r.txn != requester && modes_conflict(mode, r.mode))
        .map(|r| r.txn)
        .collect()
}

fn recompute_group(requests: &VecDeque<LockRequest>) -> GroupMode {
    if requests.is_empty() {
        GroupMode::NonLock
    } else if requests.iter().any(|r| r.mode == LockMode::Exclusive) {
        GroupMode::Exclusive
    } else {
        GroupMode::Shared
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transaction::Transaction;
    use std::sync::Barrier;

    #[test]
    fn younger_txn_aborts_immediately() {
        let lm = LockManager::new();
        let t1 = Transaction::new(1);
        let t2 = Transaction::new(2);
        let rid = Rid::new(1, 0);

        lm.lock_exclusive_on_record(&t1, 7, rid).unwrap();
        // 年轻事务（id 更大）请求同一行 → 立即中止
        assert!(matches!(
            lm.lock_exclusive_on_record(&t2, 7, rid),
            Err(EasyDbError::TransactionAbort(2))
        ));
        // 年长事务（id 更小）请求已被更年轻者持有的行 → 等待
        lm.lock_exclusive_on_record(&t2, 7, Rid::new(2, 0)).unwrap();
        let t3 = Transaction::new(3);
        lm.lock_exclusive_on_record(&t3, 7, Rid::new(2, 0))
            .unwrap_err(); // t3 年轻于 t2 → 中止
        // t1（最年长）请求 t2 持有的行 → 应等待；t2 提交释放后 t1 获得
        let lm2 = lm.clone();
        let barrier = Arc::new(Barrier::new(2));
        let b2 = barrier.clone();
        let handle = std::thread::spawn(move || {
            b2.wait();
            std::thread::sleep(std::time::Duration::from_millis(100));
            t2.set_state(crate::transaction::TransactionState::Committed);
            lm2.unlock_all(&t2);
        });
        barrier.wait();
        let started = std::time::Instant::now();
        lm.lock_exclusive_on_record(&t1, 7, Rid::new(2, 0)).unwrap();
        assert!(started.elapsed() >= std::time::Duration::from_millis(90));
        handle.join().unwrap();
    }

    #[test]
    fn same_txn_relock_is_noop() {
        let lm = LockManager::new();
        let t = Transaction::new(1);
        let rid = Rid::new(0, 0);
        lm.lock_exclusive_on_record(&t, 1, rid).unwrap();
        lm.lock_exclusive_on_record(&t, 1, rid).unwrap(); // 重复加锁无害
    }
}
