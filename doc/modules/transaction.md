# 事务管理（`src/transaction/`）

## 职责

为每个连接/请求维护 **ACID** 中的事务边界：开始、提交、回滚；与 **日志**、**锁** 协作。

## `Transaction`（`transaction.h`）

关键状态：

| 成员 | 含义 |
|------|------|
| `txn_id_` | 事务 ID |
| `state_` | `TransactionState`：如 DEFAULT、GROWING、COMMITTED、ABORTED 等 |
| `txn_mode_` | **true** = 显式事务（`BEGIN`…）；**false** = 隐式单语句 |
| `write_set_` | 写操作记录，供 **Undo** |
| `lock_set_` | 本事务持有的锁，提交/回滚时释放 |
| `prev_lsn_` | 与日志链接，用于 WAL |
| `index_latch_page_set_` / `index_deleted_page_set_` | 索引页闩锁与删除页恢复 |

## `TransactionManager`（`transaction_manager.h` / `transaction_manager.cpp`）

- **`Begin(prev, log_manager)`**：分配新 `txn_id`，初始化状态。  
- **`Commit(txn, log_manager)`**：刷日志、释放锁、改状态。  
- **`Abort(txn, log_manager)`**：按 `write_set_` 等撤销，释放锁。  
- **`ReleaseTxnOfThread`**：线程结束时清理线程局部事务对象。  
- **`CreateStaticCheckpoint`**：与检查点日志配合。

并发控制模式枚举 **`ConcurrencyMode`**：当前以 **两阶段封锁（TWO_PHASE_LOCKING）** 为主。

## 与服务端的协作

`easydb.cpp` 中：

- 每条请求 **`SetTransaction`**：若无活跃事务则 **Begin**，并 **`SetTxnMode(false)`**（隐式）。  
- 执行完毕后，若隐式事务且未 ABORTED，则 **Commit**；若 **`TransactionAbortException`** 则 **Abort**。

显式事务由 **`Optimizer`** 生成 `TxnBegin`/`TxnCommit` 等计划，在 **`run_cmd_utility`** 中切换 `txn_mode` 并调用对应 TM 接口（以 `execution_manager.cpp` 实现为准）。

## 相关文档

- 锁：[lock-manager.md](lock-manager.md)  
- 日志与恢复：[recovery-log.md](recovery-log.md)
