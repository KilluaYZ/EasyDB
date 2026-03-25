# 日志与恢复（`src/recovery/`）

## 职责

- **`LogManager`**：追加写 **WAL**，将日志记录序列化到 `db.log`（或配置名）。  
- **`RecoveryManager`**：系统启动时 **analyze → redo → undo**，与 ARIES 思路一致。

## 日志格式 — `log_defs.h`

日志记录头部字段偏移：

| 字段 | 含义 |
|------|------|
| `log_type_` | 记录类型 |
| `lsn_` | 日志序列号 |
| `log_tot_len_` | 总长度 |
| `log_tid_` | 事务 id |
| `prev_lsn_` | 事务内前一条日志 |

负载从 **`OFFSET_LOG_DATA`** 起。`FLUSH_TIMEOUT` 控制刷盘节奏（与 `config.h` 中全局 `log_timeout` 等配合）。

## `RecoveryManager`（`log_recovery.h` / `log_recovery.cpp`）

构造时可注入 `DiskManager`、`BufferPoolManager`、`SmManager`、`TransactionManager`、`LogManager`。

### 三阶段

1. **`analyze()`**  
   - 扫描日志，维护 **ATT**（活跃事务表）、**DPT**（脏页表）等结构  
   - 处理 **Checkpoint**（`analyze4chkpt`）  

2. **`redo()`**  
   - 从 **`min_rec_lsn_`** 起，对需要重做的页重放日志，使磁盘/缓冲状态前滚到故障前已提交效果  

3. **`undo()`**  
   - 对分析阶段判定为**未提交**的事务，按日志反向撤销  

内部使用 **`LogBuffer`** 读入日志文件块；`RedoLogsInPage` 等辅助结构按页聚合 redo 记录。

## 调用顺序

`easydb.cpp` 在 **`OpenDB`** 之后、**`start_server`** 之前：

```cpp
recovery->analyze();
recovery->redo();
recovery->undo();
```

## 与缓冲池、页 LSN

页的 **`page_lsn_`** 用于判断是否需要对该页 redo；刷盘时需保证 **WAL 先落盘**（具体顺序在 `LogManager::AppendLogRecord` / `FlushLogToDisk` 与 `BufferPoolManager::FlushPage` 协作中实现）。
