# 缓冲池（`src/buffer/`）

## 职责

在内存中缓存 **`PAGE_SIZE`** 对齐的页，减少磁盘 I/O；通过 **pin 计数** 与 **LRU** 决定何时可换出。

## `BufferPoolManager`

**头文件**：`buffer/buffer_pool_manager.h`

核心数据结构：

| 成员 | 作用 |
|------|------|
| `frames_` | 定长数组，每个元素一个 `Page` |
| `page_table_` | `Hash(PageId → frame_id)`，命中则无需读盘 |
| `free_frames_` | 当前未挂任何 `PageId` 的帧 |
| `replacer_` | `LRUReplacer`，候选淘汰集合 |

典型操作：

- **`FetchPage(PageId)`**：若命中则 pin++；否则选帧、必要时刷脏、从磁盘读入。  
- **`UnpinPage(PageId, is_dirty)`**：pin--；pin 为 0 时进入可替换集。  
- **`NewPage`**：分配新页号并读入空帧。  
- **`FlushPage` / `FlushAllPages(fd)` / `FlushAllDirtyPages`**：刷盘。  
- **`RemoveAllPages(fd)`**：关闭/删表时移除该文件在池中的页，避免 fd 复用导致错误。

**线程安全**：`latch_` 保护内部结构。

## `LRUReplacer`

实现 **O(1)** 近似 LRU：双向链表 + 哈希表（`pin` 为 0 的帧才可被 `Victim`）。

## `Replacer` 抽象

`replacer.h` 提供替换策略接口，当前实现为 LRU。

## 与恢复的关系

页的 **`page_lsn_`** 在恢复与刷盘时与日志一致；`RecoverPage` 在恢复路径中用于从磁盘拉取已知页。

详见 [recovery-log.md](recovery-log.md)。
