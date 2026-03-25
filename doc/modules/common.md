# 公共模块（`src/common/`）

## 职责

为全项目提供**共享配置、基础类型、错误处理、执行上下文**等，不包含具体 SQL 算子或存储算法实现。

## 核心头文件

### `common/config.h`

集中定义系统级常量（节选）：

| 符号 | 说明 |
|------|------|
| `PAGE_SIZE` | 页大小，4096 字节 |
| `BUFFER_POOL_SIZE` | 缓冲池帧数 |
| `INVALID_PAGE_ID` / `INVALID_FRAME_ID` / `INVALID_TXN_ID` / `INVALID_LSN` | 哨兵值 |
| `LOG_BUFFER_SIZE` | 日志内存缓冲大小 |
| `BUFFER_LENGTH` | 网络与通用缓冲长度 |
| `DB_META_NAME` | 库元数据文件名 `db.meta` |
| `LOG_FILE_NAME` | `db.log` |
| `TXN_START_ID` | 事务 ID 起始量级 |
| `cycle_detection_interval` | 死锁检测周期（`std::chrono`） |
| `enable_logging` / `log_timeout` | 日志开关与刷盘策略 |

类型别名：`frame_id_t`、`page_id_t`、`slot_id_t`、`txn_id_t`、`lsn_t` 等。

### `common/context.h`

**`Context`** 在一次请求中贯穿解析、优化与执行，通常持有：

- 锁管理器、日志管理器指针  
- 当前 **`Transaction*`**  
- 结果输出缓冲区及 JSON 封装（Web 使用）  

服务端在 `client_handler` 里 `new Context(...)`，请求结束 `delete`。

### `common/errors.h` / `common/exception.h`

定义 **`EASYDBError`** 及各类派生错误（列未找到、文件、锁等），供上层 `catch` 并返回客户端。

### `common/rid.h`

**`RID`**：记录逻辑地址，通常包含 **页面号 + 槽号**，供记录层与索引叶子指向堆记录。

### `common/condition.h`

表示 WHERE / JOIN 等**条件**（比较符、左右操作数），语义分析填充后由 Planner 消费。

### `common/portal.h`

定义 **`Portal`** 与 **`PortalStmt`**（见 [execution-and-portal.md](execution-and-portal.md)），此处从「公共」角度仅说明：Portal 依赖 `SmManager` 做元数据，将 **`Plan`** 转为可执行算子树。

### 其他

- **`logger.h`**：日志宏  
- **`hashutil.h` / `mergeSorter.h`**：工具  
- **`rwlatch.h`**：读写闩，可用于页或结构同步  

## 依赖关系

`common` 被几乎所有模块包含；应保持**无业务循环依赖**（不向上依赖 execution 具体类）。
