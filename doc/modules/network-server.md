# 网络服务与运行时（`easydb.cpp`）

## 角色

`src/easydb.cpp` 是 **EasyDB 服务端**的入口：负责创建全局子系统、打开数据库、执行 **崩溃恢复**，然后以 **TCP + 每连接一线程** 的方式处理客户端 SQL。

## 全局单例组件

在 `main` 成功解析 `-d <database>` 后，按顺序构造（节选）：

| 对象 | 类型 | 作用 |
|------|------|------|
| `disk_manager` | `DiskManager` | 数据库目录下文件 I/O |
| `buffer_pool_manager` | `BufferPoolManager` | 页缓存 |
| `rm_manager` | `RmManager` | 堆表文件访问 |
| `ix_manager` | `IxManager` | 索引文件访问 |
| `sm_manager` | `SmManager` | 元数据、DDL、打开库 |
| `lock_manager` | `LockManager` | 锁 |
| `txn_manager` | `TransactionManager` | 事务生命周期 |
| `planner` / `optimizer` | `Planner` / `Optimizer` | 计划生成 |
| `ql_manager` | `QlManager` | 执行 DML/结果输出 |
| `log_manager` | `LogManager` | WAL |
| `recovery` | `RecoveryManager` | 重启恢复 |
| `portal` | `Portal` | Plan → 执行器树 |
| `analyze` | `Analyze` | AST → `Query` |

若数据库目录不存在，调用 `SmManager::CreateDB`；否则 `OpenDB`。

## 启动时恢复

在 `start_server()` 之前执行：

```text
recovery->analyze();
recovery->redo();
recovery->undo();
```

即典型的 **分析 → 重做 → 撤销** 流程，保证日志与页状态一致。细节见 [recovery-log.md](recovery-log.md)。

## 命令行参数

| 参数 | 含义 |
|------|------|
| `-d <db>` | 数据库名（目录名），必填 |
| `-p <port>` | 监听端口（默认见源码中 `SOCK_PORT` 初值） |
| `-w` | Web 模式：`for_web` 为真时，响应使用 `SerializeToWithLimit` 的向量格式写回（见 `client_handler` 末尾） |
| `-h` | 帮助 |

## 客户端线程 `client_handler`

1. 循环 **`read`** SQL 文本到 `data_recv`（长度上限 `BUFFER_LENGTH`）。  
2. 特殊命令：`exit` 断开；`crash` 直接 **`exit(1)`**（测试用）。  
3. 分配 **`Context`**：传入 `LockManager*`、`LogManager*`、结果缓冲区 `data_send`、长度 `offset`。  
4. **`SetTransaction`**：若当前无有效事务，则 **`txn_manager->Begin`**，并设 **`SetTxnMode(false)`**（隐式单语句事务）。  
5. **`pthread_mutex_lock(buffer_mutex)`** 后 **`yy_scan_string` + `yyparse()`**。  
6. 解析成功则 **`analyze->do_analyze`**，再 **`optimizer->plan_query`** → **`portal->start` / `run` / `drop`**。  
7. 捕获 **`TransactionAbortException`**：写回 `abort`，并 **`txn_manager->Abort`**。  
8. 捕获 **`EASYDBError`**：错误信息写入客户端。  
9. 若 **`txn_mode == false`**（隐式事务）且未 ABORTED，则 **`Commit`**。  
10. **`Context::SerializeToWithLimit`** 生成最终输出（Web 用 vector，否则 C 字符串缓冲）。  
11. 线程结束：**`ReleaseTxnOfThread`**，关闭 fd。

## 信号处理

`SIGINT`（Ctrl+C）注册 **`sigint_handler`**：`log_manager->flush_log_to_disk()`，然后 **`longjmp`** 跳出监听循环，最后 **`SmManager::CloseDB`**。

## 与模块文档的衔接

- **事务边界**：见 [transaction.md](transaction.md)。  
- **解析互斥**：与 [sql-parser.md](sql-parser.md) 中全局 lexer 状态一致。  
- **执行路径**：见 [execution-and-portal.md](execution-and-portal.md)。
