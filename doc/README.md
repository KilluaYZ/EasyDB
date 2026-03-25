# EasyDB 技术文档中心

本目录包含 **EasyDB** 关系型数据库的完整技术说明：从网络服务入口到底层存储、从 SQL 解析到查询执行与事务恢复。文档采用 Markdown 编写，按**模块**拆分，便于按需阅读与维护。

## 文档地图

### 入门与全局视图

| 文档 | 说明 |
|------|------|
| [模块总览：分层与请求路径](modules/overview.md) | 系统分层、一次 SQL 请求的端到端路径、主要静态库依赖关系 |
| [网络服务与进程模型](modules/network-server.md) | `easydb.cpp` 服务端初始化、多线程客户端、`Context` 与事务边界 |

### 基础设施与存储栈

| 文档 | 源码目录 | 说明 |
|------|-----------|------|
| [公共模块](modules/common.md) | `src/common/` | 全局配置、上下文、异常、RID、Portal 用到的公共类型等 |
| [类型系统](modules/type-system.md) | `src/type/` | `Value`、各 `*Type`、与 `ColType` 的对应关系 |
| [目录与模式（Catalog）](modules/catalog.md) | `src/catalog/` | `Schema`、`Column`、列布局与类型 |
| [存储：磁盘、页面与表元组](modules/storage-disk-page-table.md) | `src/storage/disk/`, `page/`, `table/` | `DiskManager`、`Page`/`PageId`、`TablePage`、`Tuple` |
| [缓冲池](modules/buffer-pool.md) | `src/buffer/` | `BufferPoolManager`、`LRUReplacer`、pin/unpin 语义 |
| [记录管理](modules/record-manager.md) | `src/record/` | `RmFileHandle`、`RmScan`、`RmRecord`、表文件布局 |
| [索引：B+ 树与可扩展哈希](modules/index-bplus-and-extensible-hash.md) | `src/storage/index/` | `IxManager`、`IxIndexHandle`、可扩展哈希索引 |

### 查询处理管线

| 文档 | 源码目录 | 说明 |
|------|-----------|------|
| [SQL 词法/语法分析](modules/sql-parser.md) | `src/parser/` | Flex/Bison、`ast`、与 `yyparse` 的衔接 |
| [语义分析与 Query 对象](modules/analyze.md) | `src/analyze/` | `Analyze::do_analyze`、`Query` 结构 |
| [查询计划（Planner）](modules/planner.md) | `src/planner/` | 逻辑/物理优化入口、`Plan` 树、连接与索引选择 |
| [优化器外壳（Optimizer）](modules/optimizer-shell.md) | `src/include/optimizer/` | 非 DML 语句的计划分发、`bypass` 等快捷路径 |
| [执行与 Portal](modules/execution-and-portal.md) | `src/execution/`, `src/include/common/portal.h` | `QlManager`、各类 `Executor`、`Portal` 计划→算子树 |
| [执行器源码索引](modules/execution-executors-reference.md) | 同上 | 各 `executor_*.cpp` / 头文件与算子对应表 |

### 事务、并发与恢复

| 文档 | 源码目录 | 说明 |
|------|-----------|------|
| [事务管理](modules/transaction.md) | `src/transaction/` | `Transaction`、`TransactionManager`、隐式/显式事务 |
| [锁与并发控制](modules/lock-manager.md) | `src/concurrency/` | `LockManager`、锁模式、2PL、wait-die |
| [日志与恢复](modules/recovery-log.md) | `src/recovery/` | `LogManager`、`RecoveryManager`、Aries 风格 analyze/redo/undo |

### 系统元数据与 DDL

| 文档 | 源码目录 | 说明 |
|------|-----------|------|
| [系统管理（SmManager）](modules/system-manager.md) | `src/system/` | 数据库/表/索引元数据、`db.meta`、DDL 与统计信息 |

### 外围组件（源码树内）

| 文档 | 说明 |
|------|------|
| [client-and-web.md](modules/client-and-web.md) | 命令行客户端与 Web 前端如何对接服务端 |

源码位置：命令行见 `client/`；Web 见 `web_client/`；根目录 [README](../README.md) 含启动命令。

---

## 与原有专题文档的关系

以下文档在引入本「模块文档」之前已存在，内容侧重**某一专题**或**流程**，仍建议配合阅读：

| 路径 | 主题 |
|------|------|
| [architecture.md](architecture.md) | 早期整理的各模块技术要点（部分接口描述为概念级） |
| [SELECT查询流程详解.md](SELECT查询流程详解.md) | SELECT 端到端流程 |
| [B+树序列化与反序列化示例.md](B+树序列化与反序列化示例.md) | B+ 树页序列化示例 |
| [storage/storage.md](storage/storage.md) | 存储专题 |
| [index/index.md](index/index.md) | 索引专题 |
| [query_analyse/query_analyse.md](query_analyse/query_analyse.md) | 查询分析专题 |
| [spj/spj.md](spj/spj.md) | SPJ（选择-投影-连接）相关 |
| [optimizer/optimizer.md](optimizer/optimizer.md) | 优化器专题 |

**阅读建议**：先读 [modules/overview.md](modules/overview.md) 建立全局图景，再按任务（例如「改索引」）进入对应模块文档；需要细节时查阅上述专题文档与头文件。

---

## 构建与仓库布局（补充）

| 文档 | 说明 |
|------|------|
| [build-and-cmake.md](modules/build-and-cmake.md) | 顶层与 `src/CMakeLists.txt` 如何组织静态库与 `easydb_server` |

## 关键常量速查（代码来源：`src/include/common/config.h`）

| 常量 | 典型值 | 含义 |
|------|--------|------|
| `PAGE_SIZE` | 4096 | 页大小（字节） |
| `BUFFER_POOL_SIZE` | 1024 | 缓冲帧数量 |
| `BUFFER_LENGTH` | 8192 | 客户端交互缓冲（字节） |
| `LOG_BUFFER_SIZE` | `(BUFFER_POOL_SIZE + 1) * PAGE_SIZE` | 日志缓冲 |
| `DB_META_NAME` | `"db.meta"` | 库级元数据文件名 |
| `LOG_FILE_NAME` | `"db.log"` | 日志文件名 |

---

## 文档约定

- **命名空间**：实现代码均在 `namespace easydb` 下，除非另有说明。
- **路径**：文中 `src/...`、`include/...` 均相对于仓库根目录；公共头文件实际位于 `src/include/`。
- **链接**：模块文档之间使用相对路径互相引用，便于离线阅读与 Git 托管。
