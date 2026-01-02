# EasyDB 系统架构与技术细节

本文档详细描述了 EasyDB 数据库系统中各个模块的技术实现细节。

## 目录

- [系统概述](#系统概述)
- [存储管理模块](#存储管理模块)
- [缓冲区管理模块](#缓冲区管理模块)
- [记录管理模块](#记录管理模块)
- [索引管理模块](#索引管理模块)
- [查询处理模块](#查询处理模块)
- [事务管理模块](#事务管理模块)
- [并发控制模块](#并发控制模块)
- [恢复管理模块](#恢复管理模块)
- [系统管理模块](#系统管理模块)

---

## 系统概述

EasyDB 采用经典的数据库系统架构，遵循分层设计原则。系统从下至上分为以下几个层次：

1. **存储层**：负责数据的持久化存储，包括磁盘管理、页面管理和文件管理
2. **缓冲区层**：管理内存中的页面缓存，实现内存与磁盘之间的数据交换
3. **记录层**：管理表中的记录，提供记录的插入、删除、更新和扫描功能
4. **索引层**：提供索引结构，加速数据访问
5. **查询处理层**：负责 SQL 语句的解析、优化和执行
6. **事务管理层**：管理事务的创建、提交和回滚
7. **并发控制层**：处理多用户并发访问的同步问题
8. **恢复管理层**：处理系统故障后的数据恢复

---

## 存储管理模块

### 概述

存储管理模块是数据库系统的基础层，负责数据的持久化存储。该模块主要包括磁盘管理、页面管理和表文件管理三个子模块。

### 磁盘管理 (DiskManager)

**功能**：负责与操作系统的文件系统交互，提供底层的数据读写接口。

**核心类**：`DiskManager`

**主要功能**：
- 文件的创建、打开、关闭和删除
- 页面的读取和写入（以 4KB 为单位）
- 文件描述符的管理

**技术细节**：
- 使用 `open()`、`read()`、`write()` 等系统调用进行文件操作
- 每个数据库对应一个目录，目录中包含多个文件（表文件、索引文件、元数据文件等）
- 页面是数据存储的基本单位，大小为 4KB（`PAGE_SIZE`）

**关键接口**：
```cpp
void CreateFile(const std::string &file_name);
void DestroyFile(const std::string &file_name);
int OpenFile(const std::string &file_name);
void CloseFile(int fd);
void ReadPage(int fd, page_id_t page_no, char *buf, int num_bytes);
void WritePage(int fd, page_id_t page_no, const char *buf, int num_bytes);
```

### 页面管理 (Page)

**功能**：封装单个页面，提供页面的元数据管理和数据访问接口。

**核心类**：`Page`

**页面结构**：
```
+------------------+
|   Page Header    |  (包含页面元数据，如 page_id, pin_count, is_dirty 等)
+------------------+
|                  |
|   Page Data      |  (实际存储的数据，大小为 PAGE_SIZE - Header 大小)
|                  |
+------------------+
```

**技术细节**：
- 每个页面有唯一的 `page_id`，由文件描述符和页面号组成
- `pin_count` 用于引用计数，防止页面被替换
- `is_dirty` 标记页面是否被修改，决定是否需要写回磁盘
- `page_lsn_` 用于故障恢复，记录页面的日志序列号

**关键字段**：
```cpp
page_id_t page_id_;      // 页面标识符
int pin_count_;           // 引用计数
bool is_dirty_;           // 脏页标记
lsn_t page_lsn_;          // 日志序列号
char data_[PAGE_SIZE];    // 页面数据
```

### 表文件管理 (RmFileHandle)

**功能**：管理表文件的结构，提供记录的存储和检索功能。

**核心类**：`RmFileHandle`、`RmManager`

**文件结构**：
```
+------------------+
|  File Header     |  (第 0 页，包含文件元数据)
+------------------+
|  Page 1          |  (数据页 1)
+------------------+
|  Page 2          |  (数据页 2)
+------------------+
|  ...             |
+------------------+
```

**文件头 (RmFileHdr) 结构**：
```cpp
struct RmFileHdr {
    int record_size;              // 记录大小
    int num_pages;                // 数据页数量
    int num_records_per_page;     // 每页记录数
    int bitmap_size;              // 位图大小
    page_id_t first_free_page_no; // 第一个空闲页
};
```

**技术细节**：
- 使用位图（bitmap）管理页面中的空闲槽位
- 每个数据页包含页头、位图和记录数据三部分
- 支持记录的插入、删除和更新操作
- 使用 `RID`（Record ID）唯一标识一条记录，由页面号和槽号组成

**关键接口**：
```cpp
RID InsertRecord(const char *buf);
void DeleteRecord(const RID &rid);
void UpdateRecord(const RID &rid, const char *buf);
RmRecord GetRecord(const RID &rid);
```

---

## 缓冲区管理模块

### 概述

缓冲区管理模块负责管理内存中的页面缓存，减少磁盘 I/O 操作，提高系统性能。该模块实现了基于 LRU（Least Recently Used）的页面替换策略。

### 缓冲池管理器 (BufferPoolManager)

**功能**：管理内存中的页面缓存，提供页面的获取、固定、释放和替换功能。

**核心类**：`BufferPoolManager`

**数据结构**：
- `frames_`：缓冲池中的帧数组，每个帧可以存储一个页面
- `page_table_`：页表，维护页面 ID 到帧号的映射（哈希表）
- `free_frames_`：空闲帧列表
- `replacer_`：页面替换器（LRU 实现）

**技术细节**：
- 缓冲池大小可配置（默认由 `BUFFER_POOL_SIZE` 定义）
- 使用哈希表快速查找页面是否在缓冲池中
- 当缓冲池满时，使用 LRU 算法选择被替换的页面
- 支持页面的固定（pin）和释放（unpin），防止正在使用的页面被替换

**关键接口**：
```cpp
Page* FetchPage(PageId page_id);      // 获取页面（如果不在缓冲池中则从磁盘加载）
bool UnpinPage(PageId page_id, bool is_dirty);  // 释放页面
Page* NewPage(PageId *page_id);        // 分配新页面
bool DeletePage(PageId page_id);       // 删除页面
void FlushPage(PageId page_id);       // 将页面写回磁盘
```

### LRU 替换器 (LRUReplacer)

**功能**：实现 LRU 页面替换算法，选择最近最少使用的页面进行替换。

**核心类**：`LRUReplacer`

**数据结构**：
- 双向链表：维护页面的访问顺序，头部是最久未使用的页面
- 哈希表：快速定位链表中的节点

**技术细节**：
- 使用双向链表维护页面的访问顺序
- 使用哈希表实现 O(1) 时间复杂度的查找和删除
- 支持线程安全（使用互斥锁）

**关键接口**：
```cpp
bool Victim(frame_id_t *frame_id);  // 选择被替换的帧
void Pin(frame_id_t frame_id);      // 固定帧（从替换器中移除）
void Unpin(frame_id_t frame_id);    // 释放帧（添加到替换器）
size_t Size();                      // 返回可替换的帧数量
```

---

## 记录管理模块

### 概述

记录管理模块在页面管理的基础上，提供更高层次的记录操作接口，包括记录的插入、删除、更新和扫描。

### 记录扫描器 (RmScan)

**功能**：提供顺序扫描表记录的功能，支持条件过滤。

**核心类**：`RmScan`

**技术细节**：
- 按页面顺序扫描表中的所有记录
- 支持从指定位置开始扫描
- 可以跳过已删除的记录（通过位图判断）

**关键接口**：
```cpp
void Next();              // 移动到下一条记录
bool IsEnd();             // 判断是否扫描结束
RID GetRid();             // 获取当前记录的 RID
```

### 记录结构

**RmRecord**：封装一条记录的数据和元数据。

**技术细节**：
- 记录数据以字节数组形式存储
- 通过 Schema 解析记录中的字段值
- 支持定长和变长字段

---

## 索引管理模块

### 概述

索引管理模块提供索引结构，加速数据访问。系统实现了两种索引：B+ 树索引和可扩展哈希索引。

### B+ 树索引

**功能**：提供基于 B+ 树的索引结构，支持单点查询和范围查询。

**核心类**：`IxIndexHandle`、`IxNodeHandle`、`IxScan`、`IxManager`

**B+ 树结构**：
- 内部节点：存储键值和子节点指针，用于导航
- 叶子节点：存储键值和记录 ID（RID）的映射
- 叶子节点之间通过指针链接，支持顺序扫描

**技术细节**：
- 每个节点存储在一个页面中
- 节点分裂和合并操作保证 B+ 树的性质
- 支持多列索引（复合索引）
- 使用 `IxScan` 实现范围查询

**关键接口**：
```cpp
bool InsertEntry(const char *key, const RID &value);  // 插入索引项
bool DeleteEntry(const char *key);                    // 删除索引项
bool GetValue(const char *key, std::vector<RID> *result);  // 查找索引项
```

### 可扩展哈希索引

**功能**：提供基于可扩展哈希的索引结构，支持动态扩展。

**核心类**：`IxExtendibleHashIndexHandle`、`IxBucketHandle`

**技术细节**：
- 使用目录结构管理哈希桶
- 支持目录扩展和桶分裂
- 全局深度和局部深度的管理
- 每个桶存储在一个页面中

**关键接口**：
```cpp
bool GetValue(const char *key, std::vector<RID> *result);
page_id_t InsertEntry(const char *key, const RID &value);
bool DeleteEntry(const char *key);
```

---

## 查询处理模块

### 概述

查询处理模块负责 SQL 语句的解析、优化和执行，是数据库系统的核心模块之一。

### SQL 解析器

#### 词法分析 (Lexical Analysis)

**功能**：将 SQL 语句分解为词法单元（tokens）。

**工具**：Flex

**实现文件**：`src/parser/lex.l`

**支持的 Token 类型**：
- 关键字：SELECT、INSERT、UPDATE、DELETE、CREATE、DROP 等
- 标识符：表名、列名
- 字面量：整数、浮点数、字符串
- 操作符：=、<、>、<=、>=、!= 等

#### 语法分析 (Syntax Analysis)

**功能**：根据 SQL 语法规则，将词法单元组织成语法树。

**工具**：Bison

**实现文件**：`src/parser/yacc.y`

**语法规则**：支持标准 SQL 语法，包括：
- DDL 语句（CREATE TABLE、DROP TABLE 等）
- DML 语句（SELECT、INSERT、UPDATE、DELETE）
- 事务控制语句（BEGIN、COMMIT、ROLLBACK）

#### 语义分析 (Semantic Analysis)

**功能**：检查 SQL 语句的语义正确性，构建抽象语法树（AST）。

**核心类**：`Analyze`

**主要功能**：
- 类型检查
- 表名和列名的解析
- 条件表达式的验证
- 生成查询对象（Query）

**关键接口**：
```cpp
std::shared_ptr<Query> do_analyze(std::shared_ptr<ast::TreeNode> parse);
```

### 查询优化器

**功能**：对查询计划进行优化，提高查询性能。

**核心类**：`Optimizer`、`Planner`

#### 逻辑优化

**优化策略**：
1. **连接重排**：根据表的大小重新排列连接顺序
2. **单表条件前移**：将单表过滤条件提前执行
3. **条件下推**：将过滤条件下推到连接操作之前
4. **条件剪枝**：移除冗余或矛盾的条件

#### 物理优化

**优化策略**：
1. **基于统计信息的优化**：使用表的统计信息（记录数、最大值、最小值等）优化查询
2. **连接算法选择**：根据数据特征选择最优的连接算法（嵌套循环、归并、哈希）
3. **索引选择**：选择最优的索引加速查询

**关键接口**：
```cpp
std::shared_ptr<Plan> plan_query(std::shared_ptr<Query> query, Context *context);
```

### 查询执行器

**功能**：执行查询计划，返回查询结果。

**核心类**：`AbstractExecutor` 及其子类

**执行器类型**：
- `SeqScanExecutor`：顺序扫描执行器
- `IndexScanExecutor`：索引扫描执行器
- `NestedLoopJoinExecutor`：嵌套循环连接执行器
- `MergeJoinExecutor`：归并连接执行器
- `HashJoinExecutor`：哈希连接执行器
- `ProjectionExecutor`：投影执行器
- `SortExecutor`：排序执行器
- `AggregationExecutor`：聚合执行器
- `InsertExecutor`：插入执行器
- `DeleteExecutor`：删除执行器
- `UpdateExecutor`：更新执行器

**执行模型**：采用迭代器模型（Iterator Model），每个执行器实现以下接口：

```cpp
void beginTuple();           // 初始化执行器
std::unique_ptr<Tuple> Next();  // 获取下一个元组
bool IsEnd();                // 判断是否结束
```

**技术细节**：
- 执行器以树形结构组织，形成执行计划树
- 数据自底向上流动，每个执行器处理来自子执行器的数据
- 支持流水线执行，减少中间结果的存储

---

## 事务管理模块

### 概述

事务管理模块负责管理数据库事务，保证事务的 ACID 特性。

### 事务管理器 (TransactionManager)

**功能**：管理事务的生命周期，包括事务的创建、提交和回滚。

**核心类**：`TransactionManager`、`Transaction`

**事务状态**：
- `GROWING`：事务正在执行
- `COMMITTED`：事务已提交
- `ABORTED`：事务已中止

**技术细节**：
- 每个事务有唯一的事务 ID（`txn_id`）
- 支持显式事务（BEGIN/COMMIT）和自动提交模式
- 事务提交时，确保所有修改都写入磁盘
- 事务回滚时，撤销所有未提交的修改

**关键接口**：
```cpp
Transaction* Begin(Transaction *prev_txn, LogManager *log_manager);
void Commit(Transaction *txn, LogManager *log_manager);
void Abort(Transaction *txn, LogManager *log_manager);
```

---

## 并发控制模块

### 概述

并发控制模块处理多用户并发访问数据库时的同步问题，保证数据一致性。

### 锁管理器 (LockManager)

**功能**：管理锁的获取和释放，实现并发控制。

**核心类**：`LockManager`

**锁类型**：
- **共享锁（S Lock）**：用于读操作，多个事务可以同时持有
- **排他锁（X Lock）**：用于写操作，只能被一个事务持有

**锁粒度**：
- 表级锁
- 页面级锁
- 记录级锁

**技术细节**：
- 使用等待队列管理锁请求
- 检测死锁并回滚事务
- 支持锁升级（共享锁升级为排他锁）

**关键接口**：
```cpp
bool LockShared(Transaction *txn, const std::string &lock_obj);
bool LockExclusive(Transaction *txn, const std::string &lock_obj);
bool Unlock(Transaction *txn, const std::string &lock_obj);
```

---

## 恢复管理模块

### 概述

恢复管理模块负责处理系统故障后的数据恢复，保证数据的持久性。

### 日志管理器 (LogManager)

**功能**：管理事务日志，记录所有数据修改操作。

**核心类**：`LogManager`

**日志类型**：
- **更新日志（Update Log）**：记录数据的修改
- **提交日志（Commit Log）**：记录事务的提交
- **中止日志（Abort Log）**：记录事务的中止
- **检查点日志（Checkpoint Log）**：记录检查点

**技术细节**：
- 使用 WAL（Write-Ahead Logging）机制
- 日志先于数据写入磁盘
- 支持日志的批量刷新

**关键接口**：
```cpp
lsn_t AppendLogRecord(LogRecord *log_record);
void FlushLogToDisk();
```

### 恢复管理器 (RecoveryManager)

**功能**：在系统重启后，根据日志恢复数据库状态。

**核心类**：`RecoveryManager`

**恢复过程**：
1. **分析阶段（Analysis）**：扫描日志，确定需要恢复的事务
2. **重做阶段（Redo）**：重做已提交事务的操作
3. **撤销阶段（Undo）**：撤销未提交事务的操作

**关键接口**：
```cpp
void analyze();   // 分析阶段
void redo();      // 重做阶段
void undo();      // 撤销阶段
```

---

## 系统管理模块

### 概述

系统管理模块负责管理数据库的元数据，提供 DDL 操作的接口。

### 系统管理器 (SmManager)

**功能**：管理数据库、表和索引的元数据，提供 DDL 操作接口。

**核心类**：`SmManager`

**元数据结构**：
- `DbMeta`：数据库元数据，包含所有表的元数据
- `TabMeta`：表元数据，包含表的字段信息和索引信息
- `ColMeta`：列元数据，包含列的类型、长度等信息

**技术细节**：
- 元数据存储在 `<database>.meta` 文件中
- 支持表的创建、删除、索引的创建和删除
- 维护表的统计信息（记录数、最大值、最小值等）

**关键接口**：
```cpp
void CreateTable(const std::string &tab_name, const std::vector<ColDef> &col_defs, Context *context);
void DropTable(const std::string &tab_name, Context *context);
void CreateIndex(const std::string &tab_name, const std::vector<std::string> &col_names, Context *context);
void DropIndex(const std::string &tab_name, const std::vector<std::string> &col_names, Context *context);
```

---

## 总结

EasyDB 数据库系统实现了数据库系统的核心功能，采用模块化设计，各模块职责清晰，接口明确。系统支持完整的 SQL 功能，具备事务管理、并发控制和故障恢复能力，是一个功能完备的关系型数据库管理系统。

各模块之间通过清晰的接口进行交互，遵循数据库系统的经典架构，为后续的功能扩展和性能优化提供了良好的基础。
