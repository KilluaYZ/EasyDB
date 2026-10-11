# EasyDB Rust 重构迁移计划

> 目标：将现有 ~33k 行 C++ 实现的关系型数据库用 Rust 重写，从根上解决 C++ 版长期困扰的内存安全
> 问题（use-after-free、泄漏、悬垂指针、手工 pin/unpin 配对等）。C++ 版本（`src-cpp/`）在迁移期
> 间保留作为参照实现与行为基准，全部完成后移除。

> **进度**：2026-10-12 — 四项决议已确认（§8）；P0~P6 已完成（公共基础层、存储层
> 【M1】、B+ 树索引、SQL 前端、计划与执行器【M2】、事务/锁/恢复）；P7 持久化与
> 服务端已完成（db.meta 经 serde JSON 持久化，DDL 后落盘；`Database::open`
> 自动加载元数据 + WAL 恢复 + 表挂载；`system::format` 输出 C++ RecordPrinter
> 兼容的表格文本（列宽 16 右对齐、浮点 6 位小数、Total/empty set）；`main.rs`
> TCP 服务端（`easydb_server -p port -d dir`，文本协议 + NUL 分帧，与现有
> C++ TUI 客户端兼容，共享 Database 的多连接线程模型）），108 个单测通过、
> clippy `-D warnings` 零警告、零 unsafe。**里程碑 M3 达成。**
> 下一阶段：P8 TUI 客户端（ratatui + crossterm + tui-textarea）与收尾
> （移除 src-cpp 与 CMake 构建、更新 Nix/README，达成 M4）。

## 1. 现状评估

### 1.1 代码规模（按模块，含头文件）

| 模块 | 行数 | 内容 | Rust 迁移难度 |
|------|-----:|------|:---:|
| type | 2,456 | `Value` 类层次（BusTub 风格继承体系） | 低（大幅简化，见 §4.3） |
| execution | 2,389 | 13 个执行器 + Portal | 中 |
| storage | 2,709 | DiskManager、TablePage、Tuple、B+ 树（1,300 行）、可扩展哈希 | **高** |
| include（头文件） | 17,286 | 接口定义、模板、内联实现 | —（随各模块消化） |
| planner | 1,341 | 逻辑/物理计划生成 | 中 |
| record | 1,131 | RmFileHandle、RmScan、bitmap | 中 |
| system | 1,007 | SmManager：元数据、DDL 入口 | 中 |
| recovery | 910 | WAL LogManager、ARIES 风格 Recovery | 中 |
| buffer | 939 | BufferPoolManager、LRUReplacer | **高**（设计敏感） |
| concurrency | 782 | LockManager、2PL、wait-die | 中 |
| parser | 742 | flex/bison 词法文法 + AST | 中 |
| analyze | 700 | 语义分析 → Query | 低 |
| catalog | 213 | Schema、Column | 低 |
| transaction/common | ~240 | Transaction、Context、RID 等 | 低 |
| server/client | ~600 | pthread 一连接一线程、文本协议；TUI 客户端 | 低 |

### 1.2 有利条件

- **文档齐全**：`doc/modules/*.md` 按模块说明了源码结构、数据流与常量约定，可直接当移植规格书用。
- **测试可复用**：`test/` 下有 buffer / storage / record / concurrency / execution 等 googletest 单测，
  `comprehensive_test` 是端到端 SQL 黑盒测试。C++ 版编译产物可作为"对照实现"，同一条 SQL 分别发给
  两个服务端 diff 输出。
- **客户端协议极简**：TCP 上的换行分隔文本协议（请求 SQL 文本，响应文本）。Rust 服务端保持该协议即
  可继续复用现有 TUI 客户端与 Web 前端，`web_client/` 完全不用动。
- **B+ 树已经过缓冲池访问**：所有节点操作都走 `FetchPage/UnpinPage` 按 `page_id` 定位，没有页间裸指
  针，Rust 化的主要工作是页内字节布局的安全封装。
- **可扩展哈希索引未接入主流程**（sm_manager / execution 中无引用），可以从首期范围砍掉。

### 1.3 C++ 版内存问题高发区（正是重写的收益点）

1. **BufferPoolManager**：`FetchPage` 返回裸 `Page*`，靠调用者手工 `UnpinPage` 配对——忘记 unpin 导
   致缓冲池耗尽，unpin 后继续使用就是 use-after-free。
2. **B+ 树页内操作**：`memcpy` 在 `char*` 缓冲上移位、`reinterpret_cast` 页头结构体，越界与对齐问题
   高发。
3. **执行器基类**：`cols()` 等默认实现返回 `*(std::vector<ColMeta>*)nullptr`——本身就是 UB，靠"恰好
   没人调"存活。
4. **服务器**：`malloc` 出来的 pthread 互斥锁、全局 `yyparse` 状态靠一把全局大锁保护、每请求
   `new Context` 手工管理生命周期。
5. **Value 类型体系**：深拷贝语义、`shared_ptr<void>` 向下转型（`executor_abstract.h` 中的
   `static_pointer_cast`）等。

结论：**该项目无在线生产数据、单体部署，不适合 FFI 式绞杀者渐进迁移（C++↔Rust 边界的内存安全收益
要到最后才兑现，且 DB 类项目 FFI 边界本身很厚）。采用"分层整体重写、C++ 作对照基准"的方式**，按依
赖自底向上逐层交付，每层有独立测试，随时可编译可运行。

## 2. 目标与原则

1. **全 Rust**：服务端、客户端最终全部为 Rust；`web_client/`（Node + Python 代理）保持不动。
2. **行为兼容，格式不承诺兼容**：SQL 语法/语义、客户端协议与 C++ 版对齐；**磁盘文件格式
   （`db.meta`、表文件、B+ 树页布局）不要求与 C++ 版二进制兼容**（已确认：无需读旧数据文件，
   格式可自由重设计）。
3. **不是 1:1 翻译**：C++ 的继承/虚函数/裸指针惯用法与 Rust 冲突时，以 Rust 惯用设计为准
   （典型：`Value` 继承体系 → `enum`；手工 pin/unpin → RAII guard）。
4. **自底向上、小步交付**：每阶段结束都有可运行、可测试的产出，并移植对应的 C++ 单测作为验收。
5. **范围控制**：首期砍掉可扩展哈希索引；优化器规则按 planner 现状对齐即可，不做增强。

## 3. 技术选型

| 关注点 | 选择 | 说明 |
|--------|------|------|
| 错误处理 | `thiserror` 定义 `EasyDBError` 枚举 | 与 `errors.h` 的异常类型一一对应，全面 `Result` 化 |
| 日志 | `tracing` | 替代 spdlog/iostream |
| SQL 解析 | **LALRPOP**，移植 `yacc.y`（562 行）【已决议】 | 自持文法，语义最贴近，完整保留自定义方言（SET/LOAD/SHOW INDEX/DESC） |
| 网络 | 首选 `tokio`（一连接一 task）；求简可用 `std::net` + 线程 | 协议保持文本兼容 |
| TUI 客户端 | `ratatui` + `crossterm` + `tui-textarea`【已决议】 | 全部用现成库、不手搓：ratatui 负责渲染与内置 Table 结果集，tui-textarea 负责多行 SQL 编辑（历史/光标），可选 `syntect` 做 SQL 语法高亮 |
| 元数据持久化 | `serde` + `bincode` | `db.meta`、表/索引元数据 |
| 页内字节操作 | 手写 `from_le_bytes`/`to_le_bytes` 封装 | 禁止 `repr(C)` 结构体强转，规避对齐问题 |
| 锁 | `std::sync::{Mutex, RwLock, Condvar}`（可选 `parking_lot`） | LockManager 的条件等待用 `Condvar` |
| 缓冲池 LRU | 手写（对照 C++ 单测移植）或 `lru` crate | 建议手写，量小且要跟单测对齐 |
| 测试 | 内置 `#[test]` + `tempfile`；黑盒 SQL 用例脚本 | 保留 C++ comprehensive SQL 用例做双端对比 |
| 基准（可选） | `criterion` | 对比 C++ 版性能回归 |

初版 `Cargo.toml` 依赖大致：`thiserror`、`tracing`、`lalrpop`（build-dependency）、`serde`、
`bincode`、`tokio`、`tempfile`（dev）。

## 4. 关键设计决策（难点预研）

### 4.1 缓冲池：`PageGuard` 取代裸 `Page*`（最大技术风险，先做 spike）

C++ 的 `FetchPage → 裸指针 → 手工 Unpin` 改为 RAII：

```rust
pub struct BufferPool { frames: Vec<Frame>, page_table: HashMap<PageId, frame_id>, /* ... */ }

// FetchPage 返回 guard，Drop 时自动 unpin：
let mut page = pool.fetch_page(page_id)?;          // pin_count += 1
page.data_mut()[HEADER_OFFSET] = flag;             // &mut [u8; PAGE_SIZE]
// guard 离开作用域自动 unpin —— 一整类泄漏/UAF bug 从此不可能
```

实现要点：guard 内持 `Arc<BufferPool>` + frame 索引（不用裸指针也能做，性能足够）；脏标记在
guard 的 `mark_dirty()` 里完成；LRU 与 pin 计数的并发用每帧 `Mutex` + 池级 `Mutex`（与 C++ 版等
价）。**Phase 2 开始前先花 1~2 天写独立 spike 验证此设计**，因为执行器、B+ 树、record 层全都建在
它上面。

### 4.2 B+ 树：节点 = 页，视图结构体做编解码

- 保持现有逻辑结构：内部节点/叶节点都在 4KB 页里，兄弟指针用 `PageId`（本来就不是裸指针）。
- 页内布局用"视图 struct"：`LeafPageView<'a>` 借用 `&mut [u8; PAGE_SIZE]`，提供
  `get_key(i) -> &[u8]`、`insert_at(pos, key, rid) -> Result<()>` 等带边界检查的方法，内部全用
  `from_le_bytes`。**禁止 `#[repr(C)]` 结构体整体强转**。
- 迭代器（IxScan）持有页 guard 链或改为"每次 fetch + 恢复位置"，spike 中定。
- 验收手段：property test——随机插入/删除序列，与 `std::collections::BTreeMap` 对照结果一致。

### 4.3 `Value`：2.4k 行继承体系 → 一个 enum

C++ 的 `Value` + `Type/IntegerType/DecimalType/...` 虚函数体系在 Rust 里收敛为：

```rust
pub enum Value { Int(i32), BigInt(i64), Float(f64), String(String), DateTime(?) }
```

比较/算术/类型转换用 `match` 实现（或小 trait）。这是整个迁移中"Rust 优于 OOP"的最直观证明，
预计 2.4k 行 → 数百行，且消灭所有类型层次相关的转型 bug。

### 4.4 执行器：保持火山模型，`Box<dyn>` / enum 分发

- `Next() -> Option<Tuple>` 对应 Rust 的 `Iterator` 风格接口；13 个执行器逐一对应移植。
- C++ 里 `shared_ptr<void>` 装载算子树的 hack（`Portal`）直接换成 `enum PlanNode` /
  `Box<dyn Executor>`，无需保留。
- 执行器对 catalog 的引用用 `Arc` 快照，避免生命周期纠缠。

### 4.5 服务器与并发模型

- 首版与 C++ 等价：每连接一个 task/线程，数据库内部沿用现有锁粒度（连接级串行即可跑对）。
- 全局 `yyparse` + 全局大锁的丑陋在 Rust 中自然消失（解析无全局状态）。
- `Context`（含事务指针、输出缓冲）改为按请求构造的普通 struct，所有权清晰。

### 4.6 事务 / 锁 / 恢复

- `Transaction`：普通 struct + 状态机 enum（对应 `txn_defs.h`）。
- `LockManager`：2PL + wait-die 照搬；等待队列 + `Condvar`；死锁检测逻辑按 C++ 版对齐。
- WAL/ARIES：`LogManager`（log buffer + 落盘）与 `RecoveryManager`（analyze/redo/undo）按 C++ 版
  移植；日志记录结构用 serde 定义，**格式不需要与 C++ 版兼容**。

## 5. 分阶段计划

每阶段验收 = 移植对应 C++ 单测 + `cargo clippy -D warnings` + `cargo test` 全绿。

| 阶段 | 内容 | 产出 / 里程碑 | 预估(全职) |
|------|------|--------------|-----------|
| **P0 脚手架** | 单 crate + 模块目录骨架（mirror C++ 模块树）、CI（fmt/clippy/test）、确认磁盘格式决策 | 可编译空壳 | 0.5~1 天 |
| **P1 公共基础** | `EasyDBError`（errors.h）、config、RID、`Value` enum、Column/Schema | + 单测 | 1~2 天 |
| **P2 存储层** | DiskManager（`FileExt::read_at/write_at`）、Page、BufferPool+LRU（**先做 §4.1 spike**）、TablePage/RmFileHandle/RmScan/bitmap、Tuple 编解码 | **M1：存储引擎独立可用**，移植 test/buffer、record 相关单测 | 3~5 天 |
| **P3 索引** | B+ 树（§4.2）+ IxScan + property test（可扩展哈希已决议：首期不做，后续按需再加） | M1 补全 | 3~5 天 |
| **P4 SQL 前端** | AST（Rust enum + 生命周期归属字符串）、lexer、LALRPOP 文法（对照 yacc.y 逐条）、analyze 语义分析 → `Query` | 解析/绑定单测（对照 test/parser 用例） | 3~5 天 |
| **P5 计划与执行** | planner（逻辑+物理）、13 个执行器、Portal 等价物 | **M2：单用户、无事务、内存 catalog，SQL 子集端到端** | 4~6 天 |
| **P6 事务/锁/恢复** | Transaction、LockManager（2PL/wait-die）、LogManager、RecoveryManager，接入 Context 与执行器 | 并发与恢复单测 | 4~6 天 |
| **P7 系统管理与服务器** | SmManager：DDL、元数据 serde 持久化；TCP 服务器（协议兼容）；全链路串接 | **M3：现有 TUI 客户端直连 Rust 服务端；comprehensive SQL 用例双端 diff 通过** | 2~4 天 |
| **P8 客户端与收尾** | TUI 客户端 Rust 重写（ratatui 方案，见 §3）；删除 `src-cpp/` 与 CMake 构建；更新 Nix flake / README / doc | **M4：仓库纯 Rust** | 2~4 天 |

总预估：**全职 4~6 周**（约 21~34 个工作日），业余时间开发相应放大。预计 Rust 代码量 12k~20k 行
（删除的重复/模板代码远多于新增的安全设施代码）。

依赖顺序上 P2→P3→P5 是关键路径；P4 与 P2/P3 无依赖，可穿插或并行。如果希望更早看到端到端效果，
可在 P2 完成后插入一个"垂直切片"支线：内存版 catalog + 仅 SeqScan + 无事务的最小 SELECT 链路。

## 6. 测试与验收策略

1. **单元层**：`test/` 下 googletest 用例逐模块翻译（buffer、record、storage、concurrency、type…）。
2. **端到端黑盒**：整理 `comprehensive_test` 的 SQL 用例为脚本，同一脚本分别发给 C++ 服务端与 Rust
   服务端，diff 输出文本（协议是文本，天然可做 golden 对比）。
3. **专项**：
   - B+ 树 property test（对照 `BTreeMap`）；
   - 并发压测（多连接并发读写 + 锁冲突 + wait-die 中止路径）；
   - 崩溃恢复测试（执行事务中途 `kill -9`，重启后 redo/undo 结果正确）。
4. **静态保证**：`cargo clippy -D warnings`；目标 **零 `unsafe`**（字节操作用安全 API 即可做到，
   这是相对 C++ 的核心卖点）。

## 7. 风险与对策

| 风险 | 等级 | 对策 |
|------|:---:|------|
| 缓冲池 Rust 借别名设计走弯路 | 高 | P2 前置独立 spike（§4.1），确认 guard 方案再铺开 |
| B+ 树字节级编解码细节 bug | 高 | 边界断言 + property test 对照 `BTreeMap` |
| LALRPOP 文法移植遗漏/歧义冲突 | 中 | 逐条对照 `yacc.y`；收集 C++ 测试 SQL 做回归语料 |
| 自定义方言（SET/LOAD/DESC…）在 sqlparser 中难表达 | 中 | 首选 LALRPOP 自持文法，即为此风险兜底 |
| 范围蔓延 | 中 | 砍可扩展哈希、不动 web_client、优化器不做增强，写进本计划即冻结 |
| 旧数据文件兼容需求未浮出 | 低 | P0 中确认；若需要，P1 增加字节兼容层 |
| 事务/恢复与执行器的耦合点（Context 贯穿） | 中 | P5 先用"空事务"占位接口，P6 再接入，避免返工 |

## 8. 已确认决议（2026-10-11）

1. **磁盘格式**：不需要兼容 C++ 版旧数据文件，各阶段可自由设计文件/页格式。
2. **解析器**：采用 LALRPOP 自持文法（移植 `yacc.y`），不引入 `sqlparser-rs`。
3. **TUI 客户端**：随 P8 一并 Rust 化；优先用现成库，实现简单、界面丰富美观——
   选型 `ratatui` + `crossterm`（渲染与事件）+ `tui-textarea`（多行 SQL 编辑），
   结果集用 ratatui 内置 Table 组件，可选 `syntect` 做 SQL 语法高亮。
4. **可扩展哈希索引**：首期不做，仅实现 B+ 树；后续有需求再加。
