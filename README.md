<div align="center">
  <img src="./doc/images/icon.png" alt="EasyDB Icon" width="200">
</div>

---

EasyDB 是一个用 **Rust** 从零实现的关系型数据库管理系统（RDBMS），支持大多数标准 SQL
语句。它实现了数据库系统的核心模块：存储管理、缓冲区管理、记录与索引管理、查询解析与
优化、执行引擎、事务管理、并发控制（2PL + wait-die）以及 WAL 故障恢复。

> 本项目最初基于中国人民大学数据库系统课程实验框架（C++）实现，现已用 Rust 完整重写：
> 全库零 `unsafe`，内存安全由类型系统保证（RAII PageGuard、写集回滚、`Result` 错误链）。
> C++ 历史实现可参见 git 历史中的 `src-cpp/`。

## 项目特性

- **完整的 SQL 支持**：DDL（CREATE/DROP TABLE、CREATE/DROP INDEX、DESC、SHOW）、
  DML（INSERT/DELETE/UPDATE/SELECT，含 GROUP BY / HAVING / ORDER BY / DISTINCT /
  JOIN / IN 子查询）、事务（BEGIN/COMMIT/ABORT/ROLLBACK）
- **存储引擎**：页式存储 + LRU 缓冲池（RAII PageGuard），slotted page 记录管理
- **索引**：B+ 树索引（含自动回填、恢复后重建）
- **查询处理**：LALRPOP 自持文法解析、语义分析、条件下推与索引选择、火山模型执行器
- **事务与并发**：严格两阶段锁 + wait-die 死锁避免，语句级保存点回滚
- **故障恢复**：WAL（提交即刷盘）+ 重启恢复（重复历史 + 失败者撤销 + 索引重建）
- **C/S 架构**：TCP 文本协议服务端 + 命令行客户端

## 快速开始

### 环境要求

- Rust 工具链（rustup，edition 2024）
- 可选：Nix（flake 已内置 Rust 工具链）

### 构建与运行（Cargo）

```shell
cargo build --release
```

产物在 `target/release/` 下：

- `easydb_server`：服务端（`-p` 指定端口，`-d` 指定数据目录）
- `easydb_client`：命令行客户端（`-H/--host` 服务地址，`-p/--port` 端口）

启动服务端与客户端：

```shell
./target/release/easydb_server -p 8765 -d test.db
./target/release/easydb_client -p 8765
```

客户端内输入以分号结尾的 SQL，支持多行语句；使用上下键查看历史，输入 `exit` 或按 `Ctrl-D` 退出：

```sql
CREATE TABLE student (
    id INT,
    name VARCHAR(50),
    score FLOAT
);
INSERT INTO student VALUES (1, 'alice', 90.5);
SELECT name FROM student WHERE score >= 60 ORDER BY id;
```

也可以直接用 telnet 等原始 TCP 客户端连接（每条 SQL 以换行或 NUL 结尾）。

### 测试

```shell
cargo test
```

测试覆盖各层单元/集成用例：缓冲池与 B+ 树（含随机操作对照 `BTreeMap` 的
property test）、SQL 解析与绑定、端到端 CRUD/连接/聚合、事务回滚、wait-die
并发、崩溃恢复（redo + undo + 索引重建）、TCP 文本协议与服务端重启持久化。

## 支持的 SQL 功能

### 数据定义语言 (DDL)

- `CREATE TABLE` / `DROP TABLE` / `DESC TABLE`
- `CREATE INDEX` / `DROP INDEX` / `SHOW TABLES` / `SHOW INDEX`

### 数据操作语言 (DML)

- `SELECT`：WHERE、JOIN、GROUP BY、HAVING、ORDER BY、DISTINCT、
  聚合（COUNT/SUM/MAX/MIN）、`IN` 子查询
- `INSERT` / `DELETE` / `UPDATE`（支持 `col = col op value` 算术赋值）

### 事务与其他

- `BEGIN` / `COMMIT` / `ABORT` / `ROLLBACK`
- `SET ENABLE_NESTLOOP = ON|OFF` 等运行开关（语法兼容，执行器当前恒用
  嵌套循环）
- `LOAD` 数据加载（开发中）

## 项目结构

```
EasyDB/
├── src/                    # Rust 源码（库 + 两个可执行）
│   ├── bin/
│   │   ├── easydb_server.rs   # TCP 服务端
│   │   └── easydb_client.rs   # 命令行客户端
│   ├── parser/             # 词法器 + LALRPOP 文法 + AST
│   ├── analyze/            # 语义分析（schema 绑定）
│   ├── planner/            # 计划生成（条件下推/索引选择）
│   ├── execution/          # 火山模型执行器
│   ├── catalog/            # 列/模式定义
│   ├── storage/            # 磁盘管理与页
│   ├── buffer/             # LRU 缓冲池（PageGuard）
│   ├── record/             # 记录管理 / 元组编解码
│   ├── index/              # B+ 树索引
│   ├── transaction/        # 事务与写集
│   ├── concurrency/        # 锁管理器（2PL + wait-die）
│   ├── recovery/           # WAL 与重启恢复
│   └── system/             # 目录管理 / 持久化 / 格式化
├── doc/                    # 技术文档（含 Rust 重构迁移计划）
├── web_client/             # Web 前端（沿用原实现）
└── flake.nix               # Nix 开发环境 / 构建
```

## 技术文档

模块级架构与实现说明见 [doc/README.md](doc/README.md)；Rust 重构的动机、
决议与阶段计划见 [doc/rust-migration-plan.md](doc/rust-migration-plan.md)。

## 贡献

欢迎提交 Issue 和 Pull Request！

## 许可证

本项目采用 GNU General Public License v3.0 (GPL-3.0) 许可证。详情请参阅
[LICENSE](LICENSE) 文件。

## 致谢

本项目基于中国人民大学数据库系统课程实验框架（C++ 版）的语义设计，使用 Rust
完整重写。
