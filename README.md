# EasyDB

EasyDB 是一个功能完备的关系型数据库管理系统（RDBMS），采用 C++ 实现，支持大多数标准 SQL 语句。该系统实现了数据库系统的核心功能，包括存储管理、查询处理、事务管理、并发控制和故障恢复等模块。

## 项目特性

- **完整的 SQL 支持**：支持 DDL（CREATE、DROP、ALTER）、DML（SELECT、INSERT、UPDATE、DELETE）等标准 SQL 语句
- **高效的存储引擎**：基于页面的存储管理，支持 B+ 树索引和可扩展哈希索引
- **智能查询优化**：实现了逻辑优化（连接重排、条件剪枝等）和物理优化（基于统计信息的查询优化）
- **多种连接算法**：支持嵌套循环连接、归并连接、哈希连接等多种连接策略
- **事务支持**：实现了 ACID 特性，支持显式事务和自动提交
- **并发控制**：基于锁的并发控制机制，支持多用户并发访问
- **故障恢复**：基于 WAL（Write-Ahead Logging）的恢复机制
- **C/S 架构**：支持命令行客户端和 Web GUI 两种交互方式

## 系统架构

EasyDB 采用模块化设计，主要包含以下核心模块：

- **存储管理**：磁盘管理、页面管理、表文件管理
- **缓冲区管理**：基于 LRU 的缓冲池管理
- **记录管理**：记录的存储、检索和扫描
- **索引管理**：B+ 树索引和可扩展哈希索引
- **查询处理**：词法分析、语法分析、语义分析、查询优化、查询执行
- **事务管理**：事务的创建、提交、回滚
- **并发控制**：锁管理器，支持多粒度锁
- **恢复管理**：日志管理和故障恢复

## 快速开始

### 环境要求

- Linux 系统（推荐 Ubuntu 24.04）
- Nix 包管理器（用于依赖管理）
- CMake 3.28+
- C++ 编译器（支持 C++17）

### 安装 Nix 并开启 Flake

编译环境的构建使用了 Nix Flake，所以在使用之前需要进行相应配置。

1. 安装 Nix：

```shell
sh <(curl -L https://nixos.org/nix/install) --daemon
```

2. 修改 `/etc/nix/nix.conf`，添加以下内容以开启 Flake：

```
substituters = https://mirrors.tuna.tsinghua.edu.cn/nix-channels/store https://cache.nixos.org/
experimental-features = nix-command flakes
```

### 编译

运行以下命令，Nix 会自动下载项目依赖并进行编译。首次编译时间可能会比较长，请耐心等待。编译结果会放在项目目录下的 `result` 目录中。

```shell
nix build
```

`result` 目录结构如下：

```
result
├── bin
│   ├── easydb_client  # EasyDB 的 CLI 客户端
│   └── easydb_server  # EasyDB 的服务端
└── test               # 测试文件编译得出的二进制
```

### 开发环境

运行以下命令，Nix 会开启一个新的 shell，该 shell 已经配置好了能够编译该项目的环境。在此终端中使用 `code` 等命令开启 VSCode 可以让 IDE 找到对应的环境。

```shell
nix develop
```

进入开发环境后，可以使用传统的 CMake 方式编译：

```shell
mkdir build && cd build
cmake ..
make -j
```

## 运行

### 使用 CLI 交互

1. 启动服务端：

```shell
./result/bin/easydb_server -p 8888 -d test.db
```

2. 在另一个终端启动客户端：

```shell
./result/bin/easydb_client -p 8888
```

3. 在客户端中输入 SQL 语句，例如：

```sql
CREATE TABLE student (
    id INTEGER NOT NULL,
    name VARCHAR(50) NOT NULL,
    age INTEGER
);

INSERT INTO student VALUES (1, 'Alice', 20);
INSERT INTO student VALUES (2, 'Bob', 21);

SELECT * FROM student WHERE age > 20;
```

### 使用 Web GUI 交互

1. 启动服务端（启用 Web 模式）：

```shell
./result/bin/easydb_server -p 8888 -d test.db -w
```

2. 激活开发环境：

```shell
nix develop .
```

3. 启动代理服务器（在一个终端中）：

```shell
cd web_client
python ./proxy/proxy_server.py
```

4. 启动 Web 前端（在另一个终端中）：

```shell
cd web_client
npm install
npm run dev
```

5. 访问 http://localhost:2000/ 打开 Web 界面

## 支持的 SQL 功能

### 数据定义语言 (DDL)

- `CREATE TABLE`：创建表
- `DROP TABLE`：删除表
- `CREATE INDEX`：创建索引
- `DROP INDEX`：删除索引
- `DESC TABLE`：查看表结构
- `SHOW TABLES`：显示所有表
- `SHOW INDEX`：显示表的索引

### 数据操作语言 (DML)

- `SELECT`：查询数据，支持：
  - WHERE 条件过滤
  - JOIN 操作（嵌套循环、归并、哈希连接）
  - GROUP BY 和 HAVING
  - ORDER BY 排序
  - 聚合函数（COUNT、SUM、MAX、MIN）
  - DISTINCT/UNIQUE
- `INSERT`：插入数据
- `UPDATE`：更新数据
- `DELETE`：删除数据

### 事务控制

- `BEGIN`：开始事务
- `COMMIT`：提交事务
- `ROLLBACK`：回滚事务
- `ABORT`：中止事务

### 其他功能

- `LOAD`：从文件加载数据
- `SET`：设置系统参数（如启用/禁用特定连接算法）

## 项目结构

```
EasyDB2024/
├── src/                    # 源代码目录
│   ├── storage/           # 存储管理模块
│   ├── buffer/             # 缓冲区管理模块
│   ├── record/             # 记录管理模块
│   ├── catalog/            # 目录管理模块
│   ├── parser/             # SQL 解析器（词法/语法分析）
│   ├── analyze/             # 语义分析模块
│   ├── planner/             # 查询计划生成模块
│   ├── optimizer/           # 查询优化模块
│   ├── execution/          # 查询执行模块
│   ├── transaction/        # 事务管理模块
│   ├── concurrency/          # 并发控制模块
│   ├── recovery/          # 恢复管理模块
│   └── system/            # 系统管理模块
├── include/                # 头文件目录
├── test/                   # 测试代码
├── doc/                    # 文档目录
│   ├── storage/           # 存储管理文档
│   ├── query_analyse/     # 查询分析文档
│   ├── spj/               # SPJ 算法文档
│   └── optimizer/         # 查询优化文档
├── web_client/            # Web 客户端
├── deps/                  # 第三方依赖
├── CMakeLists.txt         # CMake 构建配置
├── flake.nix              # Nix Flake 配置
└── README.md              # 本文件
```

## 技术文档

详细的技术文档请参考 `doc/` 目录下的文档：

- [存储管理技术文档](doc/storage/storage.md)
- [查询分析技术文档](doc/query_analyse/query_analyse.md)
- [SPJ 算法技术文档](doc/spj/spj.md)
- [查询优化技术文档](doc/optimizer/optimizer.md)
- [模块技术细节文档](doc/architecture.md) - 各模块的详细技术实现

## 测试

项目包含完整的测试套件，位于 `test/` 目录下。运行测试：

```shell
cd build/test
./comprehensive_test
```

## 贡献

欢迎提交 Issue 和 Pull Request！

## 许可证

本项目采用 GNU General Public License v3.0 (GPL-3.0) 许可证。详情请参阅 [LICENSE](LICENSE) 文件。

## 致谢

本项目基于中国人民大学数据库系统课程实验框架开发。
