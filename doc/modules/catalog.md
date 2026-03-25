# 目录与模式（`src/catalog/`）

## 职责

描述**表的结构**：列名、类型、长度、偏移等，供执行器解析元组、Planner 做类型检查与输出列推导。

## 核心类

### `Column`

单列定义：名称、`ColumnValueType`、长度、是否可空等（见 `column.h`）。提供 `GetName()`、`GetType()` 等访问器。

### `Schema`

多列的集合，描述**一行元组**的布局：

- 定长列按偏移存放  
- 变长列通常配合槽/长度字段（具体布局与 `Tuple`、`RmRecord` 一致）  

执行器通过 `Schema` 与 `ColMeta`（系统元数据中扩展了表名等信息）解析二进制缓冲区。

## 与 `SmManager` 的关系

**表级元数据**（`TabMeta` / `ColMeta`）由 **`system`** 模块持久化在 `db.meta`；`Schema` 对象多在运行期从元数据**构造**，供算子使用。详见 [system-manager.md](system-manager.md)。

## 源码位置

- `src/catalog/schema.cpp`  
- `src/catalog/column.cpp`  
- 头文件：`src/include/catalog/schema.h`、`column.h`
