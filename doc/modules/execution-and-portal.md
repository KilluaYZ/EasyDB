# 执行引擎与 Portal（`src/execution/` + `portal.h`）

## 职责

- **`Plan`**（逻辑/物理计划树）→ **`AbstractExecutor`** 树（火山模型）  
- **`QlManager`** 驱动执行，将结果写入 **`Context`** 缓冲区（表格文本或 JSON）

## `QlManager`（`execution_manager.h` / `execution_manager.cpp`）

| 方法 | 用途 |
|------|------|
| `select_from(root, sel_cols, context)` | 遍历执行器，打印 SELECT 结果 |
| `run_dml(exec)` | INSERT/UPDATE/DELETE 执行 |
| `run_cmd_utility(plan, txn_id, context)` | DDL、SHOW、事务命令等 |
| `run_mutli_query(plan, context)` | 多语句/DDL 批处理 |

子查询支持 **`subquery_select_from`**，对子计划做求值。

## `Portal`

**头文件**：`src/include/common/portal.h`

### `Portal::start(plan, context)`

根据 `Plan` 动态类型构造 **`PortalStmt`**：

- **`OtherPlan` / `SetKnobPlan` / `LoadDataPlan`** → `PORTAL_CMD_UTILITY`  
- **`DDLPlan`** → `PORTAL_MULTI_QUERY`  
- **`DMLPlan`**：  
  - `T_select`：子计划转为 `ProjectionExecutor` 树根，`PORTAL_ONE_SELECT`  
  - `T_Update` / `T_Delete`：先对子计划扫描收集 **RID 列表**，再构造 `UpdateExecutor` / `DeleteExecutor`  
  - `T_Insert`：`InsertExecutor`  

### `Portal::run(portal, ql, txn_id, context)`

按 **`portalTag`** 调用 `QlManager` 对应接口（见上表）。

### `convert_plan_executor`

递归将 **`ProjectionPlan`、`ScanPlan`、`JoinPlan`、`SortPlan`、`AggregationPlan`** 等映射为：

| Plan 类型 | 执行器 |
|-----------|--------|
| `ProjectionPlan` | `ProjectionExecutor`（含 DISTINCT/UNIQUE 标志） |
| `ScanPlan`（顺序） | `SeqScanExecutor` |
| `ScanPlan`（索引） | `IndexScanExecutor` |
| `JoinPlan` | `NestedLoopJoinExecutor` / `MergeJoinExecutor` / `HashJoinExecutor` |
| `SortPlan` | `SortExecutor` |
| `AggregationPlan` | `AggregationExecutor` |

**子查询**：`ScanPlan` 的条件中若 `is_rhs_stmt`，会对右侧 `DMLPlan` 再调用 `convert_plan_executor`，把结果执行器挂到条件上。

## `AbstractExecutor`（`executor_abstract.h`）

火山模型核心接口：

- `beginTuple()` / `nextTuple()` / `IsEnd()`  
- **`Next()`** → `std::unique_ptr<Tuple>`  
- **`rid()`** → 当前记录 RID  

各具体执行器在 `src/execution/` 下实现。

## 结果输出

`RecordPrinter`（`record/record_printer.h`）负责将行格式化为客户端可读文本并写入 `Context`。

## 延伸阅读

- SELECT 全流程：[../SELECT查询流程详解.md](../SELECT查询流程详解.md)  
- SPJ 与连接：[../spj/spj.md](../spj/spj.md)
