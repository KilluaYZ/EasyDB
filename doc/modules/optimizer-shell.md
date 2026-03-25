# 优化器外壳 Optimizer（`src/include/optimizer/optimizer.h`）

## 职责

**`Optimizer`** 是 **`plan_query`** 的**统一入口**：根据 `Query` 内 AST 节点类型**分发**到不同计划构造路径。

## 主要分支（概念）

| AST / 情况 | 典型计划类型 |
|------------|----------------|
| `Help` | `OtherPlan(T_Help)` |
| `ShowTables` / `ShowIndex` / `DescTable` | `OtherPlan` 带操作子类型 |
| `TxnBegin` / `TxnCommit` / `TxnRollback` / `TxnAbort` | 对应 `OtherPlan` |
| `SetStmt` | `SetKnobPlan` |
| `LoadData` | `LoadDataPlan` |
| `CreateStaticCheckpoint` | `OtherPlan` |
| 其他 | **`planner_->do_planner(query, context)`** |

## `bypass`

对部分极简查询（如单表 `COUNT(*)` 且统计信息可用）可**绕过完整执行器**，直接由 **`SmManager::GetTableCount`** 等返回结果（见 `optimizer.h` 实现）。用于减少开销。

## 文件位置

实现为**头文件内联**为主：`src/include/optimizer/optimizer.h`（无独立 `optimizer.cpp` 时以仓库为准）。

## 与 Planner 文档的关系

- **Optimizer**：路由与快捷路径。  
- **Planner**：复杂 SELECT/DML 的**真正**计划生成。  

二者配合见 [planner.md](planner.md)。
