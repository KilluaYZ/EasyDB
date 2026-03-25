# 查询计划器 Planner（`src/planner/`）

## 职责

将 **`Query`** 转为 **`Plan`**（`plan.h` 中各类 `shared_ptr<Plan>` 派生类），包括：

- 单表扫描、索引扫描、连接、投影、过滤、排序、聚合等  
- **逻辑优化**：`logical_optimization`（条件下推、连接重排等，见 `planner.cpp`）  
- **物理优化**：`physical_optimization`、代价估计 `estimate_table_scan_cost` / `estimate_join_cost`  

## 主要接口

```cpp
std::shared_ptr<Plan> do_planner(std::shared_ptr<Query> query, Context *context);
```

## 连接实现开关

`Planner` 内布尔成员（可通过 setter 调整）：

- `enable_nestedloop_join`（默认 true）  
- `enable_sortmerge_join`  
- `enable_hash_join`  
- `enable_optimizer`：是否启用优化管线中的部分规则  

与 **`SET`** 语句/旋钮（`SetKnobPlan`）配合可在运行时改变行为（以实际 `run_cmd_utility` 实现为准）。

## 辅助方法（节选）

- `reorder_joins` / `reorder_conds_based_on_table_size`  
- `deduce_conditions_via_equijoin`  
- `get_index_cols`：从条件中提取可用索引列  

## 与 Optimizer 的关系

**`Optimizer::plan_query`** 对 HELP/SHOW/DESC/事务语句直接生成 **`OtherPlan`** 等；其余 DML/查询调用 **`planner_->do_planner`**。详见 [optimizer-shell.md](optimizer-shell.md)。

专题：[../optimizer/optimizer.md](../optimizer/optimizer.md)、[../spj/spj.md](../spj/spj.md)。
