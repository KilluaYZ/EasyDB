# 语义分析（`src/analyze/`）

## 职责

将 **AST** 转为 **`Query`**：解析表名/列名、推导类型、展开 `JOIN`/`WHERE`/`GROUP BY` 等，供 **Planner** 使用。

## 核心类型 — `Query`（`analyze.h`）

`Query` 成员（节选）：

| 字段 | 含义 |
|------|------|
| `parse` | 原始 AST 根 |
| `tables` | 涉及的表 |
| `cols` | SELECT 列表（含聚合信息） |
| `conds` | WHERE 条件 |
| `groupby_cols` / `having_conds` | 分组与 HAVING |
| `values` / `set_clauses` | INSERT / UPDATE |
| `no_result` | 优化：恒假条件等 |

另含排序、去重、子查询等扩展字段（以头文件为准）。

## `Analyze` 类

- **`do_analyze(std::shared_ptr<ast::TreeNode>)`** → **`std::shared_ptr<Query>`**  
- 内部查询 **`SmManager`** 元数据：表是否存在、列是否属于表、类型是否兼容。  
- 将 AST 中的表名、列名转为内部 **`TabCol`**、**`Condition`** 等。

## 与 Planner 的边界

- **Analyze**：名字解析、语义合法性、**逻辑内容**填充到 `Query`。  
- **Planner**：代价估计、连接顺序、索引选择、生成 **`Plan`** 树。

专题补充见 [../query_analyse/query_analyse.md](../query_analyse/query_analyse.md)。
