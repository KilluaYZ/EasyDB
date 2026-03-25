# 执行器清单（补充）

本文件列出 `src/execution/` 中与 `src/include/execution/` 中对应的**物理算子**实现文件，便于快速跳转源码。

| 头文件 / 实现 | 作用概要 |
|----------------|----------|
| `executor_abstract.h` | `AbstractExecutor` 基类与公共辅助 |
| `executor_seq_scan.h` / `.cpp` | 顺序扫描表 |
| `executor_index_scan.h` / `.cpp` | 索引扫描 |
| `executor_nestedloop_join.h` / `.cpp` | 嵌套循环连接 |
| `executor_merge_join.h` / `.cpp` | 归并连接（含 index-merge 变体标志） |
| `executor_hash_join.h`（主要为头文件内实现） | 哈希连接 |
| `executor_projection.cpp` + `executor_projection.h` | 投影 / DISTINCT |
| `executor_sort.h` / `.cpp` | 排序 |
| `executor_aggregation.h` / `.cpp` | 聚合与分组 |
| `executor_insert.h` / `.cpp` | 插入 |
| `executor_update.h` / `.cpp` | 更新 |
| `executor_delete.h` / `.cpp` | 删除 |
| `execution_manager.h` / `execution_manager.cpp` | `QlManager` 与辅助函数 |

**说明**：哈希连接主体在 `executor_hash_join.h`；其余算子多为 `.h` + `.cpp` 成对出现。

与 **`Portal::convert_plan_executor`** 的映射关系见 [execution-and-portal.md](execution-and-portal.md)。
