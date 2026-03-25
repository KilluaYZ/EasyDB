# 系统管理 SmManager（`src/system/`）

## 职责

数据库**元数据**的中心：**创建/打开/关闭库**，**DDL**（表/索引），维护运行时句柄与统计信息。

## 核心数据成员（节选）

| 成员 | 作用 |
|------|------|
| `db_` | **`DbMeta`**：当前库内所有表、索引的序列化元数据 |
| `fhs_` | 表名/文件名 → **`RmFileHandle`** |
| `ihs_` | 索引文件名 → **`IxIndexHandle`** |
| `disk_manager_` / `buffer_pool_manager_` / `rm_manager_` / `ix_manager_` | 下层子系统指针 |
| `table_count_` / 列最大最小值等 | **优化器**代价与 `Optimizer::bypass` 使用 |

## 持久化

- 库级元数据文件：**`db.meta`**（`DB_META_NAME`）  
- 每张表、每个索引对应独立数据文件，由 `DiskManager` 管理  

## 典型操作

- **`CreateDB` / `OpenDB` / `CloseDB`**  
- **`CreateTable` / `DropTable`**：创建/删除堆文件与元数据  
- **`CreateIndex` / `DropIndex`**：创建/删除索引文件并更新 `TabMeta`  
- **`GetTableCount`** 等：查询统计信息  

## 与执行路径的关系

- **Analyze** 校验表列是否存在  
- **Planner** 读取索引定义、表大小  
- **DDL/SHOW/DESC** 通过 **`QlManager::run_cmd_utility`** 直接调用 `SmManager`  

头文件：`src/include/system/sm_manager.h`，元数据结构见 **`sm_meta.h`**、**`sm_defs.h`**。
