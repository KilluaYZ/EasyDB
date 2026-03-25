# 记录管理（`src/record/`）

## 职责

在表数据文件上提供**记录的增删改查与扫描**：将表抽象为 **页号 + 槽号（RID）** 的堆文件组织。

## 文件布局要点

**`rm_defs.h`** 中：

- **`RM_FILE_HDR_PAGE = 0`**：文件头页（`RmFileHdr`）  
- **`RM_FIRST_RECORD_PAGE = 1`**：数据从第 1 页开始（与部分教材「第 0 页为头」表述一致）  
- **`RmFileHdr`**：`num_pages`、`first_free_page_no` 等（用于找空闲空间）  
- **`RmRecord`**：`data` 指针、`size`、`allocated_` 管理深拷贝与析构  

单条记录最大长度 **`RM_MAX_RECORD_SIZE`**（512 等，以头文件为准）。

## 核心类

### `RmManager`

创建/打开表文件，提供 **`RmFileHandle`**。

### `RmFileHandle`

- `insert_record` / `delete_record` / `update_record` / `get_record`  
- 内部通过 **`TablePage`** 操纵页内槽位  

### `RmScan`

顺序扫描迭代器：`next()`、`is_end()`，配合 `Rid` 遍历。

### `bitmap.h`

页内槽位占用/空闲可用位图辅助。

## 与索引的关系

插入/删除/更新堆记录时，若表上存在索引，**`IxManager` / `IxIndexHandle`** 需同步维护键 → RID；详见 [index-bplus-and-extensible-hash.md](index-bplus-and-extensible-hash.md)。

## 与执行器的关系

`SeqScanExecutor` 底层即调用 **`RmScan`** 或等价路径；`IndexScanExecutor` 先通过索引得 RID，再 `get_record`。
