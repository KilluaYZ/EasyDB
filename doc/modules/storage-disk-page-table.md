# 存储：磁盘、页面与表页（`src/storage/disk/`, `page/`, `table/`）

## 磁盘管理 — `DiskManager`

**文件**：`disk_manager.cpp` / `disk_manager.h`

职责：

- 在数据库目录下 **创建 / 打开 / 关闭** 数据文件与日志文件  
- 按 **页** 读写：`ReadPage` / `WritePage`（页号 × `PAGE_SIZE` 定位偏移）  
- 分配新页：`AllocatePage` 等（具体名称以头文件为准）

**`PageId`**（`page.h`）：由 **`fd` + `page_no`** 唯一标识一页。同一进程内多个表/索引文件对应不同 `fd`。

## 页面抽象 — `Page`

**文件**：`storage/page/page.h`

每帧对应一个 **`Page`** 对象，包含：

- 元数据：`page_id_`、`pin_count_`、`is_dirty_`、`page_lsn_`（恢复用）  
- 负载：`data_` 数组，大小为 **`PAGE_SIZE`**

**注意**：表堆、B+ 树节点、哈希桶等**复用**同一 `Page` 容器，语义由上层解释。

## 表页 — `TablePage`

**文件**：`table_page.h` / `table_page.cpp`

在 `Page` 的 `data_` 上实现**堆表页**布局：槽位、记录插入删除、元组存取。与 **记录模块**的 `RmFileHandle` 协同：文件头页 + 数据页链。

## 元组 — `Tuple`

**文件**：`tuple.h` / `tuple.cpp`

表示一行数据在内存中的视图，配合 **`Schema`** 访问各列；执行器 `Next()` 常返回 `std::unique_ptr<Tuple>`。

## 数据流

```text
DiskManager (fd, byte offset)
       ↓
BufferPoolManager → Page* → TablePage / 索引页解释
```

缓冲池细节见 [buffer-pool.md](buffer-pool.md)。
