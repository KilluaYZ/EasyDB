# 索引：B+ 树与可扩展哈希（`src/storage/index/`）

## 职责

加速等值与范围查询；二级索引的叶子存 **(键, RID)**，指向堆表记录。

## 公共定义 — `ix_defs.h`

- **B+ 树页号约定**：如 `IX_FILE_HDR_PAGE`、`IX_LEAF_HEADER_PAGE`、`IX_INIT_ROOT_PAGE`、`IX_INIT_NUM_PAGES` 等。  
- **可扩展哈希**：`IX_INIT_DIRECTORY_PAGE`、`IX_INIT_BUCKET_0_PAGE` 等初始布局常量。  
- **`ix_compare`**：按 `ColType` 对键字节做三向比较。  
- **`IxFileHdr` / `IxPageHdr` / `ExtendibleHashIxFileHdr`**：文件头与页头布局。

## `IxManager`

创建/删除/打开索引文件，分发 **`IxIndexHandle`**（B+）或 **`IxExtendibleHashIndexHandle`**（可扩展哈希，若启用）。

## B+ 树 — `IxIndexHandle` / `IxNodeHandle`

- 内部结点：键 + 子指针（页号）  
- 叶子：有序键 + RID，叶链用于范围扫描  
- 插入删除触发**分裂/合并**，需写日志并维护父指针  

范围扫描使用 **`IxScan`**。

更细的序列化示例见仓库 [../B+树序列化与反序列化示例.md](../B+树序列化与反序列化示例.md) 与 [../index/index.md](../index/index.md)。

## 可扩展哈希 — `IxExtendibleHashIndexHandle`

目录 + 桶；哈希值前缀决定目录项；桶满时**分裂**并可能**扩展全局深度**。常量 **`BUCKET_SIZE`**（`config.h`）等与桶页布局相关。

测试可参考 `test/storage/index/extendible_hash_index_test.cpp`。

## 与事务

事务对象中维护 **`index_latch_page_set_`**、**`index_deleted_page_set_`**，用于提交/回滚时释放闩锁或恢复索引页（见 `transaction.h`）。

## 依赖

- **缓冲池** 取页；**类型系统** 保证键比较与列定义一致；**记录层** 提供 RID 语义。
