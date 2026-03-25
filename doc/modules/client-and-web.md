# 客户端与 Web 界面（仓库内外围组件）

## 命令行客户端

- **路径**：`client/main.cpp`  
- **构建**：`client/CMakeLists.txt` 生成可执行文件（名称以 CMake 项目名为准，根 README 中常称为 `easydb_client`）。  
- **协议**：与 `easydb_server` 通过 **TCP** 传输 SQL 文本与结果；具体为定长/换行约定，以 `BUFFER_LENGTH` 与 `read`/`write` 循环为准（见 `easydb.cpp` 与客户端源码）。

## Web 客户端

- **路径**：`web_client/`  
- **依赖**：Node/npm 前端 + Python 代理（`web_client/proxy/proxy_server.py`）。  
- **服务端**：`easydb_server` 需 **`-w`** 开启 Web 模式，响应序列化路径不同（`SerializeToWithLimit`）。

详细启动步骤见仓库根目录 [README.md](../../README.md) 中「使用 Web GUI 交互」一节。

## 与核心模块的关系

Web/CLI **不参与**查询优化与存储；仅作为 **SQL 传输与展示层**，核心逻辑仍在 `src/` 各模块。
