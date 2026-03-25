# 构建与 CMake 结构（补充）

## 顶层

- **`CMakeLists.txt`**：项目名、C++ 标准、子目录 `deps/`、`src/`、`client/`、`test/`。  
- **`flake.nix`**：Nix 开发/构建环境（根 README 中 `nix build` / `nix develop`）。

## `src/CMakeLists.txt`

- 各子目录 **`add_subdirectory`** 产生多个静态库（如 `easydb_buffer`、`easydb_storage_disk` 等）。  
- 所有目标对象汇总为 **`easydb`** 静态库 `libeasydb.a`。  
- 可执行文件 **`easydb_server`** 由 `easydb.cpp` 链接 `easydb` 与第三方库（**Threads**、**nlohmann_json**、**murmur3** 等）。

链接库列表见 `src/CMakeLists.txt` 中 `EASYDB_LIBS`。

## 各子目录

每个模块目录下自有 **`CMakeLists.txt`**，定义该模块源文件与 `add_library`。

## 测试

`test/CMakeLists.txt` 生成综合测试等二进制（根 README 中 `comprehensive_test`）。
