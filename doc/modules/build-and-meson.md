# 构建与 Meson 结构（补充）

## 顶层

- **`meson.build`**：定义项目、编译选项、第三方依赖、Flex/Bison 代码生成，以及库、服务端、客户端和测试目标。
- **`meson_options.txt`**：定义可配置的构建选项，例如 AddressSanitizer。
- **`meson-clang.ini`**：指定默认使用 `clang` 和 `clang++`。
- **`flake.nix`**：提供 Nix 开发环境和 Meson 构建环境。

## 目标组织

- `deps/murmur3` 构建为 `easydb_murmur3` 静态库。
- `src/` 下的实现文件和生成的解析器源码汇总为 `libeasydb.a`。
- `easydb_server` 链接 `libeasydb.a`、Threads 和 nlohmann_json。
- `easydb_client` 链接 readline 和 Threads。

## 解析器生成

Meson 通过 `custom_target()` 调用 Bison 和 Flex，生成 `yacc.tab.cpp`、`yacc.tab.h` 和 `lex.yy.cpp`，并将其加入 `libeasydb.a` 的源文件列表。

## 测试

测试源码会生成独立的测试程序。可自动运行的单元测试通过 Meson 的 `test()` 注册，执行：

```shell
meson setup build --native-file meson-clang.ini -Deasydb_enable_asan=true
meson compile -C build
meson test -C build --print-errorlogs
```

`parser_ast_printer` 是需要命令行参数的工具，`comprehensive_test` 依赖仓库外的 benchmark 数据，因此只参与编译，不自动注册到测试集。
