# SQL 词法与语法分析（`src/parser/`）

## 职责

将客户端字符串解析为 **抽象语法树（AST）**，入口为 **`yyparse()`**，根节点挂在 **`ast::parse_tree`**（见 `parser.h` / `ast.h`）。

## 实现技术

- **词法**：Flex，`lex.l` 定义 token（关键字、标识符、字面量、运算符）。  
- **语法**：Bison，`yacc.y` 定义产生式，构建 `ast::TreeNode` 派生类节点。

## 关键文件

| 文件 | 作用 |
|------|------|
| `lex.l` | 词法规则 |
| `yacc.y` | 语法规则与 AST 构造 |
| `ast.h` / `ast.cpp` | AST 节点类型 |
| `parse_node.h` | 解析节点辅助 |
| `parser.h` | `Parser` 封装、`ast::parse_tree` |

## 与上层衔接

服务端在 **`client_handler`** 中：

```cpp
yy_scan_string(data_recv);
yyparse();
```

解析成功后 **`ast::parse_tree`** 交给 **`Analyze::do_analyze`**。

## 线程安全说明

Flex/Bison 通常使用**全局状态**；EasyDB 用 **`buffer_mutex`** 保证同一时刻仅一个线程进入 `yyparse`。多线程下**不要**在未持锁时并发调用。

## 错误处理

`yyparse() != 0` 时视为语法错误，`Context` 可标记 JSON 消息为 `"syntax error"`（见 `easydb.cpp`）。
