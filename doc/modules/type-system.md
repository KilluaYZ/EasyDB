# 类型系统（`src/type/`）

## 职责

实现 SQL 值在内存中的**强类型表示**与**比较、序列化**等操作，与 `defs.h` 中的 **`ColType`**（`TYPE_INT`、`TYPE_LONG`、`TYPE_FLOAT`、`TYPE_DOUBLE`、`TYPE_VARCHAR`、`TYPE_CHAR`、`TYPE_DATE` 等）对应。

## 核心类型

### `type/type_id.h`

内部类型枚举，用于 `Value` 与各 `Type` 子类分发。

### `type/value.h` — `Value`

承载单行中**一个标量**的值：类型标签 + 负载（或指向变长数据的指针）。执行器、索引键比较、`INSERT` 的 VALUES 都会使用。

### `type/type.h` 与各具体类型

抽象基类 **`Type`**，派生类包括例如：

- `IntegerType`、`BigintType`  
- `DecimalType`、`TimestampType`  
- `VarlenType`（变长字符串）  

职责通常包括：从字节缓冲区解释值、比较、长度、与 `ColType` 的映射等。

### `type/limits.h`

取值范围等常量。

## 与索引、记录的关系

- **索引键比较**：`storage/index/ix_defs.h` 中 **`ix_compare`** 按 `ColType` 对原始字节做比较，需与表列类型一致。  
- **元组**：`Tuple`（`storage/table/tuple`）按 `Schema` 布局二进制，类型系统负责**值级别**语义。

## 依赖

类型模块主要依赖标准库与少量公共头文件；`catalog` 中的列定义会间接决定使用哪种 `ColType` / `Type`。
