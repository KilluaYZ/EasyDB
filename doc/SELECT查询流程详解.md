# EasyDB SELECT 查询处理流程详解

## 查询示例
```sql
SELECT * FROM students WHERE age >= 20;
```

## 完整处理流程

### 1. 客户端连接与请求接收

**位置**: `src/easydb.cpp` - `client_handler()` 函数

- 客户端通过 Socket 连接到 EasyDB 服务器（默认端口 8765）
- 服务器为每个客户端连接创建独立的线程处理请求
- 接收客户端发送的 SQL 字符串到 `data_recv` 缓冲区

```cpp
// 接收客户端请求
i_recvBytes = read(fd, data_recv, BUFFER_LENGTH);
```

---

### 2. 事务初始化

**位置**: `src/easydb.cpp` - `SetTransaction()` 函数

- 创建或获取事务对象（`Transaction`）
- 初始化上下文信息（`Context`），包括：
  - 事务对象指针
  - 锁管理器指针
  - 日志管理器指针
  - 结果缓冲区（`data_send`）
  - 结果长度变量（`offset`）

```cpp
Context *context = new Context(lock_manager.get(), log_manager.get(),
                               nullptr, data_send, &offset);
SetTransaction(&txn_id, context);
```

---

### 3. SQL 解析（Parser）

**位置**: `src/parser/yacc.y` 和 `src/parser/lex.l`

- 使用 Flex/Bison（lex/yacc）进行词法和语法分析
- 将 SQL 字符串解析成抽象语法树（AST）
- 解析结果存储在全局变量 `ast::parse_tree` 中

```cpp
YY_BUFFER_STATE buf = yy_scan_string(data_recv);
if (yyparse() == 0) {
    if (ast::parse_tree != nullptr) {
        // 解析成功，继续处理
    }
}
```

**解析结果示例**:
- 识别为 `SelectStmt` 类型
- 表名: `students`
- 选择列: `*`（所有列）
- WHERE 条件: `age >= 20`

---

### 4. 语义分析（Analyze）

**位置**: `src/analyze/analyze.cpp` - `do_analyze()` 函数

#### 4.1 表存在性检查
- 检查表 `students` 是否存在于数据库中
- 如果不存在，抛出 `TableNotFoundError` 异常

```cpp
for (auto tab_name : query->tables) {
    if (!sm_manager_->db_.is_table(tab_name)) {
        throw TableNotFoundError(tab_name);
    }
}
```

#### 4.2 列信息处理
- 对于 `SELECT *`，自动展开为所有列
- 获取表的所有列元数据（`ColMeta`）
- 构建 `query->cols` 列表，包含所有列信息

```cpp
if (query->cols.empty()) {
    // select all columns
    for (auto &col : all_cols) {
        TabCol sel_col = {.tab_name = col.tab_name,
                          .col_name = col.name,
                          .aggregation_type = NO_AGG,
                          .new_col_name = ""};
        query->cols.push_back(sel_col);
    }
}
```

#### 4.3 WHERE 条件处理
- 解析 WHERE 子句中的条件表达式
- 将条件转换为 `Condition` 结构体
- 检查列名是否合法，填充表名信息
- 对于 `age >= 20`，生成：
  - `lhs_col`: `{tab_name: "students", col_name: "age"}`
  - `op`: `OP_GE`（大于等于）
  - `is_rhs_val`: `true`
  - `rhs_val`: `20`

```cpp
get_clause(x->conds, query->conds);
check_clause(query->tables, query->conds);
```

#### 4.4 输出
- 生成 `Query` 对象，包含：
  - `tables`: `["students"]`
  - `cols`: 所有列的列表
  - `conds`: `[{lhs_col: {tab_name: "students", col_name: "age"}, op: OP_GE, is_rhs_val: true, rhs_val: 20}]`

---

### 5. 查询计划生成（Planner）

**位置**: `src/planner/planner.cpp` - `do_planner()` 和 `generate_select_plan()`

#### 5.1 逻辑优化（Logical Optimization）

**位置**: `logical_optimization()` 函数

- 进行查询重写和逻辑优化
- 例如：条件推导、常量折叠等

```cpp
query = logical_optimization(std::move(query), context);
```

#### 5.2 物理优化（Physical Optimization）

**位置**: `physical_optimization()` 和 `make_one_rel()` 函数

##### 5.2.1 表扫描方式选择

对于每个表，系统需要决定使用**顺序扫描**还是**索引扫描**：

1. **提取表相关的条件**
   ```cpp
   auto curr_conds = pop_conds(query->conds, tables[i]);
   ```
   对于 `students` 表，提取条件 `age >= 20`

2. **检查索引可用性**
   ```cpp
   std::vector<std::string> index_col_names;
   bool index_exist = get_index_cols(tables[i], curr_conds, index_col_names);
   ```
   - `get_index_cols()` 函数检查是否存在匹配条件的索引
   - 检查条件中的列（`age`）是否有索引
   - 如果 `age` 列有索引，且操作符不是 `OP_NE`（不等于），则可以使用索引

3. **生成扫描计划**
   - **如果存在索引** (`index_exist == true`):
     ```cpp
     table_scan_executors[i] = std::make_shared<ScanPlan>(
         T_IndexScan, sm_manager_, tables[i], curr_conds, index_col_names);
     ```
   - **如果不存在索引** (`index_exist == false`):
     ```cpp
     table_scan_executors[i] = std::make_shared<ScanPlan>(
         T_SeqScan, sm_manager_, tables[i], curr_conds, index_col_names);
     ```

##### 5.2.2 连接处理（多表查询）

- 对于单表查询，跳过连接步骤
- 直接返回表扫描计划

```cpp
if (tables.size() == 1) {
    return table_scan_executors[0];
}
```

##### 5.2.3 聚合和排序处理

- 检查是否有 `GROUP BY`、`HAVING` 或聚合函数
- 检查是否有 `ORDER BY`
- 如果有，添加相应的计划节点

```cpp
plan = generate_aggregation_plan(query, std::move(plan));
plan = generate_sort_plan(query, std::move(plan));
```

##### 5.2.4 投影处理

- 在最外层添加投影计划（`ProjectionPlan`）
- 用于选择需要输出的列

```cpp
plannerRoot = std::make_shared<ProjectionPlan>(
    T_Projection, std::move(plannerRoot), std::move(sel_cols));
```

#### 5.3 最终计划结构

生成的执行计划树结构：
```
DMLPlan (T_select)
└── ProjectionPlan (T_Projection)
    └── ScanPlan (T_SeqScan 或 T_IndexScan)
        - tab_name: "students"
        - conds: [{age >= 20}]
        - index_col_names: [] 或 ["age"]
```

---

### 6. Portal 转换（Plan → Executor）

**位置**: `src/include/common/portal.h` - `start()` 和 `convert_plan_executor()` 函数

#### 6.1 计划类型识别

- 识别计划为 `DMLPlan`，类型为 `T_select`
- 提取内部的 `ProjectionPlan`

```cpp
if (auto x = std::dynamic_pointer_cast<DMLPlan>(plan)) {
    switch (x->tag) {
        case T_select: {
            std::shared_ptr<ProjectionPlan> p =
                std::dynamic_pointer_cast<ProjectionPlan>(x->subplan_);
            // ...
        }
    }
}
```

#### 6.2 执行器树构建

递归地将计划节点转换为执行器：

1. **ProjectionPlan → ProjectionExecutor**
   ```cpp
   return std::make_unique<ProjectionExecutor>(
       convert_plan_executor(x->subplan_, context), x->sel_cols_,
       x->is_unique_);
   ```

2. **ScanPlan → SeqScanExecutor 或 IndexScanExecutor**
   ```cpp
   if (x->tag == T_SeqScan) {
       return std::make_unique<SeqScanExecutor>(
           sm_manager_, x->tab_name_, x->conds_, context);
   } else {
       return std::make_unique<IndexScanExecutor>(
           sm_manager_, x->tab_name_, x->conds_, x->index_col_names_, context);
   }
   ```

#### 6.3 PortalStmt 创建

- 创建 `PortalStmt` 对象，包含：
  - `tag`: `PORTAL_ONE_SELECT`
  - `sel_cols`: 选择的列列表
  - `root`: 执行器树根节点
  - `plan`: 原始计划

---

### 7. 查询执行（Execution）

**位置**: `src/include/common/portal.h` - `run()` 函数
**位置**: `src/execution/execution_manager.cpp` - `select_from()` 函数

#### 7.1 Portal 执行

```cpp
portal->run(portalStmt, ql_manager.get(), &txn_id, context);
```

对于 `PORTAL_ONE_SELECT` 类型：
```cpp
case PORTAL_ONE_SELECT: {
    ql->select_from(std::move(portal->root), std::move(portal->sel_cols),
                    context);
    break;
}
```

#### 7.2 执行器树遍历

**位置**: `src/execution/execution_manager.cpp` - `select_from()` 函数

1. **打印表头**
   ```cpp
   RecordPrinter rec_printer(sel_cols.size());
   rec_printer.print_separator(context);
   rec_printer.print_record(captions, context);
   rec_printer.print_separator(context);
   ```

2. **遍历执行器树获取结果**
   ```cpp
   for (executorTreeRoot->beginTuple(); !executorTreeRoot->IsEnd();
        executorTreeRoot->nextTuple()) {
       auto tuple = executorTreeRoot->Next();
       // 处理每一行数据
   }
   ```

#### 7.3 顺序扫描执行（SeqScanExecutor）

**位置**: `src/execution/executor_seq_scan.cpp`

1. **初始化扫描**
   ```cpp
   void SeqScanExecutor::beginTuple() {
       scan_ = std::make_unique<RmScan>(fh_);
       rid_ = scan_->GetRid();
       while (!IsEnd() && !predicate()) {
           scan_->Next();
           rid_ = scan_->GetRid();
       }
   }
   ```
   - 创建记录管理器扫描器（`RmScan`）
   - 获取表文件句柄（`RmFileHandle`）
   - 定位到第一个满足条件的记录

2. **条件过滤（predicate）**
   ```cpp
   bool SeqScanExecutor::predicate() {
       auto tuple = *this->Next();
       bool satisfy = true;
       for (auto &cond : conds_) {
           Value lhs_v = tuple.GetValue(&schema_, cond.lhs_col.col_name);
           Value rhs_v = cond.rhs_val;
           if (!cond.satisfy(lhs_v, rhs_v)) {
               satisfy = false;
               break;
           }
       }
       return satisfy;
   }
   ```
   - 获取当前记录的 `age` 字段值
   - 与条件值 `20` 进行比较（`>=` 操作）
   - 如果满足条件，返回 `true`

3. **获取下一行**
   ```cpp
   void SeqScanExecutor::nextTuple() {
       do {
           scan_->Next();
           rid_ = scan_->GetRid();
       } while (!IsEnd() && !predicate());
   }
   ```
   - 移动到下一条记录
   - 跳过不满足条件的记录

4. **获取元组值**
   ```cpp
   std::unique_ptr<Tuple> SeqScanExecutor::Next() {
       return fh_->GetTupleValue(rid_, context_);
   }
   ```
   - 根据记录 ID（`RID`）从文件中读取完整的元组数据

#### 7.4 索引扫描执行（IndexScanExecutor，如果使用索引）

**位置**: `src/execution/executor_index_scan.cpp`

如果 `age` 列有索引，则使用索引扫描：

1. **初始化索引扫描**
   ```cpp
   void IndexScanExecutor::beginTuple() {
       // 获取索引句柄
       // 构造索引键值范围（age >= 20）
       // 使用 LowerBound 定位到第一个满足条件的索引项
   }
   ```

2. **通过索引获取记录**
   - 从索引中获取满足条件的记录 ID（`RID`）
   - 根据 `RID` 从表中读取完整记录

#### 7.5 投影执行（ProjectionExecutor）

**位置**: `src/execution/executor_projection.cpp`

- 从底层执行器获取元组
- 根据 `sel_cols` 选择需要输出的列
- 对于 `SELECT *`，输出所有列

#### 7.6 结果格式化

**位置**: `src/execution/execution_manager.cpp` - `select_from()`

```cpp
for (int column_itr = 0; column_itr < column_count; column_itr++) {
    if (tuple->IsNull(schema, column_itr)) {
        col_str = "NULL";
    } else {
        Value val = (tuple->GetValue(schema, column_itr));
        col_str = val.ToString();
    }
    columns.emplace_back(col_str);
}
rec_printer.print_record(columns, context);
```

- 将每个字段值转换为字符串
- 使用 `RecordPrinter` 格式化输出
- 写入结果缓冲区（`context->data_send_`）

---

### 8. 事务提交

**位置**: `src/easydb.cpp` - `client_handler()` 函数

- 对于单条 SQL 语句（非显式事务），自动提交事务
- 如果事务状态不是 `ABORTED`，则提交事务

```cpp
if (context->txn_->GetTxnMode() == false) {
    if (context->txn_->GetState() != TransactionState::ABORTED) {
        txn_manager->Commit(context->txn_, context->log_mgr_);
    }
}
```

---

### 9. 结果返回

**位置**: `src/easydb.cpp` - `client_handler()` 函数

1. **序列化结果**
   ```cpp
   context->SerializeToWithLimit(data_send_vec, 100);
   ```

2. **发送给客户端**
   ```cpp
   int send = write(fd, data_send_vec.data(), data_send_vec.size());
   ```

3. **清理资源**
   ```cpp
   delete context;
   ```

---

## 关键数据结构

### Query
```cpp
struct Query {
    std::shared_ptr<ast::TreeNode> parse;  // AST 根节点
    std::vector<std::string> tables;        // 表名列表
    std::vector<TabCol> cols;               // 选择的列
    std::vector<Condition> conds;           // WHERE 条件
    // ...
};
```

### Condition
```cpp
struct Condition {
    TabCol lhs_col;      // 左侧列
    CompOp op;           // 操作符（OP_GE 等）
    bool is_rhs_val;     // 右侧是否为值
    Value rhs_val;       // 右侧值（如果是值）
    TabCol rhs_col;      // 右侧列（如果是列）
    // ...
};
```

### Plan
- `ScanPlan`: 表扫描计划（顺序扫描或索引扫描）
- `ProjectionPlan`: 投影计划
- `JoinPlan`: 连接计划（多表查询）
- `SortPlan`: 排序计划
- `AggregationPlan`: 聚合计划

### Executor
- `SeqScanExecutor`: 顺序扫描执行器
- `IndexScanExecutor`: 索引扫描执行器
- `ProjectionExecutor`: 投影执行器
- `JoinExecutor`: 连接执行器（多种实现）

---

## 性能优化点

1. **索引选择**: 如果 `age` 列有索引，使用索引扫描可以大幅提升查询性能
2. **条件过滤**: 在扫描过程中即时过滤，减少不必要的数据读取
3. **投影下推**: 只读取需要的列，减少 I/O
4. **事务管理**: 使用锁机制保证并发安全

---

## 总结

整个流程可以概括为：

```
客户端请求 
  → SQL解析（Parser）
  → 语义分析（Analyze）
  → 查询计划生成（Planner）
    → 逻辑优化
    → 物理优化（选择扫描方式）
  → Portal转换（Plan → Executor）
  → 查询执行（Execution）
    → 表扫描（顺序或索引）
    → 条件过滤
    → 投影
  → 结果格式化
  → 返回客户端
```

每个步骤都有明确的职责分工，形成了完整的查询处理管道。
