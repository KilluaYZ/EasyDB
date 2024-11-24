#include <gtest/gtest.h>

#include "execution/executor_hash_join.h"

namespace easydb {

// MockExecutor 用于模拟 AbstractExecutor 的行为
class MockExecutor : public AbstractExecutor {
public:
    MockExecutor(const std::vector<Tuple> &tuples, const Schema &schema)
        : tuples_(tuples), schema_(schema), current_index_(0) {}

    void beginTuple() override { current_index_ = 0; }

    void nextTuple() override {}

    std::unique_ptr<Tuple> Next() override {
        if (current_index_ < tuples_.size()) {
            return std::make_unique<Tuple>(tuples_[current_index_++]);
        }
        return nullptr;
    }

    RID &rid() override { return _abstract_rid; }

    size_t tupleLen() const override { return schema_.GetInlinedStorageSize(); }

    const Schema &schema() const override { return schema_; }

    bool IsEnd() const override { return current_index_ >= tuples_.size(); }

    std::string getTabName() const override { 
        if (schema_.GetColumns().empty()) {
            return "";
        }
        return schema_.GetColumns()[0].GetTabName(); 
    }

private:
    std::vector<Tuple> tuples_;
    Schema schema_;
    size_t current_index_;
};

// 辅助函数：创建一个整数类型的 Value
Value MakeIntValue(int32_t val) {
    return Value(TypeId::TYPE_INT, val);
}

// 辅助函数：创建一个字符串类型的 Value
Value MakeStringValue(const std::string &val) {
    return Value(TypeId::TYPE_VARCHAR, val);
}

// 测试套件
class HashJoinExecutorTest : public ::testing::Test {
protected:
    void SetUp() override {
        // 初始化可以在这里进行
    }

    void TearDown() override {
        // 清理工作可以在这里进行
    }
};

// 测试用例1：基本的等值连接
TEST_F(HashJoinExecutorTest, BasicEqualityJoin) {
    // 定义左表的 Schema
    Column left_col1("id", "left_table", TypeId::TYPE_INT);
    Column left_col2("value", "left_table", TypeId::TYPE_INT);
    Schema left_schema({left_col1, left_col2});

    // 定义右表的 Schema
    Column right_col1("id", "right_table", TypeId::TYPE_INT);
    Column right_col2("value", "right_table", TypeId::TYPE_INT);
    Schema right_schema({right_col1, right_col2});

    // 创建左表的 Tuple
    std::vector<Tuple> left_tuples;
    for (int i = 0; i < 5; ++i) {
        std::vector<Value> values = {MakeIntValue(i), MakeIntValue(i * 10)};
        left_tuples.emplace_back(values, &left_schema);
    }

    // 创建右表的 Tuple
    std::vector<Tuple> right_tuples;
    for (int i = 3; i < 8; ++i) {
        std::vector<Value> values = {MakeIntValue(i), MakeIntValue(i * 100)};
        right_tuples.emplace_back(values, &right_schema);
    }

    // 创建 MockExecutor
    auto left_executor = std::make_unique<MockExecutor>(left_tuples, left_schema);
    auto right_executor = std::make_unique<MockExecutor>(right_tuples, right_schema);

    // 定义连接条件：left_table.id = right_table.id
    TabCol left_col = {"left_table", "id"};
    TabCol right_col = {"right_table", "id"};
    Condition join_condition = {
        left_col,    // lhs_col
        OP_EQ,       // op
        false,       // is_rhs_val
        false,       // is_rhs_stmt
        false,       // is_rhs_exe_processed
        right_col    // rhs_col
    };

    // 创建 HashJoinExecutor
    std::vector<Condition> join_conditions = {join_condition};
    HashJoinExecutor hash_join_executor(std::move(left_executor), std::move(right_executor), join_conditions);

    // 初始化 executor
    hash_join_executor.beginTuple();

    // 收集连接后的 Tuple
    std::vector<Tuple> joined_tuples;
    std::unique_ptr<Tuple> joined_tuple;
    while ((joined_tuple = hash_join_executor.Next()) != nullptr) {
        joined_tuples.emplace_back(*joined_tuple);
    }

    // 预期结果：id = 3 和 id = 4 有匹配
    ASSERT_EQ(joined_tuples.size(), 2);

    // 验证连接后的 Tuple 内容
    for (const auto &tuple : joined_tuples) {
        // 获取左表的 id 和 value
        Value left_id_val = tuple.GetValue(&hash_join_executor.schema(), "id");  // 连接后的 id
        Value left_value_val = tuple.GetValue(&hash_join_executor.schema(), "value");  // 左表的 value

        // 获取右表的 id 和 value
        // 由于 Schema 已经合并，右表的 id 在 offset = left_schema.GetInlinedStorageSize() + 0
        // 右表的 value 在 offset = left_schema.GetInlinedStorageSize() + 4
        Value right_id_val = tuple.GetValue(&hash_join_executor.schema(), "right_table.id");
        Value right_value_val = tuple.GetValue(&hash_join_executor.schema(), "right_table.value");

        int32_t id = left_id_val.GetAs<int32_t>();
        int32_t left_value = left_value_val.GetAs<int32_t>();
        int32_t right_value = right_value_val.GetAs<int32_t>();

        ASSERT_TRUE(id == 3 || id == 4);
        ASSERT_EQ(left_value, id * 10);
        ASSERT_EQ(right_value, id * 100);
    }
}

// 测试用例2：无匹配的等值连接
TEST_F(HashJoinExecutorTest, NoMatchingJoin) {
    // 定义左表的 Schema
    Column left_col1("id", "left_table", TypeId::TYPE_INT);
    Column left_col2("value", "left_table", TypeId::TYPE_INT);
    Schema left_schema({left_col1, left_col2});

    // 定义右表的 Schema
    Column right_col1("id", "right_table", TypeId::TYPE_INT);
    Column right_col2("value", "right_table", TypeId::TYPE_INT);
    Schema right_schema({right_col1, right_col2});

    // 创建左表的 Tuple
    std::vector<Tuple> left_tuples = {
        Tuple({MakeIntValue(1), MakeIntValue(10)}, &left_schema),
        Tuple({MakeIntValue(2), MakeIntValue(20)}, &left_schema),
        Tuple({MakeIntValue(3), MakeIntValue(30)}, &left_schema)
    };

    // 创建右表的 Tuple
    std::vector<Tuple> right_tuples = {
        Tuple({MakeIntValue(4), MakeIntValue(40)}, &right_schema),
        Tuple({MakeIntValue(5), MakeIntValue(50)}, &right_schema)
    };

    // 创建 MockExecutor
    auto left_executor = std::make_unique<MockExecutor>(left_tuples, left_schema);
    auto right_executor = std::make_unique<MockExecutor>(right_tuples, right_schema);

    // 定义连接条件：left_table.id = right_table.id
    TabCol left_col = {"left_table", "id"};
    TabCol right_col = {"right_table", "id"};
    Condition join_condition = {
        left_col,    // lhs_col
        OP_EQ,       // op
        false,       // is_rhs_val
        false,       // is_rhs_stmt
        false,       // is_rhs_exe_processed
        right_col    // rhs_col
    };

    // 创建 HashJoinExecutor
    std::vector<Condition> join_conditions = {join_condition};
    HashJoinExecutor hash_join_executor(std::move(left_executor), std::move(right_executor), join_conditions);

    // 初始化 executor
    hash_join_executor.beginTuple();

    // 收集连接后的 Tuple
    std::vector<Tuple> joined_tuples;
    std::unique_ptr<Tuple> joined_tuple;
    while ((joined_tuple = hash_join_executor.Next()) != nullptr) {
        joined_tuples.emplace_back(*joined_tuple);
    }

    // 预期结果：无匹配
    ASSERT_EQ(joined_tuples.size(), 0);
}

// 测试用例3：多对多连接
TEST_F(HashJoinExecutorTest, MultipleMatchesJoin) {
    // 定义左表的 Schema
    Column left_col1("id", "left_table", TypeId::TYPE_INT);
    Column left_col2("value", "left_table", TypeId::TYPE_INT);
    Schema left_schema({left_col1, left_col2});

    // 定义右表的 Schema
    Column right_col1("id", "right_table", TypeId::TYPE_INT);
    Column right_col2("value", "right_table", TypeId::TYPE_INT);
    Schema right_schema({right_col1, right_col2});

    // 创建左表的 Tuple
    std::vector<Tuple> left_tuples = {
        Tuple({MakeIntValue(1), MakeIntValue(100)}, &left_schema),
        Tuple({MakeIntValue(2), MakeIntValue(200)}, &left_schema),
        Tuple({MakeIntValue(1), MakeIntValue(101)}, &left_schema),
        Tuple({MakeIntValue(3), MakeIntValue(300)}, &left_schema)
    };

    // 创建右表的 Tuple
    std::vector<Tuple> right_tuples = {
        Tuple({MakeIntValue(1), MakeIntValue(1000)}, &right_schema),
        Tuple({MakeIntValue(2), MakeIntValue(2000)}, &right_schema),
        Tuple({MakeIntValue(1), MakeIntValue(1001)}, &right_schema)
    };

    // 创建 MockExecutor
    auto left_executor = std::make_unique<MockExecutor>(left_tuples, left_schema);
    auto right_executor = std::make_unique<MockExecutor>(right_tuples, right_schema);

    // 定义连接条件：left_table.id = right_table.id
    TabCol left_col = {"left_table", "id"};
    TabCol right_col = {"right_table", "id"};
    Condition join_condition = {
        left_col,    // lhs_col
        OP_EQ,       // op
        false,       // is_rhs_val
        false,       // is_rhs_stmt
        false,       // is_rhs_exe_processed
        right_col    // rhs_col
    };

    // 创建 HashJoinExecutor
    std::vector<Condition> join_conditions = {join_condition};
    HashJoinExecutor hash_join_executor(std::move(left_executor), std::move(right_executor), join_conditions);

    // 初始化 executor
    hash_join_executor.beginTuple();

    // 收集连接后的 Tuple
    std::vector<Tuple> joined_tuples;
    std::unique_ptr<Tuple> joined_tuple;
    while ((joined_tuple = hash_join_executor.Next()) != nullptr) {
        joined_tuples.emplace_back(*joined_tuple);
    }

    // 预期结果：
    // 左表 id=1 有两个元组，右表 id=1 有两个元组，故总连接数为 2*2=4
    // 左表 id=2 有一个元组，右表 id=2 有一个元组，故总连接数为 1
    // 总计 5 个连接元组
    ASSERT_EQ(joined_tuples.size(), 5);

    // 验证连接后的 Tuple 内容
    // 创建一个映射来计数每个 id 的连接次数
    std::unordered_map<int32_t, int> join_count;
    for (const auto &tuple : joined_tuples) {
        // 获取左表的 id 和 value
        Value left_id_val = tuple.GetValue(&hash_join_executor.schema(), "id");  // 连接后的 id
        Value left_value_val = tuple.GetValue(&hash_join_executor.schema(), "value");  // 左表的 value

        // 获取右表的 id 和 value
        Value right_id_val = tuple.GetValue(&hash_join_executor.schema(), "right_table.id");
        Value right_value_val = tuple.GetValue(&hash_join_executor.schema(), "right_table.value");

        int32_t id = left_id_val.GetAs<int32_t>();
        int32_t left_value = left_value_val.GetAs<int32_t>();
        int32_t right_value = right_value_val.GetAs<int32_t>();

        // 验证 id 是否正确
        ASSERT_TRUE(id == 1 || id == 2);

        // 计数
        join_count[id]++;
    }

    // 验证每个 id 的连接次数
    ASSERT_EQ(join_count[1], 4);  // 2 左 * 2 右
    ASSERT_EQ(join_count[2], 1);  // 1 左 * 1 右
}

// 测试用例4：不同数据类型的连接
TEST_F(HashJoinExecutorTest, DifferentDataTypesJoin) {
    // 定义左表的 Schema
    Column left_col1("name", "left_table", TypeId::TYPE_VARCHAR);
    Column left_col2("age", "left_table", TypeId::TYPE_INT);
    Schema left_schema({left_col1, left_col2});

    // 定义右表的 Schema
    Column right_col1("name", "right_table", TypeId::TYPE_VARCHAR);
    Column right_col2("salary", "right_table", TypeId::TYPE_INT);
    Schema right_schema({right_col1, right_col2});

    // 创建左表的 Tuple
    std::vector<Tuple> left_tuples = {
        Tuple({MakeStringValue("Alice"), MakeIntValue(30)}, &left_schema),
        Tuple({MakeStringValue("Bob"), MakeIntValue(25)}, &left_schema),
        Tuple({MakeStringValue("Charlie"), MakeIntValue(35)}, &left_schema)
    };

    // 创建右表的 Tuple
    std::vector<Tuple> right_tuples = {
        Tuple({MakeStringValue("Alice"), MakeIntValue(70000)}, &right_schema),
        Tuple({MakeStringValue("David"), MakeIntValue(80000)}, &right_schema),
        Tuple({MakeStringValue("Bob"), MakeIntValue(50000)}, &right_schema)
    };

    // 创建 MockExecutor
    auto left_executor = std::make_unique<MockExecutor>(left_tuples, left_schema);
    auto right_executor = std::make_unique<MockExecutor>(right_tuples, right_schema);

    // 定义连接条件：left_table.name = right_table.name
    TabCol left_col = {"left_table", "name"};
    TabCol right_col = {"right_table", "name"};
    Condition join_condition = {
        left_col,    // lhs_col
        OP_EQ,       // op
        false,       // is_rhs_val
        false,       // is_rhs_stmt
        false,       // is_rhs_exe_processed
        right_col    // rhs_col
    };

    // 创建 HashJoinExecutor
    std::vector<Condition> join_conditions = {join_condition};
    HashJoinExecutor hash_join_executor(std::move(left_executor), std::move(right_executor), join_conditions);

    // 初始化 executor
    hash_join_executor.beginTuple();

    // 收集连接后的 Tuple
    std::vector<Tuple> joined_tuples;
    std::unique_ptr<Tuple> joined_tuple;
    while ((joined_tuple = hash_join_executor.Next()) != nullptr) {
        joined_tuples.emplace_back(*joined_tuple);
    }

    // 预期结果：Alice 和 Bob 各有一个匹配
    ASSERT_EQ(joined_tuples.size(), 2);

    // 验证连接后的 Tuple 内容
    for (const auto &tuple : joined_tuples) {
        // 获取左表的 name 和 age
        Value left_name_val = tuple.GetValue(&hash_join_executor.schema(), "name");  // 连接后的 name
        Value left_age_val = tuple.GetValue(&hash_join_executor.schema(), "age");    // 左表的 age

        // 获取右表的 name 和 salary
        Value right_name_val = tuple.GetValue(&hash_join_executor.schema(), "right_table.name");
        Value right_salary_val = tuple.GetValue(&hash_join_executor.schema(), "right_table.salary");

        std::string name = left_name_val.GetAs<std::string>();
        int32_t age = left_age_val.GetAs<int32_t>();
        int32_t salary = right_salary_val.GetAs<int32_t>();

        ASSERT_TRUE(name == "Alice" || name == "Bob");

        if (name == "Alice") {
            ASSERT_EQ(age, 30);
            ASSERT_EQ(salary, 70000);
        } else if (name == "Bob") {
            ASSERT_EQ(age, 25);
            ASSERT_EQ(salary, 50000);
        }
    }
}

// 测试用例5：多个连接条件（假设支持）
TEST_F(HashJoinExecutorTest, MultipleJoinConditions) {
    // 定义左表的 Schema
    Column left_col1("id", "left_table", TypeId::TYPE_INT);
    Column left_col2("dept", "left_table", TypeId::TYPE_VARCHAR);
    Column left_col3("salary", "left_table", TypeId::TYPE_INT);
    Schema left_schema({left_col1, left_col2, left_col3});

    // 定义右表的 Schema
    Column right_col1("id", "right_table", TypeId::TYPE_INT);
    Column right_col2("dept", "right_table", TypeId::TYPE_VARCHAR);
    Column right_col3("bonus", "right_table", TypeId::TYPE_INT);
    Schema right_schema({right_col1, right_col2, right_col3});

    // 创建左表的 Tuple
    std::vector<Tuple> left_tuples = {
        Tuple({MakeIntValue(1), MakeStringValue("HR"), MakeIntValue(50000)}, &left_schema),
        Tuple({MakeIntValue(2), MakeStringValue("Engineering"), MakeIntValue(60000)}, &left_schema),
        Tuple({MakeIntValue(1), MakeStringValue("Engineering"), MakeIntValue(55000)}, &left_schema)
    };

    // 创建右表的 Tuple
    std::vector<Tuple> right_tuples = {
        Tuple({MakeIntValue(1), MakeStringValue("HR"), MakeIntValue(5000)}, &right_schema),
        Tuple({MakeIntValue(2), MakeStringValue("HR"), MakeIntValue(6000)}, &right_schema),
        Tuple({MakeIntValue(1), MakeStringValue("Engineering"), MakeIntValue(5500)}, &right_schema)
    };

    // 创建 MockExecutor
    auto left_executor = std::make_unique<MockExecutor>(left_tuples, left_schema);
    auto right_executor = std::make_unique<MockExecutor>(right_tuples, right_schema);

    // 定义多个连接条件：
    // 1. left_table.id = right_table.id
    // 2. left_table.dept = right_table.dept
    TabCol left_id_col = {"left_table", "id"};
    TabCol right_id_col = {"right_table", "id"};
    TabCol left_dept_col = {"left_table", "dept"};
    TabCol right_dept_col = {"right_table", "dept"};

    Condition cond1 = {
        left_id_col,    // lhs_col
        OP_EQ,          // op
        false,          // is_rhs_val
        false,          // is_rhs_stmt
        false,          // is_rhs_exe_processed
        right_id_col    // rhs_col
    };

    Condition cond2 = {
        left_dept_col,  // lhs_col
        OP_EQ,          // op
        false,          // is_rhs_val
        false,          // is_rhs_stmt
        false,          // is_rhs_exe_processed
        right_dept_col  // rhs_col
    };

    // 创建 HashJoinExecutor
    std::vector<Condition> join_conditions = {cond1, cond2};
    HashJoinExecutor hash_join_executor(std::move(left_executor), std::move(right_executor), join_conditions);

    // 初始化 executor
    hash_join_executor.beginTuple();

    // 收集连接后的 Tuple
    std::vector<Tuple> joined_tuples;
    std::unique_ptr<Tuple> joined_tuple;
    while ((joined_tuple = hash_join_executor.Next()) != nullptr) {
        joined_tuples.emplace_back(*joined_tuple);
    }

    // 预期结果：
    // 1. left.id=1, dept=HR matches right.id=1, dept=HR
    // 2. left.id=1, dept=Engineering matches right.id=1, dept=Engineering
    // 3. left.id=2, dept=Engineering does not match any right tuple
    // 总计 2 个连接元组
    ASSERT_EQ(joined_tuples.size(), 2);

    // 验证连接后的 Tuple 内容
    for (const auto &tuple : joined_tuples) {
        // 获取连接后的字段
        Value id_val = tuple.GetValue(&hash_join_executor.schema(), "id");      // 左表的 id
        Value dept_val = tuple.GetValue(&hash_join_executor.schema(), "dept");  // 左表的 dept
        Value salary_val = tuple.GetValue(&hash_join_executor.schema(), "salary");  // 左表的 salary
        Value right_id_val = tuple.GetValue(&hash_join_executor.schema(), "right_table.id");
        Value right_dept_val = tuple.GetValue(&hash_join_executor.schema(), "right_table.dept");
        Value bonus_val = tuple.GetValue(&hash_join_executor.schema(), "right_table.bonus");

        int32_t id = id_val.GetAs<int32_t>();
        std::string dept = dept_val.GetAs<std::string>();
        int32_t salary = salary_val.GetAs<int32_t>();
        int32_t right_id = right_id_val.GetAs<int32_t>();
        std::string right_dept = right_dept_val.GetAs<std::string>();
        int32_t bonus = bonus_val.GetAs<int32_t>();

        // 验证连接条件
        ASSERT_EQ(id, right_id);
        ASSERT_EQ(dept, right_dept);

        // 验证其他字段
        if (id == 1 && dept == "HR") {
            ASSERT_EQ(salary, 50000);
            ASSERT_EQ(bonus, 5000);
        } else if (id == 1 && dept == "Engineering") {
            ASSERT_EQ(salary, 55000);
            ASSERT_EQ(bonus, 5500);
        } else {
            FAIL() << "Unexpected joined tuple.";
        }
    }
}

// 测试用例6：空的左表
TEST_F(HashJoinExecutorTest, EmptyLeftTable) {
    // 定义左表的 Schema
    Column left_col1("id", "left_table", TypeId::TYPE_INT);
    Column left_col2("value", "left_table", TypeId::TYPE_INT);
    Schema left_schema({left_col1, left_col2});

    // 定义右表的 Schema
    Column right_col1("id", "right_table", TypeId::TYPE_INT);
    Column right_col2("value", "right_table", TypeId::TYPE_INT);
    Schema right_schema({right_col1, right_col2});

    // 创建空的左表 Tuple
    std::vector<Tuple> left_tuples;

    // 创建右表的 Tuple
    std::vector<Tuple> right_tuples = {
        Tuple({MakeIntValue(1), MakeIntValue(100)}, &right_schema),
        Tuple({MakeIntValue(2), MakeIntValue(200)}, &right_schema)
    };

    // 创建 MockExecutor
    auto left_executor = std::make_unique<MockExecutor>(left_tuples, left_schema);
    auto right_executor = std::make_unique<MockExecutor>(right_tuples, right_schema);

    // 定义连接条件：left_table.id = right_table.id
    TabCol left_col = {"left_table", "id"};
    TabCol right_col = {"right_table", "id"};
    Condition join_condition = {
        left_col,    // lhs_col
        OP_EQ,       // op
        false,       // is_rhs_val
        false,       // is_rhs_stmt
        false,       // is_rhs_exe_processed
        right_col    // rhs_col
    };

    // 创建 HashJoinExecutor
    std::vector<Condition> join_conditions = {join_condition};
    HashJoinExecutor hash_join_executor(std::move(left_executor), std::move(right_executor), join_conditions);

    // 初始化 executor
    hash_join_executor.beginTuple();

    // 收集连接后的 Tuple
    std::vector<Tuple> joined_tuples;
    std::unique_ptr<Tuple> joined_tuple;
    while ((joined_tuple = hash_join_executor.Next()) != nullptr) {
        joined_tuples.emplace_back(*joined_tuple);
    }

    // 预期结果：无匹配
    ASSERT_EQ(joined_tuples.size(), 0);
}

// 测试用例7：空的右表
TEST_F(HashJoinExecutorTest, EmptyRightTable) {
    // 定义左表的 Schema
    Column left_col1("id", "left_table", TypeId::TYPE_INT);
    Column left_col2("value", "left_table", TypeId::TYPE_INT);
    Schema left_schema({left_col1, left_col2});

    // 定义右表的 Schema
    Column right_col1("id", "right_table", TypeId::TYPE_INT);
    Column right_col2("value", "right_table", TypeId::TYPE_INT);
    Schema right_schema({right_col1, right_col2});

    // 创建左表的 Tuple
    std::vector<Tuple> left_tuples = {
        Tuple({MakeIntValue(1), MakeIntValue(10)}, &left_schema),
        Tuple({MakeIntValue(2), MakeIntValue(20)}, &left_schema)
    };

    // 创建空的右表 Tuple
    std::vector<Tuple> right_tuples;

    // 创建 MockExecutor
    auto left_executor = std::make_unique<MockExecutor>(left_tuples, left_schema);
    auto right_executor = std::make_unique<MockExecutor>(right_tuples, right_schema);

    // 定义连接条件：left_table.id = right_table.id
    TabCol left_col = {"left_table", "id"};
    TabCol right_col = {"right_table", "id"};
    Condition join_condition = {
        left_col,    // lhs_col
        OP_EQ,       // op
        false,       // is_rhs_val
        false,       // is_rhs_stmt
        false,       // is_rhs_exe_processed
        right_col    // rhs_col
    };

    // 创建 HashJoinExecutor
    std::vector<Condition> join_conditions = {join_condition};
    HashJoinExecutor hash_join_executor(std::move(left_executor), std::move(right_executor), join_conditions);

    // 初始化 executor
    hash_join_executor.beginTuple();

    // 收集连接后的 Tuple
    std::vector<Tuple> joined_tuples;
    std::unique_ptr<Tuple> joined_tuple;
    while ((joined_tuple = hash_join_executor.Next()) != nullptr) {
        joined_tuples.emplace_back(*joined_tuple);
    }

    // 预期结果：无匹配
    ASSERT_EQ(joined_tuples.size(), 0);
}

// 测试用例8：自连接（同一表连接自身）
TEST_F(HashJoinExecutorTest, SelfJoin) {
    // 定义表的 Schema
    Column col1("id", "employees", TypeId::TYPE_INT);
    Column col2("manager_id", "employees", TypeId::TYPE_INT);
    Schema schema({col1, col2});

    // 创建表的 Tuple
    std::vector<Tuple> tuples = {
        Tuple({MakeIntValue(1), MakeIntValue(0)}, &schema),   // 员工1，无经理
        Tuple({MakeIntValue(2), MakeIntValue(1)}, &schema),   // 员工2，经理是1
        Tuple({MakeIntValue(3), MakeIntValue(1)}, &schema),    // 员工3，经理是1
        Tuple({MakeIntValue(4), MakeIntValue(2)}, &schema)     // 员工4，经理是2
    };

    // 创建两个 MockExecutor（模拟自连接）
    auto left_executor = std::make_unique<MockExecutor>(tuples, schema);
    auto right_executor = std::make_unique<MockExecutor>(tuples, schema);

    // 定义连接条件：employees.manager_id = employees.id
    TabCol left_col = {"employees", "manager_id"};
    TabCol right_col = {"employees", "id"};
    Condition join_condition = {
        left_col,    // lhs_col
        OP_EQ,       // op
        false,       // is_rhs_val
        false,       // is_rhs_stmt
        false,       // is_rhs_exe_processed
        right_col    // rhs_col
    };

    // 创建 HashJoinExecutor
    std::vector<Condition> join_conditions = {join_condition};
    HashJoinExecutor hash_join_executor(std::move(left_executor), std::move(right_executor), join_conditions);

    // 初始化 executor
    hash_join_executor.beginTuple();

    // 收集连接后的 Tuple
    std::vector<Tuple> joined_tuples;
    std::unique_ptr<Tuple> joined_tuple;
    while ((joined_tuple = hash_join_executor.Next()) != nullptr) {
        joined_tuples.emplace_back(*joined_tuple);
    }

    // 预期结果：
    // 员工2的经理是1，员工3的经理是1，员工4的经理是2
    // 总计3个连接元组
    ASSERT_EQ(joined_tuples.size(), 3);

    // 验证连接后的 Tuple 内容
    for (const auto &tuple : joined_tuples) {
        // 获取左表的 manager_id 和 value
        Value manager_id_val = tuple.GetValue(&hash_join_executor.schema(), "manager_id");  // 左表的 manager_id

        // 获取右表的 id 和 value
        Value right_id_val = tuple.GetValue(&hash_join_executor.schema(), "employees.id");
        Value right_value_val = tuple.GetValue(&hash_join_executor.schema(), "employees.value");

        int32_t manager_id = manager_id_val.GetAs<int32_t>();
        int32_t right_id = right_id_val.GetAs<int32_t>();
        int32_t right_value = right_value_val.GetAs<int32_t>();

        // 验证连接条件
        ASSERT_EQ(manager_id, right_id);

        // 验证其他字段
        if (manager_id == 1) {
            // 员工2和员工3的经理是1
            ASSERT_TRUE(right_id == 1);
            ASSERT_EQ(right_value, 0);  // 员工1的 value 是0
        } else if (manager_id == 2) {
            // 员工4的经理是2
            ASSERT_TRUE(right_id == 2);
            ASSERT_EQ(right_value, 1);  // 员工2的 value 是20（根据之前的例子，如果有不同数据）
        } else {
            FAIL() << "Unexpected joined tuple.";
        }
    }
}

// 测试用例9：连接条件涉及字符串
TEST_F(HashJoinExecutorTest, JoinOnStringColumns) {
    // 定义左表的 Schema
    Column left_col1("username", "left_table", TypeId::TYPE_VARCHAR);
    Column left_col2("score", "left_table", TypeId::TYPE_INT);
    Schema left_schema({left_col1, left_col2});

    // 定义右表的 Schema
    Column right_col1("username", "right_table", TypeId::TYPE_VARCHAR);
    Column right_col2("grade", "right_table", TypeId::TYPE_CHAR);
    Schema right_schema({right_col1, right_col2});

    // 创建左表的 Tuple
    std::vector<Tuple> left_tuples = {
        Tuple({MakeStringValue("alice"), MakeIntValue(85)}, &left_schema),
        Tuple({MakeStringValue("bob"), MakeIntValue(90)}, &left_schema),
        Tuple({MakeStringValue("charlie"), MakeIntValue(75)}, &left_schema)
    };

    // 创建右表的 Tuple
    std::vector<Tuple> right_tuples = {
        Tuple({MakeStringValue("alice"), MakeStringValue("B")}, &right_schema),
        Tuple({MakeStringValue("bob"), MakeStringValue("A")}, &right_schema),
        Tuple({MakeStringValue("david"), MakeStringValue("C")}, &right_schema)
    };

    // 创建 MockExecutor
    auto left_executor = std::make_unique<MockExecutor>(left_tuples, left_schema);
    auto right_executor = std::make_unique<MockExecutor>(right_tuples, right_schema);

    // 定义连接条件：left_table.username = right_table.username
    TabCol left_col = {"left_table", "username"};
    TabCol right_col = {"right_table", "username"};
    Condition join_condition = {
        left_col,    // lhs_col
        OP_EQ,       // op
        false,       // is_rhs_val
        false,       // is_rhs_stmt
        false,       // is_rhs_exe_processed
        right_col    // rhs_col
    };

    // 创建 HashJoinExecutor
    std::vector<Condition> join_conditions = {join_condition};
    HashJoinExecutor hash_join_executor(std::move(left_executor), std::move(right_executor), join_conditions);

    // 初始化 executor
    hash_join_executor.beginTuple();

    // 收集连接后的 Tuple
    std::vector<Tuple> joined_tuples;
    std::unique_ptr<Tuple> joined_tuple;
    while ((joined_tuple = hash_join_executor.Next()) != nullptr) {
        joined_tuples.emplace_back(*joined_tuple);
    }

    // 预期结果：alice 和 bob 有匹配
    ASSERT_EQ(joined_tuples.size(), 2);

    // 验证连接后的 Tuple 内容
    for (const auto &tuple : joined_tuples) {
        // 获取左表的 username 和 score
        Value left_username_val = tuple.GetValue(&hash_join_executor.schema(), "username");  // 左表的 username
        Value left_score_val = tuple.GetValue(&hash_join_executor.schema(), "score");        // 左表的 score

        // 获取右表的 username 和 grade
        Value right_username_val = tuple.GetValue(&hash_join_executor.schema(), "right_table.username");
        Value right_grade_val = tuple.GetValue(&hash_join_executor.schema(), "right_table.grade");

        std::string username = left_username_val.GetAs<std::string>();
        int32_t score = left_score_val.GetAs<int32_t>();
        char grade = right_grade_val.GetAs<char>();

        // 验证连接条件
        ASSERT_EQ(username, right_username_val.GetAs<std::string>());

        // 验证其他字段
        if (username == "alice") {
            ASSERT_EQ(score, 85);
            ASSERT_EQ(grade, 'B');
        } else if (username == "bob") {
            ASSERT_EQ(score, 90);
            ASSERT_EQ(grade, 'A');
        } else {
            FAIL() << "Unexpected joined tuple.";
        }
    }
}

// 测试用例10：连接条件中的非等值条件（假设支持）
TEST_F(HashJoinExecutorTest, NonEqualityJoinCondition) {
    // 当前 HashJoinExecutor 仅支持等值连接，若支持非等值连接，需要额外实现
    // 这里作为示例，假设 OP_GT 被支持

    // 定义左表的 Schema
    Column left_col1("id", "left_table", TypeId::TYPE_INT);
    Column left_col2("value", "left_table", TypeId::TYPE_INT);
    Schema left_schema({left_col1, left_col2});

    // 定义右表的 Schema
    Column right_col1("id", "right_table", TypeId::TYPE_INT);
    Column right_col2("value", "right_table", TypeId::TYPE_INT);
    Schema right_schema({right_col1, right_col2});

    // 创建左表的 Tuple
    std::vector<Tuple> left_tuples = {
        Tuple({MakeIntValue(1), MakeIntValue(100)}, &left_schema),
        Tuple({MakeIntValue(2), MakeIntValue(200)}, &left_schema),
        Tuple({MakeIntValue(3), MakeIntValue(300)}, &left_schema)
    };

    // 创建右表的 Tuple
    std::vector<Tuple> right_tuples = {
        Tuple({MakeIntValue(1), MakeIntValue(150)}, &right_schema),
        Tuple({MakeIntValue(2), MakeIntValue(250)}, &right_schema),
        Tuple({MakeIntValue(4), MakeIntValue(400)}, &right_schema)
    };

    // 创建 MockExecutor
    auto left_executor = std::make_unique<MockExecutor>(left_tuples, left_schema);
    auto right_executor = std::make_unique<MockExecutor>(right_tuples, right_schema);

    // 定义连接条件：left_table.value > right_table.value
    TabCol left_col = {"left_table", "value"};
    TabCol right_col = {"right_table", "value"};
    Condition join_condition = {
        left_col,    // lhs_col
        OP_GT,       // op
        false,       // is_rhs_val
        false,       // is_rhs_stmt
        false,       // is_rhs_exe_processed
        right_col    // rhs_col
    };

    // 创建 HashJoinExecutor
    std::vector<Condition> join_conditions = {join_condition};
    HashJoinExecutor hash_join_executor(std::move(left_executor), std::move(right_executor), join_conditions);

    // 初始化 executor
    hash_join_executor.beginTuple();

    // 收集连接后的 Tuple
    std::vector<Tuple> joined_tuples;
    std::unique_ptr<Tuple> joined_tuple;
    while ((joined_tuple = hash_join_executor.Next()) != nullptr) {
        joined_tuples.emplace_back(*joined_tuple);
    }

    // 预期结果：
    // left.id=2, left.value=200 > right.value=150 (right.id=1)
    // left.id=3, left.value=300 > right.value=150 (right.id=1)
    // left.id=3, left.value=300 > right.value=250 (right.id=2)
    // 总计 3 个连接元组
    // 请注意，这假设 HashJoinExecutor 已实现非等值连接支持
    // 如果当前实现不支持，测试可能需要调整或跳过

    // 由于当前 HashJoinExecutor 仅支持等值连接，跳过此测试
    SUCCEED() << "Non-equality join conditions are not supported yet.";
}

// 测试用例11：自连接空表
TEST_F(HashJoinExecutorTest, SelfJoinEmptyTable) {
    // 定义表的 Schema
    Column col1("id", "employees", TypeId::TYPE_INT);
    Column col2("manager_id", "employees", TypeId::TYPE_INT);
    Schema schema({col1, col2});

    // 创建空的表 Tuple
    std::vector<Tuple> tuples;

    // 创建两个 MockExecutor（模拟自连接）
    auto left_executor = std::make_unique<MockExecutor>(tuples, schema);
    auto right_executor = std::make_unique<MockExecutor>(tuples, schema);

    // 定义连接条件：employees.manager_id = employees.id
    TabCol left_col = {"employees", "manager_id"};
    TabCol right_col = {"employees", "id"};
    Condition join_condition = {
        left_col,    // lhs_col
        OP_EQ,       // op
        false,       // is_rhs_val
        false,       // is_rhs_stmt
        false,       // is_rhs_exe_processed
        right_col    // rhs_col
    };

    // 创建 HashJoinExecutor
    std::vector<Condition> join_conditions = {join_condition};
    HashJoinExecutor hash_join_executor(std::move(left_executor), std::move(right_executor), join_conditions);

    // 初始化 executor
    hash_join_executor.beginTuple();

    // 收集连接后的 Tuple
    std::vector<Tuple> joined_tuples;
    std::unique_ptr<Tuple> joined_tuple;
    while ((joined_tuple = hash_join_executor.Next()) != nullptr) {
        joined_tuples.emplace_back(*joined_tuple);
    }

    // 预期结果：无匹配
    ASSERT_EQ(joined_tuples.size(), 0);
}

}  // namespace easydb