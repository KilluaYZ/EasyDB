// test_executor_nestedloop_join.cpp

#include <gtest/gtest.h>
#include <memory>
#include <vector>
#include <string>
#include <sstream>

// Include all necessary headers from your project
#include "execution/executor_abstract.h"
#include "execution/executor_nestedloop_join.h"
#include "storage/table/tuple.h"
#include "catalog/schema.h"
#include "common/errors.h"
#include "common/rid.h"
#include "type/value.h"
#include "common/condition.h"

// Namespace declaration
using namespace easydb;

// MockExecutor class to simulate AbstractExecutor behavior
class MockExecutor : public AbstractExecutor {
public:
    MockExecutor(const std::string& tab_name, Schema schema, std::vector<Tuple> tuples)
        : tab_name_(tab_name), schema_(std::move(schema)), tuples_(std::move(tuples)), current_idx_(0) {}

    void beginTuple() override {
        current_idx_ = 0;
    }

    void nextTuple() override {
        if (current_idx_ < tuples_.size()) {
            current_idx_++;
        }
    }

    std::unique_ptr<Tuple> Next() override {
        if (current_idx_ < tuples_.size()) {
            return std::make_unique<Tuple>(tuples_[current_idx_]);
        }
        return nullptr;
    }

    RID& rid() override {
        // Corrected: Cast current_idx_ to page_id_t to avoid narrowing
        dummy_rid_ = RID{static_cast<page_id_t>(current_idx_), 0};
        return dummy_rid_;
    }

    size_t tupleLen() const override {
        return schema_.GetInlinedStorageSize();
    }

    const Schema& schema() const override {
        return schema_;
    }

    bool IsEnd() const override {
        return current_idx_ >= tuples_.size();
    }

    std::string getTabName() const override {
        return tab_name_;
    }

    // Implement other virtual methods if necessary
    ColMeta get_col_offset(const TabCol &target) override {
        // Simplified for testing purposes
        for (const auto& col : schema_.GetColumns()) {
            if (col.GetName() == target.col_name && col.GetTabName() == target.tab_name) {
                // Corrected: Use existing ColMeta constructor and manually set tab_name and index
                ColMeta cm(const_cast<Column&>(col)); // Remove constness to match constructor signature
                cm.tab_name = col.GetTabName();
                cm.index = false;
                return cm;
            }
        }
        throw ColumnNotFoundError(target.col_name);
    }

private:
    std::string tab_name_;
    Schema schema_;
    std::vector<Tuple> tuples_;
    size_t current_idx_;
    RID dummy_rid_;
};

// Helper function to create a Tuple from integer and string values
Tuple CreateTupleIntStr(const Schema& schema, int32_t id, const std::string& name, const std::string& address) {
    std::vector<Value> val_vec;
    val_vec.emplace_back(TypeId::TYPE_INT, id);
    val_vec.emplace_back(TypeId::TYPE_VARCHAR, name.c_str(), name.size(), false);
    val_vec.emplace_back(TypeId::TYPE_INT, id);
    val_vec.emplace_back(TypeId::TYPE_VARCHAR, address.c_str(), address.size(), false);
    return Tuple(val_vec, &schema);
}

// Test Fixture for NestedLoopJoinExecutor
class NestedLoopJoinExecutorTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Define schemas for left and right tables
        // Left table schema: id (INT), name (VARCHAR)
        std::vector<Column> left_columns = {
            Column("id", "left_table", TypeId::TYPE_INT, 4),
            Column("name", "left_table", TypeId::TYPE_VARCHAR, 10)
        };
        left_schema = Schema(left_columns);

        // Right table schema: id (INT), address (VARCHAR)
        std::vector<Column> right_columns = {
            Column("id", "right_table", TypeId::TYPE_INT, 4),
            Column("address", "right_table", TypeId::TYPE_VARCHAR, 15)
        };
        right_schema = Schema(right_columns);

        // Create tuples for left executor
        // Example: id = 1, name = "Alice"; id = 2, name = "Bob"; id = 3, name = "Charlie"
        left_tuples.emplace_back(CreateTupleIntStr(left_schema, 1, "Alice", ""));
        left_tuples.emplace_back(CreateTupleIntStr(left_schema, 2, "Bob", ""));
        left_tuples.emplace_back(CreateTupleIntStr(left_schema, 3, "Charlie", ""));

        // Create tuples for right executor
        // Example: id = 2, address = "New York"; id = 3, address = "Los Angeles"; id = 4, address = "Chicago"
        right_tuples.emplace_back(CreateTupleIntStr(right_schema, 2, "", "New York"));
        right_tuples.emplace_back(CreateTupleIntStr(right_schema, 3, "", "Los Angeles"));
        right_tuples.emplace_back(CreateTupleIntStr(right_schema, 4, "", "Chicago"));

        // Create MockExecutors
        left_executor = std::make_unique<MockExecutor>("left_table", left_schema, left_tuples);
        right_executor = std::make_unique<MockExecutor>("right_table", right_schema, right_tuples);

        // Define join condition: left_table.id = right_table.id
        Condition cond;
        cond.lhs_col = TabCol{"left_table", "id", NO_AGG, ""};
        cond.op = OP_EQ;
        cond.is_rhs_val = false;
        cond.is_rhs_stmt = false;
        cond.is_rhs_exe_processed = false;
        cond.rhs_col = TabCol{"right_table", "id", NO_AGG, ""};
        fed_conditions.push_back(cond);
    }

    // Member variables
    Schema left_schema;
    Schema right_schema;
    std::vector<Tuple> left_tuples;
    std::vector<Tuple> right_tuples;
    std::unique_ptr<AbstractExecutor> left_executor;
    std::unique_ptr<AbstractExecutor> right_executor;
    std::vector<Condition> fed_conditions;
};

// Test case for NestedLoopJoinExecutor with equality condition
TEST_F(NestedLoopJoinExecutorTest, InnerJoinEquality) {
    // Instantiate NestedLoopJoinExecutor
    NestedLoopJoinExecutor join_executor(std::move(left_executor), std::move(right_executor), fed_conditions);

    // Begin tuple processing
    join_executor.beginTuple();

    // Collect joined tuples
    std::vector<Tuple> result_tuples;
    while (!join_executor.IsEnd()) {
        std::unique_ptr<Tuple> joined = join_executor.Next();
        if (joined) {
            result_tuples.emplace_back(*joined);
        }
        join_executor.nextTuple();
    }

    // Define expected results
    // Expected join on id: 2 and 3
    // Joined tuples: (2, "Bob", 2, "New York"), (3, "Charlie", 3, "Los Angeles")
    std::vector<Tuple> expected_tuples;
    std::vector<Value> expected_val1 = {
        Value(TypeId::TYPE_INT, 2),
        Value(TypeId::TYPE_VARCHAR, "Bob", 3, false),
        Value(TypeId::TYPE_INT, 2),
        Value(TypeId::TYPE_VARCHAR, "New York", 8, false)
    };
    std::vector<Value> expected_val2 = {
        Value(TypeId::TYPE_INT, 3),
        Value(TypeId::TYPE_VARCHAR, "Charlie", 7, false),
        Value(TypeId::TYPE_INT, 3),
        Value(TypeId::TYPE_VARCHAR, "Los Angeles", 11, false)
    };
    expected_tuples.emplace_back(Tuple(expected_val1, &join_executor.schema()));
    expected_tuples.emplace_back(Tuple(expected_val2, &join_executor.schema()));

    // Verify the results
    ASSERT_EQ(result_tuples.size(), expected_tuples.size()) << "Number of joined tuples does not match expected.";

    for (size_t i = 0; i < expected_tuples.size(); ++i) {
        ASSERT_EQ(result_tuples[i].ToString(&join_executor.schema()), expected_tuples[i].ToString(&join_executor.schema()))
            << "Tuple " << i << " does not match expected.";
    }
}

// Additional test cases can be added similarly to cover more scenarios
// For example: no matching tuples, multiple matching tuples, different join conditions, etc.

// Example: Test with no matching tuples
TEST_F(NestedLoopJoinExecutorTest, InnerJoinNoMatch) {
    // Modify right tuples to have no matching ids
    // Reset executors
    right_tuples.clear();
    right_tuples.emplace_back(CreateTupleIntStr(right_schema, 4, "", "Houston"));
    right_tuples.emplace_back(CreateTupleIntStr(right_schema, 5, "", "Phoenix"));
    right_tuples.emplace_back(CreateTupleIntStr(right_schema, 6, "", "Philadelphia"));

    // Re-create right executor
    right_executor = std::make_unique<MockExecutor>("right_table", right_schema, right_tuples);

    // Define join condition: left_table.id = right_table.id
    Condition cond;
    cond.lhs_col = TabCol{"left_table", "id", NO_AGG, ""};
    cond.op = OP_EQ;
    cond.is_rhs_val = false;
    cond.is_rhs_stmt = false;
    cond.is_rhs_exe_processed = false;
    cond.rhs_col = TabCol{"right_table", "id", NO_AGG, ""};
    std::vector<Condition> conditions = {cond};

    // Instantiate NestedLoopJoinExecutor
    NestedLoopJoinExecutor join_executor(std::move(left_executor), std::move(right_executor), conditions);

    // Begin tuple processing
    join_executor.beginTuple();

    // Collect joined tuples
    std::vector<Tuple> result_tuples;
    while (!join_executor.IsEnd()) {
        std::unique_ptr<Tuple> joined = join_executor.Next();
        if (joined) {
            result_tuples.emplace_back(*joined);
        }
        join_executor.nextTuple();
    }

    // Expecting no joined tuples
    ASSERT_EQ(result_tuples.size(), 0) << "There should be no joined tuples, but some were found.";
}

// Example: Test with multiple matching tuples
TEST_F(NestedLoopJoinExecutorTest, InnerJoinMultipleMatches) {
    // Modify right tuples to have duplicate ids
    right_tuples.clear();
    right_tuples.emplace_back(CreateTupleIntStr(right_schema, 2, "", "New York"));
    right_tuples.emplace_back(CreateTupleIntStr(right_schema, 2, "", "Los Angeles"));
    right_tuples.emplace_back(CreateTupleIntStr(right_schema, 3, "", "Chicago"));

    // Re-create right executor
    right_executor = std::make_unique<MockExecutor>("right_table", right_schema, right_tuples);

    // Define join condition: left_table.id = right_table.id
    Condition cond;
    cond.lhs_col = TabCol{"left_table", "id", NO_AGG, ""};
    cond.op = OP_EQ;
    cond.is_rhs_val = false;
    cond.is_rhs_stmt = false;
    cond.is_rhs_exe_processed = false;
    cond.rhs_col = TabCol{"right_table", "id", NO_AGG, ""};
    std::vector<Condition> conditions = {cond};

    // Instantiate NestedLoopJoinExecutor
    NestedLoopJoinExecutor join_executor(std::move(left_executor), std::move(right_executor), conditions);

    // Begin tuple processing
    join_executor.beginTuple();

    // Collect joined tuples
    std::vector<Tuple> result_tuples;
    while (!join_executor.IsEnd()) {
        std::unique_ptr<Tuple> joined = join_executor.Next();
        if (joined) {
            result_tuples.emplace_back(*joined);
        }
        join_executor.nextTuple();
    }

    // Define expected results
    // Expected join on id: 2 with two matches
    // Joined tuples: (2, "Bob", 2, "New York"), (2, "Bob", 2, "Los Angeles"), (3, "Charlie", 3, "Chicago")
    std::vector<Tuple> expected_tuples;
    std::vector<Value> expected_val1 = {
        Value(TypeId::TYPE_INT, 2),
        Value(TypeId::TYPE_VARCHAR, "Bob", 3, false),
        Value(TypeId::TYPE_INT, 2),
        Value(TypeId::TYPE_VARCHAR, "New York", 8, false)
    };
    std::vector<Value> expected_val2 = {
        Value(TypeId::TYPE_INT, 2),
        Value(TypeId::TYPE_VARCHAR, "Bob", 3, false),
        Value(TypeId::TYPE_INT, 2),
        Value(TypeId::TYPE_VARCHAR, "Los Angeles", 11, false)
    };
    std::vector<Value> expected_val3 = {
        Value(TypeId::TYPE_INT, 3),
        Value(TypeId::TYPE_VARCHAR, "Charlie", 7, false),
        Value(TypeId::TYPE_INT, 3),
        Value(TypeId::TYPE_VARCHAR, "Chicago", 7, false)
    };
    expected_tuples.emplace_back(Tuple(expected_val1, &join_executor.schema()));
    expected_tuples.emplace_back(Tuple(expected_val2, &join_executor.schema()));
    expected_tuples.emplace_back(Tuple(expected_val3, &join_executor.schema()));

    // Verify the results
    ASSERT_EQ(result_tuples.size(), expected_tuples.size()) << "Number of joined tuples does not match expected.";

    for (size_t i = 0; i < expected_tuples.size(); ++i) {
        ASSERT_EQ(result_tuples[i].ToString(&join_executor.schema()), expected_tuples[i].ToString(&join_executor.schema()))
            << "Tuple " << i << " does not match expected.";
    }
}

// Main function to run all tests
int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}