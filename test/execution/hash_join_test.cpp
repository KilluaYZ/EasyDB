#include <gtest/gtest.h>

#include "execution/executor_hash_join.h"

namespace easydb {

class MockExecutor : public AbstractExecutor {
 public:
  MockExecutor(const std::vector<Tuple> &tuples, const Schema &schema)
      : tuples_(tuples), schema_(schema), current_index_(0) {}

  void beginTuple() override { current_index_ = 0; }

  // If required by AbstractExecutor
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

  std::string getTabName() const override { return schema_.GetColumns()[0].GetTabName(); }

 private:
  std::vector<Tuple> tuples_;
  Schema schema_;
  size_t current_index_;
};

TEST(HashJoinExecutorTest, BasicTest) {
  // Define schemas
  Column left_col1("id", "left_table", TypeId::TYPE_INT);
  Column left_col2("value", "left_table", TypeId::TYPE_INT);
  Schema left_schema({left_col1, left_col2});

  Column right_col1("id", "right_table", TypeId::TYPE_INT);
  Column right_col2("value", "right_table", TypeId::TYPE_INT);
  Schema right_schema({right_col1, right_col2});

  // Create tuples for left child
  std::vector<Tuple> left_tuples;
  for (int i = 0; i < 5; ++i) {
    std::vector<Value> values = {Value(TypeId::TYPE_INT, i), Value(TypeId::TYPE_INT, i * 10)};
    left_tuples.emplace_back(values, &left_schema);
  }

  // Create tuples for right child
  std::vector<Tuple> right_tuples;
  for (int i = 3; i < 8; ++i) {
    std::vector<Value> values = {Value(TypeId::TYPE_INT, i), Value(TypeId::TYPE_INT, i * 100)};
    right_tuples.emplace_back(values, &right_schema);
  }

  // Create mock executors
  auto left_executor = std::make_unique<MockExecutor>(left_tuples, left_schema);
  auto right_executor = std::make_unique<MockExecutor>(right_tuples, right_schema);

  // Define join condition
  TabCol left_col = {"left_table", "id"};
  TabCol right_col = {"right_table", "id"};
  Condition join_condition = {
      left_col,   // lhs_col
      OP_EQ,      // op
      false,      // is_rhs_val
      false,      // is_rhs_stmt
      false,      // is_rhs_exe_processed
      right_col   // rhs_col
  };

  // Create HashJoinExecutor
  std::vector<Condition> join_conditions = {join_condition};
  HashJoinExecutor hash_join_executor(std::move(left_executor), std::move(right_executor), join_conditions);

  // Initialize executor
  std::cerr<<"1"<<std::endl;
  hash_join_executor.beginTuple();
  std::cerr<<"2"<<std::endl;

  // Collect joined tuples
  std::vector<Tuple> joined_tuples;
  std::unique_ptr<Tuple> joined_tuple;
  while ((joined_tuple = hash_join_executor.Next()) != nullptr) {
    joined_tuples.push_back(*joined_tuple);
  }

  // Check results
  ASSERT_EQ(joined_tuples.size(), 2);  // Only ids 3 and 4 match

  // Verify the joined tuples
  for (const auto &tuple : joined_tuples) {
    auto id_value = tuple.GetValue(&hash_join_executor.schema(), "id");
    int32_t id = id_value.GetAs<int32_t>();
    ASSERT_TRUE(id == 3 || id == 4);
  }
}

}  // namespace easydb