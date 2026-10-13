#include <gtest/gtest.h>

#include "storage/easystore/concurrency/lock_manager.h"

namespace easydb {

TEST(LockManagerTest, DifferentRecordsCanBeLockedConcurrently) {
  LockManager lock_manager;
  Transaction first(1);
  Transaction second(2);
  const std::vector<char> first_key{'a'};
  const std::vector<char> second_key{'b'};

  EXPECT_TRUE(lock_manager.LockExclusiveOnRecord(&first, 7, first_key));
  EXPECT_TRUE(lock_manager.LockExclusiveOnRecord(&second, 7, second_key));

  lock_manager.Unlock(&first, LockDataId(7, LockDataType::RECORD, first_key));
  lock_manager.Unlock(&second,
                      LockDataId(7, LockDataType::RECORD, second_key));
}

TEST(LockManagerTest, SharedRecordLocksAreCompatible) {
  LockManager lock_manager;
  Transaction first(1);
  Transaction second(2);
  const std::vector<char> key{'a'};

  EXPECT_TRUE(lock_manager.LockSharedOnRecord(&first, 7, key));
  EXPECT_TRUE(lock_manager.LockSharedOnRecord(&second, 7, key));

  lock_manager.Unlock(&first, LockDataId(7, LockDataType::RECORD, key));
  lock_manager.Unlock(&second, LockDataId(7, LockDataType::RECORD, key));
}

TEST(LockManagerTest, GapExclusiveLockConflictsWithInsertIntention) {
  LockManager lock_manager;
  Transaction older(1);
  Transaction younger(2);
  const std::vector<char> lower{1};
  const std::vector<char> upper{9};

  ASSERT_TRUE(lock_manager.LockExclusiveOnGap(&older, 7, lower, upper));
  EXPECT_THROW(
      lock_manager.LockInsertIntention(&younger, 7, std::vector<char>{3},
                                       std::vector<char>{4}),
      TransactionAbortException);

  lock_manager.Unlock(
      &older, LockDataId(7, LockDataType::GAP, lower, upper));
}

TEST(LockManagerTest, TableIntentionsAllowIndependentRowWrites) {
  LockManager lock_manager;
  Transaction first(1);
  Transaction second(2);
  const std::vector<char> first_key{'a'};
  const std::vector<char> second_key{'b'};

  EXPECT_TRUE(lock_manager.LockIXOnTable(&first, 7));
  EXPECT_TRUE(lock_manager.LockIXOnTable(&second, 7));
  EXPECT_TRUE(lock_manager.LockExclusiveOnRecord(&first, 7, first_key));
  EXPECT_TRUE(lock_manager.LockExclusiveOnRecord(&second, 7, second_key));

  lock_manager.Unlock(&first, LockDataId(7, LockDataType::RECORD, first_key));
  lock_manager.Unlock(&first, LockDataId(7, LockDataType::TABLE));
  lock_manager.Unlock(&second,
                      LockDataId(7, LockDataType::RECORD, second_key));
  lock_manager.Unlock(&second, LockDataId(7, LockDataType::TABLE));
}

TEST(LockManagerTest, ConflictingRecordLockAbortsYoungerTransaction) {
  LockManager lock_manager;
  Transaction older(1);
  Transaction younger(2);
  const std::vector<char> key{'a'};

  ASSERT_TRUE(lock_manager.LockExclusiveOnRecord(&older, 7, key));
  EXPECT_THROW(
      lock_manager.LockExclusiveOnRecord(&younger, 7, key),
      TransactionAbortException);

  lock_manager.Unlock(&older, LockDataId(7, LockDataType::RECORD, key));
}

TEST(LockManagerTest, InsertIntentionConflictsWithOverlappingNextKey) {
  LockManager lock_manager;
  Transaction older(1);
  Transaction younger(2);
  const std::vector<char> lower{1};
  const std::vector<char> upper{9};

  ASSERT_TRUE(lock_manager.LockSharedOnNextKey(&older, 7, lower, upper));
  EXPECT_THROW(
      lock_manager.LockInsertIntention(&younger, 7, std::vector<char>{2},
                                       std::vector<char>{3}),
      TransactionAbortException);

  lock_manager.Unlock(
      &older, LockDataId(7, LockDataType::NEXT_KEY, lower, upper));
}

TEST(TransactionTest, DefaultIsolationLevelIsRepeatableRead) {
  Transaction transaction(1);
  EXPECT_EQ(transaction.GetIsolationLevel(), IsolationLevel::REPEATABLE_READ);
}

TEST(ReadViewTest, IsolationLevelsHaveDifferentVisibilityRules) {
  TupleMeta uncommitted;
  uncommitted.trx_id_ = 2;
  ReadView snapshot(10, 1, std::unordered_set<txn_id_t>{2},
                    std::unordered_map<txn_id_t, timestamp_t>{{2, 5}});

  EXPECT_FALSE(snapshot.IsVisible(uncommitted, IsolationLevel::READ_COMMITTED));
  EXPECT_FALSE(snapshot.IsVisible(uncommitted, IsolationLevel::REPEATABLE_READ));
  EXPECT_FALSE(snapshot.IsVisible(uncommitted, IsolationLevel::SERIALIZABLE));
  EXPECT_TRUE(snapshot.IsVisible(uncommitted, IsolationLevel::READ_UNCOMMITTED));
}

}  // namespace easydb
