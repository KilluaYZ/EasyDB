
#include <cstring>
#include <filesystem>
#include <memory>
#include <string>

#include "common/config.h"
#include "common/exception.h"
#include "gtest/gtest.h"
#include "storage/disk/disk_manager.h"
#include "storage/easystore/catalog/catalog_manager.h"
#include "storage/easystore/concurrency/lock_manager.h"
#include "storage/easystore/recovery/recovery_manager.h"
#include "storage/easystore/recovery/log_manager.h"
#include "storage/easystore/table/clustered_table_manager.h"
#include "storage/easystore/transaction/transaction_manager.h"

namespace easydb {

const std::string TEST_DB_NAME = "test.easydb";  // 以数据库名作为根目录
// const std::string TEST_DB_LOG = "test.easydb.log";
const std::string TEST_TABLE_NAME = "test.table";

class DiskManagerTest : public ::testing::Test {
 protected:
  // This function is called before every test.
  void SetUp() override { std::filesystem::remove_all(TEST_DB_NAME); }

  // This function is called after every test.
  void TearDown() override { std::filesystem::remove_all(TEST_DB_NAME); };
};

// NOLINTNEXTLINE
TEST_F(DiskManagerTest, ReadWritePageTest) {
  char buf[PAGE_SIZE] = {0};
  char data[PAGE_SIZE] = {0};
  auto dm = DiskManager(TEST_DB_NAME);
  std::strncpy(data, "A test string.", sizeof(data));

  std::string path = TEST_DB_NAME + "/" + TEST_TABLE_NAME;
  if (!dm.IsFile(path)) {
    dm.CreateFile(path);
  }
  int fd = dm.OpenFile(path);

  dm.ReadPage(fd, 0, buf, PAGE_SIZE);  // tolerate empty read

  dm.WritePage(fd, 0, data, PAGE_SIZE);
  dm.ReadPage(fd, 0, buf, PAGE_SIZE);
  EXPECT_EQ(std::memcmp(buf, data, sizeof(buf)), 0);

  std::memset(buf, 0, sizeof(buf));
  dm.WritePage(fd, 5, data, PAGE_SIZE);
  dm.ReadPage(fd, 5, buf, PAGE_SIZE);
  EXPECT_EQ(std::memcmp(buf, data, sizeof(buf)), 0);

  dm.CloseFile(fd);
}

// NOLINTNEXTLINE
TEST_F(DiskManagerTest, LogPersistsAndResumesLsn) {
  lsn_t first_lsn;
  {
    DiskManager dm(TEST_DB_NAME);
    auto log_manager = std::make_unique<LogManager>(&dm);
    BeginLogRecord begin(42);
    first_lsn = log_manager->add_log_to_buffer(&begin);
    log_manager->flush_log_to_disk();

    EXPECT_EQ(first_lsn, 0);
    EXPECT_EQ(log_manager->GetPersistentLsn(), first_lsn);
    EXPECT_GT(dm.GetLogSize(), 0U);
  }

  DiskManager dm(TEST_DB_NAME);
  auto log_manager = std::make_unique<LogManager>(&dm);
  EXPECT_EQ(log_manager->GetNextLsn(), first_lsn + 1);
  EXPECT_EQ(log_manager->GetPersistentLsn(), first_lsn);

  BeginLogRecord next_begin(43);
  EXPECT_EQ(log_manager->add_log_to_buffer(&next_begin), first_lsn + 1);
  log_manager->flush_log_to_disk();
  EXPECT_GT(dm.GetLogSize(), static_cast<size_t>(LOG_HEADER_SIZE));
}

// NOLINTNEXTLINE
TEST_F(DiskManagerTest, ClusteredRedoUndoLogRoundTrip) {
  const Tuple key(std::vector<char>{'k', 'e', 'y'});
  const Tuple old_tuple(std::vector<char>{'o', 'l', 'd'});
  const Tuple new_tuple(std::vector<char>{'n', 'e', 'w'});

  RedoLogRecord redo(LogKind::REDO, UPDATE, 7, INVALID_LSN, "users", key,
                     old_tuple, new_tuple);
  std::vector<char> buffer(redo.log_tot_len_);
  redo.serialize(buffer.data());

  TupleLogRecord restored;
  restored.deserialize(buffer.data());

  EXPECT_EQ(restored.GetKind(), LogKind::REDO);
  EXPECT_EQ(restored.GetOperation(), UPDATE);
  EXPECT_EQ(restored.log_tid_, 7);
  EXPECT_EQ(restored.GetTableName(), "users");
  EXPECT_TRUE(IsTupleContentEqual(restored.GetKey(), key));
  EXPECT_TRUE(IsTupleContentEqual(restored.GetOldTuple(), old_tuple));
  EXPECT_TRUE(IsTupleContentEqual(restored.GetNewTuple(), new_tuple));

  UndoLogRecord undo(LogKind::UNDO, UPDATE, 7, redo.lsn_, "users", key,
                     old_tuple, new_tuple);
  EXPECT_EQ(undo.GetKind(), LogKind::UNDO);
}

// NOLINTNEXTLINE
TEST_F(DiskManagerTest, RecoversCommittedAndUncommittedClusteredChanges) {
  const std::filesystem::path db_dir = "recovery_test.easydb";
  std::filesystem::remove_all(db_dir);
  std::filesystem::create_directories(db_dir);
  const auto original_dir = std::filesystem::current_path();
  std::filesystem::current_path(db_dir);

  Schema schema({Column("id", TYPE_INT), Column("value", TYPE_INT)});
  const auto make_tuple = [&schema](int id, int value) {
    return Tuple({Value(TYPE_INT, static_cast<int32_t>(id)),
                  Value(TYPE_INT, static_cast<int32_t>(value))},
                 &schema);
  };
  const auto committed_tuple = make_tuple(1, 10);
  const auto uncommitted_tuple = make_tuple(2, 20);

  DiskManager disk_manager(".");
  BufferPoolManager buffer_pool_manager(64, &disk_manager);
  ClusteredTableManager table_manager(&disk_manager, &buffer_pool_manager);
  table_manager.Create("users", schema);
  table_manager.Open("users", schema);
  LogManager log_manager(&disk_manager);

  BeginLogRecord begin_committed(1);
  log_manager.add_log_to_buffer(&begin_committed);
  RedoLogRecord redo_committed(LogKind::REDO, INSERT, 1,
                               begin_committed.lsn_, "users",
                               committed_tuple, Tuple{}, committed_tuple);
  log_manager.add_log_to_buffer(&redo_committed);
  UndoLogRecord undo_committed(LogKind::UNDO, INSERT, 1, redo_committed.lsn_,
                               "users", committed_tuple, Tuple{},
                               committed_tuple);
  log_manager.add_log_to_buffer(&undo_committed);
  CommitLogRecord commit(1, undo_committed.lsn_);
  log_manager.add_log_to_buffer(&commit);

  BeginLogRecord begin_uncommitted(2);
  log_manager.add_log_to_buffer(&begin_uncommitted);
  RedoLogRecord redo_uncommitted(LogKind::REDO, INSERT, 2,
                                 begin_uncommitted.lsn_, "users",
                                 uncommitted_tuple, Tuple{},
                                 uncommitted_tuple);
  log_manager.add_log_to_buffer(&redo_uncommitted);
  UndoLogRecord undo_uncommitted(LogKind::UNDO, INSERT, 2,
                                 redo_uncommitted.lsn_, "users",
                                 uncommitted_tuple, Tuple{},
                                 uncommitted_tuple);
  log_manager.add_log_to_buffer(&undo_uncommitted);
  log_manager.flush_log_to_disk();

  RecoveryManager recovery(&table_manager, &log_manager);
  recovery.Recover();

  EXPECT_TRUE(table_manager.Get("users")->Get(committed_tuple).has_value());
  EXPECT_FALSE(
      table_manager.Get("users")->Get(uncommitted_tuple).has_value());

  table_manager.Close("users");
  std::filesystem::current_path(original_dir);
  std::filesystem::remove_all(db_dir);
}

// NOLINTNEXTLINE
TEST_F(DiskManagerTest, TransactionAbortUsesDurableUndoLog) {
  const std::filesystem::path db_dir = "abort_test.easydb";
  std::filesystem::remove_all(db_dir);
  std::filesystem::create_directories(db_dir);
  const auto original_dir = std::filesystem::current_path();
  std::filesystem::current_path(db_dir);

  Schema schema({Column("id", TYPE_INT), Column("value", TYPE_INT)});
  const Tuple tuple({Value(TYPE_INT, static_cast<int32_t>(7)),
                     Value(TYPE_INT, static_cast<int32_t>(70))},
                    &schema);

  DiskManager disk_manager(".");
  BufferPoolManager buffer_pool_manager(64, &disk_manager);
  ClusteredTableManager table_manager(&disk_manager, &buffer_pool_manager);
  table_manager.Create("users", schema);
  table_manager.Open("users", schema);
  LogManager log_manager(&disk_manager);

  LockManager lock_manager;
  CatalogManager catalog_manager;
  TransactionManager transaction_manager(
      &lock_manager, &disk_manager, &buffer_pool_manager, &catalog_manager,
      nullptr, &table_manager);

  auto *txn = transaction_manager.Begin(nullptr, &log_manager);
  RedoLogRecord redo(LogKind::REDO, INSERT, txn->GetTransactionId(),
                     txn->GetPrevLsn(), "users", tuple, Tuple{}, tuple);
  auto lsn = log_manager.add_log_to_buffer(&redo);
  txn->SetPrevLsn(lsn);
  UndoLogRecord undo(LogKind::UNDO, INSERT, txn->GetTransactionId(),
                     txn->GetPrevLsn(), "users", tuple, Tuple{}, tuple);
  lsn = log_manager.add_log_to_buffer(&undo);
  txn->SetPrevLsn(lsn);
  log_manager.flush_log_to_disk();

  ASSERT_TRUE(table_manager.Get("users")->Insert(tuple));
  ASSERT_TRUE(table_manager.Get("users")->Get(tuple).has_value());

  transaction_manager.Abort(txn, &log_manager);

  EXPECT_FALSE(table_manager.Get("users")->Get(tuple).has_value());
  EXPECT_EQ(txn->GetState(), TransactionState::ABORTED);

  TransactionManager::txn_map.erase(txn->GetTransactionId());
  delete txn;
  table_manager.Close("users");
  std::filesystem::current_path(original_dir);
  std::filesystem::remove_all(db_dir);
}

}  // namespace easydb
