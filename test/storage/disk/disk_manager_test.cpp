
#include <cstring>
#include <filesystem>
#include <memory>
#include <string>

#include "common/config.h"
#include "common/exception.h"
#include "gtest/gtest.h"
#include "storage/disk/disk_manager.h"
#include "storage/easystore/recovery/log_manager.h"

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

}  // namespace easydb
