
#include <cstring>
#include <string>

#include "common/config.h"
#include "common/exception.h"
#include "gtest/gtest.h"
#include "storage/disk/disk_manager.h"

namespace easydb {

const std::string TEST_DB_NAME = "test.easydb";  // 以数据库名作为根目录
// const std::string TEST_DB_LOG = "test.easydb.log";
const std::string TEST_TABLE_NAME = "test.table";

class DiskManagerTest : public ::testing::Test {
 protected:
  // This function is called before every test.
  void SetUp() override { remove(TEST_DB_NAME.c_str()); }

  // This function is called after every test.
  void TearDown() override { remove(TEST_DB_NAME.c_str()); };
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

}  // namespace easydb
