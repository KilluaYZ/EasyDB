#include <cstring>
#include <filesystem>
#include <memory>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"
#include "storage/disk/disk_manager.h"
#include "storage/easystore/index/clustered_index_handle.h"

namespace easydb {
namespace {

auto IntKey(int value) -> std::vector<char> {
  std::vector<char> key(sizeof(value));
  std::memcpy(key.data(), &value, sizeof(value));
  return key;
}

auto CompareIntKey(const char *lhs, const char *rhs) -> int {
  int left;
  int right;
  std::memcpy(&left, lhs, sizeof(left));
  std::memcpy(&right, rhs, sizeof(right));
  return (left > right) - (left < right);
}

}  // namespace

TEST(ClusteredIndexHandleTest, InsertsAndFindsTuplesAfterLeafSplits) {
  const std::filesystem::path db_dir = "clustered_index_test.db";
  std::filesystem::remove_all(db_dir);
  {
    DiskManager disk_manager(db_dir);
    const std::string path = (db_dir / "primary.index").string();
    disk_manager.CreateFile(path);
    const int fd = disk_manager.OpenFile(path);
    ASSERT_TRUE(
        ClusteredIndexHandle::Initialize(fd, &disk_manager, sizeof(int)));
    auto buffer_pool_manager =
        std::make_unique<BufferPoolManager>(64, &disk_manager);
    ClusteredIndexHandle index(fd, buffer_pool_manager.get(), sizeof(int),
                               CompareIntKey);

    for (int i = 0; i < 200; ++i) {
      std::vector<char> tuple(100, static_cast<char>(i));
      ASSERT_TRUE(index.Insert(IntKey(i), tuple));
    }
    EXPECT_NE(index.GetRootPage(), 1);
    EXPECT_EQ(index.Get(IntKey(-1)), std::nullopt);
    for (int i = 0; i < 200; ++i) {
      auto tuple = index.Get(IntKey(i));
      ASSERT_TRUE(tuple.has_value()) << "missing key " << i;
      EXPECT_EQ(tuple->size(), 100U);
      EXPECT_EQ((*tuple)[0], static_cast<char>(i));
    }
    EXPECT_FALSE(index.Insert(IntKey(10), {'d', 'u', 'p'}));
    index.Flush();
    buffer_pool_manager.reset();
    disk_manager.CloseFile(fd);

    const int reopened_fd = disk_manager.OpenFile(path);
    auto reopened_buffer_pool_manager =
        std::make_unique<BufferPoolManager>(64, &disk_manager);
    ClusteredIndexHandle reopened_index(reopened_fd,
                                        reopened_buffer_pool_manager.get(),
                                        sizeof(int), CompareIntKey);
    ASSERT_TRUE(reopened_index.Get(IntKey(199)).has_value());
    ASSERT_TRUE(reopened_index.Insert(IntKey(200), {'n', 'e', 'w'}));
    EXPECT_EQ(reopened_index.Get(IntKey(200)),
              std::optional<std::vector<char>>({'n', 'e', 'w'}));
    reopened_index.Flush();
    reopened_buffer_pool_manager.reset();
    disk_manager.CloseFile(reopened_fd);
  }
  std::filesystem::remove_all(db_dir);
}

}  // namespace easydb
