// ix_extendible_hash_index_handle_test.cpp

#include <gtest/gtest.h>
#include <filesystem>
#include <vector>
#include "buffer/buffer_pool_manager.h"
#include "storage/disk/disk_manager.h"
#include "storage/index/ix_defs.h"
#include "storage/index/ix_extendible_hash_index_handle.h"

namespace easydb {

// Helper function to convert int to char key
const char *IntToKey(int key_int) {
  char *key = new char[sizeof(int)];
  memcpy(key, &key_int, sizeof(int));
  return key;
}

// Helper function to clean up key
void DeleteKey(const char *key) { delete[] key; }

class IxExtendibleHashIndexHandleTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create a unique temporary directory
    temp_dir = std::filesystem::temp_directory_path() / "ix_test_dir";
    std::filesystem::create_directory(temp_dir);

    // Initialize DiskManager
    disk_manager = new DiskManager(temp_dir);

    // Create index file
    std::filesystem::path index_file = "test_index_file";
    disk_manager->CreateFile(index_file);
    fd = disk_manager->OpenFile(index_file);

    // Initialize IxFileHdr
    IxFileHdr file_hdr;
    file_hdr.first_free_page_no_ = IX_INIT_NUM_PAGES;
    file_hdr.num_pages_ = IX_INIT_NUM_PAGES;
    file_hdr.root_page_ = IX_INIT_ROOT_PAGE;
    file_hdr.col_num_ = 1;
    file_hdr.col_types_ = {TYPE_INT};
    file_hdr.col_lens_ = {sizeof(int)};
    file_hdr.col_tot_len_ = sizeof(int);
    file_hdr.btree_order_ = 4;
    file_hdr.keys_size_ = (file_hdr.btree_order_ + 1) * file_hdr.col_tot_len_;
    file_hdr.first_leaf_ = IX_INIT_ROOT_PAGE;
    file_hdr.last_leaf_ = IX_INIT_ROOT_PAGE;
    file_hdr.update_tot_len();

    // Serialize and write to IX_FILE_HDR_PAGE (page 0)
    char buffer[PAGE_SIZE];
    memset(buffer, 0, PAGE_SIZE);
    file_hdr.serialize(buffer);
    disk_manager->WritePage(fd, IX_FILE_HDR_PAGE, buffer, PAGE_SIZE);

    // Initialize BufferPoolManager with 10 frames
    buffer_pool_manager = new BufferPoolManager(10, disk_manager);

    // Initialize IxExtendibleHashIndexHandle
    index_handle = new IxExtendibleHashIndexHandle(disk_manager, buffer_pool_manager, fd);
  }

  void TearDown() override {
    delete index_handle;
    delete buffer_pool_manager;
    delete disk_manager;
    // Remove temporary directory and its contents
    std::filesystem::remove_all(temp_dir);
  }

  std::filesystem::path temp_dir;
  DiskManager *disk_manager;
  BufferPoolManager *buffer_pool_manager;
  IxExtendibleHashIndexHandle *index_handle;
  int fd;
};

TEST_F(IxExtendibleHashIndexHandleTest, InsertSingleEntry) {
  int key_int = 42;
  const char *key = IntToKey(key_int);
  Rid value = {1, 1};

  page_id_t inserted_page = index_handle->InsertEntry(key, value);
  EXPECT_NE(inserted_page, -1);

  std::vector<Rid> result;
  bool found = index_handle->GetValue(key, &result);
  EXPECT_TRUE(found);
  ASSERT_EQ(result.size(), 1);
  EXPECT_EQ(result[0].page_no, value.page_no);
  EXPECT_EQ(result[0].slot_no, value.slot_no);

  DeleteKey(key);
}

TEST_F(IxExtendibleHashIndexHandleTest, InsertMultipleEntries) {
  std::vector<int> keys_int = {10, 20, 30, 40};
  std::vector<Rid> values = {{1, 1}, {1, 2}, {1, 3}, {1, 4}};

  for (size_t i = 0; i < keys_int.size(); ++i) {
    const char *key = IntToKey(keys_int[i]);
    page_id_t inserted_page = index_handle->InsertEntry(key, values[i]);
    EXPECT_NE(inserted_page, -1);
    DeleteKey(key);
  }

  for (size_t i = 0; i < keys_int.size(); ++i) {
    const char *key = IntToKey(keys_int[i]);
    std::vector<Rid> result;
    bool found = index_handle->GetValue(key, &result);
    EXPECT_TRUE(found);
    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ(result[0].page_no, values[i].page_no);
    EXPECT_EQ(result[0].slot_no, values[i].slot_no);
    DeleteKey(key);
  }
}

TEST_F(IxExtendibleHashIndexHandleTest, RemoveEntry) {
  int key_int = 55;
  const char *key = IntToKey(key_int);
  Rid value = {2, 2};

  page_id_t inserted_page = index_handle->InsertEntry(key, value);
  EXPECT_NE(inserted_page, -1);

  // Verify insertion
  std::vector<Rid> result;
  bool found = index_handle->GetValue(key, &result);
  EXPECT_TRUE(found);
  ASSERT_EQ(result.size(), 1);
  EXPECT_EQ(result[0].page_no, value.page_no);
  EXPECT_EQ(result[0].slot_no, value.slot_no);

  // Remove the entry
  bool removed = index_handle->DeleteEntry(key);
  EXPECT_TRUE(removed);

  // Verify removal
  result.clear();
  found = index_handle->GetValue(key, &result);
  EXPECT_FALSE(found);

  DeleteKey(key);
}

TEST_F(IxExtendibleHashIndexHandleTest, HandleDuplicateKeys) {
  int key_int = 99;
  const char *key = IntToKey(key_int);
  Rid value1 = {3, 1};
  Rid value2 = {3, 2};

  // Insert first entry
  page_id_t inserted_page1 = index_handle->InsertEntry(key, value1);
  EXPECT_NE(inserted_page1, -1);

  // Insert duplicate key
  page_id_t inserted_page2 = index_handle->InsertEntry(key, value2);
  EXPECT_NE(inserted_page2, -1);

  // Verify both entries exist
  std::vector<Rid> result;
  bool found = index_handle->GetValue(key, &result);
  EXPECT_TRUE(found);
  ASSERT_EQ(result.size(), 2);
  EXPECT_EQ(result[0].page_no, value1.page_no);
  EXPECT_EQ(result[0].slot_no, value1.slot_no);
  EXPECT_EQ(result[1].page_no, value2.page_no);
  EXPECT_EQ(result[1].slot_no, value2.slot_no);

  // Clean up
  bool removed = index_handle->DeleteEntry(key);
  EXPECT_TRUE(removed);

  // Verify removal
  result.clear();
  found = index_handle->GetValue(key, &result);
  EXPECT_FALSE(found);

  DeleteKey(key);
}

TEST_F(IxExtendibleHashIndexHandleTest, BucketSplit) {
  // Assuming size_per_bucket is 4, insert 5 entries to trigger a split
  std::vector<int> keys_int = {1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15,
                               16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30};
  std::vector<Rid> values = {{1, 1},  {1, 2},  {1, 3},  {1, 4},  {2, 5},  {2, 6},  {2, 7},  {2, 8},  {3, 9},  {3, 10},
                             {3, 11}, {3, 12}, {4, 13}, {4, 14}, {4, 15}, {4, 16}, {5, 17}, {5, 18}, {5, 19}, {5, 20},
                             {6, 21}, {6, 22}, {6, 23}, {6, 24}, {7, 25}, {7, 26}, {7, 27}, {7, 28}, {8, 29}, {8, 30}};

  for (size_t i = 0; i < keys_int.size(); ++i) {
    const char *key = IntToKey(keys_int[i]);
    page_id_t inserted_page = index_handle->InsertEntry(key, values[i]);
    std::vector<Rid> result;
    bool found = index_handle->GetValue(key, &result);
    // EXPECT_EQ(result[0].page_no, values[i].page_no);
    // EXPECT_EQ(result[0].slot_no, values[i].slot_no);
    EXPECT_NE(inserted_page, -1);
    EXPECT_EQ(inserted_page, 0);
    DeleteKey(key);
  }

  // Verify all entries
  for (size_t i = 0; i < keys_int.size(); ++i) {
    const char *key = IntToKey(keys_int[i]);
    std::vector<Rid> result;
    bool found = index_handle->GetValue(key, &result);
    EXPECT_TRUE(found);
    // EXPECT_EQ(result.size(), 1) << "Key " << keys_int[i] << " has incorrect number of Rids.";
    EXPECT_EQ(result[0].page_no, values[i].page_no);
    EXPECT_EQ(result[0].slot_no, values[i].slot_no);
    DeleteKey(key);
  }

  // Optionally, verify that global depth has increased
  // Since global_depth is set to 1 initially, after a split it should be 2
  EXPECT_EQ(index_handle->get_global_depth(), 2);
}

}  // namespace easydb

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}