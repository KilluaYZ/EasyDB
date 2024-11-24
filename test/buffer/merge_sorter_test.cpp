// // merge_sorter_test.cpp
// #include <gtest/gtest.h>
// #include "common/mergeSorter.h"  // Replace with the actual path to MergeSorter.h
// #include "mock_dependencies.h"
// #include <cstdio>  // For remove()

// using namespace easydb;

// // Helper function to create a tuple from an integer
// Tuple CreateIntTuple(int value, size_t tuple_len) {
//     Tuple tuple(tuple_len);
//     memcpy(tuple.GetData(), &value, sizeof(int));
//     return tuple;
// }

// // Helper function to create a tuple from a string
// Tuple CreateStringTuple(const std::string& value, size_t tuple_len) {
//     Tuple tuple(tuple_len);
//     strncpy(tuple.GetData(), value.c_str(), std::min(value.size(), tuple_len));
//     return tuple;
// }

// // Test Fixture for MergeSorter
// class MergeSorterTest : public ::testing::Test {
// protected:
//     // Define columns
//     Column sort_col;
//     std::vector<Column> all_cols;
//     size_t tuple_len;

//     // Temporary file paths
//     std::vector<std::string> temp_files;

//     void SetUp() override {
//         // Initialize columns
//         sort_col = Column("TestTable", "SortKey", TYPE_INT, 0, sizeof(int));
//         all_cols.emplace_back(sort_col);
//         tuple_len = sizeof(int);  // For simplicity, each tuple is just an integer
//     }

//     void TearDown() override {
//         // Clean up temporary files
//         for (const auto& file : temp_files) {
//             std::remove(file.c_str());
//         }
//     }

//     // Helper to generate unique temporary file names
//     std::string GetTempFileName(const std::string& prefix) {
//         static int counter = 0;
//         return prefix + "_" + std::to_string(counter++) + ".txt";
//     }
// };

// // Test buffering and sorting in ascending order
// TEST_F(MergeSorterTest, BufferAndSortAscending) {
//     // Initialize MergeSorter for ascending order
//     MergeSorter sorter(sort_col, all_cols, tuple_len, false);

//     // Insert tuples
//     std::vector<int> input = {5, 3, 8, 1, 9, 2, 7, 4, 6};
//     for (const auto& val : input) {
//         Tuple tuple = CreateIntTuple(val, tuple_len);
//         sorter.writeBuffer(tuple);
//     }

//     // Clear buffer to ensure all data is written to disk
//     sorter.clearBuffer();

//     // Initialize merging
//     sorter.initializeMergeListAndConstructTree();

//     // Retrieve sorted records
//     std::vector<int> sorted_output;
//     while (!sorter.IsEnd()) {
//         char* record = sorter.getOneRecord();
//         if (record == nullptr) break;
//         int value;
//         memcpy(&value, record, sizeof(int));
//         sorted_output.push_back(value);
//         delete[] record;  // Assuming getOneRecord allocates memory
//     }

//     // Expected sorted order
//     std::vector<int> expected = {1, 2, 3, 4, 5, 6, 7, 8, 9};

//     ASSERT_EQ(sorted_output, expected);
// }

// // Test buffering and sorting in descending order
// TEST_F(MergeSorterTest, BufferAndSortDescending) {
//     // Initialize MergeSorter for descending order
//     MergeSorter sorter(sort_col, all_cols, tuple_len, true);

//     // Insert tuples
//     std::vector<int> input = {5, 3, 8, 1, 9, 2, 7, 4, 6};
//     for (const auto& val : input) {
//         Tuple tuple = CreateIntTuple(val, tuple_len);
//         sorter.writeBuffer(tuple);
//     }

//     // Clear buffer to ensure all data is written to disk
//     sorter.clearBuffer();

//     // Initialize merging
//     sorter.initializeMergeListAndConstructTree();

//     // Retrieve sorted records
//     std::vector<int> sorted_output;
//     while (!sorter.IsEnd()) {
//         char* record = sorter.getOneRecord();
//         if (record == nullptr) break;
//         int value;
//         memcpy(&value, record, sizeof(int));
//         sorted_output.push_back(value);
//         delete[] record;  // Assuming getOneRecord allocates memory
//     }

//     // Expected sorted order
//     std::vector<int> expected = {9, 8, 7, 6, 5, 4, 3, 2, 1};

//     ASSERT_EQ(sorted_output, expected);
// }

// // Test writing buffer to disk when buffer is full
// TEST_F(MergeSorterTest, BufferFullWritesToDisk) {
//     // Set a small buffer size to trigger multiple disk writes
//     size_t small_buffer_size = 3;  // Buffer can hold 3 records
//     MergeSorter sorter(sort_col, all_cols, tuple_len, false);
//     sorter.BUFFER_MAX_RECORD_COUNT = small_buffer_size;  // Assuming BUFFER_MAX_RECORD_COUNT is public for testing

//     // Insert 5 tuples, which should result in 2 disk writes (3 and 2 records)
//     std::vector<int> input = {10, 5, 7, 3, 8};
//     for (const auto& val : input) {
//         Tuple tuple = CreateIntTuple(val, tuple_len);
//         sorter.writeBuffer(tuple);
//     }

//     // At this point, buffer should have written 3 records to disk
//     EXPECT_EQ(sorter.file_paths.size(), 1);

//     // Clear buffer to write remaining 2 records
//     sorter.clearBuffer();
//     EXPECT_EQ(sorter.file_paths.size(), 2);

//     // Initialize merging
//     sorter.initializeMergeListAndConstructTree();

//     // Retrieve sorted records
//     std::vector<int> sorted_output;
//     while (!sorter.IsEnd()) {
//         char* record = sorter.getOneRecord();
//         if (record == nullptr) break;
//         int value;
//         memcpy(&value, record, sizeof(int));
//         sorted_output.push_back(value);
//         delete[] record;  // Assuming getOneRecord allocates memory
//     }

//     // Expected sorted order
//     std::vector<int> expected = {3, 5, 7, 8, 10};

//     ASSERT_EQ(sorted_output, expected);
// }