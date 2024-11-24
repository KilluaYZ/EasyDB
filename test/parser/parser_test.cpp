// // Filename: parser_test.cpp

// #include <gtest/gtest.h>
// #include <vector>
// #include <string>
// #include <iostream>
// #include "parser/parser.h"  // Adjust the path as necessary

// namespace easydb {

// // Define a test fixture for parser tests
// class ParserTest : public ::testing::TestWithParam<std::string> {
// protected:
//     // You can override SetUp and TearDown if needed
//     void SetUp() override {
//         // Initialize parser state if necessary
//     }

//     void TearDown() override {
//         // Clean up parser state if necessary
//         ast::parse_tree.reset();
//     }
// };

// // Define the set of SQL statements to test
// const std::vector<std::string> sql_statements = {
//     "select name from student where id in (1,2,3);",
//     "select name,count(*) as count,sum(val) as sum_val, max(val) as max_val,min(val) as min_val  from aggregate group by name;",
//     "select id,MAX(score) as max_score from grade group by id having COUNT(*) > 3;",
//     "create static_checkpoint;",
//     "load ../../src/test/performance_test/table_data/warehouse.csv into warehouse;",
//     "set output_file off",
//     "create table history (h_c_id int, h_date datetime);",
//     "exit;",
//     "help;",
//     "",
//     "update student set score = score + 1 where score > 5.5;",
//     "select * from student;",
// };

// // Register the test cases with GoogleTest
// INSTANTIATE_TEST_SUITE_P(
//     ParserTests,
//     ParserTest,
//     ::testing::ValuesIn(sql_statements)
// );

// // Define the actual test
// TEST_P(ParserTest, ParsesSQLSuccessfully) {
//     const std::string& sql = GetParam();
    
//     // Print the SQL statement being tested
//     std::cout << "Testing SQL: " << sql << std::endl;
    
//     // Scan the SQL string
//     YY_BUFFER_STATE buf = yy_scan_string(sql.c_str());
//     ASSERT_NE(buf, nullptr) << "Failed to create buffer for SQL: " << sql;
    
//     // Parse the SQL statement
//     int parse_result = yyparse();
    
//     // Delete the buffer to prevent memory leaks
//     yy_delete_buffer(buf);
    
//     // Assert that parsing was successful
//     ASSERT_EQ(parse_result, 0) << "Parsing failed for SQL: " << sql;
    
//     // Check if a parse tree was generated
//     if (ast::parse_tree != nullptr) {
//         // Print the parse tree
//         ast::TreePrinter::print(ast::parse_tree);
//         std::cout << std::endl;
//     } else {
//         // Indicate that there is no parse tree (e.g., for "exit" or "help" commands)
//         std::cout << "exit/EOF" << std::endl;
//     }
    
//     // Reset the parse tree for the next test case
//     ast::parse_tree.reset();
// }

// } // namespace easydb

// // main.cpp (if you don't already have a main for GoogleTest)
// #include <gtest/gtest.h>

// int main(int argc, char **argv) {
//     ::testing::InitGoogleTest(&argc, argv);
//     return RUN_ALL_TESTS();
// }