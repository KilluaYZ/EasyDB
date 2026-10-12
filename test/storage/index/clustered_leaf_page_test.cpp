#include <cstring>
#include <vector>

#include "gtest/gtest.h"
#include "storage/easystore/index/clustered_leaf_page.h"

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

TEST(ClusteredLeafPageTest, StoresVariableLengthTuplesInKeyOrder) {
  Page page;
  ClusteredLeafPage::Init(&page, 11, 22);
  ClusteredLeafPage leaf(&page);

  ASSERT_TRUE(leaf.Insert(0, IntKey(20), {'b', 'e', 't', 'a'}));
  ASSERT_TRUE(leaf.Insert(0, IntKey(10), {'a'}));
  ASSERT_TRUE(leaf.Insert(2, IntKey(30), {'c', 'c', 'c'}));

  EXPECT_TRUE(ClusteredLeafPage::IsValid(&page));
  EXPECT_EQ(leaf.GetSize(), 3);
  EXPECT_EQ(leaf.GetPrevLeaf(), 11);
  EXPECT_EQ(leaf.GetNextLeaf(), 22);
  EXPECT_EQ(leaf.LowerBound(IntKey(20), CompareIntKey), 1);
  EXPECT_EQ(leaf.GetEntry(0).tuple, std::vector<char>({'a'}));
  EXPECT_EQ(leaf.GetEntry(1).tuple, std::vector<char>({'b', 'e', 't', 'a'}));

  ASSERT_TRUE(leaf.Update(1, {'u', 'p', 'd', 'a', 't', 'e', 'd'}));
  EXPECT_EQ(leaf.GetEntry(1).tuple,
            std::vector<char>({'u', 'p', 'd', 'a', 't', 'e', 'd'}));

  ASSERT_TRUE(leaf.Erase(0));
  EXPECT_EQ(leaf.GetSize(), 2);
  EXPECT_EQ(leaf.GetEntry(0).key, IntKey(20));
}

TEST(ClusteredLeafPageTest, RejectsRecordsThatDoNotFit) {
  Page page;
  ClusteredLeafPage::Init(&page);
  ClusteredLeafPage leaf(&page);
  std::vector<char> tuple(PAGE_SIZE, 'x');
  EXPECT_FALSE(leaf.Insert(0, IntKey(1), tuple));
  EXPECT_EQ(leaf.GetSize(), 0);
}

}  // namespace easydb
