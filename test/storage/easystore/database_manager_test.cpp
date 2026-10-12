#include <filesystem>
#include <vector>

#include "gtest/gtest.h"
#include "storage/easystore/database_manager.h"
#include "storage/easystore/easystore.h"

namespace easydb {

TEST(DatabaseManagerTest, IsolatesDatabasesAndPersistsCatalogs) {
  const std::filesystem::path root = "database_manager_test_root";
  std::filesystem::remove_all(root);

  {
    DatabaseManager manager(root, false);
    manager.CreateDatabase("first");
    manager.CreateDatabase("second");

    EXPECT_EQ(manager.ListDatabases(),
              (std::vector<std::string>{"first", "second"}));
    EXPECT_NE(manager.GetDatabase("first"), manager.GetDatabase("second"));

    EXPECT_TRUE(std::filesystem::exists(root / "first" / "db.meta"));
    EXPECT_TRUE(std::filesystem::exists(root / "second" / "db.meta"));
  }

  {
    DatabaseManager manager(root, false);
    auto *first = manager.OpenDatabase("first");
    auto *second = manager.OpenDatabase("second");
    EXPECT_FALSE(first->HasTable("users"));
    EXPECT_FALSE(second->HasTable("users"));

    manager.DropDatabase("second");
    EXPECT_FALSE(manager.Exists("second"));
    EXPECT_TRUE(manager.Exists("first"));
  }

  std::filesystem::remove_all(root);
}

}  // namespace easydb
