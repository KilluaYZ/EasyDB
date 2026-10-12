#pragma once

#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <vector>
#include <unordered_map>

namespace easydb {

class EasyStore;

/** Owns the independent EasyStore instances under one database root. */
class DatabaseManager {
 public:
  explicit DatabaseManager(std::filesystem::path root_path,
                           bool enable_output = true);
  ~DatabaseManager();

  DatabaseManager(const DatabaseManager &) = delete;
  DatabaseManager &operator=(const DatabaseManager &) = delete;

  bool Exists(const std::string &database_name) const;
  std::vector<std::string> ListDatabases() const;

  void CreateDatabase(const std::string &database_name);
  EasyStore *OpenDatabase(const std::string &database_name);
  void CloseDatabase(const std::string &database_name);
  void DropDatabase(const std::string &database_name);

  EasyStore *GetDatabase(const std::string &database_name) const;
  const std::filesystem::path &GetRootPath() const { return root_path_; }

 private:
  std::filesystem::path DatabasePath(const std::string &database_name) const;
  void ValidateName(const std::string &database_name) const;

  std::filesystem::path root_path_;
  bool enable_output_;
  mutable std::mutex latch_;
  std::unordered_map<std::string, std::unique_ptr<EasyStore>> databases_;
};

}  // namespace easydb
