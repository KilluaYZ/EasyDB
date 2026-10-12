#include "storage/easystore/database_manager.h"

#include <algorithm>

#include "common/errors.h"
#include "storage/easystore/easystore.h"

namespace easydb {

DatabaseManager::DatabaseManager(std::filesystem::path root_path,
                                 bool enable_output)
    : root_path_(std::filesystem::absolute(std::move(root_path))),
      enable_output_(enable_output) {
  std::filesystem::create_directories(root_path_);
}

DatabaseManager::~DatabaseManager() {
  for (auto &[name, database] : databases_) database->CloseDB();
}

void DatabaseManager::ValidateName(const std::string &database_name) const {
  if (database_name.empty() || database_name == "." ||
      database_name == ".." ||
      std::filesystem::path(database_name).filename() != database_name) {
    throw InternalError("invalid database name: " + database_name);
  }
}

std::filesystem::path DatabaseManager::DatabasePath(
    const std::string &database_name) const {
  ValidateName(database_name);
  return root_path_ / database_name;
}

bool DatabaseManager::Exists(const std::string &database_name) const {
  std::scoped_lock lock(latch_);
  return std::filesystem::is_directory(DatabasePath(database_name));
}

std::vector<std::string> DatabaseManager::ListDatabases() const {
  std::scoped_lock lock(latch_);
  std::vector<std::string> result;
  if (!std::filesystem::exists(root_path_)) return result;
  for (const auto &entry : std::filesystem::directory_iterator(root_path_)) {
    if (entry.is_directory()) result.push_back(entry.path().filename().string());
  }
  std::sort(result.begin(), result.end());
  return result;
}

void DatabaseManager::CreateDatabase(const std::string &database_name) {
  std::scoped_lock lock(latch_);
  const auto path = DatabasePath(database_name);
  if (std::filesystem::exists(path)) throw DatabaseExistsError(database_name);
  std::filesystem::create_directories(path);
  auto database = std::make_unique<EasyStore>(path.string(), enable_output_);
  database->OpenDB(path.string());
  databases_.emplace(database_name, std::move(database));
}

EasyStore *DatabaseManager::OpenDatabase(const std::string &database_name) {
  std::scoped_lock lock(latch_);
  const auto path = DatabasePath(database_name);
  if (!std::filesystem::is_directory(path)) {
    throw DatabaseNotFoundError(database_name);
  }
  auto existing = databases_.find(database_name);
  if (existing != databases_.end()) return existing->second.get();

  auto database = std::make_unique<EasyStore>(path.string(), enable_output_);
  database->OpenDB(path.string());
  auto *result = database.get();
  databases_.emplace(database_name, std::move(database));
  return result;
}

void DatabaseManager::CloseDatabase(const std::string &database_name) {
  std::scoped_lock lock(latch_);
  auto it = databases_.find(database_name);
  if (it == databases_.end()) return;
  it->second->CloseDB();
  databases_.erase(it);
}

void DatabaseManager::DropDatabase(const std::string &database_name) {
  std::scoped_lock lock(latch_);
  const auto path = DatabasePath(database_name);
  if (!std::filesystem::is_directory(path)) {
    throw DatabaseNotFoundError(database_name);
  }
  auto it = databases_.find(database_name);
  if (it != databases_.end()) {
    it->second->CloseDB();
    databases_.erase(it);
  }
  std::filesystem::remove_all(path);
}

EasyStore *DatabaseManager::GetDatabase(
    const std::string &database_name) const {
  std::scoped_lock lock(latch_);
  auto it = databases_.find(database_name);
  if (it == databases_.end()) throw DatabaseNotFoundError(database_name);
  return it->second.get();
}

}  // namespace easydb
