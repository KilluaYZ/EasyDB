#include "storage/easystore/catalog/load_manager.h"

#include <algorithm>
#include <fstream>
#include <limits>
#include <set>
#include <sstream>
#include <unordered_map>

#include "buffer/buffer_pool_manager.h"
#include "common/context.h"
#include "common/errors.h"
#include "common/exception.h"
#include "storage/easystore/table/tuple.h"

namespace easydb {

void LoadManager::AsyncLoadData(const std::string &file_name,
                                const std::string &table_name, Context *) {
  load_status_ = 0;
  futures_.emplace_back(std::async(
      std::launch::async,
      [this, file_name, table_name] { LoadData(file_name, table_name); }));
}

void LoadManager::Finish() {
  for (auto &future : futures_) future.get();
  futures_.clear();
  load_status_ = 1;
}

void LoadManager::LoadData(const std::string &file_name,
                           const std::string &table_name) {
  if (!catalog_manager_->IsTable(table_name)) {
    throw TableNotFoundError(table_name);
  }
  const auto &table = catalog_manager_->GetTable(table_name);
  auto *table_handle = clustered_table_manager_->Get(table_name);
  std::ifstream input(file_name);
  if (!input.is_open()) {
    throw Exception("load data: open file failed: " + file_name);
  }

  std::vector<std::string> column_names;
  for (const auto &column : table.cols) column_names.push_back(column.name);
  std::unordered_map<std::string, float> max_values;
  std::unordered_map<std::string, float> min_values;
  std::unordered_map<std::string, float> sums;
  std::unordered_map<std::string, std::set<float>> distinct_values;
  for (const auto &name : column_names) {
    max_values[name] = 0;
    min_values[name] = std::numeric_limits<float>::max();
    sums[name] = 0;
  }

  std::string line;
  int count = 0;
  while (std::getline(input, line)) {
    std::vector<std::string> fields;
    std::stringstream stream(line);
    std::string field;
    while (std::getline(stream, field, '|')) fields.push_back(field);
    if (fields.size() != table.cols.size()) {
      throw Exception("load data: column count does not match table schema");
    }

    std::vector<Value> values;
    for (size_t i = 0; i < fields.size(); ++i) {
      const auto type = table.cols[i].type;
      if (type == TYPE_INT) {
        auto value = static_cast<float>(std::stoi(fields[i]));
        values.emplace_back(type, static_cast<int>(value));
        max_values[column_names[i]] =
            std::max(max_values[column_names[i]], value);
        min_values[column_names[i]] =
            std::min(min_values[column_names[i]], value);
        sums[column_names[i]] += value;
        distinct_values[column_names[i]].insert(value);
      } else if (type == TYPE_FLOAT || type == TYPE_DOUBLE) {
        auto value = std::stof(fields[i]);
        values.emplace_back(type, value);
        max_values[column_names[i]] =
            std::max(max_values[column_names[i]], value);
        min_values[column_names[i]] =
            std::min(min_values[column_names[i]], value);
        sums[column_names[i]] += value;
        distinct_values[column_names[i]].insert(value);
      } else {
        values.emplace_back(type, fields[i]);
      }
    }
    Tuple tuple(values, &table.schema);
    if (!table_handle->Insert(tuple)) {
      throw Exception("load data: duplicate clustered key");
    }
    ++count;
  }

  statistics_manager_->SetTableCount(table_name, count);
  for (const auto &name : column_names) {
    if (distinct_values[name].empty()) continue;
    statistics_manager_->SetTableAttrMax(table_name, name, max_values[name]);
    statistics_manager_->SetTableAttrMin(table_name, name, min_values[name]);
    statistics_manager_->SetTableAttrSum(table_name, name, sums[name]);
    statistics_manager_->SetTableAttrDistinct(table_name, name,
                                              distinct_values[name].size());
  }
  buffer_pool_manager_->FlushAllDirtyPages();
}

}  // namespace easydb
