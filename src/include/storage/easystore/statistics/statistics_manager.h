#pragma once

#include <string>
#include <unordered_map>

namespace easydb {

class StatisticsManager {
 public:
  void SetTableCount(const std::string &table_name, int count) {
    auto [it, inserted] = table_count_.emplace(table_name, count);
    if (!inserted) it->second += count;
  }

  int GetTableCount(const std::string &table_name) const {
    auto it = table_count_.find(table_name);
    return it == table_count_.end() ? -1 : it->second;
  }

  void UpdateTableCount(const std::string &table_name, int delta) {
    auto it = table_count_.find(table_name);
    if (it != table_count_.end()) it->second += delta;
  }

  void SetTableAttrMax(const std::string &table, const std::string &column,
                       float value) {
    table_attr_max_[table][column] = value;
  }
  float GetTableAttrMax(const std::string &table,
                        const std::string &column) const {
    return Get(table_attr_max_, table, column);
  }

  void SetTableAttrMin(const std::string &table, const std::string &column,
                       float value) {
    table_attr_min_[table][column] = value;
  }
  float GetTableAttrMin(const std::string &table,
                        const std::string &column) const {
    return Get(table_attr_min_, table, column);
  }

  void SetTableAttrSum(const std::string &table, const std::string &column,
                       float value) {
    table_attr_sum_[table][column] = value;
  }
  float GetTableAttrSum(const std::string &table,
                        const std::string &column) const {
    return Get(table_attr_sum_, table, column);
  }

  void SetTableAttrDistinct(const std::string &table, const std::string &column,
                            int value) {
    table_attr_distinct_[table][column] = value;
  }
  int GetTableAttrDistinct(const std::string &table,
                           const std::string &column) const {
    return Get(table_attr_distinct_, table, column);
  }

 private:
  template <typename T>
  static T Get(
      const std::unordered_map<std::string, std::unordered_map<std::string, T>>
          &values,
      const std::string &table, const std::string &column) {
    auto table_it = values.find(table);
    if (table_it == values.end()) return static_cast<T>(-1);
    auto column_it = table_it->second.find(column);
    return column_it == table_it->second.end() ? static_cast<T>(-1)
                                               : column_it->second;
  }

  std::unordered_map<std::string, int> table_count_;
  std::unordered_map<std::string, std::unordered_map<std::string, float>>
      table_attr_max_;
  std::unordered_map<std::string, std::unordered_map<std::string, float>>
      table_attr_min_;
  std::unordered_map<std::string, std::unordered_map<std::string, float>>
      table_attr_sum_;
  std::unordered_map<std::string, std::unordered_map<std::string, int>>
      table_attr_distinct_;
};

}  // namespace easydb
