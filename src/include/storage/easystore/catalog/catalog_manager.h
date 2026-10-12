#pragma once

#include <fstream>
#include <string>

#include "common/config.h"
#include "system/catalog_defs.h"
#include "system/sm_defs.h"
#include "system/sm_meta.h"

namespace easydb {

// EasyStore-owned catalog state and persistence. The schema types remain
// shared with the query layer, while this component owns their on-disk state.
class CatalogManager {
 public:
  DbMeta db_;

  bool IsTable(const std::string &table_name) const {
    return db_.is_table(table_name);
  }

  TabMeta &GetTable(const std::string &table_name) {
    return db_.get_table(table_name);
  }

  const TabMeta &GetTable(const std::string &table_name) const {
    return db_.get_table(table_name);
  }

  void SetTable(const std::string &table_name, const TabMeta &table) {
    db_.SetTabMeta(table_name, table);
  }

  void RemoveTable(const std::string &table_name) {
    db_.RemoveTable(table_name);
  }

  const std::map<std::string, TabMeta> &Tables() const { return db_.tables(); }

  void Initialize(const std::string &db_name) {
    db_.Reset(db_name);
    Flush();
  }

  void Load() {
    std::ifstream input(DB_META_NAME);
    input >> db_;
  }

  void Flush() const {
    std::ofstream output(DB_META_NAME);
    output << db_;
  }
};

}  // namespace easydb
