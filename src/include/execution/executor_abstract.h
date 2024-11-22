/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "catalog/schema.h"
#include "catalog/column.h"
#include "common/common.h"
#include "common/context.h"
#include "common/errors.h"
#include "defs.h"
#include "storage/index/ix_manager.h"
#include "storage/index/ix_scan.h"
#include "system/sm_defs.h"
#include "system/sm_meta.h"
#include "type/type_id.h"

namespace easydb {

class AbstractExecutor {
 public:
  RID _abstract_rid;

  Context *context_;

  AbstractExecutor() : _abstract_rid(), context_(nullptr) {}
  
  AbstractExecutor(std::shared_ptr<void> &ptr) {
    auto derived_ptr = std::static_pointer_cast<AbstractExecutor>(ptr);
    // 使用 derived_ptr 初始化 AbstractExecutor 的成员变量
    _abstract_rid = derived_ptr->_abstract_rid;
    context_ = derived_ptr->context_;
  }

  virtual ~AbstractExecutor() = default;

  virtual size_t tupleLen() const { return 0; };

  virtual const std::vector<ColMeta> &cols() const {
    std::vector<ColMeta> *_cols = nullptr;
    return *_cols;
  };

  virtual const Schema &schema() const {
    Schema tp;
    return tp;
  };

  virtual std::string getType() { return "AbstractExecutor"; };

  virtual void beginTuple() {};

  virtual void nextTuple() {};

  virtual bool IsEnd() const { return true; };

  virtual std::string getTabName() const {}

  virtual RID &rid() = 0;

  virtual std::unique_ptr<Tuple> Next() = 0;

  // virtual std::unique_ptr<RmRecord> Next() = 0;

  virtual ColMeta get_col_offset(const TabCol &target) { return ColMeta(); };
  virtual Column get_colu_offset(const TabCol &target) { return Column(); };

  std::vector<ColMeta>::const_iterator get_col(const std::vector<ColMeta> &rec_cols, const TabCol &target) {
    auto pos = std::find_if(rec_cols.begin(), rec_cols.end(), [&](const ColMeta &col) {
      return col.tab_name == target.tab_name && col.name == target.col_name;
    });
    if (pos == rec_cols.end()) {
      throw ColumnNotFoundError(target.tab_name + '.' + target.col_name);
    }
    return pos;
  }

  std::vector<ColMeta>::const_iterator get_col(const std::vector<ColMeta> &rec_cols, const std::string &target_tab_name,
                                               const std::string &target_col_name) {
    auto pos = std::find_if(rec_cols.begin(), rec_cols.end(), [&](const ColMeta &col) {
      return col.tab_name == target_tab_name && col.name == target_col_name;
    });
    if (pos == rec_cols.end()) {
      throw ColumnNotFoundError(target_tab_name + '.' + target_col_name);
    }
    return pos;
  }

  std::vector<Column>::const_iterator get_col(const std::vector<Column> &rec_cols, const std::string &target_tab_name,
                                               const std::string &target_col_name) {
    auto pos = std::find_if(rec_cols.begin(), rec_cols.end(), [&](const Column &col) {
      return col.GetTabName() == target_tab_name && col.GetName() == target_col_name;
    });
    if (pos == rec_cols.end()) {
      throw ColumnNotFoundError(target_tab_name + '.' + target_col_name);
    }
    return pos;
  }
};

}  // namespace easydb
