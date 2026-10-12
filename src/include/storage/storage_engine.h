#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common/context.h"
#include "storage/easystore/table/tuple.h"
#include "storage/scan.h"
#include "storage/storage_defs.h"
#include "system/catalog_defs.h"
#include "system/sm_defs.h"
#include "system/sm_meta.h"

namespace easydb {

/** Logical storage contract used by the query and execution layers. */
class StorageEngine {
 public:
  virtual ~StorageEngine() = default;

  virtual bool HasTable(const std::string &table_name) const = 0;
  virtual TabMeta &GetTableMeta(const std::string &table_name) = 0;
  virtual int GetTableLockId(const std::string &table_name) const = 0;

  virtual std::unique_ptr<TableScan> ScanTable(const std::string &table_name,
                                               Context *context) = 0;
  virtual std::unique_ptr<IndexScan> ScanIndex(
      const std::string &table_name, const std::vector<std::string> &columns,
      const std::vector<char> &lower, const std::vector<char> &upper,
      Context *context) = 0;
  virtual std::unique_ptr<Tuple> ReadTuple(const std::string &table_name,
                                           const Tuple &key,
                                           Context *context) = 0;
  virtual bool InsertTuple(const std::string &table_name, const Tuple &tuple,
                           Context *context) = 0;
  virtual bool DeleteTuple(const std::string &table_name, const Tuple &key,
                           Context *context) = 0;
  virtual bool UpdateTuple(const std::string &table_name, const Tuple &key,
                           const Tuple &tuple, Context *context) = 0;

  virtual void CreateTable(const std::string &table_name,
                           const std::vector<ColDef> &columns,
                           Context *context) = 0;
  virtual void DropTable(const std::string &table_name, Context *context) = 0;
  virtual void CreateIndex(const std::string &table_name,
                           const std::vector<std::string> &columns,
                           Context *context) = 0;
  virtual void DropIndex(const std::string &table_name,
                         const std::vector<std::string> &columns,
                         Context *context) = 0;

  virtual void ShowTables(Context *context) = 0;
  virtual void ShowIndex(const std::string &table_name, Context *context) = 0;
  virtual void DescTable(const std::string &table_name, Context *context) = 0;

  virtual void AsyncLoadData(const std::string &file_name,
                             const std::string &table_name,
                             Context *context) = 0;
  virtual int GetLoadStatus() const = 0;
  virtual void AsyncLoadDataFinish() = 0;

  virtual void SetEnableOutput(bool enabled) = 0;
  virtual bool IsEnableOutput() const = 0;

  virtual int GetTableCount(const std::string &table_name) const = 0;
  virtual int GetTableAttrDistinct(const std::string &table_name,
                                   const std::string &column_name) const = 0;
  virtual float GetTableAttrSum(const std::string &table_name,
                                const std::string &column_name) const = 0;
  virtual float GetTableAttrMax(const std::string &table_name,
                                const std::string &column_name) const = 0;
  virtual float GetTableAttrMin(const std::string &table_name,
                                const std::string &column_name) const = 0;
  virtual void UpdateTableCount(const std::string &table_name, int delta) = 0;
};

}  // namespace easydb
