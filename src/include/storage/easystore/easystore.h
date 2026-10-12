#pragma once

#include <memory>
#include <string>

#include "storage/storage_engine.h"

namespace easydb {

class DiskManager;
class BufferPoolManager;
class RmManager;
class IxManager;
class LockManager;
class TransactionManager;
class LogManager;
class RecoveryManager;
class CatalogManager;
class TableManager;
class IndexManager;
class StatisticsManager;

/** Adapter exposing the current EasyDB storage implementation. */
class EasyStore final : public StorageEngine {
 public:
  explicit EasyStore(const std::string &db_name, bool enable_output = true);
  ~EasyStore() override;

  EasyStore(const EasyStore &) = delete;
  EasyStore &operator=(const EasyStore &) = delete;

  bool IsDir(const std::string &db_name) const;
  void CreateDB(const std::string &db_name);
  void OpenDB(const std::string &db_name);
  void CloseDB();

  DiskManager *GetDiskManager() const;
  BufferPoolManager *GetBufferPoolManager() const;
  RmManager *GetRecordManager() const;
  IxManager *GetIndexManager() const;
  LockManager *GetLockManager() const;
  TransactionManager *GetTransactionManager() const;
  LogManager *GetLogManager() const;
  RecoveryManager *GetRecoveryManager() const;
  CatalogManager *GetCatalogManager() const;
  TableManager *GetTableManager() const;
  IndexManager *GetIndexComponent() const;
  StatisticsManager *GetStatisticsManager() const;

  bool HasTable(const std::string &table_name) const override;
  TabMeta &GetTableMeta(const std::string &table_name) override;
  int GetTableLockId(const std::string &table_name) const override;
  std::unique_ptr<TableScan> ScanTable(const std::string &, Context *) override;
  std::unique_ptr<IndexScan> ScanIndex(const std::string &,
                                       const std::vector<std::string> &,
                                       const std::vector<char> &,
                                       const std::vector<char> &,
                                       Context *) override;
  std::unique_ptr<Tuple> ReadTuple(const std::string &, const RID &,
                                   Context *) override;
  RID InsertTuple(const std::string &, const Tuple &, Context *) override;
  bool DeleteTuple(const std::string &, const RID &, Context *) override;
  bool UpdateTuple(const std::string &, const RID &, const Tuple &,
                   Context *) override;
  bool InsertIndexEntry(const std::string &, const std::vector<std::string> &,
                        const std::vector<char> &, const RID &,
                        Context *) override;
  void DeleteIndexEntry(const std::string &, const std::vector<std::string> &,
                        const std::vector<char> &, Context *) override;
  void WaitForIndexGap(const std::string &, const std::vector<std::string> &,
                       const std::vector<char> &, Context *) override;

  void CreateTable(const std::string &, const std::vector<ColDef> &,
                   Context *) override;
  void DropTable(const std::string &, Context *) override;
  void CreateIndex(const std::string &, const std::vector<std::string> &,
                   Context *) override;
  void DropIndex(const std::string &, const std::vector<std::string> &,
                 Context *) override;
  void ShowTables(Context *) override;
  void ShowIndex(const std::string &, Context *) override;
  void DescTable(const std::string &, Context *) override;
  void AsyncLoadData(const std::string &, const std::string &,
                     Context *) override;
  int GetLoadStatus() const override;
  void AsyncLoadDataFinish() override;
  void SetEnableOutput(bool) override;
  bool IsEnableOutput() const override;
  int GetTableCount(const std::string &) const override;
  int GetTableAttrDistinct(const std::string &,
                           const std::string &) const override;
  float GetTableAttrSum(const std::string &,
                        const std::string &) const override;
  float GetTableAttrMax(const std::string &,
                        const std::string &) const override;
  float GetTableAttrMin(const std::string &,
                        const std::string &) const override;
  void UpdateTableCount(const std::string &, int) override;

 private:
  class Runtime;
  std::unique_ptr<Runtime> runtime_;
  bool enable_output_;
};

}  // namespace easydb
