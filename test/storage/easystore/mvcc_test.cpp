#include <filesystem>
#include <thread>

#include "gtest/gtest.h"
#include "storage/easystore/easystore.h"
#include "storage/easystore/recovery/log_manager.h"
#include "storage/easystore/transaction/transaction_manager.h"

namespace easydb {

TEST(MvccIntegrationTest, RepeatableReadScanFollowsUndoChain) {
  const std::filesystem::path db_dir = "mvcc_scan_test_root";
  std::filesystem::remove_all(db_dir);

  EasyStore store(db_dir.string(), false);
  store.CreateDB(db_dir.string());
  store.OpenDB(db_dir.string());

  const std::vector<ColDef> columns = {
      {"id", TYPE_INT, sizeof(int32_t)},
      {"value", TYPE_INT, sizeof(int32_t)},
  };

  auto *txn_manager = store.GetTransactionManager();
  auto *log_manager = store.GetLogManager();
  auto *lock_manager = store.GetLockManager();

  auto *ddl_txn = txn_manager->Begin(nullptr, log_manager);
  Context ddl_context(lock_manager, log_manager, ddl_txn);
  store.CreateTable("users", columns, &ddl_context);
  txn_manager->Commit(ddl_txn, log_manager);

  const auto &schema = store.GetTableMeta("users").schema;
  const Tuple old_tuple(
      {Value(TYPE_INT, static_cast<int32_t>(1)),
       Value(TYPE_INT, static_cast<int32_t>(10))},
      &schema);
  const Tuple new_tuple(
      {Value(TYPE_INT, static_cast<int32_t>(1)),
       Value(TYPE_INT, static_cast<int32_t>(20))},
      &schema);

  auto *insert_txn = txn_manager->Begin(nullptr, log_manager);
  Context insert_context(lock_manager, log_manager, insert_txn);
  ASSERT_TRUE(store.InsertTuple("users", old_tuple, &insert_context));
  txn_manager->Commit(insert_txn, log_manager);

  // The reader creates its RR snapshot while the old version is committed.
  auto *reader_txn = txn_manager->Begin(nullptr, log_manager);
  Context reader_context(lock_manager, log_manager, reader_txn);
  auto reader_scan = store.ScanTable("users", &reader_context);
  ASSERT_FALSE(reader_scan->IsEnd());
  ASSERT_EQ(reader_scan->GetTuple()->GetValue(&schema, 1).GetAs<int32_t>(),
            10);

  auto *writer_txn = txn_manager->Begin(nullptr, log_manager);
  Context writer_context(lock_manager, log_manager, writer_txn);
  const auto old_meta =
      store.ReadTupleMeta("users", old_tuple, &writer_context).value();
  UndoLogRecord undo(LogKind::UNDO, UPDATE, writer_txn->GetTransactionId(),
                     writer_txn->GetPrevLsn(), "users", old_tuple, old_tuple,
                     new_tuple, old_meta, TupleMeta{});
  const auto undo_lsn = log_manager->add_log_to_buffer(&undo);
  writer_context.current_undo_lsn_ = undo_lsn;
  ASSERT_TRUE(store.UpdateTuple("users", old_tuple, new_tuple,
                                &writer_context));
  txn_manager->Commit(writer_txn, log_manager);

  // The reader's original snapshot must still return value=10, even though
  // the physical clustered leaf now contains value=20.
  reader_scan = store.ScanTable("users", &reader_context);
  ASSERT_FALSE(reader_scan->IsEnd());
  EXPECT_EQ(reader_scan->GetTuple()->GetValue(&schema, 1).GetAs<int32_t>(),
            10);

  // A locking read must bypass the RR snapshot and use the newest version.
  reader_context.locking_read_ = true;
  auto locking_scan = store.ScanTable("users", &reader_context);
  ASSERT_FALSE(locking_scan->IsEnd());
  EXPECT_EQ(locking_scan->GetTuple()->GetValue(&schema, 1).GetAs<int32_t>(),
            20);
  reader_context.locking_read_ = false;

  txn_manager->Abort(reader_txn, log_manager);
  txn_manager->ReleaseTxnOfThread(std::this_thread::get_id());
  store.CloseDB();
  std::filesystem::remove_all(db_dir);
}

}  // namespace easydb
