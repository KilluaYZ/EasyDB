/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "system/sm_manager.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fstream>
#include <string>
#include "catalog/schema.h"
#include "common/errors.h"
#include "common/exception.h"
#include "record/record_printer.h"
#include "record/rm_defs.h"
#include "record/rm_scan.h"
#include "storage/index/ix_defs.h"
#include "system/sm_meta.h"
#include "type/type_id.h"

namespace easydb {

/**
 * @description: 判断是否为一个文件夹
 * @return {bool} 返回是否为一个文件夹
 * @param {string&} db_name 数据库文件名称，与文件夹同名
 */
bool SmManager::IsDir(const std::string &db_name) {
  struct stat st;
  return stat(db_name.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

/**
 * @description: 创建数据库，所有的数据库相关文件都放在数据库同名文件夹下
 * @param {string&} db_name 数据库名称
 */
void SmManager::CreateDB(const std::string &db_name) {
  if (IsDir(db_name)) {
    throw DatabaseExistsError(db_name);
  }
  // 为数据库创建一个子目录
  std::string cmd = "mkdir " + db_name;
  if (system(cmd.c_str()) < 0) {  // 创建一个名为db_name的目录
    throw UnixError();
  }
  if (chdir(db_name.c_str()) < 0) {  // 进入名为db_name的目录
    throw UnixError();
  }
  // 创建系统目录
  DbMeta *new_db = new DbMeta();
  new_db->name_ = db_name;

  // 注意，此处ofstream会在当前目录创建(如果没有此文件先创建)和打开一个名为DB_META_NAME的文件
  std::ofstream ofs(DB_META_NAME);

  // 将new_db中的信息，按照定义好的operator<<操作符，写入到ofs打开的DB_META_NAME文件中
  ofs << *new_db;  // 注意：此处重载了操作符<<

  delete new_db;

  // 创建日志文件
  disk_manager_->CreateFile(LOG_FILE_NAME);

  // 回到根目录
  if (chdir("..") < 0) {
    throw UnixError();
  }
}

/**
 * @description: 删除数据库，同时需要清空相关文件以及数据库同名文件夹
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::DropDB(const std::string &db_name) {
  if (!IsDir(db_name)) {
    throw DatabaseNotFoundError(db_name);
  }
  std::string cmd = "rm -r " + db_name;
  if (system(cmd.c_str()) < 0) {
    throw UnixError();
  }
}

/**
 * @description: 打开数据库，找到数据库对应的文件夹，并加载数据库元数据和相关文件
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::OpenDB(const std::string &db_name) {
  if (!IsDir(db_name)) {
    CreateDB(db_name);
  }

  if (chdir(db_name.c_str()) < 0) {
    throw UnixError();
  }
  // load info into db_, fhs_, ihs_
  // db_ stored in file DB_META_NAME("db.meta")
  std::ifstream ifs(DB_META_NAME);
  ifs >> db_;

  // fhs_ : contains of several <filename of per table, record file ptr> items
  for (auto table : db_.tabs_) {
    // debug
    std::cout << "open table name: " << table.first << std::endl;
    // the name of record file is table name, index file is table_name.index
    fhs_.emplace(table.first, rm_manager_->OpenFile(table.first));
    if (ix_manager_->Exists(table.first, db_.tabs_[table.first].cols)) {
      ihs_.emplace(table.first, ix_manager_->OpenIndex(table.first, table.second.cols));
    }
  }

  // stay in database dir
}

/**
 * @description: 把数据库相关的元数据刷入磁盘中
 */
void SmManager::FlushMeta() {
  // 默认清空文件
  std::ofstream ofs(DB_META_NAME);
  ofs << db_;
}

/**
 * @description: 关闭数据库并把数据落盘
 */
void SmManager::CloseDB() {
  for (auto table : db_.tabs_) {
    rm_manager_->CloseFile(fhs_[table.first].get());
    if (ix_manager_->Exists(table.first, db_.tabs_[table.first].cols)) {
      ix_manager_->CloseIndex(ihs_[table.first].get());
    }
  }

  // return to father directory
  if (chdir("..") < 0) {
    throw UnixError();
  }
}

/**
 * @description: 显示所有的表,通过测试需要将其结果写入到output.txt,详情看题目文档
 * @param {Context*} context
 */
void SmManager::ShowTables(Context *context) {
  std::fstream outfile;
  if (enable_output_) {
    outfile.open("output.txt", std::ios::out | std::ios::app);
    outfile << "| Tables |\n";
  }
  RecordPrinter printer(1);
  printer.print_separator(context);
  printer.print_record({"Tables"}, context);
  printer.print_separator(context);
  for (auto &entry : db_.tabs_) {
    auto &tab = entry.second;
    printer.print_record({tab.name}, context);
    if (enable_output_) {
      outfile << "| " << tab.name << " |\n";
    }
  }
  printer.print_separator(context);
  outfile.close();
}

/**
 * @description: 显示表的元数据
 * @param {string&} tab_name 表名称
 * @param {Context*} context
 */
void SmManager::DescTable(const std::string &tab_name, Context *context) {
  TabMeta &tab = db_.get_table(tab_name);

  std::vector<std::string> captions = {"Field", "Type", "Index"};
  RecordPrinter printer(captions.size());
  // Print header
  printer.print_separator(context);
  printer.print_record(captions, context);
  printer.print_separator(context);
  // Print fields
  for (auto &col : tab.cols) {
    std::vector<std::string> field_info = {col.name, coltype2str(col.type), col.index ? "YES" : "NO"};
    printer.print_record(field_info, context);
  }
  // Print footer
  printer.print_separator(context);
}

/**
 * @description: 创建表
 * @param {string&} tab_name 表的名称
 * @param {vector<ColDef>&} col_defs 表的字段
 * @param {Context*} context
 */
void SmManager::CreateTable(const std::string &tab_name, const std::vector<ColDef> &col_defs, Context *context) {
  if (db_.is_table(tab_name)) {
    throw TableExistsError(tab_name);
  }
  // Create table meta
  int curr_offset = 0;
  TabMeta tab;
  tab.name = tab_name;
  std::vector<Column> columns;
  for (auto &col_def : col_defs) {
    ColMeta col(tab_name, col_def.name, col_def.type, col_def.len, curr_offset, false);
    curr_offset += col_def.len;
    tab.cols.push_back(col);

    Column tmp_col;
    TypeId type = col_def.type;
    switch (type) {
      case TypeId::TYPE_INT:
      case TypeId::TYPE_LONG:
      case TypeId::TYPE_FLOAT:
      case TypeId::TYPE_DOUBLE:
        tmp_col = Column(col_def.name, type);
        break;
      case TypeId::TYPE_CHAR:
      case TypeId::TYPE_VARCHAR:
        tmp_col = Column(col_def.name, type, col_def.len);
        break;
      default:
        throw Exception("unsupported type\n");
    }
    tmp_col.SetTabName(tab_name);
    columns.emplace_back(tmp_col);
  }
  Schema schema(columns);
  tab.schema = schema;

  // Create & open record file
  int record_size = curr_offset;  // record_size就是col meta所占的大小（表的元数据也是以记录的形式进行存储的）
  rm_manager_->CreateFile(tab_name, record_size);

  db_.tabs_[tab_name] = tab;
  // fhs_[tab_name] = rm_manager_->open_file(tab_name);
  fhs_.emplace(tab_name, rm_manager_->OpenFile(tab_name));

  // // lock manager
  // if (context != nullptr) {
  //   // context->lock_mgr_->lock_exclusive_on_table(context->txn_, fhs_[tab_name]->GetFd());
  // }

  FlushMeta();
}

/**
 * @description: 删除表
 * @param {string&} tab_name 表的名称
 * @param {Context*} context
 */
void SmManager::DropTable(const std::string &tab_name, Context *context) {
  if (!db_.is_table(tab_name)) {
    throw TableNotFoundError(tab_name);
  }

  // // lock manager
  // if (context != nullptr) {
  //   context->lock_mgr_->lock_exclusive_on_table(context->txn_, fhs_[tab_name]->GetFd());
  // }

  // remove record file and index file(if exist)
  TabMeta &tab = db_.get_table(tab_name);
  for (auto &index : tab.indexes) {
    DropIndex(tab_name, index.cols, context);
  }
  // if(ix_manager_->exists(tab_name, db_.tabs_[tab_name].cols)){
  //     drop_index(tab_name, db_.tabs_[tab_name].cols, context);
  // }
  // delete record page in buffer

  rm_manager_->CloseFile(fhs_[tab_name].get());
  buffer_pool_manager_->RemoveAllPages(fhs_[tab_name]->GetFd());
  rm_manager_->DestoryFile(tab_name);
  fhs_.erase(tab_name);
  db_.tabs_.erase(tab_name);
  FlushMeta();
}

/**
 * @description: 显示索引
 * @param {string&} tab_name 表名称
 * @param {Context*} context
 */
void SmManager::ShowIndex(const std::string &tab_name, Context *context) {
  TabMeta &tab = db_.get_table(tab_name);
  std::fstream outfile;
  outfile.open("output.txt", std::ios::out | std::ios::app);
  RecordPrinter printer(3);
  for (auto &index : tab.indexes) {
    std::string index_name = "(" + index.cols[0].name;
    for (int i = 1; i < index.col_num; ++i) {
      index_name += "," + index.cols[i].name;
    }
    index_name += ")";
    // | warehouse | unique | (id,name) |
    printer.print_record({tab_name, "unique", index_name}, context);
    outfile << "| " << tab_name << " | unique | " << index_name << " |\n";
  }
  // printer.print_separator(context);
  outfile.close();
}

/**
 * @description: 创建索引
 * @param {string&} tab_name 表的名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */
void SmManager::CreateIndex(const std::string &tab_name, const std::vector<std::string> &col_names, Context *context) {
  // check if tab exists
  if (!db_.is_table(tab_name)) {
    throw TableNotFoundError(tab_name);
  }

  // // lock manager
  // if (context != nullptr) {
  //   // context->lock_mgr_->lock_shared_on_table(context->txn_, fhs_[tab_name]->GetFd());
  // }

  // get colMeta
  std::vector<ColMeta> index_cols;
  std::vector<uint32_t> key_ids;
  int col_tot_len = 0;
  TabMeta &tab_meta = db_.get_table(tab_name);
  if (tab_meta.is_index(col_names)) {
    throw IndexExistsError(tab_name, col_names);
  }
  for (auto &col_name : col_names) {
    ColMeta colMetaTp = *tab_meta.get_col(col_name);
    index_cols.emplace_back(colMetaTp);
    key_ids.emplace_back(tab_meta.GetColId(col_name));
    col_tot_len += colMetaTp.len;
  }

  // construct index_meta
  IndexMeta index_meta = {.tab_name = tab_name,
                          .col_tot_len = col_tot_len,
                          .col_num = static_cast<int>(col_names.size()),
                          .cols = index_cols,
                          .col_ids = key_ids};
  auto key_schema = Schema::CopySchema(&tab_meta.schema, key_ids);

  // create index
  ix_manager_->CreateIndex(tab_name, index_cols);

  // insert the records that already in table into newly constructed index
  auto Iih = ix_manager_->OpenIndex(tab_name, index_cols);
  auto Rfh = fhs_.at(tab_name).get();
  RmScan rmScan(Rfh);

  while (!rmScan.IsEnd()) {
    auto rid = rmScan.GetRid();
    // auto rec = Rfh->GetRecord(rid, context);
    auto tuple = Rfh->GetTupleValue(rid);
    auto key_tuple = Rfh->GetKeyTuple(tab_meta.schema, key_schema, key_ids, rid);
    // construct key
    char *key = new char[index_meta.col_tot_len];
    int offset = 0;
    for (int i = 0; i < index_meta.col_num; ++i) {
      // memcpy(key + offset, rec->data + index_meta.cols[i].offset, index_meta.cols[i].len);
      auto len = index_meta.cols[i].len;
      auto val = key_tuple.GetValue(&key_schema, i);
      // memcpy(key + offset, val.GetData(), val.GetStorageSize());
      if (val.GetTypeId() == TYPE_CHAR || val.GetTypeId() == TYPE_VARCHAR) {
        memcpy(key + offset, val.GetData(), index_meta.cols[i].len);
      } else {
        assert(uint32_t(len) == Type(val.GetTypeId()).GetTypeSize(val.GetTypeId()));
        val.SerializeTo(key + offset);
      }
      offset += index_meta.cols[i].len;
    }
    // // print key
    // std::cout << "key: " << std::string(key, index_meta.col_tot_len) << std::endl;
    // for (int i = 0; i < 4; ++i) {
    //   std::cout << "0x" << std::hex << std::setw(2) << std::setfill('0') << (int)(unsigned char)key[i] << " ";
    // }
    // std::cout << std::endl;
    // std::cout << "key(int): " << std::to_string(*(int *)key) << std::endl;
    if (context != nullptr) {
      // Iih->InsertEntry(key, rid, context->txn_);
      Iih->InsertEntry(key, rid);
    } else {
      // Iih->InsertEntry(key, rid, nullptr);
      Iih->InsertEntry(key, rid);
    }
    delete[] key;
    rmScan.Next();
  }

  // update ihs and corresponding table index meta data
  auto index_name = ix_manager_->GetIndexName(tab_name, col_names);
  tab_meta.indexes.emplace_back(index_meta);
  ihs_.emplace(index_name, std::move(Iih));
  FlushMeta();
}

/**
 * @description: 删除索引
 * @param {string&} tab_name 表名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */
void SmManager::DropIndex(const std::string &tab_name, const std::vector<std::string> &col_names, Context *context) {
  if (!ix_manager_->Exists(tab_name, col_names)) {
    throw IndexEntryNotFoundError();
  }

  // // lock manager
  // if (context != nullptr) {
  //   context->lock_mgr_->lock_shared_on_table(context->txn_, fhs_[tab_name]->GetFd());
  // }

  auto index_name = ix_manager_->GetIndexName(tab_name, col_names);
  // close index and remove from ihs_
  if (ihs_.find(index_name) != ihs_.end()) {
    auto Iih = ihs_.at(index_name).get();
    ix_manager_->CloseIndex(Iih);
    // To ensure data consistency, remove all pages in buffer pool related to this index
    buffer_pool_manager_->RemoveAllPages(Iih->GetFd());
    // DbMeta
    ihs_.erase(index_name);
  }

  // delete coresponding metadata
  // table meta
  TabMeta &tab_meta = db_.get_table(tab_name);
  tab_meta.indexes.erase(tab_meta.get_index_meta(col_names));
  // delete index in disk
  ix_manager_->DestroyIndex(tab_name, col_names);
  FlushMeta();
}

/**
 * @description: 删除索引
 * @param {string&} tab_name 表名称
 * @param {vector<ColMeta>&} 索引包含的字段元数据
 * @param {Context*} context
 */
void SmManager::DropIndex(const std::string &tab_name, const std::vector<ColMeta> &cols, Context *context) {
  // fetch col_names from col metadata.
  std::vector<std::string> col_names;
  for (auto &col : cols) {
    col_names.emplace_back(col.name);
  }
  // involke drop_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context) to do
  // the real work
  DropIndex(tab_name, col_names, context);
}

/**
 * Rolls back a write operation based on the type of write record.
 *
 * @param write_record The write record containing information about the write operation.
 * @param context The context object for the current transaction.
 * @throws InternalError if the write type is invalid.
 */
void SmManager::Rollback(WriteRecord *write_record, Context *context) {
  switch (write_record->GetWriteType()) {
    case WType::INSERT_TUPLE:
      RollbackInsert(write_record->GetTableName(), write_record->GetRid(), context);
      break;
    case WType::DELETE_TUPLE:
      RollbackDelete(write_record->GetTableName(), write_record->GetRid(), write_record->GetRecord(), context);
      break;
    case WType::UPDATE_TUPLE:
      RollbackUpdate(write_record->GetTableName(), write_record->GetRid(), write_record->GetRecord(), context);
      break;
    default:
      throw InternalError("SmManager::rollback: Invalid write type");
  }
}

/**
 * Rolls back an insert operation by removing the record from the record file and
 * deleting the corresponding index entries.
 *
 * @param table_name The name of the table where the record was inserted.
 * @param rid The Rid of the inserted record.
 * @param context The context object for the current transaction.
 * @todo DeleteLogRecord
 */
void SmManager::RollbackInsert(const std::string &table_name, RID &rid, Context *context) {
  // auto fh = fhs_.at(table_name).get();
  // auto record = fh->GetRecord(rid, context);
  // // Delete from index
  // for (auto &index : db_.get_table(table_name).indexes) {
  //   auto index_name = ix_manager_->GetIndexName(table_name, index.cols);
  //   auto Iih = ihs_.at(index_name).get();
  //   char *key = new char[index.col_tot_len];
  //   int offset = 0;
  //   for (int i = 0; i < index.col_num; ++i) {
  //     memcpy(key + offset, record->data + index.cols[i].offset, index.cols[i].len);
  //     offset += index.cols[i].len;
  //   }
  //   Iih->DeleteEntry(key, context->txn_);
  //   delete[] key;
  // }
  // // Delete from table
  // fh->DeleteTuple(rid, context);

  // // TODO: DeleteLogRecord(CLR)
  // DeleteLogRecord del_log_rec(context->txn_->get_transaction_id(), *record, rid, table_name);
  // del_log_rec.prev_lsn_ = context->txn_->get_prev_lsn();
  // lsn_t lsn = context->log_mgr_->add_log_to_buffer(&del_log_rec);
  // context->txn_->set_prev_lsn(lsn);
  // // set lsn in page header
  // fh->set_page_lsn(rid.page_no, lsn);

  // // Set lsn(abort lsn) in page header(not CLR lsn)
  // fh->SetPageLSN(rid.GetPageId(), context->txn_->get_prev_lsn());
}

/**
 * Rolls back a delete operation by inserting the deleted record back into the record file and
 * re-creating the corresponding index entries.
 *
 * @param table_name The name of the table where the record was deleted.
 * @param rid The Rid of the deleted record.
 * @param record The deleted record.
 * @param context The context object for the current transaction.
 * @todo InsertLogRecord
 */
void SmManager::RollbackDelete(const std::string &table_name, RID &rid, RmRecord &record, Context *context) {
  // // insert the record back into the record file
  // auto fh = fhs_.at(table_name).get();
  // fh->InsertTuple(rid, record.data);

  // // // TODO: InsertLogRecord(CLR)
  // // InsertLogRecord insert_log_rec(context->txn_->get_transaction_id(), record, rid, table_name);
  // // insert_log_rec.prev_lsn_ = context->txn_->get_prev_lsn();
  // // lsn_t lsn = context->log_mgr_->add_log_to_buffer(&insert_log_rec);
  // // context->txn_->set_prev_lsn(lsn);
  // // // set lsn in page header
  // // fh->set_page_lsn(rid.page_no, lsn);

  // // Set lsn(abort lsn) in page header(not CLR lsn)
  // fh->SetPageLSN(rid.GetPageId(), context->txn_->get_prev_lsn());

  // // insert the index entry back into the index file
  // for (auto &index : db_.get_table(table_name).indexes) {
  //   auto index_name = ix_manager_->GetIndexName(table_name, index.cols);
  //   auto Iih = ihs_.at(index_name).get();
  //   char *key = new char[index.col_tot_len];
  //   int offset = 0;
  //   for (int i = 0; i < index.col_num; ++i) {
  //     memcpy(key + offset, record.data + index.cols[i].offset, index.cols[i].len);
  //     offset += index.cols[i].len;
  //   }
  //   auto is_insert = Iih->InsertEntry(key, rid, context->txn_);
  //   if (is_insert == -1) {
  //     // should not happen because this is logged
  //     throw InternalError("SmManager::rollback_delete: index entry not found");
  //   }
  //   delete[] key;
  // }
}

/**
 * Rolls back an update operation by reverting the updated record to its old value and
 * updating the corresponding index entries.
 *
 * @param table_name The name of the table where the record was updated.
 * @param rid The Rid of the updated record.
 * @param record The updated record.
 * @param context The context object for the current transaction.
 * @todo UpdateLogRecord
 */
void SmManager::RollbackUpdate(const std::string &table_name, RID &rid, RmRecord &record, Context *context) {
  // auto fh = fhs_.at(table_name).get();
  // // get the new record
  // auto new_record = fh->get_record(rid, context);

  // // // TODO: UpdateLogRecord(CLR)
  // // // Log: before update value because the object of new_record(a ptr) will be changed in 'update_record'
  // // UpdateLogRecord update_log_rec(context->txn_->get_transaction_id(), *new_record, record, rid, table_name);

  // // update the record to the old record
  // fh->update_record(rid, record.data, context);

  // // // Log: after update
  // // update_log_rec.prev_lsn_ = context->txn_->get_prev_lsn();
  // // lsn_t lsn = context->log_mgr_->add_log_to_buffer(&update_log_rec);
  // // context->txn_->set_prev_lsn(lsn);
  // // // set lsn in page header
  // // fh->set_page_lsn(rid.page_no, lsn);

  // // Set lsn(abort lsn) in page header(not CLR lsn)
  // fh->SetPageLSN(rid.GetPageId(), context->txn_->get_prev_lsn());

  // // update the index entry in the index file
  // for (auto &index : db_.get_table(table_name).indexes) {
  //   auto index_name = ix_manager_->GetIndexName(table_name, index.cols);
  //   auto Iih = ihs_.at(index_name).get();
  //   char *old_key = new char[index.col_tot_len];
  //   char *new_key = new char[index.col_tot_len];
  //   int offset = 0;
  //   for (int i = 0; i < index.col_num; ++i) {
  //     memcpy(old_key + offset, record.data + index.cols[i].offset, index.cols[i].len);
  //     memcpy(new_key + offset, new_record->data + index.cols[i].offset, index.cols[i].len);
  //     offset += index.cols[i].len;
  //   }
  //   // check if the key is the same as before
  //   if (memcmp(old_key, new_key, index.col_tot_len) == 0) {
  //     delete[] old_key;
  //     delete[] new_key;
  //     continue;
  //   }
  //   // check if the new key duplicated
  //   auto is_insert = Iih->InsertEntry(old_key, rid, context->txn_);
  //   if (is_insert == -1) {
  //     // should not happen because this is logged
  //     throw InternalError("SmManager::rollback_update: index entry not found");
  //   }
  //   Iih->DeleteEntry(new_key, context->txn_);
  //   delete[] old_key;
  //   delete[] new_key;
  // }
}

/**
 * @description: split string by delimiter
 * @param s: input string
 * @param delimiter: delimiter
 * @param tokens: output tokens
 */
void SmManager::Split(const std::string &s, char delimiter, std::vector<std::string> &tokens) {
  std::string token;
  std::istringstream tokenStream(s);
  while (std::getline(tokenStream, token, delimiter)) {
    tokens.push_back(token);
  }
}

/**
 * @description: split string by delimiter
 * @param start: start pointer of the string
 * @param length: length of the string
 * @param delimiter: delimiter
 * @param tokens: output tokens
 */
void SmManager::Split(const char *start, size_t length, char delimiter, std::vector<std::string> &tokens) {
  const char *end = start + length;
  const char *token_start = start;

  while (token_start < end) {
    const char *token_end = std::find(token_start, end, delimiter);

    // Add the token to the vector
    tokens.emplace_back(token_start, token_end);

    // Move to the next token
    if (token_end == end) break;  // Reached the end
    token_start = token_end + 1;
  }
}

inline int ix_compare(const char *a, const char *b, const std::vector<ColMeta> cols) {
  int offset = 0;
  for (size_t i = 0; i < cols.size(); ++i) {
    int res = ix_compare(a + offset, b + offset, cols[i].type, cols[i].len);
    if (res != 0) return res;
    offset += cols[i].len;
  }
  return 0;
}

/**
 * @description: insert record into table
 * @param file_name
 * @param table_name
 * @param context
 * @note: this function will insert one record into table
 */
RID fh_insert(RmFileHandle *fh, std::vector<Value> &values, Schema *schema) {
  Tuple tuple{values, schema};
  auto rid = fh->InsertTuple(TupleMeta{0, false}, tuple);
  auto page_id = rid->GetPageId();
  auto slot_num = rid->GetSlotNum();
  // std::cout << "[TEST] insert rid: page id: " << page_id << " slot num: " << slot_num << std::endl;
  return {page_id, slot_num};
}

/**
 * @description: load data from csv file to table
 * @param file_name
 * @param table_name
 * @param context
 * @note: this function does not create table, just load data to existing table
 */
void SmManager::LoadData(const std::string &file_name, const std::string &table_name, Context *context) {
  // std::cout << "SmManager::load_data: load data from " << file_name << " to table " << table_name << std::endl;
  // 1. Get the table object
  // check if table exists
  if (!db_.is_table(table_name)) {
    throw TableNotFoundError(table_name);
  }
  auto &tab = db_.get_table(table_name);
  auto fh = fhs_.at(table_name).get();
  size_t col_size = tab.cols.size();

  // 2. Open file and create memory mapping
  int fd = open(file_name.c_str(), O_RDONLY);
  if (fd == -1) {
    close(fd);
    auto current = std::filesystem::current_path();
    auto err_msg =
        "SmManager::load_data: open file failed, please check file relative to current directory: " + current.string();
    throw Exception(err_msg);
  }
  size_t file_size = lseek(fd, 0, SEEK_END);
  char *data = (char *)mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
  if (data == MAP_FAILED) {
    close(fd);
    throw InternalError("SmManager::load_data: mmap failed");
  }

  // 3. Parse CSV header and validate
  char *line_start = data;
  char *line_end = strchr(line_start, '\n');
  if (line_end == nullptr) {
    munmap(data, file_size);
    close(fd);
    throw InternalError("SmManager::load_data: invalid CSV file");
  }
  // std::vector<std::string> header;
  // Split(line_start, line_end - line_start, '|', header);
  // // for (int i = 0; i < col_size; ++i) {
  // //   if (header[i] != tab.cols[i].name) {
  // //     munmap(data, file_size);
  // //     close(fd);
  // //     throw InternalError("SmManager::load_data: header not match table schema");
  // //   }
  // // }
  // line_start = line_end + 1;

  // 4. Parse data and batch insert into table
  int total_records = 0;
  int page_record_count = 0;
  // int record_size = fh->GetFileHdr().record_size;
  // int num_records_per_page = fh->get_file_hdr().num_records_per_page;
  // int page_size = record_size * num_records_per_page;
  // fh->GetFileHdr()
  // char *page_data = new char[page_size];
  // memset(page_data, 0, page_size);

  // Batch data for indexes(just primary key for now)
  std::vector<std::pair<std::string, RID>> index_entries;

  while (line_start < data + file_size) {
    line_end = strchr(line_start, '\n');
    // Last line without \n
    if (line_end == nullptr) {
      line_end = data + file_size;
    }

    // Directly parse the line and fill the corresponding slot in page_data
    std::vector<Value> values;
    char *token_start = line_start;
    for (int i = 0; i < col_size; ++i) {
      char *token_end = std::find(token_start, line_end, '|');
      // Calculate the destination address in the page buffer
      // char *dest = page_data + (page_record_count * record_size) + tab.cols[i].offset;
      int len = tab.cols[i].len;
      auto type = tab.cols[i].type;
      Value _tmp_val;
      switch (type) {
        case TYPE_INT: {
          _tmp_val = Value(type, std::stoi(std::string(token_start, token_end)));
          // *reinterpret_cast<int *>(dest) = std::stoi(std::string(token_start, token_end));
          break;
        }
        case TYPE_DOUBLE:
        case TYPE_FLOAT: {
          _tmp_val = Value(type, std::stof(std::string(token_start, token_end)));
          // *reinterpret_cast<float *>(dest) = std::stof(std::string(token_start, token_end));
          break;
        }
        case TYPE_CHAR:
        case TYPE_VARCHAR: {
          // int token_len = token_end - token_start;
          // if (token_len > len) {
          //   throw StringOverflowError();
          // }
          // memset(dest, 0, len);
          // memcpy(dest, token_start, token_len);
          std::string _tmp_str = std::string(token_start, token_end);
          _tmp_val = Value(type, _tmp_str);
          break;
        }
        default:
          throw InternalError("Unsupported data type.");
      }
      values.push_back(_tmp_val);
      // Move to the next token
      token_start = token_end + 1;
    }
    auto _tmp_rid = fh_insert(fh, values, &tab.schema);

    // // Extract the key for index
    // for (auto &index : tab.indexes) {
    //   char *key = new char[index.col_tot_len];
    //   int offset = 0;
    //   for (int i = 0; i < index.col_num; ++i) {
    //     auto val = index.
    //     // memcpy(key + offset, page_data + (page_record_count * record_size) + index.cols[i].offset,
    //     // index.cols[i].len); offset += index.cols[i].len;
    //     memcpy(key + offset, )
    //   }
    //   index_entries.emplace_back(std::string(key, index.col_tot_len),
    //                              RID{fh->get_file_hdr().num_pages, page_record_count});
    //   delete[] key;
    // }
    page_record_count++;
    line_start = line_end + 1;
    total_records++;

    // // If the page is full, insert the page and reset the counter
    // if (page_record_count == num_records_per_page) {
    //   fh->insert_page(page_data, page_record_count);
    //   page_record_count = 0;
    //   memset(page_data, 0, page_size);  // Reset the page buffer
    // }
  }

  // Insert any remaining records that did not fill a full page
  // if (page_record_count > 0) {
  //   fh->insert_page(page_data, page_record_count);
  // }

  SetTableCount(table_name, total_records);

  // // Sort the index entries and insert them into the index file
  // for (auto &index : tab.indexes) {
  //   // std::sort(index_entries.begin(), index_entries.end(), [&](const std::pair<std::string, Rid>& a, const
  //   // std::pair<std::string, Rid>& b) {
  //   //     return ix_compare(a.first.c_str(), b.first.c_str(), index.cols) < 0;
  //   // });
  //   // Insert the sorted entries into the B+ tree
  //   auto index_name = ix_manager_->GetIndexName(table_name, index.cols);
  //   auto ih = ihs_.at(index_name).get();
  //   ih->build_index_bottom_up(index_entries);
  // }
  buffer_pool_manager_->FlushAllDirtyPages();
  // buffer_pool_manager_->FlushAllPages(fd);
  // 5. Cleanup resources
  index_entries.clear();
  // delete[] page_data;
  munmap(data, file_size);
  close(fd);
}

void SmManager::AsyncLoadData(const std::string &file_name, const std::string &tab_name, Context *context) {
  // -1 for not load, 0 for loading, 1 for loaded
  load_ = 0;
  futures_.emplace_back(std::async(std::launch::async, [=]() { LoadData(file_name, tab_name, context); }));
}

void SmManager::AsyncLoadDataFinish() {
  for (auto &future : futures_) {
    future.get();
  }
  futures_.clear();
  load_ = 1;
}

}  // namespace easydb
