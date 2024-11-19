#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <numeric>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>
#include "buffer/buffer_pool_manager.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/common.h"
#include "common/errors.h"
#include "common/exception.h"
#include "defs.h"
#include "gtest/gtest.h"
#include "record/rm_defs.h"
#include "record/rm_file_handle.h"
#include "record/rm_scan.h"
#include "storage/disk/disk_manager.h"
#include "storage/index/ix_defs.h"
#include "storage/index/ix_extendible_hash_index_handle.h"
#include "storage/index/ix_index_handle.h"
#include "storage/index/ix_manager.h"
#include "system/sm_meta.h"
#include "type/type_id.h"

namespace easydb {

const std::string TEST_DB_NAME = "test.easydb";              // 测试数据库名
const std::string TEST_TB_NAME = "test.table";               // 测试表名
const std::string TEST_FILE_NAME_CUSTOMER = "customer.tbl";  // 测试文件的名字
const std::string TEST_FILE_NAME_LINEITEM = "lineitem.tbl";  // 测试文件的名字
const std::string TEST_FILE_NAME_NATION = "nation.tbl";      // 测试文件的名字
const std::string TEST_FILE_NAME_ORDERS = "orders.tbl";      // 测试文件的名字
const std::string TEST_FILE_NAME_PART = "part.tbl";          // 测试文件的名字
const std::string TEST_FILE_NAME_PARTSUPP = "partsupp.tbl";  // 测试文件的名字
const std::string TEST_FILE_NAME_REGION = "region.tbl";      // 测试文件的名字
const std::string TEST_FILE_NAME_SUPPLIER = "supplier.tbl";  // 测试文件的名字
const std::string TEST_OUTPUT = "output.txt";                // 测试输出文件名

const int MAX_FILES = 32;
const int MAX_PAGES = 128;
const size_t TEST_BUFFER_POOL_SIZE = MAX_FILES * MAX_PAGES;
BufferPoolManager *bpm = nullptr;

class FileReader {
 protected:
  std::ifstream *infile = nullptr;
  std::string buffer;

 public:
  explicit FileReader(const std::string &file_path) {
    infile = new std::ifstream(file_path);
    if (!infile->is_open()) {
      throw FileNotFoundError("FileReader::File not found");
    }
  }

  ~FileReader() {
    if (infile && infile->is_open()) infile->close();
    delete infile;
  }

  std::basic_istream<char, std::char_traits<char>> &read_line() { return std::getline(*infile, buffer); }

  std::string get_buf() { return buffer; }

  std::vector<std::string> get_splited_buf(std::string split_str = "|") {
    std::string line_str = get_buf();
    if (line_str.empty()) {
      return std::vector<std::string>();
    }
    std::vector<std::string> res;
    size_t pos = 0;
    while ((pos = line_str.find(split_str, 0)) != std::string::npos) {
      res.push_back(line_str.substr(0, pos));
      line_str = line_str.substr(pos + split_str.size());
    }
    if (line_str.size() > 0) res.push_back(line_str);
    return res;
  }
};
const int MAX_RECORD_SIZE = 512;

void create_file(DiskManager *disk_manager, const std::string &filename, int record_size) {
  if (record_size < 1 || record_size > MAX_RECORD_SIZE) {
    throw InvalidRecordSizeError(record_size);
  }
  disk_manager->CreateFile(filename);
  int fd = disk_manager->OpenFile(filename);

  // 初始化file header
  RmFileHdr file_hdr{};
  file_hdr.Init();

  // 将file header写入磁盘文件（名为file name，文件描述符为fd）中的第0页
  // head page直接写入磁盘，没有经过缓冲区的NewPage，那么也就不需要FlushPage
  disk_manager->WritePage(fd, RM_FILE_HDR_PAGE, (char *)&file_hdr, sizeof(file_hdr));
  disk_manager->CloseFile(fd);
}

RID fh_insert(RmFileHandle *fh, std::vector<Value> &values, Schema *schema) {
  Tuple tuple{values, schema};
  auto rid = fh->InsertTuple(TupleMeta{0, false}, tuple);
  auto page_id = rid->GetPageId();
  auto slot_num = rid->GetSlotNum();
  std::cout << "[TEST] insert rid: page id: " << page_id << " slot num: " << slot_num << std::endl;
  return {page_id, slot_num};
}

void fh_get(RmFileHandle *fh, RID rid, Schema *schema) {
  auto [meta, tuple] = fh->GetTuple(rid);
  std::cout << "[TEST] get rid: page id: " << rid.GetPageId() << " slot num: " << rid.GetSlotNum() << std::endl;
  // std::cout << "[TEST] get tuple: " << tuple.ToString(schema) << std::endl;
  std::ofstream outfile(TEST_DB_NAME + "/" + TEST_OUTPUT, std::ios::app);
  outfile << tuple.ToString(schema) << std::endl;
  outfile.close();
}

class TB_Reader {
 protected:
  FileReader *file_reader = nullptr;
  Schema schema;
  std::string tab_name;
  std::vector<RID> rids;

 public:
  TB_Reader(std::string tab_name, std::string file_path, Schema schema) : schema(schema) {
    this->file_reader = new FileReader(file_path);
    this->tab_name = tab_name;
  }

  ~TB_Reader() { delete file_reader; }

  void parse_and_insert(RmFileHandle *fh_) {
    // DEBUG: 仅解析前n行数据
    // for (int i = 0; i < 3 && file_reader->read_line(); i++) {
    while (file_reader->read_line()) {
      auto splited_str_list = file_reader->get_splited_buf();
      std::vector<Value> values;
      Value _tmp_val(TypeId::TYPE_EMPTY);

      for (uint32_t i = 0; i < schema.GetColumnCount(); i++) {
        // get type
        const auto &col = schema.GetColumn(i);
        TypeId type = col.GetType();
        switch (type) {
          case TypeId::TYPE_INT:
            _tmp_val = Value(type, std::stoi(splited_str_list[i]));
            break;
          case TypeId::TYPE_LONG:
            _tmp_val = Value(type, std::stoll(splited_str_list[i]));
            break;
          case TypeId::TYPE_FLOAT:
          case TypeId::TYPE_DOUBLE:
            _tmp_val = Value(type, std::stod(splited_str_list[i]));
            break;
          case TypeId::TYPE_CHAR:
          case TypeId::TYPE_VARCHAR:
            _tmp_val = Value(type, splited_str_list[i]);
            break;
          default:
            throw Exception("unsupported type\n");
            break;
        }
        values.emplace_back(_tmp_val);
      }
      auto rid = fh_insert(fh_, values, &schema);
      rids.emplace_back(rid);
    }
  }

  void get_records(RmFileHandle *fh_) {
    for (auto rid : rids) {
      fh_get(fh_, rid, &schema);
    }
  }

  std::vector<ColMeta> get_cols() {
    auto columns = schema.GetColumns();
    std::vector<ColMeta> res;
    for (auto it = columns.begin(); it != columns.end(); it++) {
      res.emplace_back(ColMeta(*it));
    }
    return res;
  }
};

// 绘制Dot的类
class DotDrawer {
 public:
  std::ofstream *outfile = nullptr;
  DotDrawer(std::string _file_path) {
    outfile = new std::ofstream(_file_path, std::ios::out);
    if (!outfile->is_open()) {
      throw std::runtime_error("DotDrawer::cannot open file");
    }
  }
  ~DotDrawer() {
    if (outfile) {
      outfile->close();
      delete outfile;
      outfile = nullptr;
    }
  }
  virtual void print() = 0;
};

class BPlusTreeDrawer : public DotDrawer {
 private:
  IxIndexHandle *b_plus_tree = nullptr;
  void printNode(IxNodeHandle *_node) {
    if (_node == nullptr || _node->GetFileHdr() == nullptr || _node->GetPageHdr() == nullptr) return;

    // 先声明这个节点
    *outfile << getNodeDesc(_node) << std::endl;
    if (_node->IsLeafPage()) return;
    // 然后声明与子节点的关系
    for (int i = 0; i < _node->GetSize(); i++) {
      auto child = GetChild(_node, i);
      // if (child == nullptr || child->GetFileHdr() == nullptr || child->GetPageHdr() == nullptr) continue;
      *outfile << getNodeName(_node) << " : " << "f" << i << " : s -> " << getNodeName(child) << " : n" << std::endl;
      printNode(child);
      bpm->UnpinPage(child->GetPageId(), false);
      delete child;
    }
  }

  // 得到第i个孩子节点
  IxNodeHandle *GetChild(IxNodeHandle *_node, int i) {
    page_id_t child_page_id = _node->ValueAt(i);
    return b_plus_tree->FetchNode(child_page_id);
  }

  std::string getAnonymousNodeStr(IxNodeHandle *_node) {
    std::stringstream ss;
    int num = _node->GetSize();
    int i = 0;
    for (; i < num - 1; i++) {
      ss << "<f" << i << "> | ";
    }
    ss << "<f" << i << "> ";
    return ss.str();
  }

  std::string getNodeRidStr(IxNodeHandle *_node) {
    std::stringstream ss;
    int num = _node->GetSize();
    int i = 0;
    for (; i < num - 1; i++) {
      ss << _node->GetRid(i) << " | ";
    }
    ss << _node->GetRid(i) << " ";
    return ss.str();
  }

  std::string getNodeKeyStr(IxNodeHandle *_node, int _col_id) {
    auto keys_str = _node->GetDeserializeKeys();
    std::stringstream ss;
    if (keys_str.size() == 0) return "";
    if (keys_str[0].size() == 0) return "";
    ss << keys_str[_col_id][0];
    for (int i = 1; i < keys_str[_col_id].size(); i++) {
      ss << " | " << keys_str[_col_id][i];
    }
    return ss.str();
  }

  std::string replaceAll(std::string str, const std::string &oldChar, const std::string &newChar) {
    size_t pos = 0;
    while ((pos = str.find(oldChar, pos)) != std::string::npos) {
      str.replace(pos, oldChar.length(), newChar);
      pos += newChar.length();  // 移动到下一个位置
    }
    return str;
  }

  std::string getNodeName(IxNodeHandle *_node) {
    std::stringstream ss;
    auto keys = _node->GetDeserializeKeys();

    for (auto it = keys[0].begin(); it != keys[0].end(); it++) {
      *it = replaceAll(*it, "#", "_");
    }

    ss << "Node";
    for (auto it = keys[0].begin(); it != keys[0].end(); it++) {
      ss << "_" << *it;
    }
    return ss.str();
  }

  std::string getNodeDesc(IxNodeHandle *_node) {
    std::stringstream ss;
    ss << getNodeName(_node) << "[label=\"{{" << getNodeKeyStr(_node, 0) << "} | {" << getAnonymousNodeStr(_node)
       << "}}\"]";
    return ss.str();
  }

 public:
  BPlusTreeDrawer(std::string file_name, IxIndexHandle *_b_plus_tree)
      : DotDrawer(file_name), b_plus_tree(_b_plus_tree) {}
  void print() override {
    *outfile << "digraph btree{" << std::endl;
    *outfile << "node[shape=record, style=bold];" << std::endl;
    *outfile << "edge[style=bold];" << std::endl;

    auto root_node = b_plus_tree->GetRoot();
    assert(root_node->IsRootPage());
    printNode(root_node);
    *outfile << "}" << std::endl;

    delete root_node;
  }
};

class EasyDBTest : public ::testing::Test {
 protected:
  void SetUp() override {}

  void TearDown() override {}
};

TEST(EasyDBTest, SimpleTest) {
  std::cerr << "[TEST] => 测试开始" << std::endl;
  // system("pwd");
  // std::cout << "../../tmp/benchmark_data/" + TEST_FILE_NAME_SUPPLIER << std::endl;
  // parse create sql statement:
  // CREATE TABLE SUPPLIER ( S_SUPPKEY INTEGER NOT NULL,
  // S_NAME CHAR(25) NOT NULL,
  // S_ADDRESS VARCHAR(40) NOT NULL,
  // S_NATIONKEY INTEGER NOT NULL,
  // S_PHONE CHAR(15) NOT NULL,
  // S_ACCTBAL FLOAT NOT NULL,
  // S_COMMENT VARCHAR(101) NOT NULL);
  Column col1{"S_SUPPKEY", TypeId::TYPE_INT};
  Column col2{"S_NAME", TypeId::TYPE_CHAR, 25};
  Column col3{"S_ADDRESS", TypeId::TYPE_VARCHAR, 40};
  Column col4{"S_NATIONKEY", TypeId::TYPE_INT};
  Column col5{"S_PHONE", TypeId::TYPE_CHAR, 15};
  Column col6{"S_ACCTBAL", TypeId::TYPE_FLOAT};
  Column col7{"S_COMMENT", TypeId::TYPE_VARCHAR, 101};
  std::vector<Column> cols{col1, col2, col3, col4, col5, col6, col7};
  Schema schema{cols};
  TB_Reader tb_reader(TEST_FILE_NAME_SUPPLIER, "../../tmp/benchmark_data/" + TEST_FILE_NAME_SUPPLIER, schema);

  // 创建DiskManager
  std::cout << "[TEST] 创建DiskManager" << std::endl;
  DiskManager *dm = new DiskManager(TEST_DB_NAME);
  std::string path = TEST_DB_NAME + "/" + TEST_TB_NAME;
  create_file(dm, path, 29);

  int fd = dm->OpenFile(path);

  std::cout << "[TEST] 创建BufferPoolManager" << std::endl;
  // 创建BufferPoolManager
  bpm = new BufferPoolManager(BUFFER_POOL_SIZE, dm);

  RmFileHandle *fh_ = new RmFileHandle(dm, bpm, fd);

  std::cout << "[TEST] 开始解析和插入数据" << std::endl;
  // 解析table文件，并且将其插入到表中
  tb_reader.parse_and_insert(fh_);

  bpm->FlushAllDirtyPages();

  // tb_reader.get_records(fh_);

  std::cerr << "[TEST] 测试索引" << std::endl;
  /*------------------------------------------
                  b+树索引
  ------------------------------------------*/
  {
    std::cerr << "[TEST] => 测试B+树索引" << std::endl;
    // 增加索引
    // 准备元数据
    std::cerr << "[TEST] ==> 准备B+树索引元数据" << std::endl;
    std::vector<ColMeta> index_cols;
    index_cols.push_back(tb_reader.get_cols()[1]);
    // std::string index_col_name = "S_SUPPKEY";
    std::string index_col_name = "S_NAME";
    IndexMeta index_meta = {.tab_name = TEST_TB_NAME, .col_tot_len = 25, .col_num = 1, .cols = index_cols};
    std::vector<uint32_t> key_ids{1};
    auto key_schema = Schema::CopySchema(&schema, key_ids);

    // 创建index
    std::cerr << "[TEST] ==> 创建B+树索引" << std::endl;
    IxManager *ix_manager_ = new IxManager(dm, bpm);
    ix_manager_->CreateIndex(path, index_cols);

    // 将表中已经存在的记录插入到新创建的index中
    std::cerr << "[TEST] ==> 将表格数据加入到新建的索引中" << std::endl;
    auto Ixh = ix_manager_->OpenIndex(path, index_cols);

    RmScan scan(fh_);
    bool flag = false;
    char *delete_key = nullptr;
    RID delete_rid;
    while (!scan.IsEnd()) {
      auto rid = scan.GetRid();
      // auto rec = fh_->GetRecord(rid);
      auto key_tuple = fh_->GetKeyTuple(schema, key_schema, key_ids, rid);
      char *key = new char[index_meta.col_tot_len];
      int offset = 0;
      for (int i = 0; i < index_meta.col_num; ++i) {
        // memcpy(key + offset, rec->data + index_meta.cols[i].offset, index_meta.cols[i].len);
        auto val = key_tuple.GetValue(&key_schema, i);
        memcpy(key + offset, val.GetData(), val.GetStorageSize());
        if (!flag) {
          flag = true;
          delete_key = new char[index_meta.col_tot_len];
          delete_rid = rid;
          // memcpy(delete_key + offset, rec->data + index_meta.cols[i].offset, index_meta.cols[i].len);
          memcpy(delete_key + offset, val.GetData(), val.GetStorageSize());
        }
        offset += index_meta.cols[i].len;
      }
      Ixh->InsertEntry(key, rid);
      delete[] key;
      scan.Next();
    }
    // // 生成dot图
    // std::cerr << "[TEST] ==> 生成b+树dot图" << std::endl;
    // BPlusTreeDrawer bpt_drawer("b_plus_index.dot", &(*Ixh));
    // bpt_drawer.print();

    char *target_key = delete_key;
    // 索引查找
    std::cerr << "[TEST] ==> 查找b+树索引" << std::endl;
    std::vector<RID> target_rid;
    Ixh->GetValue(target_key, &target_rid);
    EXPECT_EQ(target_rid[0], delete_rid);

    // 修改索引
    Ixh->DeleteEntry(delete_key);
    Ixh->InsertEntry(delete_key, delete_rid);

    // 删除索引
    std::cerr << "[TEST] ===> 删除索引" << std::endl;
    EXPECT_TRUE(Ixh->DeleteEntry(delete_key));

    std::cerr << "[TEST] => B+树索引测试完毕" << std::endl;
    delete[] delete_key;
    delete ix_manager_;
  }

  /*------------------------------------------
                 可扩展哈希索引
 ------------------------------------------*/
  // {
  //   // 增加索引
  //   std::cerr << "[TEST] => 测试可扩展哈希索引" << std::endl;
  //   // 增加索引
  //   // 准备元数据
  //   std::cerr << "[TEST] ===> 准备可扩展哈希索引元数据" << std::endl;
  //   std::vector<ColMeta> index_cols;
  //   index_cols.push_back(tb_reader.get_cols()[0]);
  //   std::string index_col_name = "S_SUPPKEY";
  //   IndexMeta index_meta = {.tab_name = TEST_TB_NAME, .col_tot_len = 4, .col_num = 1, .cols = index_cols};

  //   // 创建index
  //   std::cerr << "[TEST] ===> 创建可扩展哈希索引" << std::endl;

  //   auto ix_manager = new IxManager(dm, bpm);
  //   ix_manager->CreateExtendibleHashIndex(path, index_cols);
  //   IxExtendibleHashIndexHandle *ix_handler = &(*ix_manager->OpenExtendibleHashIndex(path, index_cols));

  //   // 将表中已经存在的记录插入到新创建的index中
  //   std::cerr << "[TEST] ===> 将表格数据加入到新建的索引中" << std::endl;
  //   RmScan scan(fh_);
  //   bool flag = false;
  //   char *delete_key = nullptr;
  //   RID delete_rid;
  //   while (!scan.IsEnd()) {
  //     auto rid = scan.GetRid();
  //     auto rec = fh_->GetRecord(rid);
  //     char *key = new char[index_meta.col_tot_len];
  //     int offset = 0;
  //     for (int i = 0; i < index_meta.col_num; ++i) {
  //       memcpy(key + offset, rec->data + index_meta.cols[i].offset, index_meta.cols[i].len);
  //       if (!flag) {
  //         flag = true;
  //         delete_key = new char[index_meta.col_tot_len];
  //         memcpy(delete_key + offset, rec->data + index_meta.cols[i].offset, index_meta.cols[i].len);
  //         delete_rid = rid;
  //       }
  //       offset += index_meta.cols[i].len;
  //     }
  //     // Ixh->InsertEntry(key, rid);
  //     ix_handler->InsertEntry(key, rid);
  //     delete[] key;
  //     scan.Next();
  //   }
  //   // 生成dot图
  //   std::cerr << "[TEST] ===> 生成可扩展哈希dot图" << std::endl;
  //   // BPlusTreeDrawer bpt_drawer("b_plus_index.dot", &(*Ixh));
  //   // bpt_drawer.print();

  //   RID new_rid = delete_rid;
  //   // 索引修改
  //   ix_handler->DeleteEntry(delete_key);
  //   ix_handler->InsertEntry(delete_key, new_rid);

  //   char *target_key = delete_key;
  //   // 索引查找
  //   std::vector<RID> target_rid;
  //   ix_handler->GetValue(target_key, &target_rid);

  //   // 删除索引
  //   std::cerr << "[TEST] ===> 删除索引" << std::endl;
  //   EXPECT_TRUE(ix_handler->DeleteEntry(delete_key));
  //   delete[] delete_key;
  // }

  std::cerr << "[TEST] => 释放资源" << std::endl;
  delete fh_;
  delete bpm;
  delete dm;
  std::cerr << "[TEST] => 测试结束" << std::endl;
}
};  // namespace easydb