#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <algorithm>
#include <cassert>
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
#include "common/common.h"
#include "common/errors.h"
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

const int MAX_FILES = 32;
const int MAX_PAGES = 128;
const size_t TEST_BUFFER_POOL_SIZE = MAX_FILES * MAX_PAGES;

BufferPoolManager *bpm;

std::vector<ColMeta> columns;

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
  file_hdr.record_size = record_size;
  file_hdr.num_pages = 1;
  file_hdr.first_free_page_no = RM_NO_PAGE;

  // We have: sizeof(hdr) + (n + 7) / 8 + n * record_size <= PAGE_SIZE
  file_hdr.num_records_per_page =
      (BITMAP_WIDTH * (PAGE_SIZE - 1 - (int)sizeof(RmFileHdr)) + 1) / (1 + record_size * BITMAP_WIDTH);
  file_hdr.bitmap_size = (file_hdr.num_records_per_page + BITMAP_WIDTH - 1) / BITMAP_WIDTH;

  // 将file header写入磁盘文件（名为file name，文件描述符为fd）中的第0页
  // head page直接写入磁盘，没有经过缓冲区的NewPage，那么也就不需要FlushPage
  disk_manager->WritePage(fd, RM_FILE_HDR_PAGE, (char *)&file_hdr, sizeof(file_hdr));
  disk_manager->CloseFile(fd);
}

void fh_insert(RmFileHandle *fh_, std::vector<Value> &values_, std::vector<ColMeta> &cols_) {
  RmRecord rec(fh_->GetFileHdr().record_size);
  for (int i = 0; i < cols_.size(); i++) {
    auto &val = values_[i];
    auto &col = cols_[i];
    val.init_raw(col.len);
    std::memcpy(rec.data + col.offset, val.raw->data, col.len);
  }
  // fh_->InsertRecord(rec.data);
  auto rid = fh_->InsertRecord(rec.data);
  // std::cerr << "[TEST] insert rid: " << rid.GetPageId() << " slot num: " << rid.GetSlotNum() << std::endl;
}

class TB_Reader {
 protected:
  FileReader *file_reader = nullptr;
  std::vector<ColMeta> columns;
  std::string tab_name;

 public:
  TB_Reader(std::string tab_name, std::string file_path) {
    this->file_reader = new FileReader(file_path);
    this->tab_name = tab_name;
  }

  TB_Reader &set_col(std::string name, ColType type, int len, int offset) {
    ColMeta col;
    col.name = name;
    col.type = type;
    col.len = len;
    col.offset = offset;
    columns.push_back(col);
    return *this;
  }

  std::vector<ColMeta> get_cols() { return this->columns; }

  ~TB_Reader() { delete file_reader; }

  void parse_and_insert(RmFileHandle *fh_) {
    // for (int i = 0; i < 24 && file_reader->read_line(); i++)
    while (file_reader->read_line()) {
      auto splited_str_list = file_reader->get_splited_buf();
      std::vector<Value> values_;
      for (int i = 0; i < (int)columns.size(); i++) {
        Value _tmp_val;
        switch (columns[i].type) {
          case TYPE_CHAR:
            _tmp_val.set_char(splited_str_list[i]);
            break;
          case TYPE_VARCHAR:
            _tmp_val.set_varchar(splited_str_list[i]);
            break;
          case TYPE_INT:
            _tmp_val.set_int(std::stoi(splited_str_list[i]));
            break;
          case TYPE_LONG:
            _tmp_val.set_long(std::stoll(splited_str_list[i]));
            break;
          case TYPE_FLOAT:
            _tmp_val.set_float(std::stof(splited_str_list[i]));
            break;
          case TYPE_DOUBLE:
            _tmp_val.set_double(std::stod(splited_str_list[i]));
            break;
          default:
            break;
        }
        values_.push_back(_tmp_val);
      }
      fh_insert(fh_, values_, columns);
    }
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
  TB_Reader tb_reader(TEST_FILE_NAME_SUPPLIER, "../../tmp/benchmark_data/" + TEST_FILE_NAME_SUPPLIER);
  // 构造表元数据
  tb_reader.set_col("S_SUPPKEY", TYPE_INT, 4, 0).set_col("S_NAME", TYPE_CHAR, 25, 4);
  // .set_col("S_ADDRESS", TYPE_VARCHAR, 40, 29)
  // .set_col("S_NATIONKEY", TYPE_INT, 4, 69)
  // .set_col("S_PHONE", TYPE_CHAR, 15, 73)
  // .set_col("S_ACCTBAL", TYPE_FLOAT, 4, 88)
  // .set_col("S_COMMENT", TYPE_VARCHAR, 101, 92);

  // 创建DiskManager
  std::cerr << "[TEST] => 创建DiskManager" << std::endl;
  DiskManager *dm = new DiskManager(TEST_DB_NAME);
  std::string path = TEST_DB_NAME + "/" + TEST_TB_NAME;
  create_file(dm, path, 29);

  int fd = dm->OpenFile(path);

  std::cerr << "[TEST] => 创建BufferPoolManager" << std::endl;
  // 创建BufferPoolManager
  bpm = new BufferPoolManager(BUFFER_POOL_SIZE, dm);

  RmFileHandle *fh_ = new RmFileHandle(dm, bpm, fd);

  std::cerr << "[TEST] => 开始解析和插入数据" << std::endl;
  // 解析table文件，并且将其插入到表中
  tb_reader.parse_and_insert(fh_);

  bpm->FlushAllDirtyPages();

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
      auto rec = fh_->GetRecord(rid);
      char *key = new char[index_meta.col_tot_len];
      int offset = 0;
      for (int i = 0; i < index_meta.col_num; ++i) {
        memcpy(key + offset, rec->data + index_meta.cols[i].offset, index_meta.cols[i].len);
        if (!flag) {
          flag = true;
          delete_key = new char[index_meta.col_tot_len];
          delete_rid = rid;
          memcpy(delete_key + offset, rec->data + index_meta.cols[i].offset, index_meta.cols[i].len);
        }
        offset += index_meta.cols[i].len;
      }
      Ixh->InsertEntry(key, rid);
      delete[] key;
      scan.Next();
    }
    // 生成dot图
    std::cerr << "[TEST] ==> 生成b+树dot图" << std::endl;
    BPlusTreeDrawer bpt_drawer("b_plus_index.dot", &(*Ixh));
    bpt_drawer.print();

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
  {
    // 增加索引
    std::cerr << "[TEST] => 测试可扩展哈希索引" << std::endl;
    // 增加索引
    // 准备元数据
    std::cerr << "[TEST] ===> 准备可扩展哈希索引元数据" << std::endl;
    std::vector<ColMeta> index_cols;
    index_cols.push_back(tb_reader.get_cols()[0]);
    std::string index_col_name = "S_SUPPKEY";
    IndexMeta index_meta = {.tab_name = TEST_TB_NAME, .col_tot_len = 4, .col_num = 1, .cols = index_cols};

    // 创建index
    std::cerr << "[TEST] ===> 创建可扩展哈希索引" << std::endl;

    auto ix_manager = new IxManager(dm, bpm);
    ix_manager->CreateExtendibleHashIndex(path, index_cols);
    IxExtendibleHashIndexHandle *ix_handler = &(*ix_manager->OpenExtendibleHashIndex(path, index_cols));

    // 将表中已经存在的记录插入到新创建的index中
    std::cerr << "[TEST] ===> 将表格数据加入到新建的索引中" << std::endl;
    RmScan scan(fh_);
    bool flag = false;
    char *delete_key = nullptr;
    RID delete_rid;
    while (!scan.IsEnd()) {
      auto rid = scan.GetRid();
      auto rec = fh_->GetRecord(rid);
      char *key = new char[index_meta.col_tot_len];
      int offset = 0;
      for (int i = 0; i < index_meta.col_num; ++i) {
        memcpy(key + offset, rec->data + index_meta.cols[i].offset, index_meta.cols[i].len);
        if (!flag) {
          flag = true;
          delete_key = new char[index_meta.col_tot_len];
          memcpy(delete_key + offset, rec->data + index_meta.cols[i].offset, index_meta.cols[i].len);
          delete_rid = rid;
        }
        offset += index_meta.cols[i].len;
      }
      // Ixh->InsertEntry(key, rid);
      ix_handler->InsertEntry(key, rid);
      delete[] key;
      scan.Next();
    }
    // 生成dot图
    std::cerr << "[TEST] ===> 生成可扩展哈希dot图" << std::endl;
    // BPlusTreeDrawer bpt_drawer("b_plus_index.dot", &(*Ixh));
    // bpt_drawer.print();

    RID new_rid = delete_rid;
    // 索引修改
    ix_handler->DeleteEntry(delete_key);
    ix_handler->InsertEntry(delete_key, new_rid);

    char *target_key = delete_key;
    // 索引查找
    std::vector<RID> target_rid;
    ix_handler->GetValue(target_key, &target_rid);

    // 删除索引
    std::cerr << "[TEST] ===> 删除索引" << std::endl;
    EXPECT_TRUE(ix_handler->DeleteEntry(delete_key));
    delete[] delete_key;
  }

  std::cerr << "[TEST] => 释放资源" << std::endl;
  delete fh_;
  delete bpm;
  delete dm;
  std::cerr << "[TEST] => 测试结束" << std::endl;
}
};  // namespace easydb