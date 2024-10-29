#pragma once

#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include "buffer/buffer_pool_manager.h"
#include "common/common.h"
#include "common/errors.h"
#include "defs.h"
#include "gtest/gtest.h"
#include "record/rm_defs.h"
#include "record/rm_file_handle.h"
#include "storage/disk/disk_manager.h"
#include "system/sm_defs.h"
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
  std::cout << "insert rid:" << rid.GetPageId() << " " << rid.GetSlotNum() << std::endl;
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

  ~TB_Reader() { delete file_reader; }

  void parse_and_insert(RmFileHandle *fh_) {
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

class EasyDBTest : public ::testing::Test {
 protected:
  void SetUp() override {}

  void TearDown() override {}
};

TEST(EasyDBTest, SimpleTest) {
  system("pwd");
  std::cout << "../../tmp/benchmark_data/" + TEST_FILE_NAME_SUPPLIER << std::endl;
  TB_Reader tb_reader(TEST_FILE_NAME_SUPPLIER, "../../tmp/benchmark_data/" + TEST_FILE_NAME_SUPPLIER);
  // 构造表元数据
  tb_reader.set_col("S_SUPPKEY", TYPE_INT, 4, 0)
      .set_col("S_NAME", TYPE_CHAR, 25, 4)
      .set_col("S_ADDRESS", TYPE_VARCHAR, 40, 29)
      .set_col("S_NATIONKEY", TYPE_INT, 4, 69)
      .set_col("S_PHONE", TYPE_CHAR, 15, 73)
      .set_col("S_ACCTBAL", TYPE_FLOAT, 4, 88)
      .set_col("S_COMMENT", TYPE_VARCHAR, 101, 92);

  // 创建DiskManager
  DiskManager *dm = new DiskManager(TEST_DB_NAME);
  std::string path = TEST_DB_NAME + "/" + TEST_TB_NAME;
  create_file(dm, path, 193);

  int fd = dm->OpenFile(path);

  // 创建BufferPoolManager
  BufferPoolManager *bpm = new BufferPoolManager(100, dm);

  RmFileHandle *fh_ = new RmFileHandle(dm, bpm, fd);

  // 解析table文件，并且将其插入到表中
  tb_reader.parse_and_insert(fh_);

  bpm->FlushAllDirtyPages();

  delete fh_;
  delete bpm;
  delete dm;
}
};  // namespace easydb