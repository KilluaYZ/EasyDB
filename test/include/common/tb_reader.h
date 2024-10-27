#pragma once

#include <storage/disk/disk_manager.h>

#include <algorithm>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include "errors.hpp"
#include "common.h"
#include "record.h"

using namespace std;

namespace easydb {

class FileReader {
 protected:
  ifstream *infile = nullptr;
  string buffer;

 public:
  explicit FileReader(const string &file_path) {
    infile = new ifstream(file_path);
    if (!infile->is_open()) {
      throw FileNotFoundError("FileReader::File not found");
    }
  }

  ~FileReader() {
    if (infile && infile->is_open()) infile->close();
    delete infile;
  }

  int read_line() { return getline(infile, buffer); }

  string get_buf() { return buffer; }

  vector<string> get_splited_buf(string split_str) {
    string line_str = get_buf();
    if (line_str.empty()) {
      return vector<string>();
    }
    vector<string> res;
    size_t pos = 0;
    while ((pos = line_str.find(split_str, pos)) != string::npos) {
      res.push_back(line_str.substr(0, pos));
      line_str = line_str.substr(pos + split_str.size());
    }
    return res;
  }
};

class TB_Reader {
 protected:
  FileReader *file_reader;
  vector<ColMeta> columns;
  string tab_name;
 public:
  TB_Reader(string tab_name, string file_path) {
    this->file_reader = new FileReader(file_path);
    this->tab_name = tab_name;
  }

  TB_Reader& set_col(string name, ColType type, int len, int offset) {
    ColMeta col;
    col.name = name;
    col.type = type;
    col.len = len;
    col.offset = offset;
    columns.push_back(col);
    return *this;
  }

  ~TB_Reader() { delete file_reader; }

  void parse_and_create_tab(DiskManager* disk_manager) {
    for(int i = 0;i < columns.size(); ++i) {
         
    }
  }
};
}  // namespace easydb