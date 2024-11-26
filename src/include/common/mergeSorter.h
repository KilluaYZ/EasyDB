/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 *
 *-------------------------------------------------------------------------
 */

#pragma once
#include <algorithm>
#include <cstdio>
#include <fstream>
#include <iostream>
#include "common/common.h"
#include "defs.h"
#include "storage/index/ix_manager.h"
#include "storage/index/ix_scan.h"
#include "storage/table/tuple.h"
#include "system/sm_defs.h"
#include "system/sm_meta.h"

namespace easydb {

class MergeSorter {
 private:
  // ColMeta col_;  // the colMeta of the sort key col
  // std::vector<ColMeta> all_cols;
  Column colu_;  // the colMeta of the sort key col
  std::vector<Column> all_colus_;

  bool is_desc_;
  size_t tuple_len_;

  std::vector<Tuple> record_tmp_buffer;
  // std::vector<RmRecord> record_tmp_buffer;
  std::vector<std::ifstream> fd_list;

  std::vector<int> ls;
  std::vector<char *> merge_record_list;
  std::vector<Value> merge_value_list;
  size_t k;
  size_t total_records_count;
  size_t output_records_count;

  size_t BUFFER_MAX_RECORD_COUNT;
  size_t BUFFER_MAX_SIZE;

  std::vector<std::string> file_paths;

 public:
  MergeSorter(Column colu, std::vector<Column> colus, size_t tuple_len, bool is_desc) {
    colu_ = colu;
    all_colus_ = colus;
    tuple_len_ = tuple_len;
    is_desc_ = is_desc;
    total_records_count = 0;
    output_records_count = 0;

    k = 0;
    BUFFER_MAX_SIZE = 1 * 1024 * 1024 * 1024;  // 1Gb
    BUFFER_MAX_RECORD_COUNT = BUFFER_MAX_SIZE / tuple_len_;
    // BUFFER_MAX_RECORD_COUNT = 5;
    // printf(" tuple.len =%d, BUFFER_MAX_RECORD_COUNT = %d\n",tuple_len_,BUFFER_MAX_RECORD_COUNT);
    record_tmp_buffer.clear();
  }

  ~MergeSorter() {
    // close all the open files.
    for (auto &fd : fd_list) {
      if (fd.is_open()) {
        fd.close();
      }
    }
  }

  void writeBuffer(Tuple current_tuple) {
    if (record_tmp_buffer.size() >= BUFFER_MAX_RECORD_COUNT) {
      // buffer is full, sort and write buffer into disk. wait for multi-way merge sorting.
      k++;
      sort(record_tmp_buffer.begin(), record_tmp_buffer.end(), cmpTuple(is_desc_, colu_));
      std::string fileName = colu_.GetTabName() + "_" + colu_.GetName() + "_" + std::to_string(file_paths.size());
      std::ofstream fd;
      fd.open(fileName, std::ios::out);
      for (auto rec = record_tmp_buffer.begin(); rec != record_tmp_buffer.end(); rec++) {
        char *data = new char[tuple_len_ + sizeof(int32_t)];
        rec->SerializeTo(data);
        uint32_t size = *reinterpret_cast<const uint32_t *>(data);
        if (tuple_len_ != size) {
          tuple_len_ = size;
        }
        fd.write(data, tuple_len_ + sizeof(int32_t));
        delete[] data;
      }
      fd.close();
      record_tmp_buffer.clear();
      file_paths.push_back(fileName);
    }
    // record buffer is still available, just put into it.
    total_records_count++;
    record_tmp_buffer.push_back(current_tuple);
  }

  void clearBuffer() {
    if (!record_tmp_buffer.empty()) {
      k++;
      sort(record_tmp_buffer.begin(), record_tmp_buffer.end(), cmpTuple(is_desc_, colu_));
      std::string fileName = colu_.GetTabName() + "_" + colu_.GetName() + "_" + std::to_string(file_paths.size());
      std::ofstream fd;
      fd.open(fileName, std::ios::out);
      fd.flush();
      for (auto rec = record_tmp_buffer.begin(); rec != record_tmp_buffer.end(); rec++) {
        char *data = new char[tuple_len_ + sizeof(int32_t)];
        rec->SerializeTo(data);
        uint32_t size = *reinterpret_cast<const uint32_t *>(data);
        if (tuple_len_ != size) {
          tuple_len_ = size;
        }
        fd.write(data, tuple_len_ + sizeof(int32_t));
        delete[] data;
      }
      fd.close();
      record_tmp_buffer.clear();
      file_paths.push_back(fileName);
    }
  }

  void initializeMergeListAndConstructTree() {
    // initialize merge list
    for (auto &file_name : file_paths) {
      std::ifstream fd;
      fd.open(file_name, std::ios::in);
      char *record = new char[tuple_len_ + sizeof(int32_t)];
      Value tp;
      Tuple tuple_tp;
      fd.read(record, tuple_len_ + sizeof(int32_t));
      tuple_tp.DeserializeFrom(record);
      tp = tuple_tp.GetValue(colu_);
      // tp.get_value_from_record(record, col_);
      merge_record_list.push_back(record);
      merge_value_list.push_back(tp);

      fd_list.push_back(std::move(fd));
    }
    createLoserTree();
  }

  bool IsEnd() const {
    return merge_record_list.size() == 0 ||
           (output_records_count >= total_records_count && merge_record_list[ls[0]] == NULL);
  }

  char *getOneRecord() {
    // get a records
    if (!IsEnd() && merge_record_list.size() > ls[0]) {
      output_records_count++;
      char *res = (char *)malloc(sizeof(char) * (tuple_len_ + sizeof(int32_t)));
      memcpy(res, merge_record_list[ls[0]], tuple_len_ + sizeof(int32_t));
      char *record = (char *)malloc(sizeof(char) * (tuple_len_ + sizeof(int32_t)));
      Value tp;
      fd_list[ls[0]].read(record, tuple_len_ + sizeof(int32_t));
      if (fd_list[ls[0]].fail()) {
        merge_record_list[ls[0]] = NULL;
      } else {
        Tuple tuple_tp;
        tuple_tp.DeserializeFrom(record);
        tp = tuple_tp.GetValue(colu_);
        memcpy(merge_record_list[ls[0]], tuple_tp.GetData(), tuple_len_);
        free(record);
      }
      merge_value_list[ls[0]] = tp;  /// ! attention : tp may be empty
      adjust(ls[0]);
      return res;
    }
    // never get here
    return NULL;
  }

 private:
  void createLoserTree() {
    ls.resize(k + 1);

    for (int i = 0; i < k; i++) {
      ls[i] = -1;  // make it unavalilable.
    }

    for (int i = k - 1; i >= 0; i--) {
      adjust(i);  // adjust to initialize the loser tree
    }
  }

  void printLoserTree() {
    int i = 1;
    int count = 0;
    while (count < k) {
      for (int j = 0; j < i && count < k; j++) {
        printf("%d  ", ls[count]);
        count++;
      }
      i++;
      printf("\n");
    }
  }

  void adjust(int s) {
    // adjust from leaf merge_record_list[s] to root ls[0]
    // printLoserTree();
    int t = (s + k) / 2;  // ls[t] is the parent node of merge_record_list[s];
    while (t > 0) {
      // update s to the new winner
      // case 0: ls[t] = -1                                - ls[t] = s
      // case 1: list[s] = null, list[ls[t]] != null.      - ls[t] = s, s = ls[t]
      // case 2: list[s] != null, list[ls[t]] = null.      - ls[t] = ls[t], s = s;
      // case 3: list[s] = null, list[ls[t]] = null.       - ls[t] = ls[t], s = s;
      // case 4: list[s] != null, list[ls[t]] != null.     - ls[t] = loser, s = winner
      if (ls[t] == -1) {
        ls[t] = s;
      } else if (merge_record_list[ls[t]] != NULL &&
                 (merge_record_list[s] == NULL || !cmp(merge_value_list[s], merge_value_list[ls[t]]))) {
        // if lose, update loser
        std::swap(s, ls[t]);
      }
      t = t / 2;
    }
    ls[0] = s;
  }

  bool cmp(const Value &leftVal, const Value &rightVal) { return !is_desc_ ? leftVal < rightVal : leftVal > rightVal; }
};

}  // namespace easydb
