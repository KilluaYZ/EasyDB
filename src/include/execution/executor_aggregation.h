/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "common/errors.h"
#include "defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "storage/index/ix_manager.h"
#include "storage/index/ix_scan.h"
#include "system/sm_defs.h"
#include "system/sm_meta.h"

/**
 * e.g.
 *
 * select SUM(score) as sum_score from grade where id = 1;
 *
 * select course ,COUNT(*) as row_num , COUNT(id) as student_num
 * from grade
 * group by course;
 *
 * select id,MAX(score) as max_score,MIN(score) as min_score
 * from grade
 * group by id
 * having COUNT(*) > 1 and MIN(score) > 88;
 *
 * TYPE: COUNT MIN MAX SUM
 *
 * enum AggregationType {
 *  MAX_AGG, MIN_AGG, COUNT_AGG, SUM_AGG, NO_AGG
 *  };
 */

namespace easydb {

class AggregationExecutor : public AbstractExecutor {
 private:
  std::unique_ptr<AbstractExecutor> prev_;  // 聚合操作的儿子节点
  size_t len_;                              // 聚合计算后得到的记录的长度
  std::vector<ColMeta> cols_;               // 聚合计算后得到的字段
  std::vector<ColMeta> prev_cols_;          // 原始字段
  std::vector<ColMeta> sel_cols_;           // 聚合计算选择的字段
  std::vector<ColMeta> group_cols_;         // groupby选择的字段
  std::vector<Condition> having_conds_;     // having算子的条件

  std::vector<RmRecord> all_records_;                             // 所有records
  std::map<std::string, std::vector<RmRecord>> key_records_map_;  // 根据groupby条件分组的records <key,records>哈希表

  std::unique_ptr<RmRecord> result_;  // 用于返回的聚合结果记录
  // bool isend_;
  int key_length;
  std::map<std::string, std::vector<RmRecord>>::iterator it;
  // int traverse_idx;
 public:
  AggregationExecutor(std::unique_ptr<AbstractExecutor> prev, std::vector<TabCol> sel_col_,
                      std::vector<TabCol> group_cols, std::vector<Condition> having_conds) {
    prev_ = std::move(prev);
    prev_cols_ = prev_->cols();
    int offset = 0;
    for (auto &sel_col : sel_col_) {
      ColMeta col_tp;
      if (sel_col.col_name == "*" && sel_col.aggregation_type == AggregationType::COUNT_AGG) {
        // todo
        col_tp = {.tab_name = "",
                  .name = "*",
                  .type = TYPE_INT,
                  .len = sizeof(int),
                  .offset = offset,
                  .index = false,
                  .agg_type = AggregationType::COUNT_AGG};
        sel_cols_.push_back(col_tp);
        col_tp.name = generate_new_name(sel_col);
        cols_.push_back(col_tp);
        offset += sizeof(int);
        continue;
      }
      col_tp = *get_col(prev_cols_, sel_col);
      col_tp.agg_type = sel_col.aggregation_type;
      sel_cols_.push_back(col_tp);
      if (sel_col.aggregation_type != AggregationType::NO_AGG) {
        if (sel_col.aggregation_type == AggregationType::COUNT_AGG) {
          col_tp.type = ColType::TYPE_INT;
          col_tp.len = sizeof(int);
        } else if (sel_col.aggregation_type == AggregationType::SUM_AGG && col_tp.type == ColType::TYPE_STRING) {
          throw InternalError("string type do not supports sum aggreagation.");
        }
        col_tp.name = generate_new_name(sel_col);
      }
      col_tp.offset = offset;
      offset += col_tp.len;
      cols_.push_back(col_tp);
    }
    len_ = offset;
    key_length = 0;
    for (auto &col : group_cols) {
      ColMeta col_tp = *get_col(prev_cols_, col);
      key_length += col_tp.len;
      group_cols_.push_back(col_tp);
    }
    having_conds_ = std::move(having_conds);

    for (prev_->beginTuple(); !prev_->is_end(); prev_->nextTuple()) {
      auto record_tp = *prev_->Next();
      all_records_.push_back(record_tp);
    }

    // isend_ = false;
    // traverse_idx = 0;
  }

  void beginTuple() override {
    // group by
    if (group_cols_.size() != 0) {
      // having group by statement
      for (auto ait = all_records_.begin(); ait != all_records_.end(); ait++) {
        std::string key = "";
        int offset = 0;
        for (auto &group_col : group_cols_) {
          key += std::string(ait->data + group_col.offset, group_col.len);
          offset += group_col.len;
        }
        if (key_records_map_.find(key) == key_records_map_.end()) {
          // not grouped.
          std::vector<RmRecord> records_tp;
          records_tp.push_back(*ait);
          key_records_map_[key] = records_tp;
        } else {
          key_records_map_[key].push_back(*ait);
        }
      }
    } else {
      std::string key_tp = "*";
      key_records_map_[key_tp] = std::move(all_records_);
    }
    it = key_records_map_.begin();
    while (!is_end() && !predicate(it->second)) {
      it++;
    }
  }

  void nextTuple() override {
    do {
      it++;
    } while (!is_end() && !predicate(it->second));
  }

  std::unique_ptr<RmRecord> Next() override {
    std::vector<RmRecord> records = it->second;
    char *data = new char[len_];
    int offset = 0;
    for (auto &sel_col : sel_cols_) {
      // if(sel_col.agg_type==AggregationType::NO_AGG && !in_groupby_cols(sel_col)){
      //     throw InternalError("only cols in \"group by\" statement can be projected without aggregation.");
      // }
      Value res = aggregation_to_value(records, sel_col);
      memcpy(data + offset, res.raw->data, res.raw->size);
      offset += sel_col.len;
    }
    return std::make_unique<RmRecord>(len_, data);
  }

  RID &rid() override { return _abstract_rid; }

  size_t tupleLen() const override { return len_; };

  const std::vector<ColMeta> &cols() const override { return cols_; };

  bool is_end() const override { return it == key_records_map_.end(); };

  ColMeta get_col_offset(const TabCol &target) override {
    for (auto &col : cols_) {
      if (target.col_name == col.name && target.tab_name == col.tab_name) {
        return col;
      }
    }
    throw ColumnNotFoundError(target.col_name);
  }

 private:
  bool in_groupby_cols(ColMeta target) {
    auto pos = std::find_if(group_cols_.begin(), group_cols_.end(), [&](const ColMeta &col) {
      return col.tab_name == target.tab_name && col.name == target.name;
    });
    if (pos == group_cols_.end()) {
      return false;
    }
    return true;
  }

  bool predicate(std::vector<RmRecord> records) {
    bool satisfy = true;
    // return true only all the conditions were true
    // i.e. all conditions are connected with 'and' operator
    for (auto &cond : having_conds_) {
      Value lhs_v, rhs_v;
      if (cond.lhs_col.aggregation_type != AggregationType::NO_AGG) {
        // case 1. e.g. count(*)
        if (cond.lhs_col.col_name == "*" && cond.lhs_col.aggregation_type == AggregationType::COUNT_AGG) {
          ColMeta col_tp;
          col_tp = {.tab_name = "",
                    .name = "*",
                    .type = TYPE_INT,
                    .len = sizeof(int),
                    .offset = 0,
                    .index = false,
                    .agg_type = AggregationType::COUNT_AGG};
          lhs_v = aggregation_to_value(records, col_tp);
        } else {  // case 2. e.g. min(val)
          ColMeta col_tp;
          col_tp = {.tab_name = cond.lhs_col.tab_name,
                    .name = cond.lhs_col.col_name,
                    .type = TYPE_INT,
                    .len = sizeof(int),
                    .offset = 0,
                    .index = false,
                    .agg_type = cond.lhs_col.aggregation_type};

          lhs_v = aggregation_to_value(records, col_tp);
        }

      } else {
        throw InternalError("having-conds with no aggregation.");
      }

      if (cond.is_rhs_val) {
        rhs_v = cond.rhs_val;
      } else if (cond.rhs_col.aggregation_type != AggregationType::NO_AGG) {
        rhs_v = aggregation_to_value(records, *get_col(prev_cols_, cond.rhs_col));
      } else {
        throw InternalError("having-conds with no aggregation.");
      }

      if (!cond.satisfy(lhs_v, rhs_v)) {
        satisfy = false;
        break;
      }
    }
    return satisfy;
  }

  Value aggregation_to_value(std::vector<RmRecord> records, ColMeta target_col) {
    AggregationType type = target_col.agg_type;
    std::string col_name = target_col.name;
    int length = target_col.len;
    Value val;
    switch (type) {
      case AggregationType::NO_AGG:
        if (records.empty()) {
          val.type = TYPE_EMPTY;
          val.init_raw(0);
          return val;
        }
        if (val.get_value_from_record(records[0], sel_cols_, col_name) == nullptr) {
          throw InternalError("target column not found.");
        }
        val.init_raw(length);
        return val;
      case AggregationType::SUM_AGG:
        if (records.empty()) {
          for (auto &col_tp : sel_cols_) {
            if (col_tp.name == col_name) {
              val.type = col_tp.type;
              break;
            }
          }
          if (val.type == TYPE_STRING) {
            throw InternalError("TYPE_STRING don't support sum aggregation.");
          } else if (val.type == TYPE_FLOAT) {
            val.set_float(0);
          } else {
            val.set_int(0);
          }
          val.init_raw(length);
          return val;
        }
        if (val.get_value_from_record(records[0], sel_cols_, col_name) == nullptr) {
          throw InternalError("target column not found.");
        }
        for (int i = 1; i < records.size(); i++) {
          Value tp;
          if (tp.get_value_from_record(records[i], sel_cols_, col_name) == nullptr) {
            throw InternalError("target column not found.");
          }
          val = val + tp;
        }
        val.init_raw(length);
        return val;
      case AggregationType::COUNT_AGG:
        val.set_int(records.size());
        val.init_raw(sizeof(int));
        return val;
      case AggregationType::MAX_AGG:
        if (records.empty()) {
          val.type = TYPE_EMPTY;
          val.init_raw(0);
          return val;
        }
        if (val.get_value_from_record(records[0], sel_cols_, col_name) == nullptr) {
          throw InternalError("target column not found.");
        }
        for (auto &rec : records) {
          Value tp;
          if (tp.get_value_from_record(rec, sel_cols_, col_name) == nullptr) {
            throw InternalError("target column not found.");
          }
          if (val < tp) {
            val = tp;
          }
        }
        val.init_raw(length);
        return val;
      case AggregationType::MIN_AGG:
        if (records.empty()) {
          val.type = TYPE_EMPTY;
          val.init_raw(0);
          return val;
        }
        if (val.get_value_from_record(records[0], sel_cols_, col_name) == nullptr) {
          throw InternalError("target column not found.");
        }
        for (auto &rec : records) {
          Value tp;
          if (tp.get_value_from_record(rec, sel_cols_, col_name) == nullptr) {
            throw InternalError("target column not found.");
          }
          if (val > tp) {
            val = tp;
          }
        }
        val.init_raw(length);
        return val;
      default:
        throw InternalError("unsupported aggregation operator.");
    }
  }

  std::string generate_new_name(TabCol col) {
    if (col.new_col_name != "") {
      return col.new_col_name;
    }
    switch (col.aggregation_type) {
      case MAX_AGG:
        return "MAX(" + col.col_name + ")";
      case MIN_AGG:
        return "MIN(" + col.col_name + ")";
      case COUNT_AGG:
        return "COUNT(" + col.col_name + ")";
      case SUM_AGG:
        return "SUM(" + col.col_name + ")";
      default:
        throw InternalError("unsupported aggregation type.");
    }
  }
};

}  // namespace easydb
