/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "execution/executor_aggregation.h"

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

AggregationExecutor::AggregationExecutor(std::unique_ptr<AbstractExecutor> prev, std::vector<TabCol> sel_col_,
                                         std::vector<TabCol> group_cols, std::vector<Condition> having_conds) {
  prev_ = std::move(prev);
  prev_schema_ = prev_->schema();
  int offset = 0;
  std::vector<Column> new_colus_;
  for (auto &sel_col : sel_col_) {
    Column colu_tp;
    if (sel_col.col_name == "*" && sel_col.aggregation_type == AggregationType::COUNT_AGG) {
      // todo
      colu_tp = {.tab_name_ = "",
                 .column_name_ = "*",
                 .column_type_ = TYPE_INT,
                 .length_ = sizeof(int),
                 .column_offset_ = offset,
                 .agg_type = AggregationType::COUNT_AGG};
      sel_colus_.push_back(colu_tp);
      colu_tp.SetName(generate_new_name(sel_col));
      new_colus_.push_back(colu_tp);
      offset += sizeof(int);
      continue;
    }
    // col_tp = *get_col(prev_cols_, sel_col);

    colu_tp = prev_schema_.GetColumn(sel_col.col_name);

    colu_tp.SetAggregationType(sel_col.aggregation_type);
    sel_colus_.push_back(colu_tp);
    if (sel_col.aggregation_type != AggregationType::NO_AGG) {
      if (sel_col.aggregation_type == AggregationType::COUNT_AGG) {
        colu_tp.SetType(ColType::TYPE_INT);
        colu_tp.SetStorageSize(sizeof(int));
      } else if (sel_col.aggregation_type == AggregationType::SUM_AGG && colu_tp.GetType() == ColType::TYPE_STRING) {
        throw InternalError("string type do not supports sum aggreagation.");
      }
      colu_tp.SetName(generate_new_name(sel_col));
    }
    colu_tp.SetOffset(offset);
    offset += colu_tp.GetStorageSize();
    new_colus_.push_back(colu_tp);
  }
  schema_ = Schema(new_colus_);

  len_ = offset;
  key_length = 0;
  for (auto &col : group_cols) {
    Column colu_tp = prev_schema_.GetColumn(col.col_name);
    ;
    key_length += colu_tp.GetStorageSize();
    group_colus_.push_back(colu_tp);
  }
  having_conds_ = std::move(having_conds);

  for (prev_->beginTuple(); !prev_->IsEnd(); prev_->nextTuple()) {
    auto record_tp = *prev_->Next();
    all_records_.push_back(record_tp);
  }

  // isend_ = false;
  // traverse_idx = 0;
}

void AggregationExecutor::beginTuple() {
  // group by
  if (group_colus_.size() != 0) {
    // having group by statement
    for (auto ait = all_records_.begin(); ait != all_records_.end(); ait++) {
      std::string key = "";
      int offset = 0;
      for (auto &group_col : group_colus_) {
        key += std::string(ait->GetData() + group_col.GetOffset(), group_col.GetStorageSize());
        offset += group_col.GetStorageSize();
      }
      if (key_records_map_.find(key) == key_records_map_.end()) {
        // not grouped.
        std::vector<Tuple> tuple_tp;
        tuple_tp.push_back(*ait);
        key_records_map_[key] = tuple_tp;
      } else {
        key_records_map_[key].push_back(*ait);
      }
    }
  } else {
    std::string key_tp = "*";
    key_records_map_[key_tp] = std::move(all_records_);
  }
  it = key_records_map_.begin();
  while (!IsEnd() && !predicate(it->second)) {
    it++;
  }
}

void AggregationExecutor::nextTuple() {
  do {
    it++;
  } while (!IsEnd() && !predicate(it->second));
}

std::unique_ptr<Tuple> AggregationExecutor::Next() {
  std::vector<Tuple> records = it->second;
  char *data = new char[len_];
  int offset = 0;
  for (auto &sel_col : sel_colus_) {
    // if(sel_col.agg_type==AggregationType::NO_AGG && !in_groupby_cols(sel_col)){
    //     throw InternalError("only cols in \"group by\" statement can be projected without aggregation.");
    // }
    Value res = aggregation_to_value(records, sel_col);
    memcpy(data + offset, res.raw->data, res.raw->size);
    offset += sel_col.len;
  }
  return std::make_unique<RmRecord>(len_, data);
}

bool AggregationExecutor::in_groupby_cols(Column target) {
  auto pos = std::find_if(group_colus_.begin(), group_colus_.end(), [&](const ColMeta &col) {
    return col.tab_name == target.tab_name && col.name == target.name;
  });
  if (pos == group_colus_.end()) {
    return false;
  }
  return true;
}

bool AggregationExecutor::predicate(std::vector<RmRecord> records) {
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

Value AggregationExecutor::aggregation_to_value(std::vector<Tuple> records, Column target_colu) {
  AggregationType type = target_colu.GetAggregationType();
  std::string col_name = target_colu.GetName();
  int length = target_colu.GetStorageSize();
  Value val;
  switch (type) {
    case AggregationType::NO_AGG:
      if (records.empty()) {
        //   val.type = TYPE_EMPTY;
        //   val.init_raw(0);
        //   return val;
        return Value();
      }
      // if (val.get_value_from_record(records[0], sel_cols_, col_name) == nullptr) {
      //   throw InternalError("target column not found.");
      // }
      // val.init_raw(length);
      val = records[0].GetValue(prev_schema_, col_name);
      return val;
    case AggregationType::SUM_AGG:
      if (records.empty()) {
        for (auto &col_tp : sel_colus_) {
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
        // val.init_raw(length);
        return val;
      }
      val = records[0].GetValue(prev_schema_, col_name);

      for (int i = 1; i < records.size(); i++) {
        Value tp = records[i].GetValue(prev_schema_, col_name);
        val = val + tp;
      }
      // val.init_raw(length);
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
      val = records[0].GetValue(prev_schema_, col_name);
      for (auto &rec : records) {
        Value tp = rec.GetValue(prev_schema_, col_name);
        if (val < tp) {
          val = tp;
        }
      }
      return val;
    case AggregationType::MIN_AGG:
      if (records.empty()) {
        val.type = TYPE_EMPTY;
        val.init_raw(0);
        return val;
      }
      val = records[0].GetValue(prev_schema_, col_name);

      for (auto &rec : records) {
        Value tp = rec.GetValue(prev_schema_, col_name);
        if (val > tp) {
          val = tp;
        }
      }
      return val;
    default:
      throw InternalError("unsupported aggregation operator.");
  }
}

std::string AggregationExecutor::generate_new_name(TabCol col) {
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

}  // namespace easydb
