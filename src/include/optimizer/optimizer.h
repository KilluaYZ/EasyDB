/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <map>

#include "common/context.h"
#include "common/errors.h"
#include "execution/execution_manager.h"
#include "parser/ast.h"
#include "parser/parser.h"
#include "planner/plan.h"
#include "planner/planner.h"
#include "record_printer.h"
#include "system/sm.h"
#include "transaction/transaction_manager.h"

namespace easydb {
class Optimizer {
 private:
  SmManager *sm_manager_;
  Planner *planner_;

 public:
  Optimizer(SmManager *sm_manager, Planner *planner) : sm_manager_(sm_manager), planner_(planner) {}

  std::shared_ptr<Plan> plan_query(std::shared_ptr<Query> query, Context *context) {
    if (auto x = std::dynamic_pointer_cast<ast::Help>(query->parse)) {
      // help;
      return std::make_shared<OtherPlan>(T_Help, std::string());
    } else if (auto x = std::dynamic_pointer_cast<ast::ShowTables>(query->parse)) {
      // show tables;
      return std::make_shared<OtherPlan>(T_ShowTable, std::string());
    } else if (auto x = std::dynamic_pointer_cast<ast::ShowIndex>(query->parse)) {
      // show index;
      return std::make_shared<OtherPlan>(T_ShowIndex, x->tab_name);
    } else if (auto x = std::dynamic_pointer_cast<ast::DescTable>(query->parse)) {
      // desc table;
      return std::make_shared<OtherPlan>(T_DescTable, x->tab_name);
    } else if (auto x = std::dynamic_pointer_cast<ast::TxnBegin>(query->parse)) {
      // begin;
      return std::make_shared<OtherPlan>(T_Transaction_begin, std::string());
    } else if (auto x = std::dynamic_pointer_cast<ast::TxnAbort>(query->parse)) {
      // abort;
      return std::make_shared<OtherPlan>(T_Transaction_abort, std::string());
    } else if (auto x = std::dynamic_pointer_cast<ast::TxnCommit>(query->parse)) {
      // commit;
      return std::make_shared<OtherPlan>(T_Transaction_commit, std::string());
    } else if (auto x = std::dynamic_pointer_cast<ast::TxnRollback>(query->parse)) {
      // rollback;
      return std::make_shared<OtherPlan>(T_Transaction_rollback, std::string());
    } else if (auto x = std::dynamic_pointer_cast<ast::SetStmt>(query->parse)) {
      // Set Knob Plan
      return std::make_shared<SetKnobPlan>(x->set_knob_type_, x->bool_val_);
    } else if (auto x = std::dynamic_pointer_cast<ast::CreateStaticCheckpoint>(query->parse)) {
      // create static_checkpoint;
      return std::make_shared<OtherPlan>(T_CreateStaticCheckpoint, std::string());
    } else if (auto x = std::dynamic_pointer_cast<ast::LoadData>(query->parse)) {
      // load file_name into table_name;
      return std::make_shared<LoadDataPlan>(T_LoadData, x->file_name, x->tab_name);
    } else {
      return planner_->do_planner(query, context);
    }
  }

  bool bypass(std::shared_ptr<Query> query, Context *context) {
    if (query->tables.size() == 1 && query->cols.size() == 1 && query->conds.empty()) {
      // bypass count(*) from table_name;
      if (query->cols[0].aggregation_type == COUNT_AGG) {
        int count = sm_manager_->GetTableCount(query->tables[0]);
        if (count != -1) {
          std::vector<std::string> captions;
          captions.push_back(query->cols[0].new_col_name);
          // Print header into buffer
          RecordPrinter rec_printer(1);
          rec_printer.print_separator(context);
          rec_printer.print_record(captions, context);
          rec_printer.print_separator(context);
          // print header into file
          std::fstream outfile;
          bool enable_output = sm_manager_->IsEnableOutput();
          if (enable_output) {
            outfile.open("output.txt", std::ios::out | std::ios::app);
            outfile << "|";
            for (int i = 0; i < captions.size(); ++i) {
              outfile << " " << captions[i] << " |";
            }
            outfile << "\n";
          }

          // Print records
          std::vector<std::string> columns;
          columns.push_back(std::to_string(count));
          // print record into buffer
          rec_printer.print_record(columns, context);
          // print record into file
          if (enable_output) {
            outfile << "|";
            for (int i = 0; i < columns.size(); ++i) {
              outfile << " " << columns[i] << " |";
            }
            outfile << "\n";
          }

          outfile.close();
          // Print footer into buffer
          rec_printer.print_separator(context);
          // Print record count into buffer
          RecordPrinter::print_record_count(1, context);
          return true;
        }
      }
    }
    return false;
  }
};
};  // namespace easydb