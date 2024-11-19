/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 *
 *-------------------------------------------------------------------------
 */

#include "execution/execution_manager.h"

// #include "execution/executor_delete.h"
// #include "execution/executor_insert.h"
// #include "execution/executor_update.h"

// #include "execution/executor_index_scan.h"
// #include "execution/executor_merge_join.h"
// #include "execution/executor_nestedloop_join.h"
// #include "execution/executor_projection.h"
#include "execution/executor_seq_scan.h"
#include "storage/index/ix.h"

namespace easydb {

const char *help_info =
    "Supported SQL syntax:\n"
    "  command ;\n"
    "command:\n"
    "  CREATE TABLE table_name (column_name type [, column_name type ...])\n"
    "  DROP TABLE table_name\n"
    "  CREATE INDEX table_name (column_name)\n"
    "  DROP INDEX table_name (column_name)\n"
    "  INSERT INTO table_name VALUES (value [, value ...])\n"
    "  DELETE FROM table_name [WHERE where_clause]\n"
    "  UPDATE table_name SET column_name = value [, column_name = value ...] [WHERE where_clause]\n"
    "  SELECT selector FROM table_name [WHERE where_clause]\n"
    "type:\n"
    "  {INT | FLOAT | CHAR(n)}\n"
    "where_clause:\n"
    "  condition [AND condition ...]\n"
    "condition:\n"
    "  column op {column | value}\n"
    "column:\n"
    "  [table_name.]column_name\n"
    "op:\n"
    "  {= | <> | < | > | <= | >=}\n"
    "selector:\n"
    "  {* | column [, column ...]}\n";

// 主要负责执行DDL语句
void QlManager::run_mutli_query(std::shared_ptr<Plan> plan, Context *context) {
  if (auto x = std::dynamic_pointer_cast<DDLPlan>(plan)) {
    switch (x->tag) {
      case T_CreateTable: {
        sm_manager_->create_table(x->tab_name_, x->cols_, context);
        break;
      }
      case T_DropTable: {
        sm_manager_->drop_table(x->tab_name_, context);
        break;
      }
      case T_CreateIndex: {
        sm_manager_->create_index(x->tab_name_, x->tab_col_names_, context);
        break;
      }
      case T_DropIndex: {
        sm_manager_->drop_index(x->tab_name_, x->tab_col_names_, context);
        break;
      }
      default:
        throw InternalError("Unexpected field type");
        break;
    }
  }
}

// 执行help; show tables; desc table; begin; commit; abort;语句
void QlManager::run_cmd_utility(std::shared_ptr<Plan> plan, txn_id_t *txn_id, Context *context) {
  if (auto x = std::dynamic_pointer_cast<OtherPlan>(plan)) {
    switch (x->tag) {
      case T_Help: {
        memcpy(context->data_send_ + *(context->offset_), help_info, strlen(help_info));
        *(context->offset_) = strlen(help_info);
        break;
      }
      case T_ShowTable: {
        sm_manager_->show_tables(context);
        break;
      }
      case T_ShowIndex: {
        sm_manager_->show_index(x->tab_name_, context);
        break;
      }
      case T_DescTable: {
        sm_manager_->desc_table(x->tab_name_, context);
        break;
      }
      case T_Transaction_begin: {
        // 显示开启一个事务
        context->txn_->set_txn_mode(true);
        break;
      }
      case T_Transaction_commit: {
        context->txn_ = txn_mgr_->get_transaction(*txn_id);
        txn_mgr_->commit(context->txn_, context->log_mgr_);
        break;
      }
      case T_Transaction_rollback: {
        context->txn_ = txn_mgr_->get_transaction(*txn_id);
        txn_mgr_->abort(context->txn_, context->log_mgr_);
        break;
      }
      case T_Transaction_abort: {
        context->txn_ = txn_mgr_->get_transaction(*txn_id);
        txn_mgr_->abort(context->txn_, context->log_mgr_);
        break;
      }
      case T_CreateStaticCheckpoint: {
        context->txn_ = txn_mgr_->get_transaction(*txn_id);
        txn_mgr_->create_static_checkpoint(context->txn_, context->log_mgr_);
        break;
      }
      default:
        throw InternalError("Unexpected field type");
        break;
    }

  } else if (auto x = std::dynamic_pointer_cast<SetKnobPlan>(plan)) {
    switch (x->set_knob_type_) {
      case ast::SetKnobType::EnableNestLoop: {
        planner_->set_enable_nestedloop_join(x->bool_value_);
        break;
      }
      case ast::SetKnobType::EnableSortMerge: {
        planner_->set_enable_sortmerge_join(x->bool_value_);
        break;
      }
      case ast::SetKnobType::EnableOutput: {
        sm_manager_->set_enable_output(x->bool_value_);
        break;
      }
      default: {
        throw RMDBError("Not implemented!\n");
        break;
      }
    }
  } else if (auto x = std::dynamic_pointer_cast<LoadDataPlan>(plan)) {
    // assert(x->tag == T_LoadData);
    sm_manager_->async_load_data(x->file_name_, x->tab_name_, context);
    // sm_manager_->load_data(x->file_name_, x->tab_name_, context);
  }
}

// 执行select语句，select语句的输出除了需要返回客户端外，还需要写入output.txt文件中
void QlManager::select_from(std::unique_ptr<AbstractExecutor> executorTreeRoot, std::vector<TabCol> sel_cols,
                            Context *context) {
  std::vector<std::string> captions;
  captions.reserve(sel_cols.size());
  for (auto &sel_col : sel_cols) {
    if (!sel_col.new_col_name.empty() || sel_col.aggregation_type != NO_AGG) {
      captions.push_back(sel_col.new_col_name);
    } else {
      captions.push_back(sel_col.col_name);
    }
  }

  // Print header into buffer
  RecordPrinter rec_printer(sel_cols.size());
  rec_printer.print_separator(context);
  rec_printer.print_record(captions, context);
  rec_printer.print_separator(context);
  // print header into file
  std::fstream outfile;
  outfile.open("output.txt", std::ios::out | std::ios::app);
  bool enable_output = sm_manager_->is_enable_output();
  bool print_caption = false;

  // Print records
  size_t num_rec = 0;
  // 执行query_plan
  for (executorTreeRoot->beginTuple(); !executorTreeRoot->is_end(); executorTreeRoot->nextTuple()) {
    auto Tuple = executorTreeRoot->Next();
    std::vector<std::string> columns;
    for (auto &col : executorTreeRoot->cols()) {
      std::string col_str;
      char *rec_buf = Tuple->data + col.offset;
      if (col.type == TYPE_INT) {
        col_str = std::to_string(*(int *)rec_buf);
      } else if (col.type == TYPE_FLOAT) {
        col_str = std::to_string(*(float *)rec_buf);
      } else if (col.type == TYPE_STRING) {
        col_str = std::string((char *)rec_buf, col.len);
        col_str.resize(strlen(col_str.c_str()));
      }
      columns.push_back(col_str);
    }
    if (!print_caption && enable_output) {
      outfile << "|";
      for (int i = 0; i < captions.size(); ++i) {
        outfile << " " << captions[i] << " |";
      }
      outfile << "\n";
      print_caption = true;
    }
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
    num_rec++;
  }
  if (!print_caption && enable_output) {
    outfile << "|";
    for (int i = 0; i < captions.size(); ++i) {
      outfile << " " << captions[i] << " |";
    }
    outfile << "\n";
  }
  outfile.close();
  // Print footer into buffer
  rec_printer.print_separator(context);
  // Print record count into buffer
  RecordPrinter::print_record_count(num_rec, context);
}

/*
execute select stmt in subquery

*/
std::vector<Value> subquery_select_from(std::shared_ptr<AbstractExecutor> executorTreeRoot, TabCol sel_col) {
  std::vector<Value> outputs;
  // 执行query_plan
  for (executorTreeRoot->beginTuple(); !executorTreeRoot->is_end(); executorTreeRoot->nextTuple()) {
    auto Tuple = executorTreeRoot->Next();
    Value output;
    std::vector<std::string> columns;
    if (executorTreeRoot->cols().size() != 1) {
      throw SubqueryIllegalError("subquery executorTreeRoot->cols().size() should be 1\n");
    }

    auto col = executorTreeRoot->cols()[0];
    std::string col_str;
    char *rec_buf = Tuple->data + col.offset;
    if (col.type == TYPE_INT) {
      output.set_int(*(int *)rec_buf);
    } else if (col.type == TYPE_FLOAT) {
      output.set_float(*(float *)rec_buf);
    } else if (col.type == TYPE_STRING) {
      col_str = std::string((char *)rec_buf, col.len);
      col_str.resize(strlen(col_str.c_str()));
      output.set_str(col_str);
    }
    outputs.push_back(output);
  }
  return outputs;
}

// 执行DML语句
void QlManager::run_dml(std::unique_ptr<AbstractExecutor> exec) { exec->Next(); }

}  // namespace easydb
