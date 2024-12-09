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

#include <cassert>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include "parser/ast.h"

namespace ast {

class TreeDotPrinter {
 public:
  std::ofstream *outfile = nullptr;
  TreeDotPrinter(std::string _file_name) { outfile = new std::ofstream(_file_name); }
  ~TreeDotPrinter() {
    if (outfile && outfile->is_open()) {
      outfile->close();
      delete outfile;
      outfile = nullptr;
    }
  }
  void print(const std::shared_ptr<TreeNode> &node) {
    *outfile << "digraph btree{" << std::endl;
    *outfile << "node[shape=record, style=bold];" << std::endl;
    *outfile << "edge[style=bold];" << std::endl;
    print_node(node, 0);
    *outfile << "}" << std::endl;
  }

 private:
  std::string offset2string(int offset) { return std::string(offset, ' '); }
  int node_cnt = 0;

  int alloc_node(std::string label) {
    int _node_id = node_cnt++;
    *outfile << get_name_by_id(_node_id) << "[label=\"" << label << "\"]" << std::endl;
    return _node_id;
  }

  void print_orderby(std::shared_ptr<OrderBy> order_by, int parent) {
    if (!order_by) return;
    int _node_id = alloc_node("Order_By");
    print_edge(_node_id, parent);
    print_val(order_by->cols->col_name, _node_id);
    std::string OrderDir = "ASC";
    if (order_by->orderby_dir == OrderBy_DESC) {
      OrderDir = "DESC";
    } else if (order_by->orderby_dir == OrderBy_DEFAULT) {
      OrderDir = "DEFAULT";
    }
    print_val(OrderDir, _node_id);
  }

  std::string get_name_by_id(int _node_id) {
    std::stringstream ss;
    ss << "node_" << _node_id;
    return ss.str();
  }

  template <typename T>
  void print_val(const T &val, int parent) {
    std::stringstream ss;
    ss << val;
    int val_node_id = alloc_node(ss.str());
    print_edge(val_node_id, parent);
  }

  void print_edge(int node_id, int parent_id) {
    *outfile << get_name_by_id(parent_id) << " : s -> " << get_name_by_id(node_id) << " : n " << std::endl;
  }

  template <typename T>
  void print_val_list(const std::vector<T> &vals, int parent) {
    // std::cout << offset2string(offset) << "LIST" << std::endl;
    // offset += 2;
    for (auto &val : vals) {
      print_val(val, parent);
    }
  }

  std::string type2str(SvType type) {
    std::map<SvType, std::string> m{
        {SV_TYPE_INT, "INT"},
        {SV_TYPE_FLOAT, "FLOAT"},
        {SV_TYPE_STRING, "STRING"},
    };
    return m.at(type);
  }

  std::string op2str(SvCompOp op) {
    std::map<SvCompOp, std::string> m{
        {SV_OP_EQ, "=="},   {SV_OP_NE, "!="},   {SV_OP_LT, "\\<"}, {SV_OP_GT, "\\>"},
        {SV_OP_LE, "\\<="}, {SV_OP_GE, "\\>="}, {SV_OP_IN, "IN"},
    };
    return m.at(op);
  }

  std::string arith_op2str(SvArithOp op) {
    static std::map<SvArithOp, std::string> m{
        {SV_OP_PLUS, "+"}, {SV_OP_MINUS, "-"}, {SV_OP_MUL, "*"}, {SV_OP_DIV, "/"}};
    return m.at(op);
  }

  template <typename T>
  void print_node_list(std::vector<T> nodes, int parent) {
    // std::cout << offset2string(offset);
    // offset += 2;
    // std::cout << "LIST" << std::endl;
    int _node_id = alloc_node("LIST");
    print_edge(_node_id, parent);
    for (auto &node : nodes) {
      print_node(node, _node_id);
    }
  }

  void print_node(const std::shared_ptr<TreeNode> &node, int parent) {
    if (auto x = std::dynamic_pointer_cast<Help>(node)) {
      // std::cout << "HELP" << std::endl;
      int _node_id = alloc_node("HELP");
      print_edge(_node_id, parent);
    } else if (auto x = std::dynamic_pointer_cast<ShowTables>(node)) {
      // std::cout << "SHOW_TABLES" << std::endl;
      int _node_id = alloc_node("SHOW_TABLES");
      print_edge(_node_id, parent);
    } else if (auto x = std::dynamic_pointer_cast<ShowIndex>(node)) {
      // std::cout << "SHOW_INDEX" << std::endl;
      int _node_id = alloc_node("SHOW_INDEX");
      print_edge(_node_id, parent);
      print_val(x->tab_name, _node_id);
    } else if (auto x = std::dynamic_pointer_cast<CreateTable>(node)) {
      // std::cout << "CREATE_TABLE" << std::endl;
      int _node_id = alloc_node("CREATE_TABLE");
      print_edge(_node_id, parent);
      print_val(x->tab_name, _node_id);
      print_node_list(x->fields, _node_id);
    } else if (auto x = std::dynamic_pointer_cast<DropTable>(node)) {
      // std::cout << "DROP_TABLE" << std::endl;
      int _node_id = alloc_node("DROP_TABLE");
      print_edge(_node_id, parent);
      print_val(x->tab_name, _node_id);
    } else if (auto x = std::dynamic_pointer_cast<DescTable>(node)) {
      // std::cout << "DESC_TABLE" << std::endl;
      int _node_id = alloc_node("DESC_TABLE");
      print_edge(_node_id, parent);
      print_val(x->tab_name, _node_id);
    } else if (auto x = std::dynamic_pointer_cast<CreateIndex>(node)) {
      // std::cout << "CREATE_INDEX" << std::endl;
      int _node_id = alloc_node("CREATE_INDEX");
      print_edge(_node_id, parent);
      print_val(x->tab_name, _node_id);
      for (auto col_name : x->col_names) print_val(col_name, _node_id);
    } else if (auto x = std::dynamic_pointer_cast<DropIndex>(node)) {
      // std::cout << "DROP_INDEX" << std::endl;
      int _node_id = alloc_node("DROP_INDEX");
      print_edge(_node_id, parent);
      print_val(x->tab_name, _node_id);
      for (auto col_name : x->col_names) print_val(col_name, _node_id);
    } else if (auto x = std::dynamic_pointer_cast<CreateStaticCheckpoint>(node)) {
      // std::cout << "CREATE_STATIC_CHECKPOINT\n";
      int _node_id = alloc_node("CREATE_STATIC_CHECKPOINT");
      print_edge(_node_id, parent);
    } else if (auto x = std::dynamic_pointer_cast<LoadData>(node)) {
      // std::cout << "LOAD_DATA\n";
      int _node_id = alloc_node("LOAD_DATA");
      print_edge(_node_id, parent);
      print_val(x->file_name, _node_id);
      print_val(x->tab_name, _node_id);
    } else if (auto x = std::dynamic_pointer_cast<ColDef>(node)) {
      // std::cout << "COL_DEF" << std::endl;
      int _node_id = alloc_node("COL_DEF");
      print_edge(_node_id, parent);
      print_val(x->col_name, _node_id);
      print_node(x->type_len, _node_id);
    } else if (auto x = std::dynamic_pointer_cast<Col>(node)) {
      // std::cout << "COL" << std::endl;
      int _node_id = alloc_node("COL");
      print_edge(_node_id, parent);
      // print_val(x->tab_name, _node_id);
      print_val(x->col_name, _node_id);
    } else if (auto x = std::dynamic_pointer_cast<TypeLen>(node)) {
      // std::cout << "TYPE_LEN" << std::endl;
      int _node_id = alloc_node("TYPE_LEN");
      print_edge(_node_id, parent);
      print_val(type2str(x->type), _node_id);
      print_val(x->len, _node_id);
    } else if (auto x = std::dynamic_pointer_cast<IntLit>(node)) {
      // std::cout << "INT_LIT" << std::endl;
      int _node_id = alloc_node("INT_LIT");
      print_edge(_node_id, parent);
      print_val(x->val, _node_id);
    } else if (auto x = std::dynamic_pointer_cast<FloatLit>(node)) {
      // std::cout << "FLOAT_LIT" << std::endl;
      int _node_id = alloc_node("FLOAT_LIT");
      print_edge(_node_id, parent);
      print_val(x->val, _node_id);
    } else if (auto x = std::dynamic_pointer_cast<StringLit>(node)) {
      // std::cout << "STRING_LIT" << std::endl;
      int _node_id = alloc_node("STRING_LIT");
      print_edge(_node_id, parent);
      print_val(x->val, _node_id);
    } else if (auto x = std::dynamic_pointer_cast<SetClause>(node)) {
      // std::cout << "SET_CLAUSE" << std::endl;
      int _node_id = alloc_node("SET_CLAUSE");
      print_edge(_node_id, parent);
      print_val(x->col_name, _node_id);
      if (x->val) {
        print_node(x->val, _node_id);
      } else if (x->rhs_expr) {
        print_node(x->rhs_expr, _node_id);
      }
    } else if (auto x = std::dynamic_pointer_cast<BinaryExpr>(node)) {
      // std::cout << "BINARY_EXPR" << std::endl;
      int _node_id = alloc_node("BINARY_EXPR");
      print_edge(_node_id, parent);
      print_node(x->lhs, _node_id);
      print_val(op2str(x->op), _node_id);
      if (x->rhs) {
        print_node(x->rhs, _node_id);
      } else {
        print_node_list(x->rhs_value_list, _node_id);
      }
    } else if (auto x = std::dynamic_pointer_cast<ArithExpr>(node)) {
      // std::cout << "Arith_EXPR" << std::endl;
      int _node_id = alloc_node("Arith_EXPR");
      print_edge(_node_id, parent);
      print_val(x->lhs, _node_id);
      print_val(arith_op2str(x->op), _node_id);
      print_node(x->rhs, _node_id);
    } else if (auto x = std::dynamic_pointer_cast<InsertStmt>(node)) {
      // std::cout << "INSERT" << std::endl;
      int _node_id = alloc_node("INSERT");
      print_edge(_node_id, parent);
      print_val(x->tab_name, _node_id);
      print_node_list(x->vals, _node_id);
    } else if (auto x = std::dynamic_pointer_cast<DeleteStmt>(node)) {
      // std::cout << "DELETE" << std::endl;
      int _node_id = alloc_node("DELETE");
      print_edge(_node_id, parent);
      print_val(x->tab_name, _node_id);
      print_node_list(x->conds, _node_id);
    } else if (auto x = std::dynamic_pointer_cast<UpdateStmt>(node)) {
      // std::cout << "UPDATE" << std::endl;
      int _node_id = alloc_node("UPDATE");
      print_edge(_node_id, parent);
      print_val(x->tab_name, _node_id);
      print_node_list(x->set_clauses, _node_id);
      print_node_list(x->conds, _node_id);
    } else if (auto x = std::dynamic_pointer_cast<SelectStmt>(node)) {
      // std::cout << "SELECT" << std::endl;
      int _node_id = alloc_node("SELECT");
      print_edge(_node_id, parent);
      if (x->is_unique) {
        print_val("UNIQUE", _node_id);
      }
      print_node_list(x->cols, _node_id);
      print_val_list(x->tabs, _node_id);
      print_node_list(x->conds, _node_id);
      if (x->order) {
        print_orderby(x->order, _node_id);
      }
    } else if (auto x = std::dynamic_pointer_cast<SetStmt>(node)) {
      // std::cout << "SET_STMT\n";
      int _node_id = alloc_node("SET_STMT");
      print_edge(_node_id, parent);
      print_val(x->set_knob_type_, _node_id);
      print_val(x->bool_val_, _node_id);
    } else if (auto x = std::dynamic_pointer_cast<TxnBegin>(node)) {
      // std::cout << "BEGIN" << std::endl;
      int _node_id = alloc_node("BEGIN");
      print_edge(_node_id, parent);
    } else if (auto x = std::dynamic_pointer_cast<TxnCommit>(node)) {
      // std::cout << "COMMIT" << std::endl;
      int _node_id = alloc_node("COMMIT");
      print_edge(_node_id, parent);
    } else if (auto x = std::dynamic_pointer_cast<TxnAbort>(node)) {
      // std::cout << "ABORT" << std::endl;
      int _node_id = alloc_node("ABORT");
      print_edge(_node_id, parent);
    } else if (auto x = std::dynamic_pointer_cast<TxnRollback>(node)) {
      // std::cout << "ROLLBACK" << std::endl;
      int _node_id = alloc_node("ROLLBACK");
      print_edge(_node_id, parent);
    } else {
      assert(0);
    }
  }
};

}  // namespace ast
