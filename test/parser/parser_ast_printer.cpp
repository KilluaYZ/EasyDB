/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */
#include "parser/ast.h"
#undef NDEBUG

#include <cassert>

#include <unistd.h>
#include "parser/ast_dot_printer.h"
#include "parser/parser.h"

void print_help() { std::cout << "Usage ./parser_ast_printer -s <SQL> -o <outfile>" << std::endl; }

int main(int argc, char **argv) {
  std::string sql;
  std::string outfile;
  int opt;
  while ((opt = getopt(argc, argv, "s:o:h")) > 0) {
    switch (opt) {
      case 's':
        sql = optarg;
        break;
      case 'o':
        outfile = std::string(optarg);
        break;
      case 'h':
        print_help();
        exit(0);
      default:
        break;
    }
  }

  if (outfile.empty() || sql.empty()) {
    print_help();
    exit(1);
  }

  YY_BUFFER_STATE buf = yy_scan_string(sql.c_str());
  assert(yyparse() == 0);
  if (ast::parse_tree != nullptr) {
    ast::TreeDotPrinter tdp(outfile);
    tdp.print(ast::parse_tree);
    yy_delete_buffer(buf);
  } else {
    yy_delete_buffer(buf);
    std::cout << "exit/EOF" << std::endl;
  }
  ast::parse_tree.reset();
  return 0;
}
