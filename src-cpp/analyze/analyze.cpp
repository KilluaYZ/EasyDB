/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL
v2. You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "analyze/analyze.h"
#include "common/errors.h"
namespace easydb {

/**
 * @brief 语义分析器主入口函数
 * @description: 对解析器生成的AST进行语义分析和查询重写
 * 
 * 主要功能：
 * 1. 检查表、列是否存在
 * 2. 推断未指定的表名
 * 3. 处理SELECT *，展开为所有列
 * 4. 验证WHERE、GROUP BY、HAVING子句的合法性
 * 5. 检查聚合函数的使用是否符合SQL规范
 * 6. 将AST节点转换为Query对象，供后续查询计划生成使用
 * 
 * @param parse parser生成的AST根节点，可能是SelectStmt、UpdateStmt、DeleteStmt、InsertStmt等
 * @return Query 包含语义分析结果的Query对象，包含表名、列信息、条件等
 * @throws TableNotFoundError 如果表不存在
 * @throws ColumnNotFoundError 如果列不存在
 * @throws AmbiguousColumnError 如果列名在多表中存在且未指定表名
 * @throws AggregationIllegalError 如果聚合函数使用不合法
 */
std::shared_ptr<Query> Analyze::do_analyze(
    std::shared_ptr<ast::TreeNode> parse) {
  std::shared_ptr<Query> query = std::make_shared<Query>();
  
  // 处理LoadData语句：检查数据加载状态
  // 如果不是LoadData语句且当前没有正在加载的数据，则完成异步加载
  if (auto x = std::dynamic_pointer_cast<ast::LoadData>(parse)) {
    // LoadData语句，暂不处理
  } else {
    // 如果不是LoadData语句，检查是否需要完成异步数据加载
    if (sm_manager_->GetLoadStatus() == 0) {
      sm_manager_->AsyncLoadDataFinish();
    }
  }
  
  // ========== 处理SELECT语句 ==========
  if (auto x = std::dynamic_pointer_cast<ast::SelectStmt>(parse)) {
    // ========== 步骤1: 处理表名 ==========
    // 从AST中提取表名列表
    query->tables = std::move(x->tabs);
    
    // 检查每个表是否存在，如果不存在则抛出异常
    for (auto tab_name : query->tables) {
      if (!sm_manager_->db_.is_table(tab_name)) {
        throw TableNotFoundError(tab_name);
      }
    }

    // ========== 步骤2: 处理SELECT列表（target list）==========
    // 将AST中的列信息转换为Query中的TabCol结构，包含表名、列名、聚合类型等
    for (auto &sv_sel_col : x->cols) {
      TabCol sel_col = {.tab_name = sv_sel_col->tab_name,
                        .col_name = sv_sel_col->col_name,
                        .aggregation_type = sv_sel_col->aggregation_type,
                        .new_col_name = sv_sel_col->new_col_name};
      query->cols.push_back(sel_col);
    }

    // 获取所有表的列元数据信息，用于后续的列名推断和验证
    std::vector<ColMeta> all_cols;
    get_all_cols(query->tables, all_cols);
    
    // ========== 步骤3: 处理SELECT * ==========
    // 如果SELECT列表为空（即SELECT *），则展开为所有表的所有列
    if (query->cols.empty()) {
      // SELECT *：选择所有表的所有列
      for (auto &col : all_cols) {
        TabCol sel_col = {.tab_name = col.tab_name,
                          .col_name = col.name,
                          .aggregation_type = NO_AGG,
                          .new_col_name = ""};
        query->cols.push_back(sel_col);
      }
    } else {
      // ========== 步骤4: 列名验证和表名推断 ==========
      // 对于指定的列，检查列是否存在，并推断未指定的表名
      for (auto &sel_col : query->cols) {
        // 特殊处理：COUNT(*)的情况
        if (sel_col.col_name.empty() && sel_col.aggregation_type == COUNT_AGG) {
          sel_col.col_name = "*";
        } else {
          // 检查列是否存在，如果表名未指定则从列名推断表名
          // 如果列名在多表中存在且未指定表名，会抛出AmbiguousColumnError
          sel_col = check_column(all_cols, sel_col);
        }
      }
    }
    
    // ========== 步骤5: 处理WHERE条件 ==========
    // 将AST中的条件表达式转换为Condition结构
    get_clause(x->conds, query->conds);
    // 检查条件的合法性：列是否存在、类型是否兼容、子查询是否合法等
    check_clause(query->tables, query->conds);

    // ========== 步骤6: 处理GROUP BY子句 ==========
    if (x->has_group) {
      // 提取GROUP BY中的列信息
      for (auto &sv_groupby_col : x->group->cols) {
        TabCol groupby_col = {
            .tab_name = sv_groupby_col->tab_name,
            .col_name = sv_groupby_col->col_name,
            .aggregation_type = sv_groupby_col->aggregation_type,
            .new_col_name = sv_groupby_col->new_col_name};
        query->groupby_cols.push_back(groupby_col);
      }
      // 验证GROUP BY中的列是否存在
      for (auto &groupby_col : query->groupby_cols) {
        groupby_col = check_column(all_cols, groupby_col);
      }
    }

    // ========== 步骤7: 处理HAVING子句 ==========
    if (x->has_having) {
      // 将HAVING条件转换为Condition结构
      get_clause(x->having, query->having_conds);
      // 检查HAVING条件的合法性
      check_clause(query->tables, query->having_conds);
    }

    // ========== 步骤8: 检查聚合函数使用的合法性 ==========
    // 规则：
    // 1. SELECT列表中不能出现没有在GROUP BY子句中的非聚集列
    // 2. WHERE子句中不能用聚集函数作为条件表达式
    if (!check_aggregation_legality(x)) {
      throw AggregationIllegalError();
    }

  // ========== 处理UPDATE语句 ==========
  } else if (auto x = std::dynamic_pointer_cast<ast::UpdateStmt>(
                 parse)) {

    // 检查表是否存在
    if (!sm_manager_->db_.is_table(x->tab_name)) {
      throw TableNotFoundError(x->tab_name);
    }
    query->tables = {x->tab_name};

    // ========== 处理SET子句 ==========
    // UPDATE table SET col1 = value1, col2 = value2, ...
    // 支持两种形式：
    // 1. SET col = value（直接赋值）
    // 2. SET col = col2 op value（表达式赋值，如 col = col2 + 1）
    for (auto it : x->set_clauses) {
      SetClause set_clause;
      // 左侧：要更新的列
      set_clause.lhs = {.tab_name = x->tab_name, .col_name = it->col_name};
      
      // 检查右侧是否为表达式（如 col2 + 1）
      auto exp = it->rhs_expr;
      bool rhs_exp = false;
      if (exp != NULL) {
        // 右侧是表达式：col2 op value
        rhs_exp = true;
        set_clause.rhs_col = {.tab_name = x->tab_name, .col_name = exp->lhs};
        set_clause.op = convert_sv_arith_op(exp->op);  // 转换算术操作符
        set_clause.rhs = convert_sv_value(exp->rhs);   // 转换值
      } else {
        // 右侧是常量值
        set_clause.rhs = convert_sv_value(it->val);
      }
      set_clause.is_rhs_exp = rhs_exp;

      query->set_clauses.push_back(set_clause);
    }

    // ========== 处理WHERE条件 ==========
    get_clause(x->conds, query->conds);
    check_clause(query->tables, query->conds);

  // ========== 处理DELETE语句 ==========
  } else if (auto x = std::dynamic_pointer_cast<ast::DeleteStmt>(parse)) {
    // 提取表名
    query->tables = {x->tab_name};
    // 处理WHERE条件
    get_clause(x->conds, query->conds);
    check_clause({x->tab_name}, query->conds);
    
  // ========== 处理INSERT语句 ==========
  } else if (auto x = std::dynamic_pointer_cast<ast::InsertStmt>(parse)) {
    // 提取表名
    query->tables = {x->tab_name};
    // 处理INSERT的VALUES值列表
    // 将AST中的值节点转换为Value对象
    for (auto &sv_val : x->vals) {
      query->values.push_back(convert_sv_value(sv_val));
    }
  } else {
    // 其他类型的语句暂不处理
  }
  
  // 保存原始AST节点，供后续使用
  query->parse = std::move(parse);
  return query;
}

/**
 * @brief 检查列是否存在并推断表名
 * @description: 
 * 1. 如果表名未指定，从列名推断表名（要求列名在所有表中唯一）
 * 2. 如果表名已指定，验证列是否存在于该表中
 * 
 * @param all_cols 所有表的列元数据列表
 * @param target 要检查的列信息（可能缺少表名）
 * @return TabCol 填充了表名的列信息
 * @throws AmbiguousColumnError 如果列名在多表中存在且未指定表名
 * @throws ColumnNotFoundError 如果列不存在
 * @throws TableNotFoundError 如果指定的表不存在
 */
TabCol Analyze::check_column(const std::vector<ColMeta> &all_cols,
                             TabCol target) {
  if (target.tab_name.empty()) {
    // ========== 情况1: 表名未指定，需要从列名推断 ==========
    // 遍历所有列，查找匹配的列名
    std::string tab_name;
    for (auto &col : all_cols) {
      if (col.name == target.col_name) {
        // 如果找到多个匹配的列（在不同表中），说明列名有歧义
        if (!tab_name.empty()) {
          throw AmbiguousColumnError(target.col_name);
        }
        tab_name = col.tab_name;
      }
    }
    // 如果找不到匹配的列，说明列不存在
    if (tab_name.empty()) {
      throw ColumnNotFoundError(target.col_name);
    }
    // 填充推断出的表名
    target.tab_name = tab_name;
  } else {
    // ========== 情况2: 表名已指定，验证列是否存在 ==========
    // 首先检查表是否存在
    if (!sm_manager_->db_.is_table(target.tab_name)) {
      throw TableNotFoundError(target.tab_name);
    }
    // 在指定表中查找列
    for (auto &col : all_cols) {
      if (col.name == target.col_name && col.tab_name == target.tab_name) {
        // 找到匹配的列
        return target;
      }
    }
    // 列不存在于指定表中
    throw ColumnNotFoundError(target.col_name);
  }
  return target;
}

/**
 * @brief 获取所有表的列元数据
 * @description: 遍历指定的表列表，收集所有表的列信息到all_cols中
 * 
 * @param tab_names 表名列表
 * @param all_cols 输出参数，用于存储所有表的列元数据
 * @note 注意：这里使用db_而不是get_db()，因为需要传递指针引用
 */
void Analyze::get_all_cols(const std::vector<std::string> &tab_names,
                           std::vector<ColMeta> &all_cols) {
  // 遍历每个表，将其列信息追加到all_cols中
  for (auto &sel_tab_name : tab_names) {
    // 获取表的列元数据
    const auto &sel_tab_cols = sm_manager_->db_.get_table(sel_tab_name).cols;
    // 将当前表的列信息追加到all_cols末尾
    all_cols.insert(all_cols.end(), sel_tab_cols.begin(), sel_tab_cols.end());
  }
}

/**
 * @brief 将AST中的条件表达式转换为Condition结构
 * @description: 
 * 处理WHERE、HAVING等子句中的条件表达式，支持以下形式：
 * 1. 列与值的比较：column = value, column > 20 等
 * 2. 列与列的比较：column1 = column2
 * 3. 列与子查询的比较：column IN (SELECT ...), column > (SELECT ...)
 * 4. IN操作符：column IN (value1, value2, ...)
 * 
 * @param sv_conds AST中的条件表达式列表（BinaryExpr节点）
 * @param conds 输出参数，转换后的Condition列表
 */
void Analyze::get_clause(
    const std::vector<std::shared_ptr<ast::BinaryExpr>> &sv_conds,
    std::vector<Condition> &conds) {
  conds.clear();
  
  // 遍历每个条件表达式
  for (auto &expr : sv_conds) {
    Condition cond;
    
    // ========== 处理左侧操作数（列）==========
    cond.lhs_col = {.tab_name = expr->lhs->tab_name,
                    .col_name = expr->lhs->col_name,
                    .aggregation_type = expr->lhs->aggregation_type,
                    .new_col_name = expr->lhs->new_col_name};
    
    // ========== 转换比较操作符 ==========
    cond.op = convert_sv_comp_op(expr->op);
    cond.is_rhs_exe_processed = false;
    
    // ========== 处理右侧操作数 ==========
    // 情况1: 右侧为空，且操作符为IN，表示IN (value1, value2, ...)
    if (!expr->rhs) {
      // 只有IN操作符支持值列表
      if (cond.op != OP_IN) {
        throw SubqueryIllegalError("only allows tuples after IN");
      }
      // IN (value1, value2, ...) 形式
      cond.is_rhs_val = false;
      cond.is_rhs_stmt = true;
      cond.is_rhs_exe_processed = true;
      cond.rhs_stmt = nullptr;
      // 转换值列表中的每个值
      for (auto v : expr->rhs_value_list) {
        cond.rhs_in_col.push_back(convert_sv_value(v));
      }
    } 
    // 情况2: 右侧是常量值，如 column = 20
    else if (auto rhs_val =
                   std::dynamic_pointer_cast<ast::Value>(expr->rhs)) {
      cond.is_rhs_val = true;
      cond.is_rhs_stmt = false;
      cond.rhs_val = convert_sv_value(rhs_val);
    } 
    // 情况3: 右侧是列，如 column1 = column2
    else if (auto rhs_col = std::dynamic_pointer_cast<ast::Col>(expr->rhs)) {
      cond.is_rhs_val = false;
      cond.is_rhs_stmt = false;
      cond.rhs_col = {.tab_name = rhs_col->tab_name,
                      .col_name = rhs_col->col_name,
                      .aggregation_type = rhs_col->aggregation_type,
                      .new_col_name = rhs_col->new_col_name};
    } 
    // 情况4: 右侧是子查询，如 column > (SELECT AVG(salary) FROM employees)
    else if (auto rhs_stmt =
                   std::dynamic_pointer_cast<ast::SelectStmt>(expr->rhs)) {
      // 子查询必须只返回一列
      if (rhs_stmt->cols.size() != 1) {
        throw SubqueryIllegalError(
            "subquery should only return one column in comparison statement");
      }
      // 子查询必须指定表名
      if (rhs_stmt->tabs.empty()) {
        throw SubqueryIllegalError("Subquery should specify table name\n");
      }

      // ========== 子查询的两种使用场景 ==========
      
      // 场景A: 比较操作符（>, <, =等），子查询返回单个值
      // 例如: AVG(salary) > (SELECT AVG(salary) FROM employees)
      if (expr->op != ast::SV_OP_IN) {
        // 比较语句，子查询应该只返回一个值
        // 注意：如果子查询不使用聚合函数但只返回一个值也是合法的
        // 这个检查会延迟到plan_query阶段
        // 需要在执行器处理子查询后填充cond.rhs_val的真实值
        cond.is_rhs_val = true;
        cond.is_rhs_stmt = true;
        cond.rhs_val = init_sv_value(rhs_val);  // 初始化为空值，后续填充
        cond.rhs_col = {.tab_name = rhs_stmt->tabs[0],
                        .col_name = rhs_stmt->cols[0]->col_name,
                        .aggregation_type = rhs_stmt->cols[0]->aggregation_type,
                        .new_col_name = rhs_stmt->cols[0]->new_col_name};
      } 
      // 场景B: IN操作符，子查询返回一列
      // 例如: department_id IN (SELECT id FROM departments WHERE name = 'HR')
      else {
        // IN操作符，返回一列
        cond.is_rhs_val = false;
        cond.is_rhs_stmt = true;
        // 注意：比较的列是新生成的，不存在于当前表中
        cond.rhs_col = {.tab_name = rhs_stmt->tabs[0],
                        .col_name = rhs_stmt->cols[0]->col_name,
                        .aggregation_type = rhs_stmt->cols[0]->aggregation_type,
                        .new_col_name = rhs_stmt->cols[0]->new_col_name};
      }
      
      // 保存子查询AST节点，供后续处理
      // tab_name和col_name信息会在下一步check_clause中填充
      if (rhs_stmt) {
        cond.rhs_stmt = std::static_pointer_cast<void>(rhs_stmt);
      } else {
        throw NullptrError();
      }
    }  // end else if subquery
    
    // 将转换后的条件添加到列表
    conds.push_back(cond);
  }
}

// void Analyze::check_clause(const std::vector<std::string> &tab_names,
// std::vector<Condition> &conds) {
//     // auto all_cols = get_all_cols(tab_names);
//     std::vector<ColMeta> all_cols;
//     get_all_cols(tab_names, all_cols);
//     // Get raw values in where clause
//     for (auto &cond : conds) {
//         if(cond.lhs_col.col_name.empty() && cond.lhs_col.aggregation_type ==
//         COUNT_AGG){
//             //cond: having COUNT(*) < a
//             if(cond.lhs_col.col_name.empty() && cond.lhs_col.aggregation_type
//             == COUNT_AGG){
//                 cond.lhs_col.col_name = "*";
//                 if(!cond.is_rhs_val){
//                     throw AggregationIllegalError();
//                 }
//                 cond.rhs_val.init_raw(4);
//             }
//         }
//         else{
//             // Infer table name from column name
//             cond.lhs_col = check_column(all_cols, cond.lhs_col);
//             if (!cond.is_rhs_val) {
//                 cond.rhs_col = check_column(all_cols, cond.rhs_col);
//             }
//             TabMeta &lhs_tab =
//             sm_manager_->db_.get_table(cond.lhs_col.tab_name); auto lhs_col =
//             lhs_tab.get_col(cond.lhs_col.col_name); ColType lhs_type =
//             lhs_col->type; ColType rhs_type; if (cond.is_rhs_val) {
//                 cond.rhs_val.init_raw(lhs_col->len);
//                 rhs_type = cond.rhs_val.type;
//             } else {
//                 TabMeta &rhs_tab =
//                 sm_manager_->db_.get_table(cond.rhs_col.tab_name); auto
//                 rhs_col = rhs_tab.get_col(cond.rhs_col.col_name); rhs_type =
//                 rhs_col->type;
//             }
//             if (lhs_type != rhs_type && (lhs_type == TYPE_STRING || rhs_type
//             == TYPE_STRING )) {
//                 throw IncompatibleTypeError(coltype2str(lhs_type),
//                 coltype2str(rhs_type));
//             }
//         }
//     }
// }

/**
 * @brief 检查条件子句的合法性
 * @description: 
 * 对WHERE、HAVING等条件进行语义检查：
 * 1. 处理子查询：递归分析子查询AST，转换为Query对象
 * 2. 检查列是否存在：验证左侧和右侧列是否合法
 * 3. 推断表名：如果列名未指定表名，从上下文推断
 * 4. 类型兼容性检查：确保比较操作的两侧类型兼容
 * 
 * @param tab_names 当前查询涉及的表名列表
 * @param conds 条件列表（会被修改，填充表名等信息）
 */
void Analyze::check_clause(const std::vector<std::string> &tab_names,
                           std::vector<Condition> &conds) {
  // 获取所有表的列元数据，用于列名验证和推断
  std::vector<ColMeta> all_cols;
  get_all_cols(tab_names, all_cols);
  
  // 遍历每个条件进行检查
  for (auto &cond : conds) {
    // ========== 步骤1: 处理子查询 ==========
    // 如果右侧是子查询且尚未处理，则递归分析子查询
    if (cond.is_rhs_stmt && !cond.is_rhs_exe_processed) {
      // 将void*转换为SelectStmt
      auto select_stmt_shared_ptr =
          std::make_shared<ast::SelectStmt>(cond.rhs_stmt);
      // 转换为TreeNode类型，以便调用do_analyze
      auto tree_node_shared_ptr =
          std::static_pointer_cast<ast::TreeNode>(select_stmt_shared_ptr);
      // 递归分析子查询，生成Query对象
      auto subquery = do_analyze(tree_node_shared_ptr);
      // 将Query对象保存到条件中
      cond.rhs_stmt = std::static_pointer_cast<void>(subquery);
    }
    
    // ========== 步骤2: 处理COUNT(*)特殊情况 ==========
    // 检查左侧是否为COUNT(*)
    if (cond.lhs_col.col_name.empty() &&
        cond.lhs_col.aggregation_type == COUNT_AGG) {
      // 例如: HAVING COUNT(*) < a
      if (cond.lhs_col.col_name.empty() &&
          cond.lhs_col.aggregation_type == COUNT_AGG) {
        cond.lhs_col.col_name = "*";
        // COUNT(*)只能与值比较，不能与列比较
        if (!cond.is_rhs_val) {
          throw AggregationIllegalError();
        }
      }
    } else {
      // ========== 步骤3: 检查左侧列 ==========
      // 推断表名（如果未指定）并验证列是否存在
      cond.lhs_col = check_column(all_cols, cond.lhs_col);
      
      // ========== 步骤4: 检查右侧列（如果不是值）==========
      if (!cond.is_rhs_val) {
        if (!cond.is_rhs_stmt) {
          // 情况A: 右侧是列（非子查询），如 column1 = column2
          cond.rhs_col = check_column(all_cols, cond.rhs_col);
        } else if (cond.is_rhs_exe_processed) {
          // 情况B: 已处理的IN子查询元组，跳过
          continue;
        } else {
          // 情况C: 右侧是子查询语句，表名应该已经指定
          auto temp_rhs_stmt =
              std::static_pointer_cast<ast::SelectStmt>(cond.rhs_stmt);

          // 处理子查询中的COUNT(*)
          // 例如: WHERE a > (SELECT COUNT(*) FROM table)
          if (cond.rhs_col.col_name.empty() &&
              cond.rhs_col.aggregation_type == COUNT_AGG) {
            cond.rhs_col.col_name = "*";
            if (!cond.is_rhs_val) {
              throw AggregationIllegalError();
            }
          } else if (!cond.is_rhs_stmt) {
            // 例如: WHERE a > (SELECT MIN(val) FROM table)
            cond.rhs_col = check_column(all_cols, cond.rhs_col);
          }
        }
      }

      // ========== 步骤5: 类型兼容性检查 ==========
      // 获取左侧列的类型
      TabMeta &lhs_tab = sm_manager_->db_.get_table(cond.lhs_col.tab_name);
      auto lhs_col = lhs_tab.get_col(cond.lhs_col.col_name);
      ColType lhs_type = lhs_col->type;
      
      // 获取右侧的类型
      ColType rhs_type;
      if (cond.is_rhs_val) {
        // 右侧是值，获取值的类型
        rhs_type = cond.rhs_val.GetTypeId();
      } else {
        // 右侧是列，获取列的类型
        TabMeta &rhs_tab = sm_manager_->db_.get_table(cond.rhs_col.tab_name);
        auto rhs_col = rhs_tab.get_col(cond.rhs_col.col_name);
        rhs_type = rhs_col->type;
      }
      
      // 类型兼容性检查：字符串类型必须完全匹配
      // 例如：CHAR和VARCHAR不能与INT比较，但INT和FLOAT可以比较
      if (lhs_type != rhs_type &&
          (lhs_type == TYPE_CHAR || lhs_type == TYPE_VARCHAR ||
           rhs_type == TYPE_CHAR || rhs_type == TYPE_VARCHAR)) {
        throw IncompatibleTypeError(coltype2str(lhs_type),
                                    coltype2str(rhs_type));
      }
    }
  }
}

/**
 * @brief 在列列表中查找指定列名
 * @description: 线性搜索列列表，查找是否存在指定名称的列
 * 
 * @param cols 列列表
 * @param col_name 要查找的列名
 * @return true 如果找到，false 否则
 */
bool Analyze::find_col(std::vector<std::shared_ptr<ast::Col>> &cols,
                       std::string col_name) {
  for (auto col : cols) {
    if (col->col_name == col_name) {
      return true;
    }
  }
  return false;
}

/**
 * @brief 检查聚合函数使用的合法性
 * @description: 
 * 根据SQL规范检查聚合函数的使用是否符合规则：
 * 
 * 规则1: SELECT列表中不能出现没有在GROUP BY子句中的非聚集列
 *   - 如果使用了GROUP BY，SELECT中的非聚合列必须在GROUP BY中出现
 *   - 例如：SELECT name, COUNT(*) FROM students GROUP BY name; ✓
 *   - 例如：SELECT name, COUNT(*) FROM students GROUP BY age; ✗ (name不在GROUP BY中)
 * 
 * 规则2: WHERE子句中不能用聚集函数作为条件表达式
 *   - 聚合函数只能在SELECT、HAVING、ORDER BY中使用
 *   - 例如：WHERE age > 20; ✓
 *   - 例如：WHERE COUNT(*) > 10; ✗ (WHERE中不能使用聚合函数)
 * 
 * @param x SELECT语句的AST节点
 * @return true 如果合法，false 否则
 */
bool Analyze::check_aggregation_legality(
    const std::shared_ptr<ast::SelectStmt> &x) {
  // ========== 规则1: 检查SELECT列表中的非聚合列 ==========
  if (x->has_group) {
    auto group_cols = x->group->cols;
    for (auto &sel_col : x->cols) {
      // 如果是非聚合列，必须在GROUP BY中出现
      if (sel_col->aggregation_type == NO_AGG &&
          !find_col(group_cols, sel_col->col_name)) {
        return false;
      }
    }
  }
  
  // ========== 规则2: 检查WHERE子句中的聚合函数 ==========
  for (auto &cond : x->conds) {
    // WHERE子句中不能使用聚合函数
    if (cond->lhs->aggregation_type != NO_AGG) {
      return false;
    }
  }
  
  return true;
}
/**
 * @brief 将AST中的值节点转换为Value对象
 * @description: 根据AST值节点的类型（整数、浮点数、字符串），创建对应的Value对象
 * 
 * @param sv_val AST中的值节点，可能是IntLit、FloatLit或StringLit
 * @return Value 转换后的Value对象
 * @throws InternalError 如果遇到不支持的值类型
 */
Value Analyze::convert_sv_value(const std::shared_ptr<ast::Value> &sv_val) {
  Value val;
  
  // 整数字面量：如 20, 100
  if (auto int_lit = std::dynamic_pointer_cast<ast::IntLit>(sv_val)) {
    val = Value(TYPE_INT, int_lit->val);
  } 
  // 浮点数字面量：如 3.14, 2.5
  else if (auto float_lit =
                 std::dynamic_pointer_cast<ast::FloatLit>(sv_val)) {
    val = Value(TYPE_FLOAT, float_lit->val);
  } 
  // 字符串字面量：如 'hello', "world"
  else if (auto str_lit = std::dynamic_pointer_cast<ast::StringLit>(sv_val)) {
    val = Value(TYPE_VARCHAR, str_lit->val);
  } else {
    throw InternalError("Unexpected sv value type");
  }
  return val;
}

/**
 * @brief 初始化子查询的值（占位符）
 * @description: 
 * 对于子查询比较（如 column > (SELECT ...)），在语义分析阶段无法确定子查询的值
 * 因此先创建一个空的Value对象作为占位符
 * 实际的子查询值会在执行器处理子查询后填充
 * 
 * @param sv_val AST值节点（实际不使用）
 * @return Value 空的Value对象
 * @note 子查询的值会在执行阶段由执行器填充
 */
Value Analyze::init_sv_value(const std::shared_ptr<ast::Value> &sv_val) {
  Value val;
  // 返回一个空的Value对象，类型和值会在执行阶段填充
  return val;
}

/**
 * @brief 将AST中的比较操作符转换为Condition中的CompOp枚举
 * @description: 映射AST层的比较操作符到执行层的操作符枚举
 * 
 * @param op AST中的比较操作符（SvCompOp）
 * @return CompOp 执行层的比较操作符
 */
CompOp Analyze::convert_sv_comp_op(ast::SvCompOp op) {
  // 操作符映射表
  std::map<ast::SvCompOp, CompOp> m = {
      {ast::SV_OP_EQ, OP_EQ},   // ==
      {ast::SV_OP_NE, OP_NE},   // !=
      {ast::SV_OP_LT, OP_LT},   // <
      {ast::SV_OP_GT, OP_GT},   // >
      {ast::SV_OP_LE, OP_LE},   // <=
      {ast::SV_OP_GE, OP_GE},   // >=
      {ast::SV_OP_IN, OP_IN}};  // IN
  return m.at(op);
}

/**
 * @brief 将AST中的算术操作符转换为执行层的ArithOp枚举
 * @description: 映射AST层的算术操作符到执行层的操作符枚举
 * 
 * @param op AST中的算术操作符（SvArithOp）
 * @return ArithOp 执行层的算术操作符
 */
ArithOp Analyze::convert_sv_arith_op(ast::SvArithOp op) {
  // 算术操作符映射表
  std::map<ast::SvArithOp, ArithOp> m = {
      {ast::SV_OP_PLUS, OP_PLUS},    // +
      {ast::SV_OP_MINUS, OP_MINUS},  // -
      {ast::SV_OP_MUL, OP_MULTI},    // *
      {ast::SV_OP_DIV, OP_DIV}};     // /
  return m.at(op);
}
}  // namespace easydb