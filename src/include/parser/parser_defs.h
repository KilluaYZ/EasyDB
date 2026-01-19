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

/**
 * @brief Yacc/Bison语法分析器入口函数
 * @return 0表示成功，非0表示错误
 * @note 这是由yacc/bison生成的语法分析器函数
 */
int yyparse();

/**
 * @brief Yacc/Bison缓冲区状态类型
 * @note 用于管理词法分析器的输入缓冲区
 */
typedef struct yy_buffer_state *YY_BUFFER_STATE;

/**
 * @brief 从字符串创建词法分析器缓冲区
 * @param str 要解析的SQL字符串
 * @return 缓冲区状态指针
 * @note 用于将SQL字符串传递给词法分析器
 */
YY_BUFFER_STATE yy_scan_string(const char *str);

/**
 * @brief 删除词法分析器缓冲区
 * @param buffer 缓冲区状态指针
 * @note 释放缓冲区资源
 */
void yy_delete_buffer(YY_BUFFER_STATE buffer);
