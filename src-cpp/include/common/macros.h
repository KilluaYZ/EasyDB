/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * macros.h
 *
 * Identification: src/include/common/macros.h
 *
 *-------------------------------------------------------------------------
 */

/*
 * Original copyright:
 * Copyright (c) 2015-2019, Carnegie Mellon University Database Group
 */

#pragma once

#include <cassert>
#include <exception>
#include <optional>
#include <stdexcept>

namespace easydb {

/**
 * @brief 断言宏，用于调试时检查条件
 * @param expr 要检查的表达式
 * @param message 断言失败时显示的消息
 * @note 在DEBUG模式下，如果expr为false，程序会终止并显示message
 */
#define EASYDB_ASSERT(expr, message) assert((expr) && (message))

/**
 * @brief 未实现功能宏，抛出逻辑错误异常
 * @param message 描述未实现功能的消息
 * @note 用于标记尚未实现的功能
 */
#define UNIMPLEMENTED(message) throw std::logic_error(message)

/**
 * @brief 确保条件为真的宏，如果条件为假则终止程序
 * @param expr 要检查的表达式
 * @param message 条件为假时显示的错误消息
 * @note 与ASSERT不同，此宏在Release模式下也会执行
 */
#define EASYDB_ENSURE(expr, message)                  \
  if (!(expr)) {                                      \
    std::cerr << "ERROR: " << (message) << std::endl; \
    std::terminate();                                 \
  }

/**
 * @brief 不可达代码宏，表示不应该执行到的代码路径
 * @param message 描述为什么不应该到达此处的消息
 * @note 用于标记理论上不应该执行的代码，如果执行到会抛出异常
 */
#define UNREACHABLE(message) throw std::logic_error(message)

/**
 * @brief 禁用复制操作的宏
 * @param cname 类名
 * @note 将类的拷贝构造函数和拷贝赋值运算符设为delete
 */
#define DISALLOW_COPY(cname)     \
  cname(const cname &) = delete; \
  auto operator=(const cname &)->cname & = delete;

/**
 * @brief 禁用移动操作的宏
 * @param cname 类名
 * @note 将类的移动构造函数和移动赋值运算符设为delete
 */
#define DISALLOW_MOVE(cname) \
  cname(cname &&) = delete;  \
  auto operator=(cname &&)->cname & = delete;

/**
 * @brief 同时禁用复制和移动操作的宏
 * @param cname 类名
 * @note 同时调用DISALLOW_COPY和DISALLOW_MOVE
 */
#define DISALLOW_COPY_AND_MOVE(cname) \
  DISALLOW_COPY(cname);               \
  DISALLOW_MOVE(cname);

}  // namespace easydb
