/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * exception.h
 *
 * Identification: src/include/common/exception.h
 *
 *-------------------------------------------------------------------------
 */

/*
 * Original copyright:
 * Copyright (c) 2015-2019, Carnegie Mellon University Database Group
 */

#pragma once

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>

#include "type/type.h"

namespace easydb {

// TODO(WAN): the comment I added below is a lie, but you shouldn't need to poke
// around here. Don't worry about it.
//  Most of the exception types are type subsystem madness. I think we can get
//  rid of it at some point.
/**
 * @brief 异常类型枚举，定义了系统中所有可能抛出的异常类型
 * @note 大部分异常类型与类型子系统相关，未来可能会简化
 */
enum class ExceptionType {
  /** @brief 无效的异常类型 */
  INVALID = 0,
  /** @brief 值超出范围 */
  OUT_OF_RANGE = 1,
  /** @brief 转换/类型转换错误 */
  CONVERSION = 2,
  /** @brief 类型子系统中未知的类型 */
  UNKNOWN_TYPE = 3,
  /** @brief 十进制数相关的错误 */
  DECIMAL = 4,
  /** @brief 类型不匹配 */
  MISMATCH_TYPE = 5,
  /** @brief 除以零错误 */
  DIVIDE_BY_ZERO = 6,
  /** @brief 不兼容的类型 */
  INCOMPATIBLE_TYPE = 8,
  /** @brief 内存不足错误 */
  OUT_OF_MEMORY = 9,
  /** @brief 方法未实现 */
  NOT_IMPLEMENTED = 11,
  /** @brief 执行异常 */
  EXECUTION = 12,
};

/**
 * @brief 全局标志：是否禁用执行异常的打印输出
 * @note 设置为true时，执行器产生的异常不会被打印到控制台
 */
extern std::atomic<bool> global_disable_execution_exception_print;

/**
 * @brief 异常基类，继承自std::runtime_error
 *
 * Exception 类提供了统一的异常处理接口，支持不同类型的异常和错误消息。
 * 在DEBUG模式下，异常消息会被打印到标准错误输出。
 */
class Exception : public std::runtime_error {
 public:
  /**
   * @brief 构造一个新的异常实例（使用默认的INVALID类型）
   * @param message 异常消息
   * @param print 是否在DEBUG模式下打印异常消息（默认true）
   */
  explicit Exception(const std::string &message, bool print = true)
      : std::runtime_error(message), type_(ExceptionType::INVALID) {
#ifndef NDEBUG
    if (print) {
      std::string exception_message = "Message :: " + message + "\n";
      std::cerr << exception_message;
    }
#endif
  }

  /**
   * @brief 构造一个新的异常实例（指定异常类型）
   * @param exception_type 异常类型
   * @param message 异常消息
   * @param print 是否在DEBUG模式下打印异常消息（默认true）
   */
  Exception(ExceptionType exception_type, const std::string &message,
            bool print = true)
      : std::runtime_error(message), type_(exception_type) {
#ifndef NDEBUG
    if (print && !global_disable_execution_exception_print.load()) {
      std::string exception_message =
          "\nException Type :: " + ExceptionTypeToString(type_) +
          ", Message :: " + message + "\n\n";
      std::cerr << exception_message;
    }
#endif
  }

  /**
   * @brief 获取异常类型
   * @return 异常类型枚举值
   */
  auto GetType() const -> ExceptionType { return type_; }

  /**
   * @brief 将异常类型转换为人类可读的字符串
   * @param type 异常类型
   * @return 异常类型的字符串表示
   */
  static auto ExceptionTypeToString(ExceptionType type) -> std::string {
    switch (type) {
      case ExceptionType::INVALID:
        return "Invalid";
      case ExceptionType::OUT_OF_RANGE:
        return "Out of Range";
      case ExceptionType::CONVERSION:
        return "Conversion";
      case ExceptionType::UNKNOWN_TYPE:
        return "Unknown Type";
      case ExceptionType::DECIMAL:
        return "Decimal";
      case ExceptionType::MISMATCH_TYPE:
        return "Mismatch Type";
      case ExceptionType::DIVIDE_BY_ZERO:
        return "Divide by Zero";
      case ExceptionType::INCOMPATIBLE_TYPE:
        return "Incompatible type";
      case ExceptionType::OUT_OF_MEMORY:
        return "Out of Memory";
      case ExceptionType::NOT_IMPLEMENTED:
        return "Not implemented";
      case ExceptionType::EXECUTION:
        return "Execution";
      default:
        return "Unknown";
    }
  }

 private:
  /** @brief 异常类型 */
  ExceptionType type_;
};

/**
 * @brief 未实现异常类
 *
 * 当调用未实现的功能时抛出此异常。
 */
class NotImplementedException : public Exception {
 public:
  NotImplementedException() = delete;
  /**
   * @brief 构造未实现异常
   * @param msg 异常消息，描述未实现的功能
   */
  explicit NotImplementedException(const std::string &msg)
      : Exception(ExceptionType::NOT_IMPLEMENTED, msg) {}
};

/**
 * @brief 执行异常类
 *
 * 在执行查询或操作过程中发生错误时抛出此异常。
 */
class ExecutionException : public Exception {
 public:
  ExecutionException() = delete;
  /**
   * @brief 构造执行异常
   * @param msg 异常消息，描述执行过程中的错误
   */
  explicit ExecutionException(const std::string &msg)
      : Exception(ExceptionType::EXECUTION, msg, true) {}
};

}  // namespace easydb
