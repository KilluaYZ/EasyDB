/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * config.cpp
 *
 * Identification: src/common/config.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "common/config.h"

namespace easydb {

/**
 * @brief 日志功能的全局开关，默认关闭
 * @note 设置为true启用日志记录，false禁用日志记录
 */
std::atomic<bool> enable_logging(false);

/**
 * @brief 日志刷新到磁盘的超时时间，默认1秒
 * @note 系统每隔此时间间隔将日志缓冲区刷新到磁盘，确保数据持久性
 */
std::chrono::duration<int64_t> log_timeout = std::chrono::seconds(1);

/**
 * @brief 死锁检测的时间间隔，默认50毫秒
 * @note 系统每隔此时间间隔执行一次死锁检测，查找并处理事务等待图中的循环
 */
std::chrono::milliseconds cycle_detection_interval =
    std::chrono::milliseconds(50);

/**
 * @brief 全局标志：是否禁用执行异常的打印输出
 * @note 设置为true时，执行器产生的异常不会被打印到控制台
 */
std::atomic<bool> global_disable_execution_exception_print{false};

}  // namespace easydb
