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

std::atomic<bool> enable_logging(false);

std::chrono::duration<int64_t> log_timeout = std::chrono::seconds(1);

std::chrono::milliseconds cycle_detection_interval = std::chrono::milliseconds(50);

std::atomic<bool> global_disable_execution_exception_print{false};

}  // namespace easydb
