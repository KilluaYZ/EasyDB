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

#include "common/config.h"
#include "defs.h"
#include "storage/disk/disk_manager.h"

#include <atomic>
#include <chrono>

namespace easydb {

/**
 * @brief 日志刷新超时时间（秒）
 * @note 日志缓冲区每隔此时间间隔刷新到磁盘
 */
static constexpr std::chrono::duration<int64_t> FLUSH_TIMEOUT = std::chrono::seconds(3);

/**
 * @brief 日志头部格式说明
 * 
 * 日志记录格式：
 * | log_type_ | lsn_ | log_tot_len_ | log_tid_ | prev_lsn_ | ...log_data... |
 * 
 * 各字段的偏移量定义如下：
 */

/** @brief log_type_在日志头部中的偏移量 */
static constexpr int OFFSET_LOG_TYPE = 0;

/** @brief lsn_在日志头部中的偏移量 */
static constexpr int OFFSET_LSN = sizeof(int);

/** @brief log_tot_len_在日志头部中的偏移量 */
static constexpr int OFFSET_LOG_TOT_LEN = OFFSET_LSN + sizeof(lsn_t);

/** @brief log_tid_在日志头部中的偏移量 */
static constexpr int OFFSET_LOG_TID = OFFSET_LOG_TOT_LEN + sizeof(uint32_t);

/** @brief prev_lsn_在日志头部中的偏移量 */
static constexpr int OFFSET_PREV_LSN = OFFSET_LOG_TID + sizeof(txn_id_t);

/** @brief 日志数据在日志记录中的偏移量（日志头部之后） */
static constexpr int OFFSET_LOG_DATA = OFFSET_PREV_LSN + sizeof(lsn_t);

/** @brief 日志头部的大小（字节数） */
static constexpr int LOG_HEADER_SIZE = OFFSET_LOG_DATA;

}  // namespace easydb
