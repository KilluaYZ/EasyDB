/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * config.h
 *
 * Identification: src/include/common/config.h
 *
 *-------------------------------------------------------------------------
 */

/*
 * Original copyright:
 * Copyright (c) 2015-2019, Carnegie Mellon University Database Group
 */

#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <string>
namespace easydb {

/**
 * @brief 死锁检测的时间间隔（毫秒）
 * @note 系统每隔此时间间隔执行一次死锁检测，查找事务等待图中的循环
 */
extern std::chrono::milliseconds cycle_detection_interval;

/**
 * @brief 是否启用日志记录功能
 * @note true表示启用日志记录，false表示禁用
 */
extern std::atomic<bool> enable_logging;

/**
 * @brief 日志刷新到磁盘的超时时间
 * @note 如果enable_logging为true，系统每隔此时间间隔将日志缓冲区刷新到磁盘
 */
extern std::chrono::duration<int64_t> log_timeout;

/** @brief 无效的帧ID常量 */
static constexpr int INVALID_FRAME_ID = -1;
/** @brief 无效的页面ID常量 */
static constexpr int INVALID_PAGE_ID = -1;
/** @brief 无效的事务ID常量 */
static constexpr int INVALID_TXN_ID = -1;
/** @brief 无效的日志序列号常量 */
static constexpr int INVALID_LSN = -1;

/** @brief 数据页的大小（字节），默认4096字节（4KB） */
static constexpr int PAGE_SIZE = 4096;
/** @brief 缓冲池的大小（帧的数量），默认1024个帧 */
static constexpr int BUFFER_POOL_SIZE = 1024;
/** @brief 数据库文件在磁盘上的初始大小（页数） */
static constexpr int DEFAULT_DB_IO_SIZE = 16;
/** @brief 日志缓冲区的大小（字节），等于(BUFFER_POOL_SIZE + 1) * PAGE_SIZE */
static constexpr int LOG_BUFFER_SIZE = ((BUFFER_POOL_SIZE + 1) * PAGE_SIZE);
/** @brief 可扩展哈希表的桶大小（页数） */
static constexpr int BUCKET_SIZE = 64;
// static constexpr int LRUK_REPLACER_K = 10;  // LRU-K替换器的k值（向后k距离）

/** @brief 帧ID的类型别名 */
using frame_id_t = int32_t;
/** @brief 页面ID的类型别名 */
using page_id_t = int32_t;
/** @brief 槽ID的类型别名（用于记录管理） */
using slot_id_t = uint32_t;
/** @brief 事务ID的类型别名 */
using txn_id_t = int64_t;
/** @brief 日志序列号（Log Sequence Number）的类型别名 */
using lsn_t = int32_t;
/** @brief 槽偏移量的类型别名 */
using slot_offset_t = size_t;
/** @brief 对象ID的类型别名 */
using oid_t = uint16_t;

/** @brief 第一个事务ID的起始值 */
const txn_id_t TXN_START_ID = 1LL << 62;

/** @brief VARCHAR类型的默认长度（字节），在构造列时使用 */
static constexpr int VARCHAR_DEFAULT_LENGTH = 128;

/** @brief 日志文件名 */
static const std::string LOG_FILE_NAME = "db.log";
/** @brief 重启文件名（用于恢复） */
static const std::string RESTART_FILE_NAME = "db.restart";

/** @brief 替换器类型字符串，当前使用"LRU" */
static const std::string REPLACER_TYPE = "LRU";
/** @brief 数据库元数据文件名 */
static const std::string DB_META_NAME = "db.meta";

/** @brief 默认数据库文件名 */
static const std::string DB_NAME = "test.db";

/** @brief 缓冲区长度（字节），用于各种缓冲区操作 */
static constexpr int BUFFER_LENGTH = 8192;

}  // namespace easydb
