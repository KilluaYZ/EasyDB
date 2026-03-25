# 锁与并发控制（`src/concurrency/`）

## 职责

**`LockManager`** 实现 **两阶段封锁（2PL）** 及多种锁模式，协调多线程客户端对表/行/索引的并发访问。

## 锁模式（节选）

见 `lock_manager.h` 中 **`LockMode`**：

- **SHARED / EXCLUSIVE**：读写锁  
- **INTENTION_SHARED / INTENTION_EXCLUSIVE / SIX**：多粒度层次锁  
- **GAP**：索引间隙锁（防幻读）

**`GroupLockMode`** 描述加锁队列上**最强**的组锁类型，用于快速冲突检测。

## 数据结构

- **`LockRequest`**：`txn_id`、申请的 `LockMode`、`granted`  
- **`LockRequestQueue`**：每数据项一条 FIFO 队列 + `condition_variable`（wait-die 等策略）+ `group_lock_mode_`

## 死锁处理

注释与实现指向 **wait-die** 类策略：较老/较新事务等待或中止，避免循环等待。周期性与 **`cycle_detection_interval`**（`config.h`）相关。

## 与 `Transaction` 的交互

加锁成功后 **`LockDataId`** 记入事务的 **`lock_set_`**；提交或回滚时统一释放。

## 源码

- `src/concurrency/lock_manager.cpp`  
- `src/include/concurrency/lock_manager.h`

具体 API（`LockShared`、`LockExclusive`、行锁接口等）以头文件声明为准。
