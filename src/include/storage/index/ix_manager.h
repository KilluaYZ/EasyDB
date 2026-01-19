/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * ix_manager.h
 *
 * Identification: src/include/storage/index/ix_manager.h
 *
 *-------------------------------------------------------------------------
 */

/**
 * @file ix_manager.h
 * @brief 索引管理器头文件
 * 
 * 该文件定义了 IxManager 类，负责管理数据库索引的创建、打开、关闭和销毁等操作。
 * 支持两种索引类型：
 * 1. B+树索引（B+ Tree Index）：用于范围查询和有序访问
 * 2. 可扩展哈希索引（Extendible Hash Index）：用于等值查询
 * 
 * 索引文件命名规则：{表名}_{列名1}_{列名2}_..._{列名n}.idx
 */

#pragma once

#include "storage/index/ix_defs.h"
#include "storage/index/ix_extendible_hash_index_handle.h"
#include "storage/index/ix_index_handle.h"
#include "system/sm_meta.h"

namespace easydb {

/**
 * @class IxManager
 * @brief 索引管理器类
 * 
 * 负责管理数据库索引文件的生命周期，包括：
 * - 创建索引文件（B+树索引和可扩展哈希索引）
 * - 打开索引文件并返回索引句柄
 * - 关闭索引文件并刷新缓冲区
 * - 销毁索引文件
 * - 检查索引文件是否存在
 * 
 * 索引文件结构：
 * - B+树索引：文件头页(0) + 叶子链表头页(1) + 根节点页(2) + 其他节点页...
 * - 可扩展哈希索引：文件头页(0) + 目录页(1) + 桶页(2,3,...) + 其他桶页...
 */
class IxManager {
 private:
  DiskManager *disk_manager_;          ///< 磁盘管理器指针，用于文件I/O操作
  BufferPoolManager *buffer_pool_manager_;  ///< 缓冲池管理器指针，用于页面缓存管理

 public:
  /**
   * @brief 构造函数
   * @param disk_manager 磁盘管理器指针
   * @param buffer_pool_manager 缓冲池管理器指针
   */
  IxManager(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager)
      : disk_manager_(disk_manager),
        buffer_pool_manager_(buffer_pool_manager) {}

  /**
   * @brief 根据表名和列名列表生成索引文件名
   * @param filename 表文件名（不含扩展名）
   * @param index_cols 索引列名列表
   * @return 索引文件名，格式为：{表名}_{列名1}_{列名2}_..._{列名n}.idx
   * 
   * 示例：表名为 "student"，索引列为 ["id", "name"]
   * 返回："student_id_name.idx"
   */
  std::string GetIndexName(const std::string &filename,
                           const std::vector<std::string> &index_cols) {
    std::string index_name = filename;
    for (size_t i = 0; i < index_cols.size(); ++i)
      index_name += "_" + index_cols[i];
    index_name += ".idx";

    return index_name;
  }

  /**
   * @brief 根据表名和列元数据列表生成索引文件名
   * @param filename 表文件名（不含扩展名）
   * @param index_cols 索引列元数据列表
   * @return 索引文件名，格式为：{表名}_{列名1}_{列名2}_..._{列名n}.idx
   * 
   * 重载版本，接受列元数据而非列名字符串
   */
  std::string GetIndexName(const std::string &filename,
                           const std::vector<ColMeta> &index_cols) {
    std::string index_name = filename;
    for (size_t i = 0; i < index_cols.size(); ++i)
      index_name += "_" + index_cols[i].name;
    index_name += ".idx";

    return index_name;
  }

  /**
   * @brief 检查索引文件是否存在
   * @param filename 表文件名（不含扩展名）
   * @param index_cols 索引列元数据列表
   * @return true 如果索引文件存在，false 否则
   */
  bool Exists(const std::string &filename,
              const std::vector<ColMeta> &index_cols) {
    auto ix_name = GetIndexName(filename, index_cols);
    return disk_manager_->IsFile(ix_name);
  }

  /**
   * @brief 检查索引文件是否存在（重载版本）
   * @param filename 表文件名（不含扩展名）
   * @param index_cols 索引列名列表
   * @return true 如果索引文件存在，false 否则
   */
  bool Exists(const std::string &filename,
              const std::vector<std::string> &index_cols) {
    auto ix_name = GetIndexName(filename, index_cols);
    return disk_manager_->IsFile(ix_name);
  }

  /**
   * @brief 创建B+树索引文件
   * @param filename 表文件名（不含扩展名）
   * @param index_cols 索引列元数据列表
   * @throw InvalidColLengthError 如果索引列总长度超过最大限制
   * 
   * 创建B+树索引文件的完整流程：
   * 1. 生成索引文件名并创建文件
   * 2. 计算B+树阶数（btree_order）
   * 3. 创建并写入文件头页（页号0）
   * 4. 创建并写入叶子链表头页（页号1）
   * 5. 创建并写入根节点页（页号2）
   * 
   * B+树节点容量计算：
   * - 理论上：|page_hdr| + (|attr| + |Rid|) * n <= PAGE_SIZE
   * - 实际预留一个槽位便于插入删除：|page_hdr| + (|attr| + |Rid|) * (n + 1) <= PAGE_SIZE
   * - btree_order = (PAGE_SIZE - sizeof(IxPageHdr)) / (col_tot_len + sizeof(RID)) - 1
   * - btree_order 表示每个节点最多可插入的键值对数量（实际还多留了一个空位，但不可插入）
   */
  void CreateIndex(const std::string &filename,
                   const std::vector<ColMeta> &index_cols) {
    std::string ix_name = GetIndexName(filename, index_cols);
    // 创建索引文件
    disk_manager_->CreateFile(ix_name);
    // 打开索引文件获取文件描述符
    int fd = disk_manager_->OpenFile(ix_name);

    // 计算索引列的总长度
    // 理论上：|page_hdr| + (|attr| + |Rid|) * n <= PAGE_SIZE
    // 但为了便于插入和删除操作，我们预留一个槽位，即：
    // |page_hdr| + (|attr| + |Rid|) * (n + 1) <= PAGE_SIZE
    int col_tot_len = 0;
    int col_num = index_cols.size();
    for (auto &col : index_cols) {
      col_tot_len += col.len;
    }
    // 检查索引列总长度是否超过最大限制
    if (col_tot_len > IX_MAX_COL_LEN) {
      throw InvalidColLengthError(col_tot_len);
    }
    // 根据 |page_hdr| + (|attr| + |Rid|) * (n + 1) <= PAGE_SIZE
    // 求得n的最大值btree_order，即 n <= btree_order
    // btree_order就是每个结点最多可插入的键值对数量（实际还多留了一个空位，但其不可插入）
    int btree_order = static_cast<int>(
        (PAGE_SIZE - sizeof(IxPageHdr)) / (col_tot_len + sizeof(RID)) - 1);
    // 确保B+树阶数大于2，否则无法形成有效的B+树结构
    assert(btree_order > 2);

    // 创建文件头并写入文件（页号0）
    // 参数说明：
    // - IX_NO_PAGE: 第一个空闲页号（初始为空）
    // - IX_INIT_NUM_PAGES: 初始页数（3页：文件头+叶子头+根节点）
    // - IX_INIT_ROOT_PAGE: 根节点页号（2）
    // - col_num: 索引列数量
    // - col_tot_len: 索引列总长度
    // - btree_order: B+树阶数
    // - (btree_order + 1) * col_tot_len: 键值对总大小
    // - IX_INIT_ROOT_PAGE: 首叶节点页号（初始为根节点）
    // - IX_INIT_ROOT_PAGE: 尾叶节点页号（初始为根节点）
    IxFileHdr *fhdr =
        new IxFileHdr(IX_NO_PAGE, IX_INIT_NUM_PAGES, IX_INIT_ROOT_PAGE, col_num,
                      col_tot_len, btree_order, (btree_order + 1) * col_tot_len,
                      IX_INIT_ROOT_PAGE, IX_INIT_ROOT_PAGE);
    // 设置索引列的类型和长度信息
    for (int i = 0; i < col_num; ++i) {
      fhdr->col_types_.push_back(index_cols[i].type);
      fhdr->col_lens_.push_back(index_cols[i].len);
    }
    // 更新文件头的总长度（包含动态数组的大小）
    fhdr->UpdateTotLen();

    // 序列化文件头并写入磁盘
    char *data = new char[fhdr->tot_len_];
    fhdr->Serialize(data);
    disk_manager_->WritePage(fd, IX_FILE_HDR_PAGE, data, fhdr->tot_len_);

    // 在内存中初始化页面缓冲区，然后将其写入磁盘
    char page_buf[PAGE_SIZE];
    memset(page_buf, 0, PAGE_SIZE);
    
    // 创建叶子链表头页并写入文件（页号1）
    // 注意：leaf header页号为1，也标记为叶子结点
    // 其前一个/后一个叶子均指向root node（初始状态下只有根节点一个叶子）
    {
      memset(page_buf, 0, PAGE_SIZE);
      auto phdr = reinterpret_cast<IxPageHdr *>(page_buf);
      *phdr = {
          .next_free_page_no = IX_NO_PAGE,  // 无下一个空闲页
          .parent = IX_NO_PAGE,              // 无父节点（叶子链表头是虚拟节点）
          .num_key = 0,                      // 无键值对
          .is_leaf = true,                   // 标记为叶子节点
          .prev_leaf = IX_INIT_ROOT_PAGE,   // 前一个叶子指向根节点
          .next_leaf = IX_INIT_ROOT_PAGE,   // 后一个叶子指向根节点
      };
      disk_manager_->WritePage(fd, IX_LEAF_HEADER_PAGE, page_buf, PAGE_SIZE);
    }
    
    // 创建根节点并写入文件（页号2）
    // 注意：root node页号为2，也标记为叶子结点（初始时根节点就是叶子节点）
    // 其前一个/后一个叶子均指向leaf header（形成循环链表）
    {
      memset(page_buf, 0, PAGE_SIZE);
      auto phdr = reinterpret_cast<IxPageHdr *>(page_buf);
      *phdr = {
          .next_free_page_no = IX_NO_PAGE,      // 无下一个空闲页
          .parent = IX_NO_PAGE,                  // 无父节点（根节点）
          .num_key = 0,                          // 无键值对（空索引）
          .is_leaf = true,                       // 标记为叶子节点
          .prev_leaf = IX_LEAF_HEADER_PAGE,     // 前一个叶子指向叶子链表头
          .next_leaf = IX_LEAF_HEADER_PAGE,     // 后一个叶子指向叶子链表头
      };
      // 必须写入完整的PAGE_SIZE，以便后续FetchNode()操作能正确读取
      disk_manager_->WritePage(fd, IX_INIT_ROOT_PAGE, page_buf, PAGE_SIZE);
    }

    // 设置文件描述符对应的页号（用于调试）
    disk_manager_->SetFd2Pageno(fd, IX_INIT_NUM_PAGES - 1);

    // 关闭索引文件
    disk_manager_->CloseFile(fd);

    // 释放临时分配的内存
    delete fhdr;
    delete[] data;
  }

  /**
   * @brief 创建可扩展哈希索引文件
   * @param filename 表文件名（不含扩展名）
   * @param index_cols 索引列元数据列表
   * @throw InvalidColLengthError 如果索引列总长度超过最大限制
   * 
   * 创建可扩展哈希索引文件的完整流程：
   * 1. 生成索引文件名并创建文件
   * 2. 计算索引列总长度并验证
   * 3. 创建并写入文件头页（页号0）
   * 4. 创建并写入初始桶页0（页号2）
   * 5. 创建并写入初始桶页1（页号3）
   * 6. 创建并写入目录页（页号1），包含指向两个初始桶的指针
   * 
   * 可扩展哈希索引结构：
   * - 目录页：存储指向桶页的指针数组，根据哈希值的高位进行索引
   * - 桶页：存储实际的键值对，当桶满时会进行分裂
   * - 初始状态：目录深度为1，有2个桶（对应哈希值的最高1位）
   */
  void CreateExtendibleHashIndex(const std::string &filename,
                                 const std::vector<ColMeta> &index_cols) {
    std::string ix_name = GetIndexName(filename, index_cols);
    // 创建索引文件
    disk_manager_->CreateFile(ix_name);
    // 打开索引文件获取文件描述符
    int fd = disk_manager_->OpenFile(ix_name);

    // 计算索引列的总长度
    // 理论上：|page_hdr| + (|attr| + |rid|) * n <= PAGE_SIZE
    // 但为了便于插入和删除操作，我们预留一个槽位，即：
    // |page_hdr| + (|attr| + |rid|) * (n + 1) <= PAGE_SIZE
    int col_tot_len = 0;
    int col_num = index_cols.size();
    for (auto &col : index_cols) {
      col_tot_len += col.len;
    }
    // 检查索引列总长度是否超过最大限制
    if (col_tot_len > IX_MAX_COL_LEN) {
      throw InvalidColLengthError(col_tot_len);
    }

    // 创建文件头并写入文件（页号0）
    // 参数说明：
    // - IX_INIT_HASH_FIRST_FREE_PAGES: 第一个空闲页号（初始为4）
    // - IX_INIT_HASH_NUM_PAGES: 初始页数（4页：文件头+目录+桶0+桶1）
    // - IX_INIT_DIRECTORY_PAGE: 目录页号（1）
    // - col_num: 索引列数量
    // - col_tot_len: 索引列总长度
    // - (BUCKET_SIZE + 1) * col_tot_len: 键值对总大小（预留一个槽位）
    ExtendibleHashIxFileHdr *fhdr = new ExtendibleHashIxFileHdr(
        IX_INIT_HASH_FIRST_FREE_PAGES, IX_INIT_HASH_NUM_PAGES,
        IX_INIT_DIRECTORY_PAGE, col_num, col_tot_len,
        (BUCKET_SIZE + 1) * col_tot_len);
    // 设置索引列的类型和长度信息
    for (int i = 0; i < col_num; ++i) {
      fhdr->col_types_.push_back(index_cols[i].type);
      fhdr->col_lens_.push_back(index_cols[i].len);
    }
    // 更新文件头的总长度（包含动态数组的大小）
    fhdr->update_tot_len();

    // 序列化文件头并写入磁盘
    char *data = new char[fhdr->tot_len_];
    fhdr->serialize(data);
    disk_manager_->WritePage(fd, IX_FILE_HDR_PAGE, data, fhdr->tot_len_);
    delete[] data;

    // 在内存中初始化页面缓冲区，然后将其写入磁盘
    char page_buf[PAGE_SIZE];
    memset(page_buf, 0, PAGE_SIZE);

    // 创建初始桶页0并写入文件（页号2）
    {
      memset(page_buf, 0, PAGE_SIZE);
      auto phdr = reinterpret_cast<IxExtendibleHashPageHdr *>(page_buf);
      *phdr = {
          .next_free_page_no = IX_NO_PAGE,  // 无下一个空闲页
          .is_valid = true,                  // 标记为有效桶
          .local_depth = 1,                  // 本地深度为1（初始状态）
          .key_nums = 0,                     // 无键值对（空桶）
          .size = BUCKET_SIZE                // 桶容量
      };
      // 必须写入完整的PAGE_SIZE，以便后续fetch_node()操作能正确读取
      disk_manager_->WritePage(fd, IX_INIT_BUCKET_0_PAGE, page_buf, PAGE_SIZE);
    }

    // 创建初始桶页1并写入文件（页号3）
    {
      memset(page_buf, 0, PAGE_SIZE);
      auto phdr = reinterpret_cast<IxExtendibleHashPageHdr *>(page_buf);
      *phdr = {
          .next_free_page_no = IX_NO_PAGE,  // 无下一个空闲页
          .is_valid = true,                  // 标记为有效桶
          .local_depth = 1,                  // 本地深度为1（初始状态）
          .key_nums = 0,                     // 无键值对（空桶）
          .size = BUCKET_SIZE                // 桶容量
      };
      // 必须写入完整的PAGE_SIZE，以便后续fetch_node()操作能正确读取
      disk_manager_->WritePage(fd, IX_INIT_BUCKET_1_PAGE, page_buf, PAGE_SIZE);
    }

    // 创建目录页并写入文件（页号1）
    // 目录页存储指向各个桶页的指针，初始状态有2个指针分别指向桶0和桶1
    {
      memset(page_buf, 0, PAGE_SIZE);
      auto phdr = reinterpret_cast<IxExtendibleHashPageHdr *>(page_buf);
      *phdr = {
          .next_free_page_no = IX_NO_PAGE,  // 无下一个空闲页
          .is_valid = true,                  // 标记为有效页
          .local_depth = -1,                 // 目录页的本地深度为-1（特殊标记）
          .key_nums = 2,                     // 目录中有2个条目（指向2个桶）
          .size = BUCKET_SIZE                // 目录大小
      };
      // 计算键值对在页面中的位置
      char *tp_keys = page_buf + sizeof(IxExtendibleHashPageHdr);
      RID *tp_rids = reinterpret_cast<RID *>(tp_keys + fhdr->keys_size_);
      // 设置目录条目0：指向桶页0（只有page_no有效，slot_num无效）
      tp_rids[0].Set(IX_INIT_BUCKET_0_PAGE, IX_NO_PAGE);
      // 设置目录条目1：指向桶页1（只有page_no有效，slot_num无效）
      tp_rids[1].Set(IX_INIT_BUCKET_1_PAGE, IX_NO_PAGE);
      // 必须写入完整的PAGE_SIZE，以便后续fetch_node()操作能正确读取
      disk_manager_->WritePage(fd, IX_INIT_DIRECTORY_PAGE, page_buf, PAGE_SIZE);
    }
    
    // 设置文件描述符对应的页号（用于调试）
    disk_manager_->SetFd2Pageno(fd, IX_INIT_HASH_NUM_PAGES - 1);
    delete fhdr;
    
    // 关闭索引文件
    disk_manager_->CloseFile(fd);
  }

  /**
   * @brief 销毁B+树索引文件
   * @param filename 表文件名（不含扩展名）
   * @param index_cols 索引列元数据列表
   * 
   * 从磁盘上删除索引文件，释放相关资源
   */
  void DestroyIndex(const std::string &filename,
                    const std::vector<ColMeta> &index_cols) {
    std::string ix_name = GetIndexName(filename, index_cols);
    disk_manager_->DestroyFile(ix_name);
  }

  /**
   * @brief 销毁B+树索引文件（重载版本）
   * @param filename 表文件名（不含扩展名）
   * @param index_cols 索引列名列表
   * 
   * 从磁盘上删除索引文件，释放相关资源
   */
  void DestroyIndex(const std::string &filename,
                    const std::vector<std::string> &index_cols) {
    std::string ix_name = GetIndexName(filename, index_cols);
    disk_manager_->DestroyFile(ix_name);
  }

  /**
   * @brief 打开B+树索引文件并返回索引句柄
   * @param filename 表文件名（不含扩展名）
   * @param index_cols 索引列元数据列表
   * @return 指向IxIndexHandle的unique_ptr，用于操作索引
   * 
   * 注意：这里打开文件，创建并返回了index file handle的指针
   * 调用者负责管理返回的unique_ptr的生命周期
   */
  std::unique_ptr<IxIndexHandle> OpenIndex(
      const std::string &filename, const std::vector<ColMeta> &index_cols) {
    std::string ix_name = GetIndexName(filename, index_cols);
    int fd = disk_manager_->OpenFile(ix_name);
    return std::make_unique<IxIndexHandle>(disk_manager_, buffer_pool_manager_,
                                           fd);
  }

  /**
   * @brief 打开B+树索引文件并返回索引句柄（重载版本）
   * @param filename 表文件名（不含扩展名）
   * @param index_cols 索引列名列表
   * @return 指向IxIndexHandle的unique_ptr，用于操作索引
   * 
   * 注意：这里打开文件，创建并返回了index file handle的指针
   * 调用者负责管理返回的unique_ptr的生命周期
   */
  std::unique_ptr<IxIndexHandle> OpenIndex(
      const std::string &filename, const std::vector<std::string> &index_cols) {
    std::string ix_name = GetIndexName(filename, index_cols);
    int fd = disk_manager_->OpenFile(ix_name);
    return std::make_unique<IxIndexHandle>(disk_manager_, buffer_pool_manager_,
                                           fd);
  }

  /**
   * @brief 打开可扩展哈希索引文件并返回索引句柄
   * @param filename 表文件名（不含扩展名）
   * @param index_cols 索引列元数据列表
   * @return 指向IxExtendibleHashIndexHandle的原始指针，用于操作索引
   * 
   * 注意：返回的是原始指针，调用者需要负责释放内存（使用delete）
   * 建议在使用完毕后调用CloseExtendibleHashIndex()来关闭索引
   */
  IxExtendibleHashIndexHandle *OpenExtendibleHashIndex(
      const std::string &filename, const std::vector<ColMeta> &index_cols) {
    std::string ix_name = GetIndexName(filename, index_cols);
    int fd = disk_manager_->OpenFile(ix_name);
    IxExtendibleHashIndexHandle *tp = new IxExtendibleHashIndexHandle(
        disk_manager_, buffer_pool_manager_, fd);
    return tp;
  }

  /**
   * @brief 关闭B+树索引文件
   * @param ih 索引句柄指针
   * 
   * 关闭索引文件的完整流程：
   * 1. 序列化文件头并写回磁盘（保存最新的元数据）
   * 2. 将缓冲池中该文件的所有页刷到磁盘（确保数据持久化）
   * 3. 关闭文件描述符
   * 
   * 注意：缓冲区的所有页刷到磁盘必须在close_file之前执行，
   * 否则可能导致数据丢失
   */
  void CloseIndex(const IxIndexHandle *ih) {
    // 序列化文件头并写回磁盘
    char *data = new char[ih->file_hdr_->tot_len_];
    ih->file_hdr_->Serialize(data);
    disk_manager_->WritePage(ih->fd_, IX_FILE_HDR_PAGE, data,
                             ih->file_hdr_->tot_len_);
    delete[] data;
    
    // 缓冲区的所有页刷到磁盘，注意这句话必须写在close_file前面
    // 这样可以确保所有修改过的页面都被持久化到磁盘
    buffer_pool_manager_->FlushAllPages(ih->fd_);
    
    // 关闭文件描述符
    disk_manager_->CloseFile(ih->fd_);
  }

  /**
   * @brief 关闭可扩展哈希索引文件
   * @param ih 可扩展哈希索引句柄指针
   * 
   * 关闭索引文件的完整流程：
   * 1. 序列化文件头并写回磁盘（保存最新的元数据）
   * 2. 将缓冲池中该文件的所有页刷到磁盘（确保数据持久化）
   * 3. 关闭文件描述符
   * 
   * 注意：缓冲区的所有页刷到磁盘必须在close_file之前执行，
   * 否则可能导致数据丢失
   */
  void CloseExtendibleHashIndex(const IxExtendibleHashIndexHandle *ih) {
    // 序列化文件头并写回磁盘
    char *data = new char[ih->file_hdr_->tot_len_];
    ih->file_hdr_->serialize(data);
    disk_manager_->WritePage(ih->fd_, IX_FILE_HDR_PAGE, data,
                             ih->file_hdr_->tot_len_);
    delete[] data;
    
    // 缓冲区的所有页刷到磁盘，注意这句话必须写在close_file前面
    // 这样可以确保所有修改过的页面都被持久化到磁盘
    buffer_pool_manager_->FlushAllPages(ih->fd_);
    
    // 关闭文件描述符
    disk_manager_->CloseFile(ih->fd_);
  }
};

}  // namespace easydb
