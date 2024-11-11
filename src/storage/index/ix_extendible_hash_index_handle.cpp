/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * ix_extendible_hash_index_handle.cpp
 *
 * Identification: src/storage/index/ix_extendible_hash_index_handle.cpp
 *
 *-------------------------------------------------------------------------
 */

#pragma once
#include "storage/index/ix_extendible_hash_index_handle.h"
#include <cmath>
#include "murmur3/MurmurHash3.h"
#include "storage/index/ix_defs.h"

namespace easydb {

/**
 * @brief 用于在结点中插入单个键值对。
 * 函数返回插入后的键值对数量
 *
 * @param (key, value) 要插入的键值对
 * @return int 键值对数量
 */
int IxBucketHandle::Insert(const char *key, const Rid &value) {
  std::cerr << "[INDEX ----- Insert] " << std::endl;
  int key_size = file_hdr->col_tot_len_;
  int pos = page_hdr->key_nums;

  // Insert new keys and RIDs
  memcpy(keys + pos * key_size, key, key_size);
  rids[pos] = value;

  // Update the number of keys in the bucket
  page_hdr->key_nums += 1;

  // Return the updated number of key-value pairs
  return page_hdr->key_nums;
}

/**
 * @brief 用于在结点中插入单个键值对。
 * 函数返回插入后的键值对数量
 *
 * @param pos 插入的位置
 * @param (key, value) 要插入的键值对
 * @return int 键值对数量
 */
int IxBucketHandle::Insert(int pos, const char *key, const Rid &value) {
  std::cerr << "[INDEX ----- Insert] pos = " << pos << std::endl;
  int key_size = file_hdr->col_tot_len_;

  // Insert new keys and RIDs
  memcpy(keys + pos * key_size, key, key_size);
  rids[pos] = value;

  // Update the number of keys in the bucket
  page_hdr->key_nums += 1;

  // Return the updated number of key-value pairs
  return page_hdr->key_nums;
}

void IxBucketHandle::Update(int old_idx, int new_idx) {
  int key_size = file_hdr->col_tot_len_;
  memcpy(keys + old_idx * key_size, keys + new_idx * key_size, key_size);
  memcpy(rids + old_idx, rids + new_idx, sizeof(Rid));
}

/**
 * @brief 用于在结点中删除所有key为指定key的键值对。函数返回删除后的键值对数量
 *
 * @param key 要删除的键值对key值
 * @return 完成删除操作后的键值对数量
 * @note 如果要删除的键值对不存在，则返回键值对数量不变
 */
int IxBucketHandle::Remove(const char *key) {
  // Find the position of the key-value pair to delete
  int cur_idx = 0;
  while (cur_idx < page_hdr->key_nums) {
    if (ix_compare(get_key(cur_idx), key, file_hdr->col_types_, file_hdr->col_lens_) == 0) {
      Reorganize(cur_idx);
      page_hdr->key_nums--;
    } else {
      cur_idx++;
    }
  }

  // Return the updated number of key-value pairs
  return page_hdr->key_nums;
}

/**
 * @brief 将pos后的键值对前移，覆盖第pos个键值对
 *
 * @param pos
 */
void IxBucketHandle::Reorganize(int pos) {
  int key_size = file_hdr->col_tot_len_;
  int key_nums_to_move = page_hdr->key_nums - pos - 1;
  memmove(keys + pos * key_size, keys + (pos + 1) * key_size, key_nums_to_move * key_size);
  memmove(rids + pos, rids + pos + 1, key_nums_to_move * sizeof(Rid));
}

/**
 * @brief 查找是否有key为指定key的键值对。
 *
 * @param key 要查找的键值对key值
 * @return 查找成功返回true，否则false
 */
bool IxBucketHandle::Find(const char *key) {
  // Find the the key-value pair
  for (int cur_idx = 0; cur_idx < page_hdr->key_nums; cur_idx++) {
    if (ix_compare(get_key(cur_idx), key, file_hdr->col_types_, file_hdr->col_lens_) == 0) {
      return true;
    }
  }
  return false;
}

/**
 * @brief 查找key为指定key的键值对。
 *
 * @param key 要查找的键值对key值
 * @param result 用于存放结果的容器
 *
 * @return 查找成功返回true，否则false
 */
int IxBucketHandle::Find(const char *key, std::vector<Rid> *result) {
  std::cerr << "[INDEX ----- Find] " << std::endl;
  // Find the the key-value pair
  bool find = false;
  for (int cur_idx = 0; cur_idx < page_hdr->key_nums; cur_idx++) {
    if (ix_compare(get_key(cur_idx), key, file_hdr->col_types_, file_hdr->col_lens_) == 0) {
      find = true;
      Rid *rid = get_rid(cur_idx);
      result->push_back(*rid);
    }
  }
  return find ? 1 : 0;
}

void IxBucketHandle::DoubleDirectory(int new_size) {
  int old_size = new_size / 2;
  std::cerr << "[INDEX ----- DoubleDirectory] old_size = " << old_size << " new_size = " << new_size << std::endl;
  int key_size = file_hdr->col_tot_len_;
  page_hdr->size = new_size;
  page_hdr->key_nums = new_size;
  // not neccessary
  // memcpy(keys + old_size * key_size, keys, key_size * (new_size - old_size));
  // double directory, the back half directory points to the original bucket.
  for (int i = old_size; i < new_size; i++) {
    rids[i] = rids[i - old_size];
  }

  // Rid *old_rids = rids;
  // // Double the size of the old rids
  // rids = new Rid[new_size];
  // for (int i = 0; i < old_size; i++) {
  //   rids[i] = old_rids[i];
  // }
  // // For the second half of the new rids
  // // set the pointers to the existing rids
  // for (int i = old_size; i < new_size; i++) {
  //   rids[i] = old_rids[i - old_size];
  // }
  // delete[] old_rids;
}

IxExtendibleHashIndexHandle::IxExtendibleHashIndexHandle(DiskManager *disk_manager,
                                                         BufferPoolManager *buffer_pool_manager, int fd)
    : disk_manager_(disk_manager), buffer_pool_manager_(buffer_pool_manager), fd_(fd) {
  // init file_hdr_
  // TOCHECK: no need to read file_hdr_ from disk?
  // disk_manager_->read_page(fd, IX_FILE_HDR_PAGE, (char *)&file_hdr_, sizeof(file_hdr_));
  char *buf = new char[PAGE_SIZE];
  memset(buf, 0, PAGE_SIZE);
  disk_manager_->ReadPage(fd, IX_FILE_HDR_PAGE, buf, PAGE_SIZE);
  file_hdr_ = new ExtendibleHashIxFileHdr();
  file_hdr_->deserialize(buf);
  delete[] buf;
  // disk_manager管理的fd对应的文件中，设置从file_hdr_->num_pages开始分配page_no
  int now_page_no = disk_manager_->GetFd2Pageno(fd);
  disk_manager_->SetFd2Pageno(fd, now_page_no + 1);
  global_depth = 1;
  size_per_bucket = BUCKET_SIZE;
}

IxExtendibleHashIndexHandle::~IxExtendibleHashIndexHandle() { delete file_hdr_; }

/**
 * @brief 用于查找指定键在桶中的对应的值result
 *
 * @param key 查找的目标key值
 * @param result 用于存放结果的容器
 * @return bool 返回目标键值对是否存在
 */
bool IxExtendibleHashIndexHandle::GetValue(const char *key, std::vector<Rid> *result) {
  std::cerr << "[INDEX ----- GetValue] " << std::endl;
  // Lock the root to prevent concurrent modifications
  std::scoped_lock lock{root_latch_};

  IxBucketHandle *target_bucket = FindBucketPage(key);
  int find = target_bucket->Find(key, result);
  buffer_pool_manager_->UnpinPage(target_bucket->get_page_id(), false);
  delete target_bucket;
  return find;
}

/**
 * @brief 将指定键值对插入到B+树中
 * @param (key, value) 要插入的键值对
 * @param transaction 事务指针
 * @return page_id_t 插入到的桶的page_no
 * @note 若插入成功，则返回插入到的桶的page_no；若插入失败(重复的key)，则返回-1
 */
page_id_t IxExtendibleHashIndexHandle::InsertEntry(const char *key, const Rid &value) {
  // std::scoped_lock lock{root_latch_};
  int index = HashFunction(key, global_depth);
  std::cerr << "[INDEX ----- InsertEntry] index = " << index << " rid = {" << value.page_no << "," << value.slot_no
            << "}" << std::endl;
  IxBucketHandle *target_bucket = FindBucketPage(index);
  if (!target_bucket->IsFull()) {
    // bucket is not full, just insert.
    target_bucket->Insert(key, value);
    buffer_pool_manager_->UnpinPage(target_bucket->get_page_id(), true);
    page_id_t res = target_bucket->get_page_no();
    delete target_bucket;
    return res;
  } else {
    int old_local_depth = target_bucket->GetLocalDepth();
    buffer_pool_manager_->UnpinPage(target_bucket->get_page_id(), true);
    delete target_bucket;
    int new_local_depth = old_local_depth + 1;
    if (global_depth == old_local_depth) {
      // Double the directory size
      DoubleDirectory();
      index = HashFunction(key, global_depth);
    }
    // If the global depth is greater
    // Get the bucket that needs to be split
    int original_index = HashFunction(key, old_local_depth);
    IxBucketHandle *original_bucket = FindBucketPage(original_index);
    original_bucket->IncrementLocalDepth();
    // Get the pointer in the directory that needs a new bucket
    // Create a new bucket with the bucket_size and the new local_depth
    // And assign the pointer to the new bucket based on the hash value
    int pair_index = original_index + pow(2, old_local_depth);
    IxBucketHandle *pair_bucket = CreateBucket(pair_index, key, new_local_depth);
    // Update pointers in the directory
    // UpdatePointers(pair_index, new_local_depth);
    // Distribute the values in the original bucket between the original and the new bucket
    SplitBucket(original_index);
    InsertEntry(key, value);
    // pair_bucket->Insert(key, value);
    buffer_pool_manager_->UnpinPage(original_bucket->get_page_id(), true);
    delete original_bucket;
    buffer_pool_manager_->UnpinPage(pair_bucket->get_page_id(), true);
    int tp_page_no = pair_bucket->get_page_no();
    delete pair_bucket;
    return tp_page_no;
  }
}

/**
 * @brief 用于删除B+树中含有指定key的键值对
 * @param key 要删除的key值
 * @param transaction 事务指针
 * @return bool 是否删除成功
 * @note TODO: 并发删除
 * @note TOCHECK: unpin_page before coalesce_or_redistribute
 */
bool IxExtendibleHashIndexHandle::DeleteEntry(const char *key) {
  std::cerr << "[INDEX ----- DeleteEntry] " << std::endl;
  std::scoped_lock lock{root_latch_};
  IxBucketHandle *target_bucket = FindBucketPage(key);
  target_bucket->Remove(key);
  buffer_pool_manager_->UnpinPage(target_bucket->get_page_id(), true);
  delete target_bucket;
  return true;
}

// /**
//  * @brief 这里把iid转换成了rid，即iid的slot_no作为bucket的rid_idx(key_idx)
//  * bucket其实就是把slot_no作为键值对数组的下标
//  * 换而言之，每个iid对应的索引槽存了一对(key,rid)，指向了(要建立索引的属性首地址,插入/删除记录的位置)
//  *
//  * @param iid
//  * @return Rid
//  * @note iid和rid存的不是一个东西，rid是上层传过来的记录位置，iid是索引内部生成的索引槽位置
//  */
// Rid IxExtendibleHashIndexHandle::GetRid(const Iid &iid) const {
//   IxBucketHandle *bucket = FetchBucket(iid.page_no);
//   if (iid.slot_no >= bucket->get_size()) {
//     throw IndexEntryNotFoundError();
//   }
//   buffer_pool_manager_->UnpinPage(bucket->get_page_id(), false);  // unpin it!
//   return *bucket->get_rid(iid.slot_no);
// }

/**
 * @brief 删除bucket时，更新file_hdr_.num_pages
 *
 * @param bucket
 */
void IxExtendibleHashIndexHandle::ReleaseBucketHandle(IxBucketHandle &bucket) { file_hdr_->num_pages_--; }

/**
 * @brief 删除buffer中的所有index page。
 */
bool IxExtendibleHashIndexHandle::Erase() {
  std::cerr << "[INDEX ----- Erase] " << std::endl;
  for (int page_no = 0; page_no < file_hdr_->num_pages_; page_no++) {
    PageId page_id = {fd_, page_no};
    while (buffer_pool_manager_->UnpinPage(page_id, true));
    buffer_pool_manager_->DeletePage(page_id);
  }
  return true;
}

/**
 * @brief 获取一个指定桶
 *
 * @param page_no
 * @return IxBucketHandle*
 * @note pin the page, remember to unpin it outside!
 */
IxBucketHandle *IxExtendibleHashIndexHandle::FetchBucket(int page_no) const {
  std::cerr << "[INDEX ----- FetchBucket] " << std::endl;
  Page *page = buffer_pool_manager_->FetchPage(PageId{fd_, page_no});
  if (page == nullptr) {
    // fail to fetch page.
    return nullptr;
  }
  IxBucketHandle *bucket = new IxBucketHandle(file_hdr_, page);

  return bucket;
}

/**
 * @brief 用于查找指定键所在的桶
 * @param key 要查找的目标key值
 * @note need to Unlatch and unpin the leaf bucket outside!
 * 注意：用了FindLeafPage之后一定要unlatch桶，否则下次latch该结点会堵塞！
 * @note TOCHECK: find_first, then root_is_latched
 */
IxBucketHandle *IxExtendibleHashIndexHandle::FindBucketPage(const char *key) {
  std::cerr << "[INDEX ----- FindBucketPage] " << std::endl;
  IxBucketHandle *directory_bucket = FetchBucket(file_hdr_->directory_page_);
  int index = HashFunction(key, global_depth);
  if (index >= directory_bucket->GetNumOfKeys()) {
    // invalid index
    return nullptr;
  }
  page_id_t target_page_no = directory_bucket->value_at(index);
  IxBucketHandle *current_bucket = FetchBucket(target_page_no);
  buffer_pool_manager_->UnpinPage(directory_bucket->get_page_id(), false);
  delete directory_bucket;
  return current_bucket;
}

/**
 * @brief 用于查找指定键所在的桶
 * @param index
 * @note need to Unlatch and unpin the leaf bucket outside!
 * 注意：用了FindLeafPage之后一定要unlatch桶，否则下次latch该结点会堵塞！
 * @note TOCHECK: find_first, then root_is_latched
 */
IxBucketHandle *IxExtendibleHashIndexHandle::FindBucketPage(int index) {
  std::cerr << "[INDEX ----- FindBucketPage] index = " << index << std::endl;
  IxBucketHandle *directory_bucket = FetchBucket(file_hdr_->directory_page_);
  if (index >= directory_bucket->GetNumOfKeys()) {
    // invalid index
    return nullptr;
  }
  page_id_t target_page_no = directory_bucket->value_at(index);
  IxBucketHandle *current_bucket = FetchBucket(target_page_no);
  buffer_pool_manager_->UnpinPage(directory_bucket->get_page_id(), false);
  delete directory_bucket;
  return current_bucket;
}

/**
 * @brief 创建一个新结点
 *
 * @return IxBucketHandle*
 * @note pin the page, remember to unpin it outside!
 * 注意：对于Index的处理是，删除某个页面后，认为该被删除的页面是free_page
 * 而first_free_page实际上就是最新被删除的页面，初始为IX_NO_PAGE
 * 在最开始插入时，一直是create bucket，那么first_page_no一直没变，一直是IX_NO_PAGE
 * 与Record的处理不同，Record将未插入满的记录页认为是free_page
 */
IxBucketHandle *IxExtendibleHashIndexHandle::CreateBucket(int index, const char *key, int new_local_depth) {
  std::cerr << "[INDEX ----- CreateBucket] index = " << index << " new_local_depth = " << new_local_depth << std::endl;
  IxBucketHandle *directory_bucket = FetchBucket(file_hdr_->directory_page_);

  IxBucketHandle *bucket;
  file_hdr_->num_pages_++;

  PageId new_page_id = {.fd = fd_, .page_no = INVALID_PAGE_ID};
  Page *page = buffer_pool_manager_->NewPage(&new_page_id);
  bucket = new IxBucketHandle(file_hdr_, page, new_local_depth, BUCKET_SIZE, false);
  Rid tmp;
  tmp.page_no = bucket->get_page_no();  // slot_number is not used
  directory_bucket->Insert(index, key, tmp);
  buffer_pool_manager_->UnpinPage(directory_bucket->get_page_id(), false);
  delete directory_bucket;
  return bucket;
}

// // Update pointers in the directory
// void IxExtendibleHashIndexHandle::UpdatePointers(int index, int new_local_depth) {
//   std::cerr << "[INDEX ----- UpdatePointers] index = " << index << " new_local_depth = " << new_local_depth
//             << std::endl;
//   // Get the number of entries in the directory
//   IxBucketHandle *directory_bucket = FetchBucket(file_hdr_->directory_page_);
//   int numOfEntries = pow(2, global_depth);
//   // Find the indices in the directory that have the same value with the index
//   // based on the rightmost n bitsq
//   for (int i = 0; i < numOfEntries; i++) {
//     if ( == index) {
//       directory_bucket->Update(i, index);
//     }
//   }
//   buffer_pool_manager_->UnpinPage(directory_bucket->get_page_id(), false);
//   delete directory_bucket;
// }

void IxExtendibleHashIndexHandle::DoubleDirectory() {
  std::cerr << "[INDEX ----- DoubleDirectory] global_depth = " << global_depth << std::endl;
  // Increment the global depth
  global_depth++;
  // The directory size should the 2 to the power of global depth
  int directory_size = pow(2, global_depth);
  // int old_size = directory_size / 2;
  IxBucketHandle *directory_bucket = FetchBucket(file_hdr_->directory_page_);
  directory_bucket->DoubleDirectory(directory_size);
  buffer_pool_manager_->UnpinPage(directory_bucket->get_page_id(), false);
  delete directory_bucket;
}

void IxExtendibleHashIndexHandle::SplitBucket(int original_index) {
  std::cerr << "[INDEX ----- SplitBucket] original_index = " << original_index << std::endl;
  IxBucketHandle *original_bucket = FindBucketPage(original_index);
  int numOfKeys = original_bucket->GetNumOfKeys();
  IxBucketHandle temp_bucket;
  memcpy(&temp_bucket, original_bucket, sizeof(temp_bucket));
  original_bucket->Clear();
  for (int i = 0; i < numOfKeys; i++) {
    InsertEntry(temp_bucket.get_key(i), *temp_bucket.get_rid(i));
  }
  delete original_bucket;
}

int IxExtendibleHashIndexHandle::HashFunction(const char *key, int n) {
  int key_size = file_hdr_->col_tot_len_;
  uint64_t hash[2];
  int a = 0;
  for (int i = 0; i < key_size; i++) {        // Correct: iterate over actual key size
    a += static_cast<unsigned char>(key[i]);  // Ensure proper casting
  }
  hash[0] = a;
  // Alternatively, use a proper hash function like MurmurHash3
  // MurmurHash3_x64_128(reinterpret_cast<const void *>(key), key_size, 0, reinterpret_cast<void *>(&hash));
  std::cerr << " hash[" << (*key - char(0)) << "]=" << hash[0] << " hash res = " << (hash[0] % (1 << n)) << std::endl;
  return hash[0] % (1 << n);

  // int key_size = file_hdr_->col_tot_len_;
  // uint64_t hash[2];
  // murmur3::MurmurHash3_x64_128(reinterpret_cast<const void *>(&key), key_size, 0, reinterpret_cast<void *>(&hash));
  // int hash_res = hash[0] % (1 << n);
  // std::cerr << " hash["<<(*key-char(0))<<"]=" << hash[0] << " hash res = " << hash_res << std::endl;
  // return hash[0] % (1 << n);
}

}  // namespace easydb
