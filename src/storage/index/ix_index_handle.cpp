/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * ix_index_handle.cpp
 *
 * Identification: src/storage/index/ix_index_handle.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "storage/index/ix_index_handle.h"
#include <memory>
#include "storage/index/ix_defs.h"

namespace easydb {

/**
 * @brief 在当前node中查找第一个>=target的key_idx
 *
 * @return key_idx，范围为[0,num_key)，如果返回的key_idx=num_key，则表示target大于最后一个key
 * @note 返回key index（同时也是rid index），作为slot no
 */
int IxNodeHandle::LowerBound(const char *target) const {
  // Todo:
  // 查找当前节点中第一个大于等于target的key，并返回key的位置给上层
  // 提示: 可以采用多种查找方式，如顺序遍历、二分查找等；使用ix_compare()函数进行比较
  // return -1;
  // return -1;

  // Binary search
  int left = 0;
  int right = page_hdr->num_key;
  while (left < right) {
    int mid = left + (right - left) / 2;
    int cmp = IxCompare(GetKey(mid), target, file_hdr->col_types_, file_hdr->col_lens_);
    if (cmp < 0) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }

  return left;
}

/**
 * @brief 在当前node中查找第一个>target的key_idx
 *
 * @return key_idx，范围为[1,num_key)，如果返回的key_idx=num_key，则表示target大于等于最后一个key
 * @note 注意此处的范围从1开始
 * @note 修改原有note，从0开始，因为可能第一个key > target
 */
int IxNodeHandle::UpperBound(const char *target) const {
  // Todo:
  // 查找当前节点中第一个大于target的key，并返回key的位置给上层
  // 提示: 可以采用多种查找方式：顺序遍历、二分查找等；使用ix_compare()函数进行比较
  // return -1;

  // Binary search
  int left = 0;
  int right = page_hdr->num_key;
  while (left < right) {
    int mid = left + (right - left) / 2;
    int cmp = IxCompare(GetKey(mid), target, file_hdr->col_types_, file_hdr->col_lens_);
    if (cmp <= 0) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }

  return left;
}

/**
 * @brief 用于叶子结点根据key来查找该结点中的键值对
 * 值value作为传出参数，函数返回是否查找成功
 *
 * @param key 目标key
 * @param[out] value 传出参数，目标key对应的Rid
 * @return 目标key是否存在
 */
bool IxNodeHandle::LeafLookup(const char *key, RID **value) {
  // Todo:
  // 1. 在叶子节点中获取目标key所在位置
  // 2. 判断目标key是否存在
  // 3. 如果存在，获取key对应的Rid，并赋值给传出参数value
  // 提示：可以调用lower_bound()和get_rid()函数。
  // return false;

  // 1. Get the position of the target key in the leaf node
  int pos = LowerBound(key);

  // 2. Check if the target key Exists
  if (pos < page_hdr->num_key && IxCompare(GetKey(pos), key, file_hdr->col_types_, file_hdr->col_lens_) == 0) {
    // 3. If it Exists, assign Rid to value
    *value = GetRid(pos);
    return true;
  }
  // return false;
  return false;
}

/**
 * 用于内部结点（非叶子节点）查找目标key所在的孩子结点（子树）
 * @param key 目标key
 * @return page_id_t 目标key所在的孩子节点（子树）的存储页面编号
 */
page_id_t IxNodeHandle::InternalLookup(const char *key) {
  // Todo:
  // 1. 查找当前非叶子节点中目标key所在孩子节点（子树）的位置
  // 2. 获取该孩子节点（子树）所在页面的编号
  // 3. 返回页面编号
  // return -1;

  // 1. Find the position of the target key
  int pos = UpperBound(key);
  // Decrement position to get the correct child node
  pos--;

  // 2. Get the child node's page ID
  page_id_t child_page_id = ValueAt(pos);

  return child_page_id;
}

/**
 * @brief 在指定位置插入n个连续的键值对
 * 将key的前n位插入到原来keys中的pos位置；将rid的前n位插入到原来rids中的pos位置
 *
 * @param pos 要插入键值对的位置
 * @param (key, rid) 连续键值对的起始地址，也就是第一个键值对，可以通过(key, rid)来获取n个键值对
 * @param n 键值对数量
 * @note 会更新当前节点的键数量(+=n)
 * @note [0,pos)           [pos,num_key)
 *                            key_slot
 *                            /      \
 *                           /        \
 *       [0,pos)     [pos,pos+n)   [pos+n,num_key+n)
 *                      key           key_slot
 */
void IxNodeHandle::InsertPairs(int pos, const char *key, const RID *rid, int n) {
  // Todo:
  // 1. 判断pos的合法性
  // 2. 通过key获取n个连续键值对的key值，并把n个key值插入到pos位置
  // 3. 通过rid获取n个连续键值对的rid值，并把n个rid值插入到pos位置
  // 4. 更新当前节点的键数量

  // 1. Validate the position
  if (pos < 0 || pos > page_hdr->num_key) {
    throw InternalError("IxNodeHandle::InsertPairs Error: Invalid position");
  }

  // 2. Shift existing keys and RIDs to make space for new pairs
  int key_size = file_hdr->col_tot_len_;
  int num_keys_to_move = page_hdr->num_key - pos;
  if (num_keys_to_move > 0) {
    memmove(keys + (pos + n) * key_size, keys + pos * key_size, num_keys_to_move * key_size);
    memmove(rids + pos + n, rids + pos, num_keys_to_move * sizeof(RID));
  }

  // 3. Insert new keys and RIDs
  for (int i = 0; i < n; ++i) {
    memcpy(keys + (pos + i) * key_size, key + i * key_size, key_size);
    rids[pos + i] = rid[i];
  }

  // 4. Update the number of keys in the node
  page_hdr->num_key += n;
}

/**
 * @brief 用于在结点中插入单个键值对。
 * 函数返回插入后的键值对数量
 *
 * @param (key, value) 要插入的键值对
 * @return int 键值对数量
 * @note 注意：如果key重复，则不会插入，返回键值对数量不变
 */
int IxNodeHandle::Insert(const char *key, const RID &value) {
  // Todo:
  // 1. 查找要插入的键值对应该插入到当前节点的哪个位置
  // 2. 如果key重复则不插入
  // 3. 如果key不重复则插入键值对
  // 4. 返回完成插入操作之后的键值对数量
  // return -1;

  // 1. Find the position where the key-value pair should be inserted
  int pos = LowerBound(key);

  // 2. Check for duplicate keys
  if (pos < page_hdr->num_key && IxCompare(GetKey(pos), key, file_hdr->col_types_, file_hdr->col_lens_) == 0) {
    // Key already Exists, do not Insert
    return page_hdr->num_key;
  }

  // 3. Insert the key-value pair
  InsertPairs(pos, key, &value, 1);

  // 4. Return the updated number of key-value pairs
  return page_hdr->num_key;
}

/**
 * @brief 用于在结点中的指定位置删除单个键值对
 *
 * @param pos 要删除键值对的位置
 */
void IxNodeHandle::ErasePair(int pos) {
  // Todo:
  // 1. 删除该位置的key
  // 2. 删除该位置的rid
  // 3. 更新结点的键值对数量

  // Validate the position
  if (pos < 0 || pos >= page_hdr->num_key) {
    throw InternalError("IxNodeHandle::ErasePair Error: Invalid position");
  }

  // Calculate the sizes
  int key_size = file_hdr->col_tot_len_;
  int num_keys_to_move = page_hdr->num_key - pos - 1;

  // Shift keys and rids to Remove the key-value pair at position pos
  if (num_keys_to_move > 0) {
    memmove(keys + pos * key_size, keys + (pos + 1) * key_size, num_keys_to_move * key_size);
    memmove(rids + pos, rids + pos + 1, num_keys_to_move * sizeof(RID));
  }

  // Update the number of keys in the node
  page_hdr->num_key--;
}

/**
 * @brief 用于在结点中删除指定key的键值对。函数返回删除后的键值对数量
 *
 * @param key 要删除的键值对key值
 * @return 完成删除操作后的键值对数量
 * @note 如果要删除的键值对不存在，则返回键值对数量不变
 */
int IxNodeHandle::Remove(const char *key) {
  // Todo:
  // 1. 查找要删除键值对的位置
  // 2. 如果要删除的键值对存在，删除键值对
  // 3. 返回完成删除操作后的键值对数量
  // return -1;

  // 1. Find the position of the key-value pair to delete
  int pos = LowerBound(key);

  // 2. Check if the key Exists at the found position
  if (pos < page_hdr->num_key && IxCompare(GetKey(pos), key, file_hdr->col_types_, file_hdr->col_lens_) == 0) {
    // Key Exists, Remove the key-value pair
    ErasePair(pos);
  }

  // 3. Return the updated number of key-value pairs
  return page_hdr->num_key;
}

IxIndexHandle::IxIndexHandle(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, int fd)
    : disk_manager_(disk_manager), buffer_pool_manager_(buffer_pool_manager), fd_(fd) {
  // init file_hdr_
  // TOCHECK: no need to read file_hdr_ from disk?
  // disk_manager_->read_page(fd, IX_FILE_HDR_PAGE, (char *)&file_hdr_, sizeof(file_hdr_));
  char *buf = new char[PAGE_SIZE];
  memset(buf, 0, PAGE_SIZE);
  disk_manager_->ReadPage(fd, IX_FILE_HDR_PAGE, buf, PAGE_SIZE);
  // file_hdr_ = new IxFileHdr();
  file_hdr_ = std::make_unique<IxFileHdr>();
  file_hdr_->Deserialize(buf);

  // disk_manager管理的fd对应的文件中，设置从file_hdr_->num_pages开始分配page_no
  int now_page_no = disk_manager_->GetFd2Pageno(fd);
  disk_manager_->SetFd2Pageno(fd, now_page_no + 1);

  delete[] buf;
}

/**
 * @brief 用于查找指定键所在的叶子结点
 * @param key 要查找的目标key值
 * @param operation 查找到目标键值对后要进行的操作类型
 * @param transaction 事务参数，如果不需要则默认传入nullptr
 * @return [leaf node] and [root_is_latched] 返回目标叶子结点以及根结点是否加锁
 * @note need to Unlatch and unpin the leaf node outside!
 * 注意：用了FindLeafPage之后一定要unlatch叶结点，否则下次latch该结点会堵塞！
 * @note TOCHECK: find_first, then root_is_latched
 */
std::pair<IxNodeHandle *, bool> IxIndexHandle::FindLeafPage(const char *key, Operation operation, bool find_first) {
  // Todo:
  // 1. 获取根节点
  // 2. 从根节点开始不断向下查找目标key
  // 3. 找到包含该key值的叶子结点停止查找，并返回叶子节点
  // return std::make_pair(nullptr, false);

  // 1. Fetch the root node
  IxNodeHandle *current_node = FetchNode(file_hdr_->root_page_);
  // bool root_is_latched = true;

  // 2. Traverse the tree from root to leaf
  while (!current_node->IsLeafPage()) {
    // Get the Next child page id
    page_id_t child_page_no = current_node->InternalLookup(key);

    // Unpin the current node before moving to the child
    buffer_pool_manager_->UnpinPage(current_node->GetPageId(), false);

    // free the last node
    if (current_node != nullptr) delete current_node;

    // Fetch the child node
    current_node = FetchNode(child_page_no);
  }

  // 3. Found the leaf node, return it
  // return std::make_pair(current_node, root_is_latched);
  return std::make_pair(current_node, find_first);
}

/**
 * @brief 用于查找指定键在叶子结点中的对应的值result
 *
 * @param key 查找的目标key值
 * @param result 用于存放结果的容器
 * @param transaction 事务指针
 * @return bool 返回目标键值对是否存在
 */
bool IxIndexHandle::GetValue(const char *key, std::vector<RID> *result) {
  // Todo:
  // 1. 获取目标key值所在的叶子结点
  // 2. 在叶子节点中查找目标key值的位置，并读取key对应的rid
  // 3. 把rid存入result参数中
  // 提示：使用完buffer_pool提供的page之后，记得unpin page；记得处理并发的上锁
  // return false;

  // Lock the root to prevent concurrent modifications
  std::scoped_lock lock{root_latch_};

  // 1. Find the leaf node containing the target key
  auto [leaf_node, root_is_latched] = FindLeafPage(key, Operation::FIND);

  if (leaf_node == nullptr) {
    return false;
  }

  // 2. Look up the key in the leaf node
  RID *Rid = nullptr;
  bool found = leaf_node->LeafLookup(key, &Rid);

  if (found) {
    // 3. Store the found Rid in the result vector
    result->push_back(*Rid);
  }

  // Unpin the leaf node pinned in find_leaf_page
  buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false);
  delete leaf_node;

  return found;
}

/**
 * @brief  将传入的一个node拆分(Split)成两个结点，在node的右边生成一个新结点new node
 * @param node 需要拆分的结点
 * @return 拆分得到的new_node
 * @note need to unpin the new node outside
 * 注意：本函数执行完毕后，原node和new node都需要在函数外面进行unpin
 * @note TOCHECK: 中文注意和note冲突！！！note才正确(unpin the new node outside)
 */
IxNodeHandle *IxIndexHandle::Split(IxNodeHandle *node) {
  // Todo:
  // 1. 将原结点的键值对平均分配，右半部分分裂为新的右兄弟结点
  //    需要初始化新节点的page_hdr内容
  // 2. 如果新的右兄弟结点是叶子结点，更新新旧节点的prev_leaf和next_leaf指针
  //    为新节点分配键值对，更新旧节点的键值对数记录
  // 3. 如果新的右兄弟结点不是叶子结点，更新该结点的所有孩子结点的父节点信息(使用IxIndexHandle::MaintainChild())
  // return nullptr;

  // 1. Create a new node(right sibling)
  IxNodeHandle *new_node = CreateNode();
  // Determine the Split point (half the number of keys)
  int total_keys = node->page_hdr->num_key;
  int split_point = total_keys / 2;
  int new_num = total_keys - split_point;
  // Initialize the new node's header
  new_node->page_hdr->next_free_page_no = node->page_hdr->next_free_page_no;  // unused
  new_node->SetParentPageNo(node->GetParentPageNo());
  new_node->SetSize(0);
  new_node->page_hdr->is_leaf = node->page_hdr->is_leaf;

  // Insert the right half of the keys and RIDs into the new node
  new_node->InsertPairs(0, node->GetKey(split_point), node->GetRid(split_point), new_num);
  // Update the original node's header
  node->SetSize(split_point);

  // 2. If the node is a leaf, update the leaf pointers
  if (node->IsLeafPage()) {
    new_node->SetNextLeaf(node->GetNextLeaf());
    new_node->SetPrevLeaf(node->GetPageNo());
    node->SetNextLeaf(new_node->GetPageNo());
    // Update the Next leaf node of the new node
    auto next_leaf = new_node->GetNextLeaf();
    if (next_leaf != IX_NO_PAGE) {
      IxNodeHandle *next_leaf_node = FetchNode(next_leaf);
      next_leaf_node->SetPrevLeaf(new_node->GetPageNo());
      buffer_pool_manager_->UnpinPage(next_leaf_node->GetPageId(), true);
      delete next_leaf_node;
    }
  } else {
    // 3. If the node is not a leaf, update the parent pointers of all children in the new node
    for (int i = 0; i < new_num; ++i) {
      MaintainChild(new_node, i);
    }
  }

  return new_node;
}

/**
 * @brief Insert key & value pair into internal page after Split
 * 拆分(Split)后，向上找到old_node的父结点
 * 将new_node的第一个key插入到父结点，其位置在 父结点指向old_node的孩子指针 之后
 * 如果插入后>=maxsize，则必须继续拆分父结点，然后在其父结点的父结点再插入，即需要递归
 * 直到找到的old_node为根结点时，结束递归（此时将会新建一个根R，关键字为key，old_node和new_node为其孩子）
 *
 * @param (old_node, new_node) 原结点为old_node，old_node被分裂之后产生了新的右兄弟结点new_node
 * @param key 要插入parent的key
 * @note 一个结点插入了键值对之后需要分裂，分裂后左半部分的键值对保留在原结点，在参数中称为old_node，
 * 右半部分的键值对分裂为新的右兄弟节点，在参数中称为new_node（参考Split函数来理解old_node和new_node）
 * @note 本函数执行完毕后，new node和old node都需要在函数外面进行unpin
 * @note TOCHECK: 注意！个人修改，外面无需 unpin
 * @note new_root 没有unpin, 且 old root(old_node) 会被调用者 unpin(FetchNode/find_leaf_page)
 */
void IxIndexHandle::InsertIntoParent(IxNodeHandle *old_node, const char *key, IxNodeHandle *new_node) {
  // Todo:
  // 1. 分裂前的结点（原结点, old_node）是否为根结点，如果为根结点需要分配新的root
  // 2. 获取原结点（old_node）的父亲结点
  // 3. 获取key对应的rid，并将(key, Rid)插入到父亲结点
  // 4. 如果父亲结点仍需要继续分裂，则进行递归插入
  // 提示：记得unpin page

  // 1. Check if old_node is the root
  if (old_node->IsRootPage()) {
    // Create a new root node
    IxNodeHandle *new_root = CreateNode();
    new_node->page_hdr->next_free_page_no = IX_NO_PAGE;  // unused
    new_root->SetParentPageNo(IX_NO_PAGE);               // New root has no parent
    new_root->SetSize(0);
    new_root->page_hdr->is_leaf = false;

    // Insert (old_node, new_node) into new root
    // Note: InsertPair will update num_key
    new_root->InsertPair(0, old_node->GetKey(0), {old_node->GetPageNo(), 0});
    new_root->InsertPair(1, new_node->GetKey(0), {new_node->GetPageNo(), 0});

    // Update old and new nodes to point to new root
    int new_root_page_no = new_root->GetPageNo();
    old_node->SetParentPageNo(new_root_page_no);
    new_node->SetParentPageNo(new_root_page_no);

    // Update root in file header
    file_hdr_->root_page_ = new_root_page_no;

    // // Unpin the new root
    // buffer_pool_manager_->UnpinPage(new_root->GetPageId(), true);
    delete new_root;
    return;
  }

  // 2. Get the parent node
  page_id_t parent_page_no = old_node->GetParentPageNo();
  IxNodeHandle *parent_node = FetchNode(parent_page_no);

  // 3. Insert (key, new_node) into parent node
  RID new_node_rid = {new_node->GetPageNo(), 0};
  int insert_pos = parent_node->FindChild(old_node) + 1;
  parent_node->InsertPair(insert_pos, key, new_node_rid);

  // 4. Check if the parent node needs to be Split
  if (parent_node->GetSize() >= parent_node->GetMaxSize()) {
    IxNodeHandle *new_sibling = Split(parent_node);
    char *middle_key = new_sibling->GetKey(0);
    InsertIntoParent(parent_node, middle_key, new_sibling);
    // unpin for Split: TOCHECK
    buffer_pool_manager_->UnpinPage(new_sibling->GetPageId(), true);
    delete new_sibling;
  }

  // Unpin the parent node
  buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
  delete parent_node;
}

/**
 * @brief 将指定键值对插入到B+树中
 * @param (key, value) 要插入的键值对
 * @param transaction 事务指针
 * @return page_id_t 插入到的叶结点的page_no
 * @note 若插入成功，则返回插入到的叶结点的page_no；若插入失败(重复的key)，则返回-1
 */
page_id_t IxIndexHandle::InsertEntry(const char *key, const RID &value) {
  // Todo:
  // 1. 查找key值应该插入到哪个叶子节点
  // 2. 在该叶子节点中插入键值对
  // 3. 如果结点已满，分裂结点，并把新结点的相关信息插入父节点
  // 提示：记得unpin page；若当前叶子节点是最右叶子节点，则需要更新file_hdr_.last_leaf；记得处理并发的上锁
  // return -1;

  std::scoped_lock lock{root_latch_};

  // 1. Find the leaf node where the key should be inserted
  auto [leaf_node, root_is_latched] = FindLeafPage(key, Operation::INSERT);

  if (leaf_node == nullptr) {
    throw InternalError("IxIndexHandle::insert_entry Error: Leaf node not found");
  }

  // 2. Insert the key-value pair into the leaf node
  int old_size = leaf_node->GetSize();
  int new_size = leaf_node->Insert(key, value);
  if (new_size == old_size) {
    // Key already Exists, return -1
    buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false);
    delete leaf_node;
    return -1;
  }

  // 3. Check if the leaf node needs to be Split
  if (new_size >= leaf_node->GetMaxSize()) {
    // Split the leaf node
    IxNodeHandle *new_sibling = Split(leaf_node);

    // Update the last_leaf pointer in the file header if necessary
    if (leaf_node->GetPageNo() == file_hdr_->last_leaf_) {
      file_hdr_->last_leaf_ = new_sibling->GetPageNo();
    }

    // Get the first key of the new sibling
    char *middle_key = new_sibling->GetKey(0);

    // Insert the middle key into the parent node
    InsertIntoParent(leaf_node, middle_key, new_sibling);

    // Unpin the new sibling pinned in 'Split'
    buffer_pool_manager_->UnpinPage(new_sibling->GetPageId(), true);
    delete new_sibling;
  }

  auto page_no = leaf_node->GetPageNo();

  // Unpin leaf node that was pinned in 'find_leaf_page'
  buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);

  delete leaf_node;

  if (file_hdr_->root_page_ == 4) {
    auto root = FetchNode(file_hdr_->root_page_);
    delete root;
  }
  return page_no;
}

/**
 * @brief 用于删除B+树中含有指定key的键值对
 * @param key 要删除的key值
 * @param transaction 事务指针
 * @return bool 是否删除成功
 * @note TODO: 并发删除
 * @note TOCHECK: unpin_page before CoalesceOrRedistribute
 */
bool IxIndexHandle::DeleteEntry(const char *key) {
  // Todo:
  // 1. 获取该键值对所在的叶子结点
  // 2. 在该叶子结点中删除键值对
  // 3. 如果删除成功需要调用CoalesceOrRedistribute来进行合并或重分配操作，并根据函数返回结果判断是否有结点需要删除
  // 4. 如果需要并发，并且需要删除叶子结点，则需要在事务的delete_page_set中添加删除结点的对应页面；记得处理并发的上锁
  // return false;

  std::scoped_lock lock{root_latch_};

  // 1. Find the leaf node where the key should be deleted
  auto [leaf_node, root_is_latched] = FindLeafPage(key, Operation::DELETE);

  if (leaf_node == nullptr) {
    // return false;
    throw InternalError("IxIndexHandle::delete_entry Error: Leaf node not found");
  }

  // 2. Delete the key-value pair from the leaf node
  int old_size = leaf_node->GetSize();
  int new_size = leaf_node->Remove(key);
  if (new_size == old_size) {
    // Key does not exist, return false
    buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false);
    return false;
  }

  // Unpin the leaf node handle(tocheck: leaf_node will be deleted below)
  buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);

  // 3. Coalesce or Redistribute the nodes if necessary
  bool should_delete_node = CoalesceOrRedistribute(leaf_node, &root_is_latched);

  // TODO: 4. Handle concurrent deletion and node removal if necessary
  if (should_delete_node) {
    ReleaseNodeHandle(*leaf_node);
  }

  return true;
}

/**
 * @brief 用于处理合并和重分配的逻辑，用于删除键值对后调用
 *
 * @param node 执行完删除操作的结点
 * @param transaction 事务指针
 * @param root_is_latched 传出参数：根节点是否上锁，用于并发操作
 * @return 是否需要删除结点
 * @note User needs to first find the sibling of input page.
 * If sibling's size + input page's size >= 2 * page's minsize, then Redistribute.
 * Otherwise, merge(Coalesce).
 * @note TOOPT: 1.2 如果传入del的位置(0 or size-1)，可以避免不必要的matain_parent操作
 */
bool IxIndexHandle::CoalesceOrRedistribute(IxNodeHandle *node, bool *root_is_latched) {
  // Todo:
  // 1. 判断node结点是否为根节点
  //    1.1 如果是根节点，需要调用AdjustRoot() 函数来进行处理，返回根节点是否需要被删除
  //    1.2 如果不是根节点，并且不需要执行合并或重分配操作，则直接返回false，否则执行2
  // 2. 获取node结点的父亲结点
  // 3. 寻找node结点的兄弟结点（优先选取前驱结点）
  // 4. 如果node结点和兄弟结点的键值对数量之和，能够支撑两个B+树结点（即node.size+neighbor.size >=
  // NodeMinSize*2)，则只需要重新分配键值对（调用Redistribute函数）
  // 5. 如果不满足上述条件，则需要合并两个结点，将右边的结点合并到左边的结点（调用Coalesce函数）
  // return false;

  // 1. Check if the node is the root
  if (node->IsRootPage()) {
    // 1.1 If it is the root, call AdjustRoot()
    return AdjustRoot(node);
  }
  // 1.2 If the node is not the root and does not need coalescing or redistribution, return false
  if (node->GetSize() >= node->GetMinSize()) {
    MaintainParent(node);
    return false;
  }

  // 2. Get the parent node
  page_id_t parent_page_no = node->GetParentPageNo();
  IxNodeHandle *parent_node = FetchNode(parent_page_no);

  // 3. Find the sibling node (prefer the predecessor node)
  int node_index = parent_node->FindChild(node);
  int sibling_index = (node_index == 0) ? 1 : node_index - 1;
  IxNodeHandle *sibling_node = FetchNode(parent_node->ValueAt(sibling_index));

  bool delete_node;
  // 4. Check if redistribution is possible
  if (node->GetSize() + sibling_node->GetSize() >= 2 * node->GetMinSize()) {
    Redistribute(sibling_node, node, parent_node, node_index);
    delete_node = false;
  } else {
    // 5. Otherwise, Coalesce the nodes
    bool del_parent = Coalesce(&sibling_node, &node, &parent_node, node_index, root_is_latched);
    // If Coalesce() returns true, it means the parent node also needs to be deleted
    // This can be reached by deleting a key in the parent node,
    // which also causes the parent to Coalesce.
    // A special case is when the parent node is the root node,
    // in which case a new root and the old root need to be deleted
    if (del_parent) {
      ReleaseNodeHandle(*parent_node);
    }
    delete_node = true;
  }

  // Unpin the parent and sibling nodes that were pinned in 'FetchNode'
  buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(sibling_node->GetPageId(), true);
  delete parent_node;
  delete sibling_node;

  return delete_node;
}

/**
 * @brief 用于当根结点被删除了一个键值对之后的处理
 * @param old_root_node 原根节点
 * @return bool 根结点是否需要被删除
 * @note size of root page can be less than min size and this method is only called within CoalesceOrRedistribute()
 */
bool IxIndexHandle::AdjustRoot(IxNodeHandle *old_root_node) {
  // Todo:
  // 1. 如果old_root_node是内部结点，并且大小为1，则直接把它的孩子更新成新的根结点
  // 2. 如果old_root_node是叶结点，且大小为0，则直接更新root page
  // 3. 除了上述两种情况，不需要进行操作
  // return false;

  // 1. If the old root node is an internal node and its size is 1,
  //    update its only child to become the new root node
  if (!old_root_node->IsLeafPage() && old_root_node->GetSize() == 1) {
    page_id_t new_root_page_no = old_root_node->ValueAt(0);
    IxNodeHandle *new_root_node = FetchNode(new_root_page_no);
    new_root_node->SetParentPageNo(IX_NO_PAGE);

    // Update the file header to point to the new root
    file_hdr_->root_page_ = new_root_page_no;

    // Unpin the new root node
    buffer_pool_manager_->UnpinPage(new_root_node->GetPageId(), true);
    delete new_root_node;

    return true;
  }

  // 2. If the old root node is a leaf node and its size is 0,
  //    update the root page to reflect the removal
  if (old_root_node->IsLeafPage() && old_root_node->GetSize() == 0) {
    file_hdr_->root_page_ = IX_NO_PAGE;

    return true;
  }

  // 3. For other cases, no adjustments are needed
  return false;
}

/**
 * @brief 重新分配node和兄弟结点neighbor_node的键值对
 * Redistribute key & value pairs from one page to its sibling page. If index == 0, move sibling page's first key
 * & value pair into end of input "node", otherwise move sibling page's last key & value pair into head of input "node".
 *
 * @param neighbor_node sibling page of input "node"
 * @param node input from method coalesceOrRedistribute()
 * @param parent the parent of "node" and "neighbor_node"
 * @param index node在parent中的rid_idx
 * @note node是之前刚被删除过一个key的结点
 * index=0，则neighbor是node后继结点，表示：node(left)      neighbor(right)
 * index>0，则neighbor是node前驱结点，表示：neighbor(left)  node(right)
 * 注意更新parent结点的相关kv对
 */
void IxIndexHandle::Redistribute(IxNodeHandle *neighbor_node, IxNodeHandle *node, IxNodeHandle *parent, int index) {
  // Todo:
  // 1. 通过index判断neighbor_node是否为node的前驱结点
  // 2. 从neighbor_node中移动一个键值对到node结点中
  // 3. 更新父节点中的相关信息，并且修改移动键值对对应孩字结点的父结点信息（maintain_child函数）
  // 注意：neighbor_node的位置不同，需要移动的键值对不同，需要分类讨论

  // neighbor_node -> node
  if (index > 0) {
    // neighbor_node is the predecessor
    // Move the last key-value pair from neighbor_node to the front of node
    int neighbor_last_index = neighbor_node->GetSize() - 1;

    // Insert the last key-value pair from neighbor_node to the front of node
    node->InsertPairs(0, neighbor_node->GetKey(neighbor_last_index), neighbor_node->GetRid(neighbor_last_index), 1);

    // Remove the last key-value pair from neighbor_node
    neighbor_node->ErasePair(neighbor_last_index);

    // Update the key in the parent node
    MaintainParent(node);
    // // This below line is not enough because of cascading updates
    // char *parent_key = node->GetKey(0);
    // parent->SetKey(index, parent_key);

    // Update the parent pointer of the affected child node
    MaintainChild(node, 0);
  } else {
    // neighbor_node is the successor(node -> neighbor_node)
    // Move the first key-value pair from neighbor_node to the end of node
    char *neighbor_first_key = neighbor_node->GetKey(0);
    RID neighbor_first_rid = *neighbor_node->GetRid(0);

    // Insert the first key-value pair from neighbor_node to the end of node
    node->InsertPairs(node->GetSize(), neighbor_first_key, &neighbor_first_rid, 1);

    // Remove the first key-value pair from neighbor_node
    neighbor_node->ErasePair(0);

    // Update the key in the parent node
    MaintainParent(neighbor_node);
    // // This below line is not enough because of cascading updates
    // char *parent_key = neighbor_node->GetKey(0);
    // auto neighbor_parent = FetchNode(neighbor_node->GetParentPageNo());
    // neighbor_parent->SetKey(neighbor_parent->FindChild(neighbor_node), parent_key);
    // buffer_pool_manager_->UnpinPage(neighbor_parent->GetPageId(), true);

    // Update the parent pointer of the affected child node
    MaintainChild(node, node->GetSize() - 1);
  }
}

/**
 * @brief 合并(Coalesce)函数是将node和其直接前驱进行合并，也就是和它左边的neighbor_node进行合并；
 * 假设node一定在右边。如果上层传入的index=0，说明node在左边，那么交换node和neighbor_node，保证node在右边；合并到左结点，实际上就是删除了右结点；
 * Move all the key & value pairs from one page to its sibling page, and notify buffer pool manager to delete this page.
 * Parent page must be adjusted to take info of deletion into account. Remember to deal with Coalesce or Redistribute
 * recursively if necessary.
 *
 * @param neighbor_node sibling page of input "node" (neighbor_node是node的前结点)
 * @param node input from method coalesceOrRedistribute() (node结点是需要被删除的)
 * @param parent parent page of input "node"
 * @param index node在parent中的rid_idx
 * @return true means parent node should be deleted, false means no deletion happend
 * @note Assume that *neighbor_node is the left sibling of *node (neighbor -> node)
 */
bool IxIndexHandle::Coalesce(IxNodeHandle **neighbor_node, IxNodeHandle **node, IxNodeHandle **parent, int index,
                             bool *root_is_latched) {
  // Todo:
  // 1. 用index判断neighbor_node是否为node的前驱结点，若不是则交换两个结点，让neighbor_node作为左结点，node作为右结点
  // 2. 把node结点的键值对移动到neighbor_node中，并更新node结点孩子结点的父节点信息（调用maintain_child函数）
  // 3. 释放和删除node结点，并删除parent中node结点的信息，返回parent是否需要被删除
  // 提示：如果是叶子结点且为最右叶子结点，需要更新file_hdr_.last_leaf
  // return false;

  // 1. Ensure neighbor_node is the left sibling
  if (index == 0) {
    std::swap(*neighbor_node, *node);
    index = 1;
  }

  // 2. Move key-value pairs from node to neighbor_node
  int start_pos = (*neighbor_node)->GetSize();
  int num_to_move = (*node)->GetSize();
  (*neighbor_node)->InsertPairs(start_pos, (*node)->GetKey(0), (*node)->GetRid(0), num_to_move);

  // If internal node, update children's parent pointers
  if (!(*node)->IsLeafPage()) {
    for (int i = 0; i < num_to_move; ++i) {
      MaintainChild(*neighbor_node, start_pos + i);
    }
  } else {
    // If leaf node, update prev/next_leaf
    EraseLeaf(*node);
    // Handle the special case for the last leaf
    // if ((*neighbor_node)->GetNextLeaf() == IX_NO_PAGE) {
    if ((*node)->GetPageNo() == file_hdr_->last_leaf_) {
      file_hdr_->last_leaf_ = (*neighbor_node)->GetPageNo();
    }
  }

  // 3. Remove node and update parent
  ReleaseNodeHandle(**node);
  (*parent)->ErasePair(index);
  delete *node;

  // Check if the parent node needs to be deleted
  return CoalesceOrRedistribute(*parent, root_is_latched);
}

/**
 * @brief 这里把iid转换成了rid，即iid的slot_no作为node的rid_idx(key_idx)
 * node其实就是把slot_no作为键值对数组的下标
 * 换而言之，每个iid对应的索引槽存了一对(key,Rid)，指向了(要建立索引的属性首地址,插入/删除记录的位置)
 *
 * @param iid
 * @return Rid
 * @note iid和rid存的不是一个东西，rid是上层传过来的记录位置，iid是索引内部生成的索引槽位置
 */
RID IxIndexHandle::GetRid(const Iid &iid) const {
  IxNodeHandle *node = FetchNode(iid.page_no);
  if (iid.slot_no >= node->GetSize()) {
    throw IndexEntryNotFoundError();
  }
  buffer_pool_manager_->UnpinPage(node->GetPageId(), false);  // unpin it!
  return *node->GetRid(iid.slot_no);
}

/**
 * @brief FindLeafPage + LowerBound
 *
 * @param key
 * @return Iid
 * @note 上层传入的key本来是int类型，通过(const char *)&key进行了转换
 * 可用*(int *)key转换回去
 */
Iid IxIndexHandle::LowerBound(const char *key) {
  // return Iid{-1, -1};

  // std::scoped_lock latch{root_latch_};

  // 1. Find the leaf page containing the target key
  auto [leaf_node, root_is_latched] = FindLeafPage(key, Operation::FIND);

  if (leaf_node == nullptr) {
    throw InternalError("IxIndexHandle::LowerBound Error: Leaf node not found");
    // return Iid{-1, -1};
  }

  // 2. Use the LowerBound method in IxNodeHandle to find the appropriate key index within the leaf node
  int key_index = leaf_node->LowerBound(key);

  Iid result;
  // target key > all keys in leaf node
  if (key_index == leaf_node->GetSize()) {
    if (leaf_node->GetPageNo() == file_hdr_->last_leaf_) {
      // the last leaf node
      result = Iid{leaf_node->GetPageNo(), leaf_node->GetSize()};
    } else {
      // the leaf node has Next leaf
      result = Iid{leaf_node->GetNextLeaf(), 0};
    }
  } else {
    result = Iid{leaf_node->GetPageNo(), key_index};
  }

  // 3. Unpin the leaf node that pinned in find_leaf_page()
  buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false);

  delete leaf_node;
  return result;
}

/**
 * @brief FindLeafPage + UpperBound
 *
 * @param key
 * @return Iid
 */
Iid IxIndexHandle::UpperBound(const char *key) {
  // return Iid{-1, -1};

  // std::scoped_lock latch{root_latch_};

  // 1. Find the leaf page containing the target key
  auto [leaf_node, root_is_latched] = FindLeafPage(key, Operation::FIND);

  if (leaf_node == nullptr) {
    throw InternalError("IxIndexHandle::UpperBound Error: Leaf node not found");
    // return Iid{-1, -1};
  }

  // 2. Use the UpperBound method in IxNodeHandle to find the appropriate key index within the leaf node
  int key_index = leaf_node->UpperBound(key);

  Iid result;
  // target key >= all keys in leaf node
  if (key_index == leaf_node->GetSize()) {
    if (leaf_node->GetPageNo() == file_hdr_->last_leaf_) {
      // the last leaf node
      result = Iid{leaf_node->GetPageNo(), leaf_node->GetSize()};
    } else {
      // the leaf node has Next leaf
      result = Iid{leaf_node->GetNextLeaf(), 0};
    }
  } else {
    result = Iid{leaf_node->GetPageNo(), key_index};
  }

  // 3. Unpin the leaf node that pinned in find_leaf_page()
  buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false);

  delete leaf_node;
  return result;
}

/**
 * @brief 指向最后一个叶子的最后一个结点的后一个
 * 用处在于可以作为IxScan的最后一个
 *
 * @return Iid
 */
Iid IxIndexHandle::LeafEnd() const {
  IxNodeHandle *node = FetchNode(file_hdr_->last_leaf_);
  Iid iid = {.page_no = file_hdr_->last_leaf_, .slot_no = node->GetSize()};
  buffer_pool_manager_->UnpinPage(node->GetPageId(), false);  // unpin it!
  return iid;
}

/**
 * @brief 指向第一个叶子的第一个结点
 * 用处在于可以作为IxScan的第一个
 *
 * @return Iid
 */
Iid IxIndexHandle::LeafBegin() const {
  Iid iid = {.page_no = file_hdr_->first_leaf_, .slot_no = 0};
  return iid;
}

/**
 * @brief 获取根节点
 *
 * @return IxNodeHandle*
 */
IxNodeHandle *IxIndexHandle::GetRoot() const { return FetchNode(file_hdr_->root_page_); }

/**
 * @brief 获取一个指定结点
 *
 * @param page_no
 * @return IxNodeHandle*
 * @note pin the page, remember to unpin it outside!
 */
IxNodeHandle *IxIndexHandle::FetchNode(int page_no) const {
  Page *page = buffer_pool_manager_->FetchPage(PageId{fd_, page_no});
  IxNodeHandle *node = new IxNodeHandle(file_hdr_.get(), page);
  // auto node = std::make_unique<IxNodeHandle>(file_hdr_, page);

  return node;
}

/**
 * @brief 创建一个新结点
 *
 * @return IxNodeHandle*
 * @note pin the page, remember to unpin it outside!
 * 注意：对于Index的处理是，删除某个页面后，认为该被删除的页面是free_page
 * 而first_free_page实际上就是最新被删除的页面，初始为IX_NO_PAGE
 * 在最开始插入时，一直是create node，那么first_page_no一直没变，一直是IX_NO_PAGE
 * 与Record的处理不同，Record将未插入满的记录页认为是free_page
 */
IxNodeHandle *IxIndexHandle::CreateNode() {
  IxNodeHandle *node;
  file_hdr_->num_pages_++;

  PageId new_page_id = {.fd = fd_, .page_no = INVALID_PAGE_ID};
  // 从3开始分配page_no，第一次分配之后，new_page_id.page_no=3，file_hdr_.num_pages=4
  Page *page = buffer_pool_manager_->NewPage(&new_page_id);
  node = new IxNodeHandle(file_hdr_.get(), page);
  return node;
}

/**
 * @brief 从node开始更新其父节点的第一个key，一直向上更新直到根节点
 *
 * @param node
 */
void IxIndexHandle::MaintainParent(IxNodeHandle *node) {
  IxNodeHandle *curr = node;
  while (curr->GetParentPageNo() != IX_NO_PAGE) {
    // Load its parent
    IxNodeHandle *parent = FetchNode(curr->GetParentPageNo());
    int rank = parent->FindChild(curr);
    char *parent_key = parent->GetKey(rank);
    char *child_first_key = curr->GetKey(0);
    if (memcmp(parent_key, child_first_key, file_hdr_->col_tot_len_) == 0) {
      assert(buffer_pool_manager_->UnpinPage(parent->GetPageId(), true));
      break;
    }
    memcpy(parent_key, child_first_key, file_hdr_->col_tot_len_);  // 修改了parent node
    curr = parent;

    assert(buffer_pool_manager_->UnpinPage(parent->GetPageId(), true));
  }
}

/**
 * @brief 要删除leaf之前调用此函数，更新leaf前驱结点的next指针和后继结点的prev指针
 *
 * @param leaf 要删除的leaf
 */
void IxIndexHandle::EraseLeaf(IxNodeHandle *leaf) {
  assert(leaf->IsLeafPage());

  IxNodeHandle *prev = FetchNode(leaf->GetPrevLeaf());
  prev->SetNextLeaf(leaf->GetNextLeaf());
  buffer_pool_manager_->UnpinPage(prev->GetPageId(), true);

  IxNodeHandle *Next = FetchNode(leaf->GetNextLeaf());
  Next->SetPrevLeaf(leaf->GetPrevLeaf());  // 注意此处是SetPrevLeaf()
  buffer_pool_manager_->UnpinPage(Next->GetPageId(), true);
}

/**
 * @brief 删除node时，更新file_hdr_.num_pages
 *
 * @param node
 */
void IxIndexHandle::ReleaseNodeHandle(IxNodeHandle &node) { file_hdr_->num_pages_--; }

/**
 * @brief 将node的第child_idx个孩子结点的父节点置为node
 */
void IxIndexHandle::MaintainChild(IxNodeHandle *node, int child_idx) {
  if (!node->IsLeafPage()) {
    //  Current node is inner node, load its child and set its parent to current node
    int child_page_no = node->ValueAt(child_idx);
    IxNodeHandle *child = FetchNode(child_page_no);
    child->SetParentPageNo(node->GetPageNo());
    buffer_pool_manager_->UnpinPage(child->GetPageId(), true);
  }
}

/**
 * @brief 删除buffer中的所有index page。存疑。
 */
bool IxIndexHandle::Erase() {
  for (int page_no = 0; page_no < file_hdr_->num_pages_; page_no++) {
    PageId page_id = {fd_, page_no};
    while (buffer_pool_manager_->UnpinPage(page_id, true));
    buffer_pool_manager_->DeletePage(page_id);
  }
  return true;
}

}  // namespace easydb
