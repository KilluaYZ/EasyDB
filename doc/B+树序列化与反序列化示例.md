# B+树序列化、存储、反序列化和访问完整流程示例

## 场景设定

假设我们有一个学生表的索引，索引字段是 `student_id`（INT类型，4字节），B+树阶数为3（每个节点最多3个键值对）。

**B+树结构：**
```
         [根节点，页号=2]
         Keys: [50]
         RIDs: [→页3, →页4]
              /        \
    [叶子节点，页号=3]  [叶子节点，页号=4]
    Keys: [10,20,30]   Keys: [50,60,70]
    RIDs: [(1,0),(1,1),(1,2)]  RIDs: [(1,3),(1,4),(1,5)]
```

---

## 第一部分：序列化并写入磁盘

### 步骤1：创建索引文件

```cpp
// 用户执行：CREATE INDEX ON students(student_id)
IxManager::CreateIndex("students", {ColMeta("student_id", TYPE_INT, 4)})
```

### 步骤2：创建文件头（IxFileHdr）

```cpp
// 计算B+树阶数
int col_tot_len = 4;  // INT类型4字节
int btree_order = (4096 - sizeof(IxPageHdr)) / (4 + sizeof(RID)) - 1;
// 假设计算出来 btree_order = 3

// 创建文件头对象
IxFileHdr file_hdr;
file_hdr.first_free_page_no_ = -1;      // 无空闲页
file_hdr.num_pages_ = 3;                 // 初始3页（0=文件头，1=叶子头，2=根节点）
file_hdr.root_page_ = 2;                 // 根节点在第2页
file_hdr.col_num_ = 1;                   // 1个字段
file_hdr.col_types_ = {TYPE_INT};        // INT类型
file_hdr.col_lens_ = {4};                // 长度4字节
file_hdr.col_tot_len_ = 4;               // 总长度4字节
file_hdr.btree_order_ = 3;               // 阶数3
file_hdr.keys_size_ = (3+1) * 4 = 16;    // 预留4个键的空间
file_hdr.first_leaf_ = 3;                // 第一个叶子节点
file_hdr.last_leaf_ = 4;                 // 最后一个叶子节点
```

### 步骤3：序列化文件头

```cpp
void IxFileHdr::Serialize(char *dest) {
    int offset = 0;
    
    // 写入 tot_len_ (4字节)
    memcpy(dest + offset, &tot_len_, 4);  // 假设 tot_len_ = 60
    offset += 4;
    
    // 写入 first_free_page_no_ (4字节)
    memcpy(dest + offset, &first_free_page_no_, 4);  // -1
    offset += 4;
    
    // 写入 num_pages_ (4字节)
    memcpy(dest + offset, &num_pages_, 4);  // 3
    offset += 4;
    
    // 写入 root_page_ (4字节) ⭐关键！
    memcpy(dest + offset, &root_page_, 4);  // 2
    offset += 4;
    
    // 写入 col_num_ (4字节)
    memcpy(dest + offset, &col_num_, 4);  // 1
    offset += 4;
    
    // 写入 col_types_ (1字节 × 1)
    memcpy(dest + offset, &col_types_[0], 1);  // TYPE_INT
    offset += 1;
    
    // 写入 col_lens_ (4字节 × 1)
    memcpy(dest + offset, &col_lens_[0], 4);  // 4
    offset += 4;
    
    // 写入其他字段...
    // col_tot_len_, btree_order_, keys_size_, first_leaf_, last_leaf_
    
    // 总共写入约60字节到dest缓冲区
}
```

### 步骤4：写入文件头到磁盘（第0页）

```cpp
char *data = new char[file_hdr.tot_len_];
file_hdr.Serialize(data);

// 写入到索引文件的第0页
disk_manager_->WritePage(fd, 0, data, file_hdr.tot_len_);
```

**磁盘第0页的二进制内容（简化版）：**
```
偏移量  内容（十六进制）          含义
─────────────────────────────────────────────
0x0000  3C 00 00 00              tot_len_ = 60
0x0004  FF FF FF FF              first_free_page_no_ = -1
0x0008  03 00 00 00              num_pages_ = 3
0x000C  02 00 00 00              root_page_ = 2 ⭐根节点页号
0x0010  01 00 00 00              col_num_ = 1
0x0014  00                       col_types_[0] = TYPE_INT
0x0015  04 00 00 00              col_lens_[0] = 4
...      ...                     其他字段
```

### 步骤5：创建并写入根节点（第2页）

```cpp
// 创建根节点页面
char page_buf[4096];
memset(page_buf, 0, 4096);

IxPageHdr *page_hdr = reinterpret_cast<IxPageHdr *>(page_buf);
page_hdr->parent = -1;           // 根节点无父节点
page_hdr->num_key = 1;            // 1个键
page_hdr->is_leaf = false;        // 内部节点
page_hdr->prev_leaf = -1;         // 非叶子节点
page_hdr->next_leaf = -1;         // 非叶子节点

// 写入Keys数组（从偏移量sizeof(IxPageHdr)开始）
char *keys = page_buf + sizeof(IxPageHdr);
int key_50 = 50;
memcpy(keys + 0 * 4, &key_50, 4);  // Keys[0] = 50

// 写入RIDs数组（从keys + keys_size_开始）
RID *rids = reinterpret_cast<RID *>(keys + 16);  // keys_size_ = 16
rids[0].Set(3, 0);  // RIDs[0] = (页3, 槽0) → 指向左子节点
rids[1].Set(4, 0);  // RIDs[1] = (页4, 槽0) → 指向右子节点

// 写入到磁盘第2页
disk_manager_->WritePage(fd, 2, page_buf, 4096);
```

**磁盘第2页的二进制内容（简化版）：**
```
偏移量  内容（十六进制）          含义
─────────────────────────────────────────────
0x0000  FF FF FF FF              parent = -1
0x0004  01 00 00 00              num_key = 1
0x0008  00                       is_leaf = false
0x0009  FF FF FF FF              prev_leaf = -1
0x000D  FF FF FF FF              next_leaf = -1
0x0011  [填充到32字节]            IxPageHdr结束
─────────────────────────────────────────────
0x0020  32 00 00 00              Keys[0] = 50 (0x32)
0x0024  [填充到0x0030]            Keys数组结束
─────────────────────────────────────────────
0x0030  03 00 00 00              RIDs[0].page_id = 3
0x0034  00 00 00 00              RIDs[0].slot_num = 0
0x0038  04 00 00 00              RIDs[1].page_id = 4
0x003C  00 00 00 00              RIDs[1].slot_num = 0
...      [填充到4096字节]        页面剩余空间
```

### 步骤6：创建并写入叶子节点（第3页和第4页）

类似地，写入两个叶子节点：
- 第3页：Keys=[10,20,30], RIDs=[(1,0),(1,1),(1,2)]
- 第4页：Keys=[50,60,70], RIDs=[(1,3),(1,4),(1,5)]

**磁盘文件结构：**
```
students_student_id.idx (索引文件)
├─ 第0页 (4096字节): IxFileHdr - 包含root_page_=2
├─ 第1页 (4096字节): 叶子节点链表头（预留）
├─ 第2页 (4096字节): 根节点 - Keys=[50], RIDs=[→3, →4]
├─ 第3页 (4096字节): 叶子节点1 - Keys=[10,20,30]
└─ 第4页 (4096字节): 叶子节点2 - Keys=[50,60,70]
```

---

## 第二部分：系统启动时反序列化

### 步骤1：打开数据库

```cpp
// 系统启动
SmManager::OpenDB("test_db");
```

### 步骤2：打开索引文件

```cpp
// 在OpenDB中，为每个表打开索引
IxManager::OpenIndex("students", {ColMeta("student_id", TYPE_INT, 4)})
```

### 步骤3：创建IxIndexHandle（读取文件头）

```cpp
IxIndexHandle::IxIndexHandle(disk_manager, buffer_pool_manager, fd) {
    // 1. 分配缓冲区
    char *buf = new char[4096];
    memset(buf, 0, 4096);
    
    // 2. 从磁盘读取第0页（文件头页）
    disk_manager_->ReadPage(fd, 0, buf, 4096);
    // 此时buf中包含了序列化的IxFileHdr数据
    
    // 3. 创建文件头对象
    file_hdr_ = std::make_unique<IxFileHdr>();
    
    // 4. 反序列化文件头
    file_hdr_->Deserialize(buf);
    
    // 现在file_hdr_中包含了：
    //   - root_page_ = 2  ⭐关键！知道根节点在哪
    //   - col_types_ = {TYPE_INT}
    //   - col_lens_ = {4}
    //   - btree_order_ = 3
    //   - keys_size_ = 16
    //   - first_leaf_ = 3
    //   - last_leaf_ = 4
    
    delete[] buf;
}
```

**反序列化过程：**
```cpp
void IxFileHdr::Deserialize(char *src) {
    int offset = 0;
    
    // 读取 tot_len_
    tot_len_ = *reinterpret_cast<const int *>(src + offset);  // 60
    offset += 4;
    
    // 读取 first_free_page_no_
    first_free_page_no_ = *reinterpret_cast<const page_id_t *>(src + offset);  // -1
    offset += 4;
    
    // 读取 num_pages_
    num_pages_ = *reinterpret_cast<const int *>(src + offset);  // 3
    offset += 4;
    
    // ⭐读取 root_page_ - 这是还原B+树的关键！
    root_page_ = *reinterpret_cast<const page_id_t *>(src + offset);  // 2
    offset += 4;
    
    // 读取 col_num_
    col_num_ = *reinterpret_cast<const int *>(src + offset);  // 1
    offset += 4;
    
    // 读取 col_types_
    for (int i = 0; i < col_num_; ++i) {
        ColType type = *reinterpret_cast<const ColType *>(src + offset);
        col_types_.push_back(type);  // TYPE_INT
        offset += sizeof(ColType);
    }
    
    // 读取 col_lens_
    for (int i = 0; i < col_num_; ++i) {
        int len = *reinterpret_cast<const int *>(src + offset);
        col_lens_.push_back(len);  // 4
        offset += sizeof(int);
    }
    
    // 读取其他字段...
    // 现在file_hdr_已经完全还原了！
}
```

**此时内存中的状态：**
```
IxIndexHandle对象（在内存中）
├─ file_hdr_ (IxFileHdr对象)
│  ├─ root_page_ = 2          ⭐知道根节点在第2页
│  ├─ col_types_ = {TYPE_INT}
│  ├─ col_lens_ = {4}
│  ├─ btree_order_ = 3
│  └─ keys_size_ = 16
└─ fd_ = 文件描述符
```

**注意：此时B+树的节点还没有加载到内存！只有文件头信息。**

---

## 第三部分：按需访问节点

### 场景：用户查询 `SELECT * FROM students WHERE student_id = 60`

### 步骤1：调用GetValue查找键值60

```cpp
std::vector<RID> result;
ix_handle->GetValue(&key_60, &result, nullptr);
```

### 步骤2：FindLeafPage - 从根节点开始查找

```cpp
std::pair<IxNodeHandle*, bool> FindLeafPage(const char *key, ...) {
    // 1. 获取根节点（第2页）
    // ⭐第一次访问节点！需要从磁盘加载
    IxNodeHandle *current_node = FetchNode(file_hdr_->root_page_);  // root_page_ = 2
    // 此时会调用 BufferPoolManager::FetchPage({fd, 2})
    
    // 2. 从根节点向下遍历
    while (!current_node->IsLeafPage()) {  // 根节点不是叶子节点
        // 在内部节点中查找应该走哪个子节点
        page_id_t child_page_no = current_node->InternalLookup(key);  // key=60
        // InternalLookup会：
        //   - 在Keys中查找：Keys[0]=50 < 60
        //   - 返回RIDs[1].page_id = 4（应该走右子树）
        
        // 释放当前节点
        buffer_pool_manager_->UnpinPage(current_node->GetPageId(), false);
        delete current_node;
        
        // ⭐加载子节点（第4页）
        current_node = FetchNode(child_page_no);  // page_no = 4
    }
    
    // 3. 到达叶子节点（第4页）
    return std::make_pair(current_node, false);
}
```

### 步骤3：FetchNode - 从磁盘加载节点

```cpp
IxNodeHandle *FetchNode(int page_no) const {
    // 1. 通过BufferPoolManager获取页面
    Page *page = buffer_pool_manager_->FetchPage(PageId{fd_, page_no});
    // FetchPage内部会：
    //   a) 检查页面是否在缓冲区中
    //   b) 如果不在，从磁盘读取
    
    // 2. 创建节点句柄，解析页面数据
    IxNodeHandle *node = new IxNodeHandle(file_hdr_.get(), page);
    // IxNodeHandle构造函数会设置指针：
    //   - page_hdr → 页面头部
    //   - keys → Keys数组起始位置
    //   - rids → RIDs数组起始位置
    
    return node;
}
```

**BufferPoolManager::FetchPage的详细流程：**
```cpp
Page *BufferPoolManager::FetchPage(PageId page_id) {
    // page_id = {fd=索引文件fd, page_no=2}
    
    // 1. 检查页面是否在缓冲区中
    auto it = page_table_.find(page_id);
    if (it != page_table_.end()) {
        // 页面已在内存中，直接返回
        Page *frame = &frames_[it->second];
        frame->pin_count_++;
        return frame;
    }
    
    // 2. 页面不在内存中，需要从磁盘加载
    //    找到一个空闲的frame（或驱逐一个页面）
    frame_id_t frame_id;
    FindVictimPage(&frame_id);
    Page *frame = &frames_[frame_id];
    
    // 3. 如果frame中有脏页，先写回磁盘
    if (frame->is_dirty_) {
        disk_manager_->WritePage(frame->page_id_.fd, 
                                  frame->page_id_.page_no, 
                                  frame->GetData(), 
                                  4096);
    }
    
    // 4. ⭐从磁盘读取目标页面到frame
    disk_manager_->ReadPage(page_id.fd,      // 索引文件fd
                             page_id.page_no, // 2（根节点页号）
                             frame->GetData(), // frame的数据缓冲区
                             4096);            // 读取4096字节
    
    // 5. 更新page_table_，将page_id映射到frame_id
    page_table_[page_id] = frame_id;
    
    // 6. Pin页面，防止被驱逐
    frame->pin_count_ = 1;
    frame->page_id_ = page_id;
    
    return frame;
}
```

**磁盘读取过程：**
```cpp
void DiskManager::ReadPage(int fd, page_id_t page_id, char *page_data, size_t num_bytes) {
    // fd = 索引文件描述符
    // page_id = 2（根节点页号）
    // page_data = frame的数据缓冲区
    // num_bytes = 4096
    
    // 1. 计算文件偏移量
    size_t offset = static_cast<size_t>(page_id) * PAGE_SIZE;
    // offset = 2 * 4096 = 8192（第2页从文件偏移8192开始）
    
    // 2. 移动文件指针到目标位置
    lseek(fd, 8192, SEEK_SET);
    
    // 3. 从磁盘读取4096字节到page_data
    read(fd, page_data, 4096);
    // 此时page_data中包含了第2页的完整数据：
    //   - IxPageHdr（页面头部）
    //   - Keys数组（Keys[0]=50）
    //   - RIDs数组（RIDs[0]→页3, RIDs[1]→页4）
}
```

### 步骤4：IxNodeHandle构造函数 - 解析页面数据

```cpp
IxNodeHandle::IxNodeHandle(const IxFileHdr *file_hdr_, Page *page_) 
    : file_hdr(file_hdr_), page(page_) {
    
    // page->GetData() 指向从磁盘读取的4096字节数据
    
    // 1. 解析页面头部（页面起始位置）
    page_hdr = reinterpret_cast<IxPageHdr *>(page->GetData());
    // 现在可以通过page_hdr访问：
    //   page_hdr->parent = -1
    //   page_hdr->num_key = 1
    //   page_hdr->is_leaf = false
    
    // 2. Keys数组起始位置（页面头部之后）
    keys = page->GetData() + sizeof(IxPageHdr);
    // keys指向Keys数组，可以通过keys[0*4]访问Keys[0]=50
    
    // 3. RIDs数组起始位置（Keys数组之后）
    rids = reinterpret_cast<RID *>(keys + file_hdr->keys_size_);
    // rids指向RIDs数组，可以通过rids[0]访问RIDs[0]=(3,0)
    
    // ⭐现在节点数据已经完全解析，可以使用了！
}
```

**内存中的节点结构：**
```
IxNodeHandle对象（根节点，第2页）
├─ page_hdr (IxPageHdr*)
│  ├─ parent = -1
│  ├─ num_key = 1
│  └─ is_leaf = false
├─ keys (char*) → 指向Keys数组
│  └─ Keys[0] = 50
└─ rids (RID*) → 指向RIDs数组
   ├─ RIDs[0] = (page_id=3, slot=0)
   └─ RIDs[1] = (page_id=4, slot=0)
```

### 步骤5：InternalLookup - 在内部节点中查找子节点

```cpp
page_id_t IxNodeHandle::InternalLookup(const char *key) {
    // key = 60（要查找的键）
    
    // 1. 找到第一个大于key的位置
    int pos = UpperBound(key);  // 二分查找
    // Keys = [50], key = 60
    // UpperBound返回1（第一个大于60的位置）
    
    // 2. 调整位置
    pos = (pos > 0) ? pos - 1 : pos;  // pos = 0
    
    // 3. 返回对应的子节点页号
    page_id_t child_page_id = ValueAt(pos);  // RIDs[0].page_id = 3
    // 但60 > 50，应该走右子树，所以应该是RIDs[1].page_id = 4
    
    // 实际上，InternalLookup的逻辑是：
    // - 找到第一个大于key的位置pos
    // - 返回RIDs[pos-1]（因为Keys[i]对应RIDs[i+1]）
    // 对于key=60，pos=1，返回RIDs[0]...不对
    
    // 正确的逻辑应该是：
    // 对于key=60，应该返回RIDs[1]（因为60 > Keys[0]=50）
    return 4;  // 返回右子节点页号
}
```

### 步骤6：加载叶子节点（第4页）

```cpp
// 继续FindLeafPage中的循环
current_node = FetchNode(4);  // 加载第4页（叶子节点）

// FetchNode会：
// 1. 调用BufferPoolManager::FetchPage({fd, 4})
// 2. 从磁盘读取第4页（偏移量4*4096=16384）
// 3. 创建IxNodeHandle解析页面数据
```

**第4页的磁盘内容被加载到内存：**
```
IxNodeHandle对象（叶子节点，第4页）
├─ page_hdr
│  ├─ parent = 2
│  ├─ num_key = 3
│  └─ is_leaf = true
├─ keys
│  ├─ Keys[0] = 50
│  ├─ Keys[1] = 60
│  └─ Keys[2] = 70
└─ rids
   ├─ RIDs[0] = (page_id=1, slot=3)
   ├─ RIDs[1] = (page_id=1, slot=4)
   └─ RIDs[2] = (page_id=1, slot=5)
```

### 步骤7：在叶子节点中查找键值

```cpp
// 回到GetValue函数
bool found = leaf_node->LeafLookup(key, &Rid);
// key = 60

int IxNodeHandle::LeafLookup(const char *key, RID **value) {
    // 1. 找到key的位置
    int pos = LowerBound(key);  // 二分查找，返回1（Keys[1]=60）
    
    // 2. 检查key是否存在
    if (pos < page_hdr->num_key && 
        IxCompare(GetKey(pos), key, ...) == 0) {
        // Keys[1] == 60，找到了！
        *value = GetRid(pos);  // 返回RIDs[1] = (1,4)
        return true;
    }
    
    return false;
}
```

### 步骤8：返回结果并清理

```cpp
// GetValue函数
if (found) {
    result->push_back(*Rid);  // result = [(1,4)]
}

// 释放叶子节点
buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false);
delete leaf_node;

return found;  // 返回true
```

---

## 完整流程图总结

```
【序列化阶段】
1. 创建索引 → IxManager::CreateIndex()
2. 创建文件头 → IxFileHdr对象
3. 序列化文件头 → Serialize() → 二进制数据
4. 写入磁盘第0页 → WritePage(fd, 0, data, size)
5. 创建节点 → 写入第2、3、4页

【磁盘存储】
索引文件：students_student_id.idx
├─ 第0页：IxFileHdr（包含root_page_=2）
├─ 第1页：预留
├─ 第2页：根节点（Keys=[50], RIDs=[→3,→4]）
├─ 第3页：叶子节点1（Keys=[10,20,30]）
└─ 第4页：叶子节点2（Keys=[50,60,70]）

【反序列化阶段 - 系统启动】
1. OpenDB() → 打开数据库
2. OpenIndex() → 打开索引文件
3. IxIndexHandle构造函数
   ├─ ReadPage(fd, 0, buf, 4096)  ⭐读取第0页
   └─ Deserialize(buf)  ⭐反序列化文件头
4. 获得root_page_=2（知道根节点位置）
5. ⚠️ 节点还未加载到内存！

【按需访问阶段 - 查询时】
1. GetValue(key=60)
2. FindLeafPage(key=60)
   ├─ FetchNode(2)  ⭐第一次访问节点
   │  ├─ BufferPoolManager::FetchPage({fd,2})
   │  │  ├─ 检查缓冲区（不在）
   │  │  ├─ ReadPage(fd, 2, frame->data, 4096)  ⭐从磁盘读取
   │  │  └─ 返回Page对象
   │  └─ IxNodeHandle构造函数  ⭐解析页面数据
   │     ├─ page_hdr → 页面头部
   │     ├─ keys → Keys数组
   │     └─ rids → RIDs数组
   ├─ InternalLookup(60) → 返回子节点页号4
   ├─ UnpinPage(2) → 释放根节点
   └─ FetchNode(4)  ⭐第二次访问节点
      ├─ BufferPoolManager::FetchPage({fd,4})
      │  └─ ReadPage(fd, 4, frame->data, 4096)  ⭐从磁盘读取
      └─ IxNodeHandle构造函数  ⭐解析页面数据
3. LeafLookup(60) → 在叶子节点中找到
4. 返回RID = (1,4)
5. UnpinPage(4) → 释放叶子节点
```

---

## 关键点总结

1. **序列化**：将B+树结构信息（特别是root_page_）写入文件头，节点数据写入各个页面
2. **磁盘存储**：每个节点占一个4KB页面，文件头包含根节点位置
3. **反序列化**：启动时只读取文件头，获取root_page_，节点不立即加载
4. **按需加载**：查询时才通过FetchNode从磁盘加载需要的节点
5. **缓冲区管理**：BufferPoolManager缓存已加载的页面，避免重复磁盘I/O
6. **页面解析**：IxNodeHandle通过固定布局快速解析页面中的Keys和RIDs数组
