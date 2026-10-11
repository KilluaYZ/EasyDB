//! 可扩展哈希索引。
//!
//! 目录使用全局深度，桶使用局部深度；桶分裂时必要时将目录翻倍。
//! 索引项以编码后的复合键和 RID 保存，状态序列化到索引文件中。

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use crate::common::errors::EasyDbError;
use crate::common::rid::Rid;
use crate::storage::disk::DiskManager;

const MAGIC: &[u8] = b"EASYDB_EXT_HASH\n";
const BUCKET_CAPACITY: usize = 64;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Entry {
    key: Vec<u8>,
    rids: Vec<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Bucket {
    local_depth: u8,
    entries: Vec<Entry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct State {
    global_depth: u8,
    directory: Vec<usize>,
    buckets: Vec<Bucket>,
}

pub struct ExtendibleHashIndex {
    path: PathBuf,
    fd: i32,
    state: Mutex<State>,
}

fn hash_key(key: &[u8]) -> u64 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

fn bucket_for(state: &State, key: &[u8]) -> usize {
    let mask = (1usize << state.global_depth) - 1;
    state.directory[(hash_key(key) as usize) & mask]
}

impl ExtendibleHashIndex {
    pub fn create(path: impl AsRef<Path>, disk: &DiskManager) -> Result<Arc<Self>, EasyDbError> {
        let path = path.as_ref().to_path_buf();
        disk.create_file(&path)?;
        let fd = disk.open_file(&path)?;
        let index = Arc::new(Self {
            path,
            fd,
            state: Mutex::new(State {
                global_depth: 1,
                directory: vec![0, 1],
                buckets: vec![
                    Bucket {
                        local_depth: 1,
                        entries: Vec::new(),
                    },
                    Bucket {
                        local_depth: 1,
                        entries: Vec::new(),
                    },
                ],
            }),
        });
        index.persist()?;
        Ok(index)
    }

    pub fn open(path: impl AsRef<Path>, disk: &DiskManager) -> Result<Arc<Self>, EasyDbError> {
        let path = path.as_ref().to_path_buf();
        let fd = disk.open_file(&path)?;
        let bytes = std::fs::read(&path)?;
        let payload = bytes
            .strip_prefix(MAGIC)
            .ok_or_else(|| EasyDbError::Internal("not an extendible hash index".into()))?;
        let state = serde_json::from_slice(payload)
            .map_err(|e| EasyDbError::Internal(format!("invalid hash index: {e}")))?;
        Ok(Arc::new(Self {
            path,
            fd,
            state: Mutex::new(state),
        }))
    }

    pub fn fd(&self) -> i32 {
        self.fd
    }

    fn persist(&self) -> Result<(), EasyDbError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut bytes = MAGIC.to_vec();
        bytes
            .extend(serde_json::to_vec(&*state).map_err(|e| EasyDbError::Internal(e.to_string()))?);
        std::fs::write(&self.path, bytes)?;
        Ok(())
    }

    fn split_bucket(state: &mut State, bucket_id: usize) {
        let old_depth = state.buckets[bucket_id].local_depth;
        if old_depth == state.global_depth {
            let old = state.directory.clone();
            state.directory.extend_from_slice(&old);
            state.global_depth += 1;
        }
        let new_id = state.buckets.len();
        state.buckets.push(Bucket {
            local_depth: old_depth + 1,
            entries: Vec::new(),
        });
        state.buckets[bucket_id].local_depth += 1;
        let new_depth = state.buckets[bucket_id].local_depth;
        for (i, dir) in state.directory.iter_mut().enumerate() {
            if *dir == bucket_id && ((i >> (new_depth - 1)) & 1) == 1 {
                *dir = new_id;
            }
        }
        let entries = std::mem::take(&mut state.buckets[bucket_id].entries);
        for entry in entries {
            let id = bucket_for(state, &entry.key);
            state.buckets[id].entries.push(entry);
        }
    }

    pub fn insert_entry(&self, key: &[u8], rid: Rid) -> Result<(), EasyDbError> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        loop {
            let id = bucket_for(&state, key);
            if let Some(entry) = state.buckets[id].entries.iter_mut().find(|e| e.key == key) {
                let encoded = rid.encoded();
                if !entry.rids.contains(&encoded) {
                    entry.rids.push(encoded);
                }
                break;
            }
            if state.buckets[id].entries.len() < BUCKET_CAPACITY {
                state.buckets[id].entries.push(Entry {
                    key: key.to_vec(),
                    rids: vec![rid.encoded()],
                });
                break;
            }
            Self::split_bucket(&mut state, id);
        }
        drop(state);
        self.persist()
    }

    pub fn delete_entry(&self, key: &[u8], rid: Rid) -> Result<(), EasyDbError> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let id = bucket_for(&state, key);
        if let Some(pos) = state.buckets[id].entries.iter().position(|e| e.key == key) {
            let entry = &mut state.buckets[id].entries[pos];
            entry.rids.retain(|r| *r != rid.encoded());
            if entry.rids.is_empty() {
                state.buckets[id].entries.remove(pos);
            }
        }
        drop(state);
        self.persist()
    }

    pub fn get_equal(&self, key: &[u8]) -> Result<Vec<Rid>, EasyDbError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let id = bucket_for(&state, key);
        Ok(state.buckets[id]
            .entries
            .iter()
            .find(|e| e.key == key)
            .map(|e| e.rids.iter().map(|r| Rid::from_encoded(*r)).collect())
            .unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn insert_lookup_delete_and_reopen() {
        let dir = tempdir().unwrap();
        let disk = std::sync::Arc::new(DiskManager::new(dir.path()).unwrap());
        let path = dir.path().join("hash.idx");
        let index = ExtendibleHashIndex::create(&path, &disk).unwrap();
        let rid1 = Rid::new(1, 2);
        let rid2 = Rid::new(3, 4);
        index.insert_entry(b"a", rid1).unwrap();
        index.insert_entry(b"a", rid2).unwrap();
        assert_eq!(index.get_equal(b"a").unwrap().len(), 2);
        index.delete_entry(b"a", rid1).unwrap();
        assert_eq!(index.get_equal(b"a").unwrap(), vec![rid2]);
        disk.close_file(index.fd()).unwrap();
        let reopened = ExtendibleHashIndex::open(&path, &disk).unwrap();
        assert_eq!(reopened.get_equal(b"a").unwrap(), vec![rid2]);
    }
}
