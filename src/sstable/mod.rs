use std::fs::{File, create_dir_all, OpenOptions};
use std::path::{Path, PathBuf};
use serde::Serialize;
use crate::Result;
use std::io::{Seek, SeekFrom};
use std::marker::PhantomData;
use std::cmp::Ordering;
use serde::de::DeserializeOwned;
use std::cell::RefCell;
use std::ops::DerefMut;
use std::mem;
use std::sync::Mutex;

const CONTENT_FILENAME: &str = "content.sst";
const KEY_OFFSET_FILENAME: &str = "key_offset.sstm";

struct RWMeta {
    reader_count: usize,
    writing: bool,
}

pub struct SSTable<K: Serialize, V: Serialize> {
    meta: Mutex<RWMeta>,
    path: PathBuf,
    phantom_1: PhantomData<[K]>,
    phantom_2: PhantomData<[V]>,
}

pub struct SSTableReader<'a, K: Serialize, V: Serialize> {
    file_descriptor: File,
    belong_to: &'a SSTable<K, V>,
}

impl<'a, K: Serialize, V: Serialize> Drop for SSTableReader<'a, K, V> {
    fn drop(&mut self) {
        self.belong_to.meta.lock().unwrap().reader_count -= 1;
    }
}

pub struct SSTableWriter<'a, K: Serialize, V: Serialize> {
    file_descriptor: File,
    belong_to: &'a SSTable<K, V>,
}

impl<'a, K: Serialize, V: Serialize> Drop for SSTableWriter<'a, K, V> {
    fn drop(&mut self) {
        self.belong_to.meta.lock().unwrap().writing = false;
    }
}

impl<K: Serialize + DeserializeOwned + Ord, V: Serialize + DeserializeOwned> SSTable<K, V> {
    pub fn new<P: AsRef<Path>>(path: P, name: &str) -> Result<Self> {
        let folder_path = path.as_ref().join(name);
        create_dir_all(&folder_path)?;
        Ok(SSTable {
            path: folder_path,
            phantom_1: PhantomData,
            phantom_2: PhantomData,
        })
    }

    pub fn new<P: AsRef<Path>>(path: P, name: &str) -> Result<Self> {
        let folder_path = path.as_ref().join(name);
        create_dir_all(&folder_path)?;
        Ok(SSTable {
            path: folder_path,
            phantom_1: PhantomData,
            phantom_2: PhantomData,
        })
    }

    fn append(content: &mut File, key_offset: &mut File, key: K, value: V) -> Result<()> {
        let current_offset = content.metadata()?.len();
        bincode::serialize_into(key_offset, &current_offset)?;
        bincode::serialize_into(content, &key)?;
        bincode::serialize_into(content, &value)?;
        Ok(())
    }


    fn read<T: DeserializeOwned>(&self) -> Result<T> {
        Ok(bincode::deserialize_from(self.content.borrow_mut().deref_mut())?)
    }

    fn read_at<T: DeserializeOwned>(&self, offset: u64) -> Result<T> {
        self.content.borrow_mut().seek(SeekFrom::Start(offset))?;
        Ok(bincode::deserialize_from(self.content.borrow_mut().deref_mut())?)
    }

    pub fn binary_search(&self, key: K) -> Result<Option<V>> {
        let u64_serialized_size = bincode::serialized_size(&0u64)?;
        let kv_pair_count = self.key_offset.borrow_mut().metadata()?.len() / u64_serialized_size;
        let mut left = 0;
        let mut right = kv_pair_count;
        while left < right {
            let mid = (left + right) / 2;
            self.key_offset.borrow_mut().seek(SeekFrom::Start(mid * u64_serialized_size))?;
            let offset = bincode::deserialize_from(self.key_offset.borrow_mut().deref_mut())?;
            let key_found = self.read_at(offset)?;
            match key.cmp(&key_found) {
                Ordering::Equal => {
                    let result = Ok(Some(bincode::deserialize_from(self.content.borrow_mut().deref_mut())?));
                    self.content.borrow_mut().seek(SeekFrom::End(0))?;
                    return result;
                }
                Ordering::Less => right = mid,
                Ordering::Greater => left = mid + 1,
            }
        }
        Ok(None)
    }
}

pub struct SSTableIterator<'a, K: Serialize, V: Serialize> {
    refer_to: &'a SSTable<K, V>,
    peeked: Option<(K, V)>,
}

impl<'a, K: Serialize + DeserializeOwned + Ord, V: Serialize + DeserializeOwned> Iterator for SSTableIterator<'a, K, V> {
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        let key: Option<K> = self.refer_to.read().ok();
        let value: Option<V> = self.refer_to.read().ok();
        let mut result = None;
        if let (Some(key), Some(value)) = (key, value) {
            result = mem::replace(&mut self.peeked, Some((key, value)));
        } else {
            self.peeked = None
        }
        result
    }
}

impl<'a, K: Serialize + DeserializeOwned + Ord, V: Serialize + DeserializeOwned> SSTableIterator<'a, K, V> {
    pub fn new(refer_to: &'a SSTable<K, V>) -> Self {
        let mut result = Self {
            refer_to,
            peeked: None,
        };
        let key: Option<K> = refer_to.read().ok();
        let value: Option<V> = refer_to.read().ok();
        if let (Some(key), Some(value)) = (key, value) {
            result.peeked = Some((key, value));
        }
        result
    }
    pub fn peek(&self) -> &Option<(K, V)> {
        &self.peeked
    }
}

impl<K: Serialize + DeserializeOwned + Ord, V: Serialize + DeserializeOwned> SSTable<K, V> {
    pub fn iter(&self) -> Result<SSTableIterator<K, V>> {
        self.key_offset.borrow_mut().seek(SeekFrom::Start(0))?;
        self.content.borrow_mut().seek(SeekFrom::Start(0))?;
        Ok(SSTableIterator::new(self))
    }
}

#[test]
fn test_sstable() {
    use tempfile;

    let dir = tempfile::tempdir().unwrap();
    let mut table = SSTable::new(dir.path(), "0").unwrap();
    table.append(1u64, 11u64).unwrap();
    table.append(4u64, 44u64).unwrap();
    table.append(9u64, 99u64).unwrap();
    assert_eq!(table.binary_search(9).unwrap(), Some(99u64));
    assert_eq!(table.binary_search(4).unwrap(), Some(44u64));
    assert_eq!(table.binary_search(5).unwrap(), None);
    let mut iter = table.iter().unwrap();
    assert_eq!(iter.next(), Some((1, 11)));
    assert_eq!(iter.next(), Some((4, 44)));
    assert_eq!(iter.next(), Some((9, 99)));
}