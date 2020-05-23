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
use std::{mem, thread};
use std::sync::Mutex;

const CONTENT_FILENAME: &str = "content.sst";
const KEY_OFFSET_FILENAME: &str = "key_offset.sstm";

#[derive(Default)]
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
    content_file_descriptor: RefCell<File>,
    key_offset_file_descriptor: RefCell<File>,
    belong_to: &'a SSTable<K, V>,
}

impl<'a, K: DeserializeOwned + Ord + Serialize, V: Serialize + DeserializeOwned> SSTableReader<'a, K, V> {
    fn read<T: DeserializeOwned>(&self) -> Result<T> {
        Ok(bincode::deserialize_from(self.content_file_descriptor.borrow_mut().deref_mut())?)
    }

    fn read_at<T: DeserializeOwned>(&self, offset: u64) -> Result<T> {
        self.content_file_descriptor.borrow_mut().seek(SeekFrom::Start(offset))?;
        Ok(bincode::deserialize_from(self.content_file_descriptor.borrow_mut().deref_mut())?)
    }

    pub fn binary_search(&self, key: K) -> Result<Option<V>> {
        let u64_serialized_size = bincode::serialized_size(&0u64)?;
        let kv_pair_count = self.key_offset_file_descriptor.borrow_mut().metadata()?.len() / u64_serialized_size;
        let mut left = 0;
        let mut right = kv_pair_count;
        while left < right {
            let mid = (left + right) / 2;
            self.key_offset_file_descriptor.borrow_mut().seek(SeekFrom::Start(mid * u64_serialized_size))?;
            let offset = bincode::deserialize_from(self.key_offset_file_descriptor.borrow_mut().deref_mut())?;
            let key_found = self.read_at(offset)?;
            match key.cmp(&key_found) {
                Ordering::Equal => {
                    let result = Ok(Some(bincode::deserialize_from(self.content_file_descriptor.borrow_mut().deref_mut())?));
                    self.key_offset_file_descriptor.borrow_mut().seek(SeekFrom::End(0))?;
                    return result;
                }
                Ordering::Less => right = mid,
                Ordering::Greater => left = mid + 1,
            }
        }
        Ok(None)
    }

    pub fn into_iter(self) -> Result<SSTableIterator<'a, K, V>> {
        self.key_offset_file_descriptor.borrow_mut().seek(SeekFrom::Start(0))?;
        self.content_file_descriptor.borrow_mut().seek(SeekFrom::Start(0))?;
        Ok(SSTableIterator::new(self))
    }
}

impl<'a, K: Serialize, V: Serialize> Drop for SSTableReader<'a, K, V> {
    fn drop(&mut self) {
        let mut guard = self.belong_to.meta.lock().unwrap();
        guard.reader_count -= 1;
    }
}

pub struct SSTableWriter<'a, K: Serialize, V: Serialize> {
    content_file_descriptor: File,
    key_offset_file_descriptor: File,
    belong_to: &'a SSTable<K, V>,
}

impl<'a, K: Serialize, V: Serialize> SSTableWriter<'a, K, V> {
    pub fn append(&mut self, key: K, value: V) -> Result<()> {
        let current_offset = self.content_file_descriptor.metadata()?.len();
        bincode::serialize_into(&self.key_offset_file_descriptor, &current_offset)?;
        bincode::serialize_into(&self.content_file_descriptor, &key)?;
        bincode::serialize_into(&self.content_file_descriptor, &value)?;
        Ok(())
    }
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
            meta: Mutex::new(Default::default()),
            path: folder_path,
            phantom_1: PhantomData,
            phantom_2: PhantomData,
        })
    }
    pub fn reader(&self) -> SSTableReader<'_, K, V> {
        loop {
            let mut guard = self.meta.lock().unwrap();
            if !guard.writing {
                guard.reader_count += 1;
                break;
            }
            drop(guard);
            thread::yield_now();
        }
        SSTableReader {
            content_file_descriptor: RefCell::new(OpenOptions::new()
                .read(true)
                .open(self.path.join(CONTENT_FILENAME))
                .unwrap()),
            key_offset_file_descriptor: RefCell::new(OpenOptions::new()
                .read(true)
                .open(self.path.join(KEY_OFFSET_FILENAME))
                .unwrap()),
            belong_to: &self,
        }
    }
    pub fn writer(&self) -> SSTableWriter<'_, K, V> {
        loop {
            let mut guard = self.meta.lock().unwrap();
            if !guard.writing && guard.reader_count == 0 {
                guard.writing = true;
                break;
            }
            drop(guard);
            thread::yield_now();
        }
        SSTableWriter {
            content_file_descriptor: OpenOptions::new()
                .write(true)
                .create(true)
                .open(self.path.join(CONTENT_FILENAME))
                .unwrap(),
            key_offset_file_descriptor: OpenOptions::new()
                .write(true)
                .create(true)
                .open(self.path.join(KEY_OFFSET_FILENAME))
                .unwrap(),
            belong_to: &self,
        }
    }
}

pub struct SSTableIterator<'a, K: Serialize, V: Serialize> {
    refer_to: SSTableReader<'a, K, V>,
    peeked: Option<(K, V)>,
}

impl<'a, K: Serialize + DeserializeOwned + Ord, V: Serialize + DeserializeOwned> Iterator for SSTableIterator<'a, K, V> {
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        let new = if let (Ok(key), Ok(value)) = (self.refer_to.read(), self.refer_to.read()) {
            Some((key, value))
        } else {
            None
        };
        mem::replace(&mut self.peeked, new)
    }
}

impl<'a, K: Serialize + DeserializeOwned + Ord, V: Serialize + DeserializeOwned> SSTableIterator<'a, K, V> {
    pub fn new(refer_to: SSTableReader<'a, K, V>) -> Self {
        let mut result = Self {
            refer_to,
            peeked: None,
        };
        if let (Ok(key), Ok(value)) = (result.refer_to.read(), result.refer_to.read()) {
            result.peeked = Some((key, value));
        }
        result
    }
}

impl<'a, K: Serialize + DeserializeOwned + Ord + Clone, V: Serialize + DeserializeOwned + Clone> SSTableIterator<'a, K, V> {
    pub fn peek(&self) -> Option<(K, V)> {
        self.peeked.clone()
    }
}

#[test]
fn test_sstable() {
    use tempfile;

    let dir = tempfile::tempdir().unwrap();
    let mut table = SSTable::new(dir.path(), "0").unwrap();
    let mut writer = table.writer();
    writer.append(1u64, 11u64).unwrap();
    writer.append(4u64, 44u64).unwrap();
    writer.append(9u64, 99u64).unwrap();
    drop(writer);
    let mut reader = table.reader();
    assert_eq!(reader.binary_search(9).unwrap(), Some(99u64));
    assert_eq!(reader.binary_search(4).unwrap(), Some(44u64));
    assert_eq!(reader.binary_search(5).unwrap(), None);
    let mut iter = reader.into_iter().unwrap();
    assert_eq!(iter.next(), Some((1, 11)));
    assert_eq!(iter.next(), Some((4, 44)));
    assert_eq!(iter.next(), Some((9, 99)));
}