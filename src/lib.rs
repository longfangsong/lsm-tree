use crate::error::Error;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::path::{PathBuf, Path};
use std::sync::{RwLock, Arc};
use std::fs::{File, OpenOptions};
use crate::sstable::SSTable;
use std::str::FromStr;
use std::{fs, thread};
use std::io::{Write, Seek, SeekFrom};
use std::ops::Deref;
use std::collections::BTreeMap;

mod error;
mod sstable;

const UNSORTED_COMPACT_FILE_SIZE: u64 = 1024 * 1024;

const COMPACT_FILE_LIMIT: usize = 10;

pub type Result<T> = std::result::Result<T, Error>;

pub struct LSMTree<K: Serialize + DeserializeOwned + Ord, V: Serialize + DeserializeOwned> {
    path: PathBuf,
    unsorted: RwLock<File>,
    // sorted[0][m] is the most recent compacted file
    // please lock these in "the most recent to the least recent" order!
    sorted: RwLock<Vec<Vec<SSTable<K, V>>>>,
}

fn number_file_name_count<P: AsRef<Path>>(path: P) -> Result<usize> {
    Ok(fs::read_dir(path)?
        .filter_map(|it| it.ok())
        .filter_map(|it| it.file_name().into_string().ok())
        .filter_map(|it| usize::from_str(&it).ok())
        .max()
        .map(|it| it + 1)
        .unwrap_or(0))
}

impl<K: Serialize + DeserializeOwned + Ord + Clone + Send + 'static, V: Serialize + DeserializeOwned + Clone + Send + 'static> LSMTree<K, V> {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let level_count = number_file_name_count(&path)?;
        let mut sorted = vec![];
        for i in 0..level_count {
            let mut level = vec![];
            let level_path = &path.as_ref().join(format!("{}", i));
            let max_file_id = number_file_name_count(level_path)?;
            for i in 0..max_file_id {
                let sstable: SSTable<K, V> = SSTable::new(&level_path, &format!("{}", i))?;
                level.push(sstable);
            }
            sorted.push(level);
        }
        let unsorted = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path.as_ref().join("unordered.sst"))?;
        Ok(Self {
            path: path.as_ref().to_path_buf(),
            unsorted: RwLock::new(unsorted),
            sorted: RwLock::new(sorted),
        })
    }

    fn compact_level(from: &[SSTable<K, V>], to: &mut SSTable<K, V>) {
        let mut remaining_files: Vec<_> = from.iter()
            .rev() // latest inserted file's index is the largest
            .map(|it| it.reader().into_iter().unwrap())
            .collect();
        let mut writer = to.writer();
        while !remaining_files.is_empty() {
            let (k, v) = remaining_files.iter()
                .map(|it| it.peek().unwrap())
                .min_by_key(|it| it.0.clone())
                .unwrap();
            for remaining_file in remaining_files.iter_mut() {
                if remaining_file.peek().clone().unwrap().0 == k {
                    remaining_file.next();
                }
            }
            remaining_files.retain(|it| it.peek().is_some());
            writer.append(k.clone(), v.clone()).unwrap();
        }
    }

    fn compact(self: Arc<Self>) {
        let mut unsorted = self.unsorted.write().unwrap();
        unsorted.seek(SeekFrom::Start(0)).unwrap();
        let mut unsorted_content: BTreeMap<K, V> = BTreeMap::new();
        while let (Ok(k), Ok(v)) = (bincode::deserialize_from(unsorted.deref()), bincode::deserialize_from(unsorted.deref())) {
            unsorted_content.insert(k, v);
        }

        let mut sorted_guard = self.sorted.write().unwrap();
        if sorted_guard.len() == 0 {
            sorted_guard.push(vec![]);
        }
        let level_id = "0";
        let file_id = sorted_guard[0].len();
        let path = self.path.join(level_id);
        let new_table = SSTable::new(path, &format!("{}", file_id)).unwrap();
        let mut writer = new_table.writer();
        for (k, v) in unsorted_content {
            writer.append(k, v).unwrap();
        }
        drop(writer);
        // now all data in unsorted is in new table, it is safe to clear the old table
        unsorted.set_len(0).unwrap();
        unsorted.seek(SeekFrom::Start(0)).unwrap();
        drop(unsorted);

        sorted_guard[0].push(new_table);
        for level_id in 0..sorted_guard.len() {
            if sorted_guard[level_id].len() < COMPACT_FILE_LIMIT {
                break;
            } else {
                if level_id == sorted_guard.len() - 1 {
                    sorted_guard.push(vec![]);
                }
                let mut new_table = SSTable::new(
                    self.path.join(&format!("{}", level_id + 1)),
                    &sorted_guard.get(level_id + 1)
                        .map(|it| it.len())
                        .map(|it| format!("{}", it))
                        .unwrap_or_else(|| "0".to_string())).unwrap();
                Self::compact_level(&sorted_guard[level_id], &mut new_table);
                sorted_guard[level_id + 1].push(new_table);
                sorted_guard[level_id].clear();
                for i in 0..COMPACT_FILE_LIMIT {
                    fs::remove_dir_all(self.path
                        .join(format!("{}", level_id))
                        .join(format!("{}", i)))
                        .unwrap();
                }
            }
        }
    }
}

trait LSMTreeExt<K, V> {
    fn insert(&mut self, key: K, value: V) -> Result<()>;
}

impl<K: Serialize + DeserializeOwned + Ord + Clone + Send + 'static, V: Serialize + DeserializeOwned + Clone + Send + 'static> LSMTreeExt<K, V> for Arc<LSMTree<K, V>> {
    fn insert(&mut self, key: K, value: V) -> Result<()> {
        let mut guard = self.unsorted.write().unwrap();
        guard.write_all(&bincode::serialize(&key)?)?;
        guard.write_all(&bincode::serialize(&value)?)?;
        let s = self.clone();
        if guard.metadata()?.len() >= UNSORTED_COMPACT_FILE_SIZE {
            thread::spawn(move || LSMTree::compact(s));
        }
        Ok(())
    }
}

unsafe impl<K: Serialize + DeserializeOwned + Ord + Clone + Send, V: Serialize + DeserializeOwned + Clone + Send> Sync for LSMTree<K, V> {}

#[cfg(test)]
mod tests {
    use crate::sstable::SSTable;
    use crate::LSMTree;
    use std::sync::Arc;
    use std::thread::sleep;
    use std::{thread, time};

    #[test]
    fn test_compact_level() {
        use tempfile;

        let dir = tempfile::tempdir().unwrap();
        let level0_path = dir.path().join("0");
        let level0_file0: SSTable<u64, u64> = SSTable::new(&level0_path, "0").unwrap();
        let mut writer = level0_file0.writer();
        for i in 1..20 {
            writer.append(i, i).unwrap();
        }
        drop(writer);
        let level0_file1 = SSTable::new(&level0_path, "1").unwrap();
        let mut writer = level0_file1.writer();
        for i in (1..20).step_by(2) {
            writer.append(i, 100 + i).unwrap();
        }
        drop(writer);
        let level0 = vec![level0_file0, level0_file1];
        let level1_path = dir.path().join("1");
        let mut level1_file0: SSTable<u64, u64> = SSTable::new(&level1_path, "0").unwrap();
        LSMTree::compact_level(&level0, &mut level1_file0);
        let reader = level1_file0.reader();
        assert_eq!(reader.binary_search(1).unwrap().unwrap(), 101);
        assert_eq!(reader.binary_search(2).unwrap().unwrap(), 2);
        assert_eq!(reader.binary_search(3).unwrap().unwrap(), 103);
        assert_eq!(reader.binary_search(4).unwrap().unwrap(), 4);
        assert_eq!(reader.binary_search(100).unwrap(), None);
    }

    #[test]
    fn test_compact() {
        use tempfile;
        use std::time;
        use crate::LSMTreeExt;
        let dir = tempfile::tempdir().unwrap();
        for i in 0..9 {
            let level0_path = dir.path().join("0");
            let level0_file: SSTable<u64, u64> = SSTable::new(&level0_path, &format!("{}", i)).unwrap();
            let mut writer = level0_file.writer();
            for i in 0..65536 {
                writer.append(i, i + 100 * i).unwrap();
            }
        }

        let mut tree: Arc<LSMTree<u64, u64>> = Arc::new(LSMTree::new(dir.path()).unwrap());
        assert_eq!(tree.sorted.read().unwrap()[0].len(), 9);
        for i in 0..65536 {
            tree.insert(i, i).unwrap();
        }
        thread::sleep(time::Duration::from_secs(1));
        assert_eq!(tree.sorted.read().unwrap()[1].len(), 1);
        assert_eq!(tree.sorted.read().unwrap()[0].len(), 0);
    }
}
