use std::collections::{BTreeMap, btree_map::Range};

use crate::error::Result;
use crate::storage::engine::{Engine, Status};

#[derive(Default)]
pub struct Memory(BTreeMap<Vec<u8>, Vec<u8>>);

impl Memory {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Engine for Memory {
    type ScanIterator<'a> = ScanIterator<'a>;

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.0.remove(key);
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.0.get(key).cloned())
    }

    fn scan(&mut self, range: impl std::ops::RangeBounds<Vec<u8>>) -> Self::ScanIterator<'_>
    where
        Self: Sized,
    {
        ScanIterator(self.0.range(range))
    }

    fn scan_dyn(
        &mut self,
        range: (std::ops::Bound<Vec<u8>>, std::ops::Bound<Vec<u8>>),
    ) -> Box<dyn super::engine::ScanIterator + '_> {
        Box::new(self.scan(range))
    }

    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.0.insert(key.to_vec(), value);
        Ok(())
    }

    fn status(&mut self) -> Result<super::engine::Status> {
        Ok(Status {
            name: "memory".to_string(),
            keys: self.0.len() as u64,
            size: self.0.iter().map(|(k, v)| (k.len() + v.len()) as u64).sum(),
            disk_size: 0,
            live_disk_size: 0,
        })
    }
}

pub struct ScanIterator<'a>(Range<'a, Vec<u8>, Vec<u8>>);

impl Iterator for ScanIterator<'_> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|(k, v)| Ok((k.clone(), v.clone())))
    }
}

impl DoubleEndedIterator for ScanIterator<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0.next_back().map(|(k, v)| Ok((k.clone(), v.clone())))
    }
}
