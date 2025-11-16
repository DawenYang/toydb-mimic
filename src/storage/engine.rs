use std::ops::{Bound, RangeBounds};

use crate::{encoding::keycode, error::Result};
use serde::{Deserialize, Serialize};

pub trait Engine: Send {
    type ScanIterator<'a>: ScanIterator + 'a
    where
        Self: Sized + 'a;

    fn delete(&mut self, key: &[u8]) -> Result<()>;

    fn flush(&mut self) -> Result<()>;

    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    fn scan(&mut self, range: impl RangeBounds<Vec<u8>>) -> Self::ScanIterator<'_>
    where
        Self: Sized;

    fn scan_dyn(&mut self, range: (Bound<Vec<u8>>, Bound<Vec<u8>>)) -> Box<dyn ScanIterator + '_>;

    fn scan_prefix(&mut self, prefix: &[u8]) -> Self::ScanIterator<'_>
    where
        Self: Sized,
    {
        self.scan(keycode::prefix_range(prefix))
    }

    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()>;

    fn status(&mut self) -> Result<Status>;
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Status {
    pub name: String,
    pub keys: u64,
    pub size: u64,
    pub disk_size: u64,
    pub live_disk_size: u64,
}

pub trait ScanIterator: DoubleEndedIterator<Item = Result<(Vec<u8>, Vec<u8>)>> {}

impl<I: DoubleEndedIterator<Item = Result<(Vec<u8>, Vec<u8>)>>> ScanIterator for I {}

impl Status {
    pub fn garbage_disk_size(&self) -> u64 {
        self.disk_size - self.live_disk_size
    }

    pub fn garbage_disk_percent(&self) -> f64 {
        if self.disk_size == 0 {
            return 0.0;
        }
        self.garbage_disk_size() as f64 / self.disk_size as f64 * 100.0
    }
}
