use serde::{Deserialize, Serialize};

pub trait Engine: Send {
    type ScanIterator<'a>: ScanIte
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
