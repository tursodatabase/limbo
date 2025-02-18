//! Normal memory page using btree

use crate::Result;
use std::collections::BTreeMap;

use super::MemPage;

#[derive(Debug)]
pub struct MemoryPages {
    inner: BTreeMap<usize, MemPage>,
}

impl MemoryPages {
    pub fn new(_len: usize) -> Result<Self> {
        Ok(MemoryPages {
            inner: BTreeMap::new(),
        })
    }
}
