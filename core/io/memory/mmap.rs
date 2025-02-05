//! Mmap code using memmap2

use crate::Result;
use core::ops::{Deref, DerefMut};

use memmap2::MmapMut;

#[derive(Debug)]
pub struct MmapAnon {
    inner: MmapMut,
}

impl MmapAnon {
    pub fn new(len: usize) -> Result<Self> {
        Ok(MmapAnon {
            inner: MmapMut::map_anon(len)?,
        })
    }
}

impl Deref for MmapAnon {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.inner.deref()
    }
}

impl DerefMut for MmapAnon {
    fn deref_mut(&mut self) -> &mut [u8] {
        self.inner.deref_mut()
    }
}

impl AsRef<[u8]> for MmapAnon {
    fn as_ref(&self) -> &[u8] {
        self.deref()
    }
}

impl AsMut<[u8]> for MmapAnon {
    fn as_mut(&mut self) -> &mut [u8] {
        self.deref_mut()
    }
}
