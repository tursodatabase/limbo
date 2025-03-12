#[cfg(target_family = "unix")]
mod mmap_rustix;

#[cfg(target_family = "unix")]
use mmap_rustix::MemoryPages;

#[cfg(not(target_family = "unix"))]
mod btree;

#[cfg(not(target_family = "unix"))]
use btree::MemoryPages;

use super::{Buffer, Completion, File, OpenFlags, IO};
use crate::Result;

use std::{
    cell::{Cell, RefCell, UnsafeCell},
    collections::BTreeMap,
    sync::Arc,
};
use tracing::debug;

pub struct MemoryIO {}
unsafe impl Send for MemoryIO {}

// TODO: page size flag
const PAGE_SIZE: usize = 4096;
type MemPage = Box<[u8; PAGE_SIZE]>;

impl MemoryIO {
    #[allow(clippy::arc_with_non_send_sync)]
    pub fn new() -> Self {
        debug!("Using IO backend 'memory'");
        Self {}
    }
}

impl Default for MemoryIO {
    fn default() -> Self {
        Self::new()
    }
}

impl IO for MemoryIO {
    fn open_file(&self, _path: &str, _flags: OpenFlags, _direct: bool) -> Result<Arc<dyn File>> {
        Ok(Arc::new(MemoryFile {
            pages: BTreeMap::new().into(),
            size: 0.into(),
        }))
    }

    fn run_once(&self) -> Result<()> {
        // nop
        Ok(())
    }

    fn generate_random_number(&self) -> i64 {
        let mut buf = [0u8; 8];
        getrandom::getrandom(&mut buf).unwrap();
        i64::from_ne_bytes(buf)
    }

    fn get_current_time(&self) -> String {
        chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string()
    }
}

pub struct MemoryFile {
    pages: UnsafeCell<BTreeMap<usize, MemPage>>,
    size: Cell<usize>,
}
unsafe impl Send for MemoryFile {}
unsafe impl Sync for MemoryFile {}

impl File for MemoryFile {
    fn lock_file(&self, _exclusive: bool) -> Result<()> {
        Ok(())
    }
    fn unlock_file(&self) -> Result<()> {
        Ok(())
    }

    fn pread(&self, pos: usize, c: Completion) -> Result<()> {
        let r = c.as_read();
        let buf_len = r.buf().len();
        if buf_len == 0 {
            c.complete(0);
            return Ok(());
        }

        let file_size = self.size.get();
        if pos >= file_size {
            c.complete(0);
            return Ok(());
        }

        let read_len = buf_len.min(file_size - pos);
        {
            let mut read_buf = r.buf_mut();
            let mut offset = pos;
            let mut remaining = read_len;
            let mut buf_offset = 0;

            while remaining > 0 {
                let page_no = offset / PAGE_SIZE;
                let page_offset = offset % PAGE_SIZE;
                let bytes_to_read = remaining.min(PAGE_SIZE - page_offset);
                if let Some(page) = self.get_page(page_no) {
                    read_buf.as_mut_slice()[buf_offset..buf_offset + bytes_to_read]
                        .copy_from_slice(&page[page_offset..page_offset + bytes_to_read]);
                } else {
                    read_buf.as_mut_slice()[buf_offset..buf_offset + bytes_to_read].fill(0);
                }

                offset += bytes_to_read;
                buf_offset += bytes_to_read;
                remaining -= bytes_to_read;
            }
        }
        c.complete(read_len as i32);
        Ok(())
    }

    fn pwrite(&self, pos: usize, buffer: Arc<RefCell<Buffer>>, c: Completion) -> Result<()> {
        let buf = buffer.borrow();
        let buf_len = buf.len();
        if buf_len == 0 {
            c.complete(0);
            return Ok(());
        }

        let mut offset = pos;
        let mut remaining = buf_len;
        let mut buf_offset = 0;
        let data = &buf.as_slice();

        while remaining > 0 {
            let page_no = offset / PAGE_SIZE;
            let page_offset = offset % PAGE_SIZE;
            let bytes_to_write = remaining.min(PAGE_SIZE - page_offset);

            {
                let page = self.get_or_allocate_page(page_no);
                page[page_offset..page_offset + bytes_to_write]
                    .copy_from_slice(&data[buf_offset..buf_offset + bytes_to_write]);
            }

            offset += bytes_to_write;
            buf_offset += bytes_to_write;
            remaining -= bytes_to_write;
        }

        self.size
            .set(core::cmp::max(pos + buf_len, self.size.get()));

        c.complete(buf_len as i32);
        Ok(())
    }

    fn sync(&self, c: Completion) -> Result<()> {
        // no-op
        c.complete(0);
        Ok(())
    }

    fn size(&self) -> Result<u64> {
        Ok(self.size.get() as u64)
    }
}

impl Drop for MemoryFile {
    fn drop(&mut self) {
        // no-op
    }
}

impl MemoryFile {
    #[allow(clippy::mut_from_ref)]
    fn get_or_allocate_page(&self, page_no: usize) -> &mut MemPage {
        unsafe {
            let pages = &mut *self.pages.get();
            pages
                .entry(page_no)
                .or_insert_with(|| Box::new([0; PAGE_SIZE]))
        }
    }

    fn get_page(&self, page_no: usize) -> Option<&MemPage> {
        unsafe { (*self.pages.get()).get(&page_no) }
    }
}
