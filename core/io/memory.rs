use super::{Buffer, Completion, File, OpenFlags, IO};
use crate::Result;

use log::debug;
#[cfg(target_os = "linux")]
use memmap2::RemapOptions;

use memmap2::MmapMut;
use std::{
    cell::{Cell, OnceCell, RefCell, UnsafeCell},
    rc::Rc,
    sync::Arc,
};

pub struct MemoryIO {
    size: Cell<usize>,
    pages: UnsafeCell<MmapMut>,
}

// TODO: page size flag
const PAGE_SIZE: usize = 4096;
type MemPage = [u8];
const INITIAL_PAGES: OnceCell<usize> = OnceCell::new();

impl MemoryIO {
    #[allow(clippy::arc_with_non_send_sync)]
    pub fn new() -> Result<Arc<Self>> {
        debug!("Using IO backend 'memory'");
        // INITIAL PAGES changes in CI to test for resizing of memory pages
        let initial_pages =
            INITIAL_PAGES.get_or_init(|| std::env::var("CI").map(|_| 5).unwrap_or_else(|_| 16)).clone();
        Ok(Arc::new(Self {
            size: 0.into(),
            // Initial size of 4kb * 16 = 64kb
            pages: MmapMut::map_anon(PAGE_SIZE * initial_pages)?.into(),
        }))
    }

    #[allow(clippy::mut_from_ref)]
    fn get_or_allocate_page(&self, page_no: usize) -> Result<&mut MemPage> {
        let start = page_no * PAGE_SIZE;
        let end = start + PAGE_SIZE;
        let len_pages = unsafe {
            let page = &mut *self.pages.get();
            page.len()
        };

        if end > len_pages {
            let cap_pages = len_pages / PAGE_SIZE;
            self.resize(cap_pages * 2)?;
        }

        let page = unsafe {
            let page = &mut *self.pages.get();
            &mut page[start..end]
        };
        Ok(page)
    }

    fn get_page(&self, page_no: usize) -> Option<&MemPage> {
        let start = page_no * PAGE_SIZE;
        let end = start + PAGE_SIZE;
        let pages = unsafe { &mut *self.pages.get() };

        // Make sure that we are accessing into a valid memory page
        assert!(end <= pages.len());

        let page = &pages[start..end];
        if page.is_empty() {
            None
        } else {
            Some(page)
        }
    }

    fn resize(&self, capacity: usize) -> Result<()> {
        let pages = unsafe { &mut *self.pages.get() };
        let new_cap = PAGE_SIZE * capacity;
        #[cfg(target_os = "linux")]
        {
            unsafe {
                pages.remap(new_cap, RemapOptions::new().may_move(true))?;
            }
        }

        #[cfg(not(target_os = "linux"))]
        {
            let mut new_pages = MmapMut::map_anon(new_cap)?;
            new_pages[..pages.len()].clone_from_slice(pages);

            *pages = new_pages;
        }

        Ok(())
    }
}

impl IO for Arc<MemoryIO> {
    fn open_file(&self, _path: &str, _flags: OpenFlags, _direct: bool) -> Result<Rc<dyn File>> {
        Ok(Rc::new(MemoryFile {
            io: Arc::clone(self),
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
    io: Arc<MemoryIO>,
}

impl File for MemoryFile {
    fn lock_file(&self, _exclusive: bool) -> Result<()> {
        Ok(())
    }
    fn unlock_file(&self) -> Result<()> {
        Ok(())
    }

    fn pread(&self, pos: usize, c: Completion) -> Result<()> {
        let r = match &c {
            Completion::Read(ref r) => r,
            _ => unreachable!(),
        };
        let buf_len = r.buf().len();
        if buf_len == 0 {
            c.complete(0);
            return Ok(());
        }

        let file_size = self.io.size.get();
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

                if let Some(page) = self.io.get_page(page_no) {
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

    fn pwrite(&self, pos: usize, buffer: Rc<RefCell<Buffer>>, c: Completion) -> Result<()> {
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
                let page = self.io.get_or_allocate_page(page_no)?;
                page[page_offset..page_offset + bytes_to_write]
                    .copy_from_slice(&data[buf_offset..buf_offset + bytes_to_write]);
            }

            offset += bytes_to_write;
            buf_offset += bytes_to_write;
            remaining -= bytes_to_write;
        }

        self.io
            .size
            .set(core::cmp::max(pos + buf_len, self.io.size.get()));

        c.complete(buf_len as i32);
        Ok(())
    }

    fn sync(&self, c: Completion) -> Result<()> {
        // no-op
        c.complete(0);
        Ok(())
    }

    fn size(&self) -> Result<u64> {
        Ok(self.io.size.get() as u64)
    }
}

impl Drop for MemoryFile {
    fn drop(&mut self) {
        // no-op
        // TODO ideally we could have some flags
        // in Memory File, that when this is dropped
        // if the persist flag is true we could flush the mmap to disk
        // We would also have to store the file name here in this case
    }
}
