use crate::Result;
use bitflags::bitflags;
use cfg_block::cfg_block;
use std::fmt;
use std::sync::Arc;
use std::{
    cell::{Cell, Ref, RefCell, RefMut},
    fmt::Debug,
    mem::ManuallyDrop,
    pin::Pin,
    rc::Rc,
};

pub trait File: Send + Sync {
    fn lock_file(&self, exclusive: bool) -> Result<()>;
    fn unlock_file(&self) -> Result<()>;
    fn pread(&self, pos: usize, c: Arc<Completion>) -> Result<()>;
    fn pwrite(&self, pos: usize, buffer: Arc<RefCell<Buffer>>, c: Arc<Completion>) -> Result<()>;
    fn sync(&self, c: Arc<Completion>) -> Result<()>;
    fn size(&self) -> Result<u64>;
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct OpenFlags(i32);

bitflags! {
    impl OpenFlags: i32 {
        const None = 0b00000000;
        const Create = 0b0000001;
        const ReadOnly = 0b0000010;
    }
}

impl Default for OpenFlags {
    fn default() -> Self {
        Self::Create
    }
}

pub trait IO: Clock + Send + Sync {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>>;

    fn run_once(&self) -> Result<()>;

    fn wait_for_completion(&self, c: Arc<Completion>) -> Result<()>;

    fn generate_random_number(&self) -> i64;

    fn get_memory_io(&self) -> Arc<MemoryIO>;
}

pub type Complete = dyn Fn(Arc<RefCell<Buffer>>);
pub type WriteComplete = dyn Fn(i32);
pub type SyncComplete = dyn Fn(i32);

pub enum Completion {
    Read(ReadCompletion),
    Write(WriteCompletion),
    Sync(SyncCompletion),
}

pub struct ReadCompletion {
    pub buf: Arc<RefCell<Buffer>>,
    pub complete: Box<Complete>,
    pub is_completed: Cell<bool>,
}

impl Completion {
    pub fn is_completed(&self) -> bool {
        match self {
            Self::Read(r) => r.is_completed.get(),
            Self::Write(w) => w.is_completed.get(),
            Self::Sync(s) => s.is_completed.get(),
        }
    }

    pub fn complete(&self, result: i32) {
        match self {
            Self::Read(r) => r.complete(),
            Self::Write(w) => w.complete(result),
            Self::Sync(s) => s.complete(result), // fix
        }
    }

    /// only call this method if you are sure that the completion is
    /// a ReadCompletion, panics otherwise
    pub fn as_read(&self) -> &ReadCompletion {
        match self {
            Self::Read(ref r) => r,
            _ => unreachable!(),
        }
    }
}

pub struct WriteCompletion {
    pub complete: Box<WriteComplete>,
    pub is_completed: Cell<bool>,
}

pub struct SyncCompletion {
    pub complete: Box<SyncComplete>,
    pub is_completed: Cell<bool>,
}

impl ReadCompletion {
    pub fn new(buf: Arc<RefCell<Buffer>>, complete: Box<Complete>) -> Self {
        Self {
            buf,
            complete,
            is_completed: Cell::new(false),
        }
    }

    pub fn buf(&self) -> Ref<'_, Buffer> {
        self.buf.borrow()
    }

    pub fn buf_mut(&self) -> RefMut<'_, Buffer> {
        self.buf.borrow_mut()
    }

    pub fn complete(&self) {
        (self.complete)(self.buf.clone());
        self.is_completed.set(true);
    }
}

impl WriteCompletion {
    pub fn new(complete: Box<WriteComplete>) -> Self {
        Self {
            complete,
            is_completed: Cell::new(false),
        }
    }

    pub fn complete(&self, bytes_written: i32) {
        (self.complete)(bytes_written);
        self.is_completed.set(true);
    }
}

impl SyncCompletion {
    pub fn new(complete: Box<SyncComplete>) -> Self {
        Self {
            complete,
            is_completed: Cell::new(false),
        }
    }

    pub fn complete(&self, res: i32) {
        (self.complete)(res);
        self.is_completed.set(true);
    }
}

pub type BufferData = Pin<Vec<u8>>;

pub type BufferDropFn = Rc<dyn Fn(BufferData)>;

#[derive(Clone)]
pub struct Buffer {
    data: ManuallyDrop<BufferData>,
    drop: BufferDropFn,
}

impl Debug for Buffer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.data)
    }
}

impl Drop for Buffer {
    fn drop(&mut self) {
        let data = unsafe { ManuallyDrop::take(&mut self.data) };
        (self.drop)(data);
    }
}

impl Buffer {
    pub fn allocate(size: usize, drop: BufferDropFn) -> Self {
        let data = ManuallyDrop::new(Pin::new(vec![0; size]));
        Self { data, drop }
    }

    pub fn new(data: BufferData, drop: BufferDropFn) -> Self {
        let data = ManuallyDrop::new(data);
        Self { data, drop }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.data
    }

    pub fn as_ptr(&self) -> *const u8 {
        self.data.as_ptr()
    }

    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.data.as_mut_ptr()
    }
}

cfg_block! {
    #[cfg(all(target_os = "linux", feature = "io_uring"))] {
        mod io_uring;
        #[cfg(feature = "fs")]
        pub use io_uring::UringIO;
        mod unix;
        #[cfg(feature = "fs")]
        pub use unix::UnixIO;
        pub use unix::UnixIO as SyscallIO;
        pub use unix::UnixIO as PlatformIO;
    }

    #[cfg(any(all(target_os = "linux",not(feature = "io_uring")), target_os = "macos"))] {
        mod unix;
        #[cfg(feature = "fs")]
        pub use unix::UnixIO;
        pub use unix::UnixIO as PlatformIO;
        pub use PlatformIO as SyscallIO;
    }

    #[cfg(target_os = "windows")] {
        mod windows;
        pub use windows::WindowsIO as PlatformIO;
        pub use PlatformIO as SyscallIO;
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))] {
        mod generic;
        pub use generic::GenericIO as PlatformIO;
        pub use PlatformIO as SyscallIO;
    }
}

mod memory;
#[cfg(feature = "fs")]
mod vfs;
pub use memory::MemoryIO;
pub mod clock;
mod common;
pub use clock::Clock;
