use limbo_ext::{register_extension, scalar, Result, ResultCode, VfsDerive, VfsExtension, VfsFile};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};

register_extension! {
    vfs: { TestFS },
    scalars: { test_scalar },
}

struct TestFile {
    file: File,
}

#[derive(VfsDerive, Default)]
struct TestFS;

// Test that we can have additional extension types in the same file
// and still register the vfs at comptime if linking staticly
#[scalar(name = "test_scalar")]
fn test_scalar(_args: limbo_ext::Value) -> limbo_ext::Value {
    limbo_ext::Value::from_integer(42)
}

impl VfsExtension for TestFS {
    const NAME: &'static str = "testvfs";
    type File = TestFile;
    fn open_file(&self, path: &str, flags: i32, _direct: bool) -> Result<Self::File> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(flags & 1 != 0)
            .open(path)
            .map_err(|_| ResultCode::Error)?;
        Ok(TestFile { file })
    }
}

impl VfsFile for TestFile {
    fn read(&mut self, buf: &mut [u8], count: usize, offset: i64) -> Result<i32> {
        if self.file.seek(SeekFrom::Start(offset as u64)).is_err() {
            return Err(ResultCode::Error);
        }
        self.file
            .read(&mut buf[..count])
            .map_err(|_| ResultCode::Error)
            .map(|n| n as i32)
    }

    fn write(&mut self, buf: &[u8], count: usize, offset: i64) -> Result<i32> {
        if self.file.seek(SeekFrom::Start(offset as u64)).is_err() {
            return Err(ResultCode::Error);
        }
        self.file
            .write(&buf[..count])
            .map_err(|_| ResultCode::Error)
            .map(|n| n as i32)
    }

    fn sync(&self) -> Result<()> {
        self.file.sync_all().map_err(|_| ResultCode::Error)
    }

    fn size(&self) -> i64 {
        self.file.metadata().map(|m| m.len() as i64).unwrap_or(-1)
    }
}
