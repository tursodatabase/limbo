use limbo_ext::{register_extension, Result, ResultCode, VfsDerive, VfsExtension, VfsFile};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};

register_extension! {
    vfs: { TestFS },
}

struct TestFile {
    file: File,
}

#[derive(VfsDerive, Default)]
struct TestFS;

impl VfsExtension for TestFS {
    const NAME: &'static str = "testfs";
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

    fn run_once(&self) -> Result<()> {
        Ok(())
    }

    fn close(&self, file: Self::File) -> Result<()> {
        drop(file);
        Ok(())
    }

    fn get_current_time(&self) -> String {
        chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string()
    }

    fn generate_random_number(&self) -> i64 {
        let mut buf = [0u8; 8];
        getrandom::fill(&mut buf).unwrap();
        i64::from_ne_bytes(buf)
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

    fn lock(&mut self, _exclusive: bool) -> Result<()> {
        Ok(())
    }

    fn unlock(&self) -> Result<()> {
        Ok(())
    }

    fn size(&self) -> i64 {
        self.file.metadata().map(|m| m.len() as i64).unwrap_or(-1)
    }
}
