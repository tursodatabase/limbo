use limbo_ext::{register_extension, ResultCode, VfsDerive, VfsExtension};
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

    fn open(&self, path: &str, flags: i32, _direct: bool) -> Option<Self::File> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(flags & 1 != 0)
            .open(path)
            .ok()?;
        Some(TestFile { file })
    }

    fn close(&self, file: Self::File) -> ResultCode {
        drop(file);
        ResultCode::OK
    }

    fn read(&self, file: &mut Self::File, buf: &mut [u8], count: usize, offset: i64) -> i32 {
        match file.file.seek(SeekFrom::Start(offset as u64)) {
            Ok(_) => {}
            Err(_) => return -1,
        }
        match file.file.read(&mut buf[..count]) {
            Ok(n) => n as i32,
            Err(_) => -1,
        }
    }

    fn write(&self, file: &mut Self::File, buf: &[u8], count: usize, offset: i64) -> i32 {
        match file.file.seek(SeekFrom::Start(offset as u64)) {
            Ok(_) => {}
            Err(_) => return -1,
        }
        match file.file.write(&buf[..count]) {
            Ok(n) => n as i32,
            Err(_) => -1,
        }
    }

    fn sync(&self, file: &Self::File) -> i32 {
        file.file.sync_all().map(|_| 0).unwrap_or(-1)
    }

    fn lock(&self, _file: &Self::File, _exclusive: bool) -> ResultCode {
        ResultCode::OK
    }

    fn unlock(&self, _file: &Self::File) -> ResultCode {
        ResultCode::OK
    }

    fn size(&self, file: &Self::File) -> i64 {
        file.file.metadata().map(|m| m.len() as i64).unwrap_or(-1)
    }
}
