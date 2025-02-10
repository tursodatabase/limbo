use aes::{cipher::KeyInit, Aes256};
use limbo_ext::{register_extension, ResultCode, VfsDerive, VfsExtension};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
};
use xts_mode::{get_tweak_default, Xts128};
const DEMO_KEY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

const PAGE_SIZE: usize = 4096;

register_extension! {
    vfs: { CryptFS },
}

#[derive(VfsDerive)]
struct CryptFS {
    // TODO(pthorpe): configurable cipher/length
    cipher: Xts128<Aes256>,
}

impl Default for CryptFS {
    // TODO(pthorpe): when we are supporting URI's for opening a new db,
    // we can get arguments to pass in to init instead of relying on default
    fn default() -> Self {
        let key = std::env::var("CRYPTFS_KEY").unwrap_or(String::from(DEMO_KEY));
        let key = key.as_bytes();
        if key.len() != 64 {
            eprintln!("CRYPTFS_KEY must be 64 bytes long for AES256");
            std::process::exit(1);
        }
        let mut key_bytes = [0u8; 64];
        key_bytes.copy_from_slice(key);
        CryptFS::new(key_bytes)
    }
}

struct CfsFile {
    file: File,
}

impl VfsExtension for CryptFS {
    const NAME: &'static str = "cryptfs";
    type File = CfsFile;

    fn open(&self, path: &str, flags: i32, _direct: bool) -> Option<Self::File> {
        let create = flags & 1 != 0;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(create)
            .open(path);
        match file {
            Ok(f) => Some(CfsFile { file: f }),
            Err(_) => None,
        }
    }

    fn close(&self, file: Self::File) -> ResultCode {
        drop(file);
        ResultCode::OK
    }

    fn read(&self, file: &mut Self::File, buf: &mut [u8], count: usize, offset: i64) -> i32 {
        let mut temp_buf = vec![0u8; count];
        match self.read_region(&mut file.file, offset as u64, &mut temp_buf) {
            Ok(n) => {
                buf[..n].copy_from_slice(&temp_buf[..n]);
                n as i32
            }
            Err(_) => -1,
        }
    }

    fn write(&self, file: &mut Self::File, buf: &[u8], count: usize, offset: i64) -> i32 {
        match self.write_region(&mut file.file, offset as u64, &buf[..count]) {
            Ok(n) => n as i32,
            Err(_) => -1,
        }
    }

    fn sync(&self, file: &Self::File) -> i32 {
        match file.file.sync_all() {
            Ok(_) => 0,
            Err(_) => -1,
        }
    }

    fn lock(&self, _file: &Self::File, _exclusive: bool) -> ResultCode {
        ResultCode::OK
    }

    fn unlock(&self, _file: &Self::File) -> ResultCode {
        ResultCode::OK
    }

    fn size(&self, file: &Self::File) -> i64 {
        match file.file.metadata() {
            Ok(meta) => meta.len() as i64,
            Err(_) => -1,
        }
    }
}

impl CryptFS {
    pub fn new(key: [u8; 64]) -> Self {
        if String::from_utf8_lossy(&key).contains('\0') {
            panic!("key contains null bytes");
        }
        if String::from_utf8_lossy(&key).eq(DEMO_KEY) {
            eprintln!("Using the default key, this is not secure");
        }
        let Ok(c1) = Aes256::new_from_slice(&key[..32]) else {
            panic!(
                "wrong length for key, expected 32 bytes but got {}",
                key.len()
            );
        };
        let Ok(c2) = Aes256::new_from_slice(&key[32..]) else {
            panic!(
                "wrong length for key, expected 32 bytes but got {}",
                key.len()
            );
        };
        Self {
            // we can store the cipher if we use a new tweak for each sector
            cipher: Xts128::new(c1, c2),
        }
    }

    fn decrypt_sector(&self, sector_index: u64, data: &mut [u8]) -> std::io::Result<()> {
        if data.len() != PAGE_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Data buffer is not the full sector size",
            ));
        }
        self.cipher
            .decrypt_sector(data, get_tweak_default(sector_index as u128));
        Ok(())
    }

    fn encrypt_sector(&self, sector_index: u64, data: &mut [u8]) -> std::io::Result<()> {
        if data.len() != PAGE_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Data buffer is not the full sector size",
            ));
        }
        self.cipher
            .encrypt_sector(data, get_tweak_default(sector_index as u128));
        Ok(())
    }

    fn read_region(&self, file: &mut File, offset: u64, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut total_read = 0;
        let mut current_offset = offset;
        while total_read < buf.len() {
            let sector_index = current_offset / PAGE_SIZE as u64;
            let sector_offset = (current_offset % PAGE_SIZE as u64) as usize;
            // read one full sector
            let mut sector_buf = vec![0u8; PAGE_SIZE];
            file.seek(SeekFrom::Start(sector_index * PAGE_SIZE as u64))?;
            file.read_exact(&mut sector_buf)?;

            self.decrypt_sector(sector_index, &mut sector_buf)?;
            // copy the needed portion
            let available = PAGE_SIZE - sector_offset;
            let to_copy = std::cmp::min(available, buf.len() - total_read);
            buf[total_read..total_read + to_copy]
                .copy_from_slice(&sector_buf[sector_offset..sector_offset + to_copy]);
            total_read += to_copy;
            current_offset += to_copy as u64;
        }
        Ok(total_read)
    }

    /// write a possibly partial region to the file
    /// performs a read-modify-write on sectors that are only partially overwritten.
    fn write_region(&self, file: &mut File, offset: u64, buf: &[u8]) -> std::io::Result<usize> {
        let mut total_written = 0;
        let mut current_offset = offset;
        while total_written < buf.len() {
            let sector_index = current_offset / PAGE_SIZE as u64;
            let sector_offset = (current_offset % PAGE_SIZE as u64) as usize;
            let available = PAGE_SIZE - sector_offset;
            let to_copy = std::cmp::min(available, buf.len() - total_written);
            // read the full current sector
            let mut sector_buf = vec![0u8; PAGE_SIZE];
            file.seek(SeekFrom::Start(sector_index * PAGE_SIZE as u64))?;
            file.read_exact(&mut sector_buf)?;

            self.decrypt_sector(sector_index, &mut sector_buf)?;
            // modify the plaintext with the new data
            sector_buf[sector_offset..sector_offset + to_copy]
                .copy_from_slice(&buf[total_written..total_written + to_copy]);

            self.encrypt_sector(sector_index, &mut sector_buf)?;
            // write the full encrypted sector back
            file.seek(SeekFrom::Start(sector_index * PAGE_SIZE as u64))?;
            file.write_all(&sector_buf)?;
            total_written += to_copy;
            current_offset += to_copy as u64;
        }
        Ok(total_written)
    }
}
