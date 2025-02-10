use limbo_ext::{VfsFile, VfsImpl};
use std::{cell::RefCell, rc::Rc};

use crate::Result;

use super::{Buffer, Completion, File, OpenFlags, IO};

impl IO for *const VfsImpl {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Rc<dyn File>> {
        if self.is_null() {
            return Err(crate::LimboError::ExtensionError("VFS is null".to_string()));
        }
        let vfs = unsafe { &**self };
        let path = std::ffi::CString::new(path).unwrap();
        let ctx = { *self as *mut std::ffi::c_void };
        let file = unsafe {
            (vfs.open)(
                ctx,
                path.as_ptr() as *const std::ffi::c_char,
                flags.bits(),
                direct,
            )
        };
        if file.is_null() {
            return Err(crate::LimboError::ExtensionError(
                "File not found".to_string(),
            ));
        }
        Ok(Rc::new(VfsFile::new(file as *mut std::ffi::c_void, vfs)))
    }

    fn run_once(&self) -> Result<()> {
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

impl File for VfsFile {
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        if self.vfs.is_null() {
            return Err(crate::LimboError::ExtensionError("VFS is null".to_string()));
        }
        let vfs = unsafe { &*self.vfs };
        let result = unsafe { (vfs.lock)(self.file, exclusive) };
        if result.is_ok() {
            return Err(crate::LimboError::ExtensionError(result.to_string()));
        }
        Ok(())
    }

    fn unlock_file(&self) -> Result<()> {
        if self.vfs.is_null() {
            return Err(crate::LimboError::ExtensionError("VFS is null".to_string()));
        }
        let vfs = unsafe { &*self.vfs };
        let result = unsafe { (vfs.unlock)(self.file) };
        if result.is_ok() {
            return Err(crate::LimboError::ExtensionError(result.to_string()));
        }
        Ok(())
    }

    fn pread(&self, pos: usize, c: Rc<Completion>) -> Result<()> {
        if self.vfs.is_null() {
            return Err(crate::LimboError::ExtensionError("VFS is null".to_string()));
        }
        let r = match &*c {
            Completion::Read(ref r) => r,
            _ => unreachable!(),
        };
        let mut buf = r.buf_mut();
        let count = buf.len();
        let vfs = unsafe { &*self.vfs };
        let result = unsafe { (vfs.read)(self.file, buf.as_mut_ptr(), count, pos as i64) };

        if result < 0 {
            Err(crate::LimboError::ExtensionError(
                "pread failed".to_string(),
            ))
        } else {
            c.complete(0);
            Ok(())
        }
    }

    fn pwrite(&self, pos: usize, buffer: Rc<RefCell<Buffer>>, c: Rc<Completion>) -> Result<()> {
        let buf = buffer.borrow();
        let count = buf.as_slice().len();
        if self.vfs.is_null() {
            return Err(crate::LimboError::ExtensionError("VFS is null".to_string()));
        }
        let vfs = unsafe { &*self.vfs };
        let result = unsafe {
            (vfs.write)(
                self.file,
                buf.as_slice().as_ptr() as *mut u8,
                count,
                pos as i64,
            )
        };

        if result < 0 {
            Err(crate::LimboError::ExtensionError(
                "pwrite failed".to_string(),
            ))
        } else {
            c.complete(result);
            Ok(())
        }
    }

    fn sync(&self, c: Rc<Completion>) -> Result<()> {
        if self.vfs.is_null() {
            return Err(crate::LimboError::ExtensionError("VFS is null".to_string()));
        }
        let vfs = unsafe { &*self.vfs };
        let result = unsafe { (vfs.sync)(self.file) };
        if result < 0 {
            Err(crate::LimboError::ExtensionError("sync failed".to_string()))
        } else {
            c.complete(0);
            Ok(())
        }
    }

    fn size(&self) -> Result<u64> {
        if self.vfs.is_null() {
            return Err(crate::LimboError::ExtensionError("VFS is null".to_string()));
        }
        let vfs = unsafe { &*self.vfs };
        let result = unsafe { (vfs.size)(self.file) };
        if result < 0 {
            Err(crate::LimboError::ExtensionError("size failed".to_string()))
        } else {
            Ok(result as u64)
        }
    }
}
