use crate::{LimboError, Result};
use limbo_ext::{VfsFileImpl, VfsImpl};
use std::ffi::{c_void, CString};
use std::{cell::RefCell, rc::Rc};

use super::{Buffer, Completion, File, OpenFlags, IO};

impl IO for *const VfsImpl {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Rc<dyn File>> {
        let c_path = CString::new(path).map_err(|_| {
            LimboError::ExtensionError("Failed to convert path to CString".to_string())
        })?;
        let ctx = (*self) as *mut c_void;
        let vfs = unsafe { &**self };
        let file = unsafe { (vfs.open)(ctx, c_path.as_ptr(), flags.bits(), direct) };
        if file.is_null() {
            return Err(LimboError::ExtensionError("File not found".to_string()));
        }
        Ok(Rc::new(limbo_ext::VfsFileImpl::new(file, *self)?))
    }

    fn run_once(&self) -> Result<()> {
        unsafe {
            if self.is_null() {
                return Err(LimboError::ExtensionError("VFS is null".to_string()));
            }
            let vfs = &**self;
            let result = (vfs.run_once)(vfs.vfs);
            if !result.is_ok() {
                return Err(LimboError::ExtensionError(result.to_string()));
            }
            Ok(())
        }
    }

    fn generate_random_number(&self) -> i64 {
        let vfs = unsafe { &**self };
        unsafe { (vfs.gen_random_number)() }
    }

    fn get_current_time(&self) -> String {
        unsafe {
            let vfs = &**self;
            let chars = (vfs.current_time)();
            let cstr = CString::from_raw(chars as *mut i8);
            cstr.to_string_lossy().into_owned()
        }
    }
}

impl File for VfsFileImpl {
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        let vfs = unsafe { &*self.vfs };
        let result = unsafe { (vfs.lock)(self.file, exclusive) };
        if result.is_ok() {
            return Err(LimboError::ExtensionError(result.to_string()));
        }
        Ok(())
    }

    fn unlock_file(&self) -> Result<()> {
        if self.vfs.is_null() {
            return Err(LimboError::ExtensionError("VFS is null".to_string()));
        }
        let vfs = unsafe { &*self.vfs };
        let result = unsafe { (vfs.unlock)(self.file) };
        if result.is_ok() {
            return Err(LimboError::ExtensionError(result.to_string()));
        }
        Ok(())
    }

    fn pread(&self, pos: usize, c: Completion) -> Result<()> {
        let r = match &c {
            Completion::Read(ref r) => r,
            _ => unreachable!(),
        };
        let result = {
            let mut buf = r.buf_mut();
            let count = buf.len();
            let vfs = unsafe { &*self.vfs };
            unsafe { (vfs.read)(self.file, buf.as_mut_ptr(), count, pos as i64) }
        };
        if result < 0 {
            Err(LimboError::ExtensionError("pread failed".to_string()))
        } else {
            c.complete(0);
            Ok(())
        }
    }

    fn pwrite(&self, pos: usize, buffer: Rc<RefCell<Buffer>>, c: Completion) -> Result<()> {
        let buf = buffer.borrow();
        let count = buf.as_slice().len();
        if self.vfs.is_null() {
            return Err(LimboError::ExtensionError("VFS is null".to_string()));
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
            Err(LimboError::ExtensionError("pwrite failed".to_string()))
        } else {
            c.complete(result);
            Ok(())
        }
    }

    fn sync(&self, c: Completion) -> Result<()> {
        let vfs = unsafe { &*self.vfs };
        let result = unsafe { (vfs.sync)(self.file) };
        if result < 0 {
            Err(LimboError::ExtensionError("sync failed".to_string()))
        } else {
            c.complete(0);
            Ok(())
        }
    }

    fn size(&self) -> Result<u64> {
        let vfs = unsafe { &*self.vfs };
        let result = unsafe { (vfs.size)(self.file) };
        if result < 0 {
            Err(LimboError::ExtensionError("size failed".to_string()))
        } else {
            Ok(result as u64)
        }
    }
}
