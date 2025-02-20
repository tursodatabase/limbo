#[cfg(all(feature = "io_uring", target_os = "linux"))]
use crate::UringIO;
use crate::{function::ExternalFunc, io::IO, Database, LimboError, MemoryIO, PlatformIO};
use limbo_ext::{
    ExtensionApi, InitAggFunction, ResultCode, ScalarFunction, VTabKind, VTabModuleImpl, VfsImpl,
};
pub use limbo_ext::{FinalizeFunction, StepFunction, Value as ExtValue, ValueType as ExtValueType};
use std::{
    ffi::{c_char, c_void, CStr, CString},
    rc::Rc,
    sync::Arc,
};

type ExternAggFunc = (InitAggFunction, StepFunction, FinalizeFunction);

#[derive(Clone)]
pub struct VTabImpl {
    pub module_kind: VTabKind,
    pub implementation: Rc<VTabModuleImpl>,
}

unsafe extern "C" fn register_scalar_function(
    ctx: *mut c_void,
    name: *const c_char,
    func: ScalarFunction,
) -> ResultCode {
    let c_str = unsafe { CStr::from_ptr(name) };
    let name_str = match c_str.to_str() {
        Ok(s) => s.to_string(),
        Err(_) => return ResultCode::InvalidArgs,
    };
    if ctx.is_null() {
        return ResultCode::Error;
    }
    let db = unsafe { &*(ctx as *const Database) };
    db.register_scalar_function_impl(&name_str, func)
}

unsafe extern "C" fn register_aggregate_function(
    ctx: *mut c_void,
    name: *const c_char,
    args: i32,
    init_func: InitAggFunction,
    step_func: StepFunction,
    finalize_func: FinalizeFunction,
) -> ResultCode {
    let c_str = unsafe { CStr::from_ptr(name) };
    let name_str = match c_str.to_str() {
        Ok(s) => s.to_string(),
        Err(_) => return ResultCode::InvalidArgs,
    };
    if ctx.is_null() {
        return ResultCode::Error;
    }
    let db = unsafe { &*(ctx as *const Database) };
    db.register_aggregate_function_impl(&name_str, args, (init_func, step_func, finalize_func))
}

unsafe extern "C" fn register_module(
    ctx: *mut c_void,
    name: *const c_char,
    module: VTabModuleImpl,
    kind: VTabKind,
) -> ResultCode {
    if name.is_null() || ctx.is_null() {
        return ResultCode::Error;
    }
    let c_str = unsafe { CString::from_raw(name as *mut i8) };
    let name_str = match c_str.to_str() {
        Ok(s) => s.to_string(),
        Err(_) => return ResultCode::Error,
    };
    if ctx.is_null() {
        return ResultCode::Error;
    }
    let db = unsafe { &mut *(ctx as *mut Database) };

    db.register_module_impl(&name_str, module, kind)
}

unsafe extern "C" fn register_vfs(
    ctx: *mut c_void,
    name: *const c_char,
    vfs: *const VfsImpl,
) -> ResultCode {
    if ctx.is_null() || name.is_null() || vfs.is_null() {
        return ResultCode::Error;
    }
    let c_str = unsafe { CString::from_raw(name as *mut i8) };
    let name_str = match c_str.to_str() {
        Ok(s) => s.to_string(),
        Err(_) => return ResultCode::Error,
    };
    let db = unsafe { &mut *(ctx as *mut Database) };
    db.register_vfs_impl(name_str, vfs)
}

/// Get pointers to all the vfs extensions that need to be built in at compile time.
/// any other types that are defined in the same extension will not be registered
/// until the database is opened and `register_builtins` is called.
pub fn register_builtin_vfs_extensions(
    api: Option<ExtensionApi>,
) -> crate::Result<Vec<(String, *const VfsImpl)>> {
    let mut vfslist: Vec<*const VfsImpl> = Vec::new();
    let mut api = match api {
        None => ExtensionApi {
            ctx: std::ptr::null_mut(),
            register_scalar_function,
            register_aggregate_function,
            register_vfs,
            register_module,
            builtin_vfs: vfslist.as_mut_ptr(),
            builtin_vfs_count: 0,
        },
        Some(mut api) => {
            api.builtin_vfs = vfslist.as_mut_ptr();
            api
        }
    };
    add_static_vfs_modules(&mut api);
    let mut vfslist = Vec::with_capacity(api.builtin_vfs_count as usize);
    let slice =
        unsafe { std::slice::from_raw_parts_mut(api.builtin_vfs, api.builtin_vfs_count as usize) };
    for vfs in slice {
        if vfs.is_null() {
            continue;
        }
        let vfsimpl = unsafe { &**vfs };
        let name = unsafe {
            CString::from_raw(vfsimpl.name as *mut i8)
                .to_str()
                .map_err(|_| {
                    LimboError::ExtensionError("unable to register vfs extension".to_string())
                })?
                .to_string()
        };
        vfslist.push((name, *vfs));
    }
    Ok(vfslist)
}

fn add_static_vfs_modules(_api: &mut ExtensionApi) {
    #[cfg(feature = "testvfs")]
    unsafe {
        limbo_testvfs::register_extension_static(_api);
    }
}

impl Database {
    /// Open a new database, using a VFS registered in the current symbol table
    #[cfg(feature = "fs")]
    #[allow(clippy::arc_with_non_send_sync)]
    pub fn open_with_vfs(
        &self,
        path: &str,
        vfs: &str,
    ) -> crate::Result<(Arc<dyn IO>, Arc<Database>)> {
        let io: Arc<dyn IO> = match vfs {
            "memory" => Arc::new(MemoryIO::new()?),
            "syscall" => Arc::new(PlatformIO::new()?),
            "io_uring" => {
                if cfg!(all(target_os = "linux", feature = "io_uring")) {
                    Arc::new(UringIO::new()?)
                } else {
                    return Err(LimboError::ExtensionError(
                        "io_uring not enabled".to_string(),
                    ));
                }
            }
            other => {
                let syms = self.syms.borrow();
                let vfs = syms.vfs_modules.iter().find(|v| v.0 == vfs);
                match vfs {
                    Some((_, vfs)) => Arc::new(*vfs),
                    None => {
                        return Err(LimboError::InvalidArgument(format!(
                            "no such VFS: {}",
                            other
                        )));
                    }
                }
            }
        };
        let db = Self::open_file(io.clone(), path)?;
        Ok((
            io,
            Database {
                syms: self.syms.clone(),
                pager: db.pager.clone(),
                schema: db.schema.clone(),
                header: db.header.clone(),
                _shared_wal: db._shared_wal.clone(),
                _shared_page_cache: db._shared_page_cache.clone(),
            }
            .into(),
        ))
    }

    fn register_scalar_function_impl(&self, name: &str, func: ScalarFunction) -> ResultCode {
        self.syms.borrow_mut().functions.insert(
            name.to_string(),
            Rc::new(ExternalFunc::new_scalar(name.to_string(), func)),
        );
        ResultCode::OK
    }

    fn register_aggregate_function_impl(
        &self,
        name: &str,
        args: i32,
        func: ExternAggFunc,
    ) -> ResultCode {
        self.syms.borrow_mut().functions.insert(
            name.to_string(),
            Rc::new(ExternalFunc::new_aggregate(name.to_string(), args, func)),
        );
        ResultCode::OK
    }

    fn register_module_impl(
        &mut self,
        name: &str,
        module: VTabModuleImpl,
        kind: VTabKind,
    ) -> ResultCode {
        let module = Rc::new(module);
        let vmodule = VTabImpl {
            module_kind: kind,
            implementation: module,
        };
        self.syms
            .borrow_mut()
            .vtab_modules
            .insert(name.to_string(), vmodule.into());
        ResultCode::OK
    }

    pub fn build_limbo_ext(&self) -> ExtensionApi {
        ExtensionApi {
            ctx: self as *const _ as *mut c_void,
            register_scalar_function,
            register_aggregate_function,
            register_module,
            register_vfs,
            builtin_vfs: std::ptr::null_mut(),
            builtin_vfs_count: 0,
        }
    }

    pub fn register_vfs_impl(&self, name: String, vfs: *const VfsImpl) -> ResultCode {
        if vfs.is_null() {
            return ResultCode::Error;
        }
        if self
            .syms
            .borrow()
            .vfs_modules
            .iter()
            .any(|(n, _)| n.eq(&name))
        {
            return ResultCode::Error;
        }
        self.syms.borrow_mut().vfs_modules.push((name, vfs));
        ResultCode::OK
    }

    pub fn register_builtins(&self) -> Result<(), String> {
        #[allow(unused_variables)]
        let mut ext_api = self.build_limbo_ext();
        #[cfg(feature = "uuid")]
        if unsafe { !limbo_uuid::register_extension_static(&mut ext_api).is_ok() } {
            return Err("Failed to register uuid extension".to_string());
        }
        #[cfg(feature = "percentile")]
        if unsafe { !limbo_percentile::register_extension_static(&mut ext_api).is_ok() } {
            return Err("Failed to register percentile extension".to_string());
        }
        #[cfg(feature = "regexp")]
        if unsafe { !limbo_regexp::register_extension_static(&mut ext_api).is_ok() } {
            return Err("Failed to register regexp extension".to_string());
        }
        #[cfg(feature = "time")]
        if unsafe { !limbo_time::register_extension_static(&mut ext_api).is_ok() } {
            return Err("Failed to register time extension".to_string());
        }
        #[cfg(feature = "crypto")]
        if unsafe { !limbo_crypto::register_extension_static(&mut ext_api).is_ok() } {
            return Err("Failed to register crypto extension".to_string());
        }
        #[cfg(feature = "series")]
        if unsafe { !limbo_series::register_extension_static(&mut ext_api).is_ok() } {
            return Err("Failed to register series extension".to_string());
        }
        let vfslist = register_builtin_vfs_extensions(Some(ext_api)).map_err(|e| e.to_string())?;
        for (name, vfs) in vfslist {
            self.register_vfs_impl(name, vfs);
        }
        Ok(())
    }
}
