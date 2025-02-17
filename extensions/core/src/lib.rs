mod types;
pub use limbo_macros::{register_extension, scalar, AggregateDerive, VTabModuleDerive, VfsDerive};
use std::ffi::{c_char, c_void};
pub use types::{ResultCode, Value, ValueType};

pub type Result<T> = std::result::Result<T, ResultCode>;

#[repr(C)]
pub struct ExtensionApi {
    pub ctx: *mut c_void,

    pub register_scalar_function: unsafe extern "C" fn(
        ctx: *mut c_void,
        name: *const c_char,
        func: ScalarFunction,
    ) -> ResultCode,

    pub register_aggregate_function: unsafe extern "C" fn(
        ctx: *mut c_void,
        name: *const c_char,
        args: i32,
        init_func: InitAggFunction,
        step_func: StepFunction,
        finalize_func: FinalizeFunction,
    ) -> ResultCode,

    pub register_module: unsafe extern "C" fn(
        ctx: *mut c_void,
        name: *const c_char,
        module: VTabModuleImpl,
    ) -> ResultCode,

    pub declare_vtab: unsafe extern "C" fn(
        ctx: *mut c_void,
        name: *const c_char,
        sql: *const c_char,
    ) -> ResultCode,

    pub register_vfs: unsafe extern "C" fn(
        ctx: *mut c_void,
        name: *const c_char,
        vfs: *const VfsImpl,
    ) -> ResultCode,
}

pub trait VfsExtension: Default {
    const NAME: &'static str;
    type File: VfsFile;
    fn open_file(&self, path: &str, flags: i32, direct: bool) -> Result<Self::File>;
    fn close(&self, file: Self::File) -> Result<()>;
    fn run_once(&self) -> Result<()>;
    fn generate_random_number(&self) -> i64;
    fn get_current_time(&self) -> String;
}

pub trait VfsFile: Sized {
    fn lock(&mut self, exclusive: bool) -> Result<()>;
    fn unlock(&self) -> Result<()>;
    fn read(&mut self, buf: &mut [u8], count: usize, offset: i64) -> Result<i32>;
    fn write(&mut self, buf: &[u8], count: usize, offset: i64) -> Result<i32>;
    fn sync(&self) -> Result<()>;
    fn size(&self) -> i64;
}

#[repr(C)]
pub struct VfsImpl {
    pub vfs: *const c_void,
    pub open: VfsOpen,
    pub close: VfsClose,
    pub read: VfsRead,
    pub write: VfsWrite,
    pub sync: VfsSync,
    pub lock: VfsLock,
    pub unlock: VfsUnlock,
    pub size: VfsSize,
    pub run_once: VfsRunOnce,
    pub current_time: VfsGetCurrentTime,
    pub gen_random_number: VfsGenerateRandomNumber,
}

pub type VfsOpen = unsafe extern "C" fn(
    ctx: *const c_void,
    path: *const c_char,
    flags: i32,
    direct: bool,
) -> *const c_void;

pub type VfsClose = unsafe extern "C" fn(file: *const c_void) -> ResultCode;

pub type VfsRead =
    unsafe extern "C" fn(file: *const c_void, buf: *mut u8, count: usize, offset: i64) -> i32;

pub type VfsWrite =
    unsafe extern "C" fn(file: *const c_void, buf: *const u8, count: usize, offset: i64) -> i32;

pub type VfsSync = unsafe extern "C" fn(file: *const c_void) -> i32;

pub type VfsLock = unsafe extern "C" fn(file: *const c_void, exclusive: bool) -> ResultCode;

pub type VfsUnlock = unsafe extern "C" fn(file: *const c_void) -> ResultCode;

pub type VfsSize = unsafe extern "C" fn(file: *const c_void) -> i64;

pub type VfsRunOnce = unsafe extern "C" fn(file: *const c_void) -> ResultCode;

pub type VfsGetCurrentTime = unsafe extern "C" fn() -> *const c_char;

pub type VfsGenerateRandomNumber = unsafe extern "C" fn() -> i64;

#[repr(C)]
pub struct VfsFileImpl {
    pub file: *const c_void,
    pub vfs: *const VfsImpl,
}

impl VfsFileImpl {
    pub fn new(file: *const c_void, vfs: *const VfsImpl) -> Result<Self> {
        if file.is_null() || vfs.is_null() {
            return Err(ResultCode::Error);
        }
        Ok(Self { file, vfs })
    }
}

impl ExtensionApi {
    pub fn declare_virtual_table(&self, name: &str, sql: &str) -> ResultCode {
        let Ok(name) = std::ffi::CString::new(name) else {
            return ResultCode::Error;
        };
        let Ok(sql) = std::ffi::CString::new(sql) else {
            return ResultCode::Error;
        };
        unsafe { (self.declare_vtab)(self.ctx, name.as_ptr(), sql.as_ptr()) }
    }
}

pub type ExtensionEntryPoint = unsafe extern "C" fn(api: *const ExtensionApi) -> ResultCode;
pub type ScalarFunction = unsafe extern "C" fn(argc: i32, *const Value) -> Value;

pub type InitAggFunction = unsafe extern "C" fn() -> *mut AggCtx;
pub type StepFunction = unsafe extern "C" fn(ctx: *mut AggCtx, argc: i32, argv: *const Value);
pub type FinalizeFunction = unsafe extern "C" fn(ctx: *mut AggCtx) -> Value;

#[repr(C)]
pub struct AggCtx {
    pub state: *mut c_void,
}

pub trait AggFunc {
    type State: Default;
    const NAME: &'static str;
    const ARGS: i32;

    fn step(state: &mut Self::State, args: &[Value]);
    fn finalize(state: Self::State) -> Value;
}

#[repr(C)]
#[derive(Clone, Debug)]
pub struct VTabModuleImpl {
    pub name: *const c_char,
    pub connect: VtabFnConnect,
    pub open: VtabFnOpen,
    pub filter: VtabFnFilter,
    pub column: VtabFnColumn,
    pub next: VtabFnNext,
    pub eof: VtabFnEof,
}

pub type VtabFnConnect = unsafe extern "C" fn(api: *const c_void) -> ResultCode;

pub type VtabFnOpen = unsafe extern "C" fn() -> *mut c_void;

pub type VtabFnFilter =
    unsafe extern "C" fn(cursor: *mut c_void, argc: i32, argv: *const Value) -> ResultCode;

pub type VtabFnColumn = unsafe extern "C" fn(cursor: *mut c_void, idx: u32) -> Value;

pub type VtabFnNext = unsafe extern "C" fn(cursor: *mut c_void) -> ResultCode;

pub type VtabFnEof = unsafe extern "C" fn(cursor: *mut c_void) -> bool;

pub trait VTabModule: 'static {
    type VCursor: VTabCursor;
    const NAME: &'static str;

    fn connect(api: &ExtensionApi) -> ResultCode;
    fn open() -> Self::VCursor;
    fn filter(cursor: &mut Self::VCursor, arg_count: i32, args: &[Value]) -> ResultCode;
    fn column(cursor: &Self::VCursor, idx: u32) -> Value;
    fn next(cursor: &mut Self::VCursor) -> ResultCode;
    fn eof(cursor: &Self::VCursor) -> bool;
}

pub trait VTabCursor: Sized {
    type Error: std::fmt::Display;
    fn rowid(&self) -> i64;
    fn column(&self, idx: u32) -> Value;
    fn eof(&self) -> bool;
    fn next(&mut self) -> ResultCode;
}

#[repr(C)]
pub struct VTabImpl {
    pub module: VTabModuleImpl,
}
