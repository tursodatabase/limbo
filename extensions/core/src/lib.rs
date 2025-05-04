mod functions;
mod types;
#[cfg(feature = "vfs")]
mod vfs_modules;
mod vtabs;
pub use functions::{
    AggCtx, AggFunc, FinalizeFunction, InitAggFunction, ScalarFunction, StepFunction,
};
use functions::{RegisterAggFn, RegisterScalarFn};
#[cfg(feature = "vfs")]
pub use limbo_macros::VfsDerive;
pub use limbo_macros::{register_extension, scalar, AggregateDerive, VTabModuleDerive};
use std::os::raw::c_void;
pub use types::{ResultCode, StepResult, Value, ValueType};
#[cfg(feature = "vfs")]
pub use vfs_modules::{RegisterVfsFn, VfsExtension, VfsFile, VfsFileImpl, VfsImpl, VfsInterface};
pub use vtabs::{
    Conn, Connection, ConstraintInfo, ConstraintOp, ConstraintUsage, ExtIndexInfo, IndexInfo,
    OrderByInfo, Statement, Stmt, VTabCursor, VTabKind, VTabModule, VTabModuleImpl,
};
use vtabs::{ConnectFn, RegisterModuleFn};

pub type ExtResult<T> = std::result::Result<T, ResultCode>;

pub type ExtensionEntryPoint = unsafe extern "C" fn(api: *const ExtensionApi) -> ResultCode;

#[repr(C)]
pub struct ExtensionApi {
    pub ctx: *mut c_void,
    pub conn: *mut Conn,
    pub register_scalar_function: RegisterScalarFn,
    pub register_aggregate_function: RegisterAggFn,
    pub register_vtab_module: RegisterModuleFn,
    pub connect: ConnectFn,
    #[cfg(feature = "vfs")]
    pub vfs_interface: VfsInterface,
}

unsafe impl Send for ExtensionApi {}
unsafe impl Send for ExtensionApiRef {}

#[repr(C)]
pub struct ExtensionApiRef {
    pub api: *const ExtensionApi,
}
