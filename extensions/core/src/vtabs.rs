use crate::{ResultCode, Value};
use std::ffi::{c_char, c_void};

pub type RegisterModuleFn = unsafe extern "C" fn(
    ctx: *mut c_void,
    name: *const c_char,
    module: VTabModuleImpl,
    kind: VTabKind,
) -> ResultCode;

#[repr(C)]
#[derive(Clone, Debug)]
pub struct VTabModuleImpl {
    pub ctx: *const c_void,
    pub name: *const c_char,
    pub create_schema: VtabFnCreateSchema,
    pub open: VtabFnOpen,
    pub filter: VtabFnFilter,
    pub column: VtabFnColumn,
    pub next: VtabFnNext,
    pub eof: VtabFnEof,
    pub update: VtabFnUpdate,
    pub rowid: VtabRowIDFn,
    pub destroy: VtabFnDestroy,
}

#[cfg(feature = "core_only")]
impl VTabModuleImpl {
    pub fn init_schema(&self, args: Vec<Value>) -> crate::ExtResult<String> {
        let schema = unsafe { (self.create_schema)(args.as_ptr(), args.len() as i32) };
        if schema.is_null() {
            return Err(ResultCode::InvalidArgs);
        }
        for arg in args {
            unsafe { arg.__free_internal_type() };
        }
        let schema = unsafe { std::ffi::CString::from_raw(schema) };
        Ok(schema.to_string_lossy().to_string())
    }
}

pub type VtabFnCreateSchema = unsafe extern "C" fn(args: *const Value, argc: i32) -> *mut c_char;

pub type VtabFnOpen = unsafe extern "C" fn(*const c_void) -> *const c_void;

pub type VtabFnFilter =
    unsafe extern "C" fn(cursor: *const c_void, argc: i32, argv: *const Value) -> ResultCode;

pub type VtabFnColumn = unsafe extern "C" fn(cursor: *const c_void, idx: u32) -> Value;

pub type VtabFnNext = unsafe extern "C" fn(cursor: *const c_void) -> ResultCode;

pub type VtabFnEof = unsafe extern "C" fn(cursor: *const c_void) -> bool;

pub type VtabRowIDFn = unsafe extern "C" fn(cursor: *const c_void) -> i64;

pub type VtabFnUpdate = unsafe extern "C" fn(
    vtab: *const c_void,
    argc: i32,
    argv: *const Value,
    p_out_rowid: *mut i64,
) -> ResultCode;

pub type VtabFnDestroy = unsafe extern "C" fn(vtab: *const c_void) -> ResultCode;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum VTabKind {
    VirtualTable,
    TableValuedFunction,
}

pub trait VTabModule: 'static {
    type VCursor: VTabCursor<Error = Self::Error>;
    const VTAB_KIND: VTabKind;
    const NAME: &'static str;
    type Error: std::fmt::Display;

    fn create_schema(args: &[Value]) -> String;
    fn open(&self) -> Result<Self::VCursor, Self::Error>;
    fn filter(cursor: &mut Self::VCursor, args: &[Value]) -> ResultCode;
    fn column(cursor: &Self::VCursor, idx: u32) -> Result<Value, Self::Error>;
    fn next(cursor: &mut Self::VCursor) -> ResultCode;
    fn eof(cursor: &Self::VCursor) -> bool;
    fn update(&mut self, _rowid: i64, _args: &[Value]) -> Result<(), Self::Error> {
        Ok(())
    }
    fn insert(&mut self, _args: &[Value]) -> Result<i64, Self::Error> {
        Ok(0)
    }
    fn delete(&mut self, _rowid: i64) -> Result<(), Self::Error> {
        Ok(())
    }
    fn destroy(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

pub trait VTabCursor: Sized {
    type Error: std::fmt::Display;
    fn rowid(&self) -> i64;
    fn column(&self, idx: u32) -> Result<Value, Self::Error>;
    fn eof(&self) -> bool;
    fn next(&mut self) -> ResultCode;
}
