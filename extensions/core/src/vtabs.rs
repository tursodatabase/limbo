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
    pub best_idx: BestIdxFn,
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

pub type VtabFnFilter = unsafe extern "C" fn(
    cursor: *const c_void,
    argc: i32,
    argv: *const Value,
    idx_str: *const c_char,
    idx_num: i32,
) -> ResultCode;

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

pub type BestIdxFn = unsafe extern "C" fn(
    constraints: *const ConstraintInfo,
    constraint_len: i32,
    order_by: *const OrderByInfo,
    order_by_len: i32,
) -> ExtIndexInfo;

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
    fn filter(
        cursor: &mut Self::VCursor,
        args: &[Value],
        idx_info: Option<(&str, i32)>,
    ) -> ResultCode;
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
    fn best_index(_constraints: &[ConstraintInfo], _order_by: &[OrderByInfo]) -> IndexInfo {
        IndexInfo {
            idx_num: 0,
            idx_str: None,
            order_by_consumed: false,
            estimated_cost: 1_000_000.0,
            estimated_rows: u32::MAX,
            constraint_usages: _constraints
                .iter()
                .map(|_| ConstraintUsage {
                    argv_index: Some(0),
                    omit: false,
                })
                .collect(),
        }
    }
}

pub trait VTabCursor: Sized {
    type Error: std::fmt::Display;
    fn rowid(&self) -> i64;
    fn column(&self, idx: u32) -> Result<Value, Self::Error>;
    fn eof(&self) -> bool;
    fn next(&mut self) -> ResultCode;
}

#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ConstraintOp {
    Eq = 2,
    Lt = 4,
    Le = 8,
    Gt = 16,
    Ge = 32,
    Match = 64,
    Like = 65,
    Glob = 66,
    Regexp = 67,
    Ne = 68,
    IsNot = 69,
    IsNotNull = 70,
    IsNull = 71,
    Is = 72,
    In = 73,
}

#[repr(C)]
#[derive(Copy, Clone)]
/// Describes an ORDER BY clause in a query involving a virtual table.
/// Passed along with the constraints to xBestIndex.
pub struct OrderByInfo {
    /// The index of the column referenced in the ORDER BY clause.
    pub column_index: u32,
    /// Whether or not the clause is in descending order.
    pub desc: bool,
}

/// The internal (core) representation of an 'index' on a virtual table.
/// Returned from xBestIndex and then processed and passed to VFilter.
#[derive(Debug, Clone)]
pub struct IndexInfo {
    /// The index number, used to identify the index internally by the VTab
    pub idx_num: i32,
    /// Optional index name. these are passed to vfilter in a tuple (idx_num, idx_str)
    pub idx_str: Option<String>,
    /// Whether the index is used for order by
    pub order_by_consumed: bool,
    /// TODO: for eventual cost based query planning
    pub estimated_cost: f64,
    /// Estimated number of rows that the query will return
    pub estimated_rows: u32,
    /// List of constraints that can be used to optimize the query.
    pub constraint_usages: Vec<ConstraintUsage>,
}
impl Default for IndexInfo {
    fn default() -> Self {
        Self {
            idx_num: 0,
            idx_str: None,
            order_by_consumed: false,
            estimated_cost: 1_000_000.0,
            estimated_rows: u32::MAX,
            constraint_usages: Vec::new(),
        }
    }
}

impl IndexInfo {
    ///
    /// Converts IndexInfo to an FFI-safe `ExtIndexInfo`.
    /// This method transfers ownership of `constraint_usages` and `idx_str`,
    /// which must later be reclaimed using `from_ffi` to prevent leaks.
    pub fn to_ffi(self) -> ExtIndexInfo {
        let len = self.constraint_usages.len();
        let ptr = Box::into_raw(self.constraint_usages.into_boxed_slice()) as *mut ConstraintUsage;
        let idx_str_len = self.idx_str.as_ref().map(|s| s.len()).unwrap_or(0);
        let c_idx_str = self
            .idx_str
            .map(|s| std::ffi::CString::new(s).unwrap().into_raw())
            .unwrap_or(std::ptr::null_mut());
        ExtIndexInfo {
            idx_num: self.idx_num,
            estimated_cost: self.estimated_cost,
            estimated_rows: self.estimated_rows,
            order_by_consumed: self.order_by_consumed,
            constraint_usages_ptr: ptr,
            constraint_usage_len: len,
            idx_str: c_idx_str as *mut _,
            idx_str_len,
        }
    }

    /// Reclaims ownership of `constraint_usages` and `idx_str` from an FFI-safe `ExtIndexInfo`.
    /// # Safety
    /// This method is unsafe because it can cause memory leaks if not used correctly.
    /// to_ffi and from_ffi are meant to send index info across ffi bounds then immediately reclaim it.
    pub unsafe fn from_ffi(ffi: ExtIndexInfo) -> Self {
        let constraint_usages = unsafe {
            Box::from_raw(std::slice::from_raw_parts_mut(
                ffi.constraint_usages_ptr,
                ffi.constraint_usage_len,
            ))
            .to_vec()
        };
        let idx_str = if ffi.idx_str.is_null() {
            None
        } else {
            Some(unsafe {
                std::ffi::CString::from_raw(ffi.idx_str as *mut _)
                    .to_string_lossy()
                    .into_owned()
            })
        };
        Self {
            idx_num: ffi.idx_num,
            idx_str,
            order_by_consumed: ffi.order_by_consumed,
            estimated_cost: ffi.estimated_cost,
            estimated_rows: ffi.estimated_rows,
            constraint_usages,
        }
    }
}

#[repr(C)]
#[derive(Clone, Debug)]
/// FFI representation of IndexInfo.
pub struct ExtIndexInfo {
    pub idx_num: i32,
    pub idx_str: *const u8,
    pub idx_str_len: usize,
    pub order_by_consumed: bool,
    pub estimated_cost: f64,
    pub estimated_rows: u32,
    pub constraint_usages_ptr: *mut ConstraintUsage,
    pub constraint_usage_len: usize,
}

/// Returned from xBestIndex to describe how the virtual table
/// can use the constraints in the WHERE clause of a query.
#[derive(Debug, Clone, Copy)]
pub struct ConstraintUsage {
    /// 1 based index of the argument in the WHERE clause.
    pub argv_index: Option<u32>,
    /// If true, core can omit this constraint in the vdbe layer.
    pub omit: bool,
}

#[derive(Clone, Copy, Debug)]
#[repr(C)]
/// The primary argument to xBestIndex, which describes a constraint
/// in a query involving a virtual table.
pub struct ConstraintInfo {
    /// The index of the column referenced in the WHERE clause.
    pub column_index: u32,
    /// The operator used in the clause.
    pub op: ConstraintOp,
    /// Whether or not constraint is garaunteed to be enforced.
    pub usable: bool,
    ///
    pub pred_idx: usize,
}
