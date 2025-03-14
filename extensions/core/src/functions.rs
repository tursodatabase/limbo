use crate::{ResultCode, Value};
use std::{
    ffi::{c_char, c_void},
    fmt::Display,
};

pub type ScalarFunction = unsafe extern "C" fn(argc: i32, *const Value) -> Value;

pub type RegisterScalarFn =
    unsafe extern "C" fn(ctx: *mut c_void, name: *const c_char, func: ScalarFunction) -> ResultCode;

pub type RegisterAggFn = unsafe extern "C" fn(
    ctx: *mut c_void,
    name: *const c_char,
    args: i32,
    init: InitAggFunction,
    step: StepFunction,
    finalize: FinalizeFunction,
) -> ResultCode;

pub type InitAggFunction = unsafe extern "C" fn() -> *mut AggCtx;
pub type StepFunction = unsafe extern "C" fn(ctx: *mut AggCtx, argc: i32, argv: *const Value);
pub type FinalizeFunction = unsafe extern "C" fn(ctx: *mut AggCtx) -> Value;

#[repr(C)]
pub struct AggCtx {
    pub state: *mut c_void,
}

pub trait AggFunc {
    type State: Default;
    type Error: Display;
    const NAME: &'static str;
    const ARGS: i32;

    fn step(state: &mut Self::State, args: &[Value]);
    fn finalize(state: Self::State) -> Result<Value, Self::Error>;
}
