use std::ffi::c_void;
#[allow(dead_code)]
#[repr(C)]
pub enum ResultCode {
    Error = -1,
    Ok = 0,
    Row = 1,
    Busy = 2,
    Io = 3,
    Interrupt = 4,
    Invalid = 5,
    Null = 6,
    NoMem = 7,
    ReadOnly = 8,
    NoData = 9,
    Done = 10,
}

#[repr(C)]
pub enum ValueType {
    Integer = 0,
    Text = 1,
    Blob = 2,
    Real = 3,
    Null = 4,
}

#[repr(C)]
pub struct TursoValue {
    pub value_type: ValueType,
    pub value: ValueUnion,
}

#[repr(C)]
pub union ValueUnion {
    pub int_val: i64,
    pub real_val: f64,
    pub text_ptr: *const u8,
    pub blob_ptr: *const c_void,
}

#[repr(C)]
pub struct Blob {
    pub data: *const u8,
    pub len: usize,
}

impl Blob {
    pub fn to_ptr(&self) -> *const c_void {
        self as *const Blob as *const c_void
    }
}

#[no_mangle]
pub extern "C" fn free_blob(blob_ptr: *mut c_void) {
    if blob_ptr.is_null() {
        return;
    }
    unsafe {
        let _ = Box::from_raw(blob_ptr as *mut Blob);
    }
}

impl ValueUnion {
    fn from_str(s: &str) -> Self {
        ValueUnion {
            text_ptr: s.as_ptr(),
        }
    }

    fn from_bytes(b: &[u8]) -> Self {
        ValueUnion {
            blob_ptr: Blob {
                data: b.as_ptr(),
                len: b.len(),
            }
            .to_ptr(),
        }
    }
    fn from_int(i: i64) -> Self {
        ValueUnion { int_val: i }
    }
    fn from_real(r: f64) -> Self {
        ValueUnion { real_val: r }
    }
    fn from_null() -> Self {
        ValueUnion { int_val: 0 }
    }
}

impl TursoValue {
    pub fn new(value_type: ValueType, value: ValueUnion) -> Self {
        TursoValue { value_type, value }
    }

    #[allow(clippy::wrong_self_convention)]
    pub fn to_ptr(self) -> *const c_void {
        Box::into_raw(Box::new(self)) as *const c_void
    }

    pub fn from_value(value: &limbo_core::Value<'_>) -> Self {
        match value {
            limbo_core::Value::Integer(i) => {
                TursoValue::new(ValueType::Integer, ValueUnion::from_int(*i))
            }
            limbo_core::Value::Float(r) => {
                TursoValue::new(ValueType::Real, ValueUnion::from_real(*r))
            }
            limbo_core::Value::Text(s) => TursoValue::new(ValueType::Text, ValueUnion::from_str(s)),
            limbo_core::Value::Blob(b) => {
                TursoValue::new(ValueType::Blob, ValueUnion::from_bytes(b))
            }
            limbo_core::Value::Null => TursoValue::new(ValueType::Null, ValueUnion::from_null()),
        }
    }
}
