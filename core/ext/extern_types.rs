use crate::{
    schema::Type,
    types::{OwnedValue, OwnedValueType},
};
use std::ffi::c_void;

pub struct TypeRegistry(Vec<(String, ExternType)>);
impl TypeRegistry {
    pub fn new() -> Self {
        Self(Vec::new())
    }

    pub fn register(&mut self, name: &str, ctx: *const c_void, _type: OwnedValueType) {
        self.0
            .push((name.to_string(), ExternType::new(name, ctx, _type)));
    }

    pub fn get(&self, name: &str) -> Option<&ExternType> {
        self.0.iter().find(|(n, _)| n == name).map(|(_, t)| t)
    }
}
pub struct ExternType {
    pub name: String,
    ctx: *const c_void,
    type_of: OwnedValueType,
}

impl ExternType {
    pub fn new(name: &str, ctx: *const c_void, type_of: OwnedValueType) -> Self {
        Self {
            name: name.to_string(),
            ctx,
            type_of,
        }
    }

    pub fn type_of(&self) -> Type {
        self.type_of.into()
    }

    pub fn generate(&self, rowid: i64) -> crate::Result<OwnedValue> {
        if self.ctx.is_null() {
            return Err(crate::LimboError::ExtensionError(
                "Context is null".to_string(),
            ));
        }
        let ctx = unsafe { &*(self.ctx as *const limbo_ext::ExtensionType) };
        let value = unsafe { (ctx.generate)(rowid) };
        let owned = OwnedValue::from_ffi(&value);
        unsafe { value.free() };
        owned
    }

    pub fn validate(&self, val: &OwnedValue) -> bool {
        if self.ctx.is_null() {
            return false;
        }
        let ctx = unsafe { &*(self.ctx as *const limbo_ext::ExtensionType) };
        let extval = val.to_ffi();
        let result = unsafe { (ctx.validate)(&extval) };
        unsafe { extval.free() };
        result
    }

    pub fn cast(&self, val: &OwnedValue) -> OwnedValue {
        if self.ctx.is_null() {
            return OwnedValue::Null;
        }
        let ctx = unsafe { &*(self.ctx as *const limbo_ext::ExtensionType) };
        let extval = val.to_ffi();
        let value = unsafe { (ctx.cast)(&extval) };
        let owned = OwnedValue::from_ffi(&value);
        unsafe { value.free() };
        owned.unwrap_or(OwnedValue::Null)
    }
}
