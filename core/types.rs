use std::fmt::Display;
use std::{cell::Ref, rc::Rc};

use anyhow::Result;

#[derive(Debug, Clone, PartialEq)]
pub enum Value<'a> {
    Null,
    Integer(i64),
    Float(f64),
    Text(&'a String),
    Blob(&'a Vec<u8>),
}

impl<'a> Display for Value<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Integer(i) => write!(f, "{}", i),
            Value::Float(fl) => write!(f, "{}", fl),
            Value::Text(s) => write!(f, "{}", s),
            Value::Blob(b) => write!(f, "{:?}", b),
        }
    }
}

impl Value {
    pub fn try_as_string(&self) -> Value {
        match self {
            Value::Text(s) => Value::Text(s),
            Value::Blob(b) => Value::Text(&String::from_utf8_lossy(b).to_string()),
            Value::Integer(i) => Value::Text(&i.to_string()),
            Value::Float(f) => Value::Text(&f.to_string()),
            Value::Null => Value::Null,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum OwnedValue {
    Null,
    Integer(i64),
    Float(f64),
    Text(Rc<String>),
    Blob(Rc<Vec<u8>>),
    Agg(Box<AggContext>), // TODO(pere): make this without Box. Currently this might cause cache miss but let's leave it for future analysis
    Record(OwnedRecord),
}

impl Display for OwnedValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OwnedValue::Null => write!(f, "NULL"),
            OwnedValue::Integer(i) => write!(f, "{}", i),
            OwnedValue::Float(fl) => write!(f, "{:?}", fl),
            OwnedValue::Text(s) => write!(f, "{}", s),
            OwnedValue::Blob(b) => write!(f, "{:?}", b),
            OwnedValue::Agg(a) => match a.as_ref() {
                AggContext::Avg(acc, _count) => write!(f, "{}", acc),
                AggContext::Sum(acc) => write!(f, "{}", acc),
                AggContext::Count(count) => write!(f, "{}", count),
                AggContext::Max(max) => write!(f, "{}", max),
                AggContext::Min(min) => write!(f, "{}", min),
                AggContext::GroupConcat(s) => write!(f, "{}", s),
            },
            OwnedValue::Record(r) => write!(f, "{:?}", r),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum AggContext {
    Avg(OwnedValue, OwnedValue), // acc and count
    Sum(OwnedValue),
    Count(OwnedValue),
    Max(OwnedValue),
    Min(OwnedValue),
    GroupConcat(OwnedValue),
}

impl std::cmp::PartialOrd<OwnedValue> for OwnedValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (OwnedValue::Integer(int_left), OwnedValue::Integer(int_right)) => {
                int_left.partial_cmp(int_right)
            }
            (OwnedValue::Integer(int_left), OwnedValue::Float(float_right)) => {
                float_right.partial_cmp(&(*int_left as f64))
            }
            (OwnedValue::Float(float_left), OwnedValue::Integer(int_right)) => {
                float_left.partial_cmp(&(*int_right as f64))
            }
            (OwnedValue::Float(float_left), OwnedValue::Float(float_right)) => {
                float_left.partial_cmp(float_right)
            }
            (OwnedValue::Text(text_left), OwnedValue::Text(text_right)) => {
                text_left.partial_cmp(text_right)
            }
            (OwnedValue::Blob(blob_left), OwnedValue::Blob(blob_right)) => {
                blob_left.partial_cmp(blob_right)
            }
            (OwnedValue::Null, OwnedValue::Null) => Some(std::cmp::Ordering::Equal),
            _ => None,
        }
    }
}

impl std::ops::Add<OwnedValue> for OwnedValue {
    type Output = OwnedValue;

    fn add(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (OwnedValue::Integer(int_left), OwnedValue::Integer(int_right)) => {
                OwnedValue::Integer(int_left + int_right)
            }
            (OwnedValue::Integer(int_left), OwnedValue::Float(float_right)) => {
                OwnedValue::Float(int_left as f64 + float_right)
            }
            (OwnedValue::Float(float_left), OwnedValue::Integer(int_right)) => {
                OwnedValue::Float(float_left + int_right as f64)
            }
            (OwnedValue::Float(float_left), OwnedValue::Float(float_right)) => {
                OwnedValue::Float(float_left + float_right)
            }
            (OwnedValue::Text(string_left), OwnedValue::Text(string_right)) => {
                OwnedValue::Text(Rc::new(string_left.to_string() + &string_right.to_string()))
            }
            (OwnedValue::Text(string_left), OwnedValue::Integer(int_right)) => {
                OwnedValue::Text(Rc::new(string_left.to_string() + &int_right.to_string()))
            }
            (OwnedValue::Integer(int_left), OwnedValue::Text(string_right)) => {
                OwnedValue::Text(Rc::new(int_left.to_string() + &string_right.to_string()))
            }
            (OwnedValue::Text(string_left), OwnedValue::Float(float_right)) => {
                let string_right = OwnedValue::Float(float_right).to_string();
                OwnedValue::Text(Rc::new(string_left.to_string() + &string_right))
            }
            (OwnedValue::Float(float_left), OwnedValue::Text(string_right)) => {
                let string_left = OwnedValue::Float(float_left).to_string();
                OwnedValue::Text(Rc::new(string_left + &string_right.to_string()))
            }
            (lhs, OwnedValue::Null) => lhs,
            (OwnedValue::Null, rhs) => rhs,
            _ => OwnedValue::Float(0.0),
        }
    }
}

impl std::ops::Add<f64> for OwnedValue {
    type Output = OwnedValue;

    fn add(self, rhs: f64) -> Self::Output {
        match self {
            OwnedValue::Integer(int_left) => OwnedValue::Float(int_left as f64 + rhs),
            OwnedValue::Float(float_left) => OwnedValue::Float(float_left + rhs),
            _ => unreachable!(),
        }
    }
}

impl std::ops::Add<i64> for OwnedValue {
    type Output = OwnedValue;

    fn add(self, rhs: i64) -> Self::Output {
        match self {
            OwnedValue::Integer(int_left) => OwnedValue::Integer(int_left + rhs),
            OwnedValue::Float(float_left) => OwnedValue::Float(float_left + rhs as f64),
            _ => unreachable!(),
        }
    }
}

impl std::ops::AddAssign for OwnedValue {
    fn add_assign(&mut self, rhs: Self) {
        *self = self.clone() + rhs;
    }
}

impl std::ops::AddAssign<i64> for OwnedValue {
    fn add_assign(&mut self, rhs: i64) {
        *self = self.clone() + rhs;
    }
}

impl std::ops::AddAssign<f64> for OwnedValue {
    fn add_assign(&mut self, rhs: f64) {
        *self = self.clone() + rhs;
    }
}

impl std::ops::Div<OwnedValue> for OwnedValue {
    type Output = OwnedValue;

    fn div(self, rhs: OwnedValue) -> Self::Output {
        match (self, rhs) {
            (OwnedValue::Integer(int_left), OwnedValue::Integer(int_right)) => {
                OwnedValue::Integer(int_left / int_right)
            }
            (OwnedValue::Integer(int_left), OwnedValue::Float(float_right)) => {
                OwnedValue::Float(int_left as f64 / float_right)
            }
            (OwnedValue::Float(float_left), OwnedValue::Integer(int_right)) => {
                OwnedValue::Float(float_left / int_right as f64)
            }
            (OwnedValue::Float(float_left), OwnedValue::Float(float_right)) => {
                OwnedValue::Float(float_left / float_right)
            }
            _ => unreachable!(),
        }
    }
}

impl std::ops::DivAssign<OwnedValue> for OwnedValue {
    fn div_assign(&mut self, rhs: OwnedValue) {
        *self = self.clone() / rhs;
    }
}

pub fn to_value(value: &OwnedValue) -> Value<'_> {
    match value {
        OwnedValue::Null => Value::Null,
        OwnedValue::Integer(i) => Value::Integer(*i),
        OwnedValue::Float(f) => Value::Float(*f),
        OwnedValue::Text(s) => Value::Text(s),
        OwnedValue::Blob(b) => Value::Blob(b),
        OwnedValue::Agg(a) => match a.as_ref() {
            AggContext::Avg(acc, _count) => to_value(acc), // we assume aggfinal was called
            AggContext::Sum(acc) => to_value(acc),
            AggContext::Count(count) => to_value(count),
            AggContext::Max(max) => to_value(max),
            AggContext::Min(min) => to_value(min),
            AggContext::GroupConcat(s) => to_value(s),
        },
        OwnedValue::Record(_) => todo!(),
    }
}

pub trait FromValue<'a> {
    fn from_value(value: &Value<'a>) -> Result<Self>
    where
        Self: Sized + 'a;
}

impl<'a> FromValue<'a> for i64 {
    fn from_value(value: &Value<'a>) -> Result<Self> {
        match value {
            Value::Integer(i) => Ok(*i),
            _ => anyhow::bail!("Expected integer value"),
        }
    }
}

impl<'a> FromValue<'a> for String {
    fn from_value(value: &Value<'a>) -> Result<Self> {
        match value {
            Value::Text(s) => Ok(s.to_string()),
            _ => anyhow::bail!("Expected text value"),
        }
    }
}

impl<'a> FromValue<'a> for &'a str {
    fn from_value(value: &Value<'a>) -> Result<&'a str> {
        match value {
            Value::Text(s) => Ok(s),
            _ => anyhow::bail!("Expected text value"),
        }
    }
}

#[derive(Debug)]
pub struct Record<'a> {
    pub values: Vec<Value<'a>>,
}

impl<'a> Record<'a> {
    pub fn new(values: Vec<Value<'a>>) -> Self {
        Self { values }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct OwnedRecord {
    pub values: Vec<OwnedValue>,
}

impl OwnedRecord {
    pub fn new(values: Vec<OwnedValue>) -> Self {
        Self { values }
    }
}

pub enum CursorResult<T> {
    Ok(T),
    IO,
}

pub trait Cursor {
    fn is_empty(&self) -> bool;
    fn rewind(&mut self) -> Result<CursorResult<()>>;
    fn next(&mut self) -> Result<CursorResult<()>>;
    fn wait_for_completion(&mut self) -> Result<()>;
    fn rowid(&self) -> Result<Ref<Option<u64>>>;
    fn record(&self) -> Result<Ref<Option<OwnedRecord>>>;
    fn insert(&mut self, record: &OwnedRecord) -> Result<()>;
}
