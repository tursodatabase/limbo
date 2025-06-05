use std::{fmt::Display, ops::Deref};

use limbo_core::{numeric::Numeric, types};
use limbo_sqlite3_parser::ast;
use regex::{Regex, RegexBuilder};
use serde::{Deserialize, Serialize};

pub(crate) struct Name(pub(crate) String);

impl Deref for Name {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Table {
    pub(crate) rows: Vec<Vec<SimValue>>,
    pub(crate) name: String,
    pub(crate) columns: Vec<Column>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Column {
    pub(crate) name: String,
    pub(crate) column_type: ColumnType,
    pub(crate) primary: bool,
    pub(crate) unique: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum ColumnType {
    Integer,
    Float,
    Text,
    Blob,
}

impl Display for ColumnType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Integer => write!(f, "INTEGER"),
            Self::Float => write!(f, "REAL"),
            Self::Text => write!(f, "TEXT"),
            Self::Blob => write!(f, "BLOB"),
        }
    }
}

fn float_to_string<S>(float: &f64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(&format!("{}", float))
}

fn string_to_float<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    s.parse().map_err(serde::de::Error::custom)
}

#[derive(Clone, Debug, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct SimValue(pub limbo_core::Value);

fn to_sqlite_blob(bytes: &[u8]) -> String {
    format!(
        "X'{}'",
        bytes
            .iter()
            .fold(String::new(), |acc, b| acc + &format!("{:02X}", b))
    )
}

impl Display for SimValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            types::Value::Null => write!(f, "NULL"),
            types::Value::Integer(i) => write!(f, "{}", i),
            types::Value::Float(fl) => write!(f, "{}", fl),
            value @ types::Value::Text(..) => write!(f, "'{}'", value.to_string()),
            types::Value::Blob(b) => write!(f, "{}", to_sqlite_blob(b)),
        }
    }
}

impl SimValue {
    pub const FALSE: Self = SimValue(types::Value::Integer(0));
    pub const TRUE: Self = SimValue(types::Value::Integer(1));

    pub fn into_bool(&self) -> bool {
        Numeric::from(&self.0).try_into_bool().unwrap_or_default()
    }

    // TODO: support more predicates
    /// Returns a Value::TRUE or VALUE::FALSE
    pub fn binary_compare(&self, other: &Self, operator: ast::Operator) -> SimValue {
        match operator {
            ast::Operator::Add => todo!(),
            ast::Operator::And => self.into_bool() && other.into_bool(),
            ast::Operator::ArrowRight => todo!(),
            ast::Operator::ArrowRightShift => todo!(),
            ast::Operator::BitwiseAnd => todo!(),
            ast::Operator::BitwiseOr => todo!(),
            ast::Operator::BitwiseNot => todo!(),
            ast::Operator::Concat => todo!(),
            ast::Operator::Equals => self == other,
            ast::Operator::Divide => todo!(),
            ast::Operator::Greater => self > other,
            ast::Operator::GreaterEquals => self >= other,
            ast::Operator::Is => todo!(),
            ast::Operator::IsNot => todo!(),
            ast::Operator::LeftShift => todo!(),
            ast::Operator::Less => self < other,
            ast::Operator::LessEquals => self <= other,
            ast::Operator::Modulus => todo!(),
            ast::Operator::Multiply => todo!(),
            ast::Operator::NotEquals => self != other,
            ast::Operator::Or => self.into_bool() || other.into_bool(),
            ast::Operator::RightShift => todo!(),
            ast::Operator::Subtract => todo!(),
        }
        .into()
    }

    // TODO: support more operators. Copy the implementation for exec_glob
    pub fn like_compare(&self, other: &Self, operator: ast::LikeOperator) -> bool {
        match operator {
            ast::LikeOperator::Glob => todo!(),
            ast::LikeOperator::Like => {
                // TODO: support ESCAPE `expr` option in AST
                exec_like(self.to_string().as_str(), other.to_string().as_str())
            }
            ast::LikeOperator::Match => todo!(),
            ast::LikeOperator::Regexp => todo!(),
        }
    }

    pub fn unary_exec(&self, operator: ast::UnaryOperator) -> SimValue {
        let new_value = match operator {
            ast::UnaryOperator::BitwiseNot => self.0.exec_bit_not(),
            ast::UnaryOperator::Negative => todo!(),
            ast::UnaryOperator::Not => todo!(),
            ast::UnaryOperator::Positive => todo!(),
        };
        Self(new_value)
    }
}

/// This function is a duplication of the exec_like function in core/vdbe/mod.rs at commit 9b9d5f9b4c9920e066ef1237c80878f4c3968524
/// Any updates to the original function should be reflected here, otherwise the test will be incorrect.
fn construct_like_regex(pattern: &str) -> Regex {
    let mut regex_pattern = String::with_capacity(pattern.len() * 2);

    regex_pattern.push('^');

    for c in pattern.chars() {
        match c {
            '\\' => regex_pattern.push_str("\\\\"),
            '%' => regex_pattern.push_str(".*"),
            '_' => regex_pattern.push('.'),
            ch => {
                if regex_syntax::is_meta_character(c) {
                    regex_pattern.push('\\');
                }
                regex_pattern.push(ch);
            }
        }
    }

    regex_pattern.push('$');

    RegexBuilder::new(&regex_pattern)
        .case_insensitive(true)
        .dot_matches_new_line(true)
        .build()
        .unwrap()
}

fn exec_like(pattern: &str, text: &str) -> bool {
    let re = construct_like_regex(pattern);
    re.is_match(text)
}

impl From<ast::Literal> for SimValue {
    fn from(value: ast::Literal) -> Self {
        Self::from(&value)
    }
}

impl From<&ast::Literal> for SimValue {
    fn from(value: &ast::Literal) -> Self {
        let new_value = match value {
            ast::Literal::Null => types::Value::Null,
            ast::Literal::Numeric(number) => Numeric::from(number).into(),
            ast::Literal::String(string) => types::Value::build_text(string),
            ast::Literal::Blob(blob) => types::Value::Blob(
                blob.as_bytes()
                    .chunks_exact(2)
                    .map(|pair| {
                        // We assume that sqlite3-parser has already validated that
                        // the input is valid hex string, thus unwrap is safe.
                        let hex_byte = std::str::from_utf8(pair).unwrap();
                        u8::from_str_radix(hex_byte, 16).unwrap()
                    })
                    .collect(),
            ),
            lit => unimplemented!("{:?}", lit),
        };
        Self(new_value)
    }
}

impl From<SimValue> for ast::Literal {
    fn from(value: SimValue) -> Self {
        Self::from(&value)
    }
}

impl From<&SimValue> for ast::Literal {
    fn from(value: &SimValue) -> Self {
        match &value.0 {
            types::Value::Null => Self::Null,
            types::Value::Integer(i) => Self::Numeric(i.to_string()),
            types::Value::Float(f) => Self::Numeric(f.to_string()),
            text @ types::Value::Text(..) => Self::String(format!("'{}'", text)),
            types::Value::Blob(blob) => Self::Blob(hex::encode(blob)),
        }
    }
}

impl From<bool> for SimValue {
    fn from(value: bool) -> Self {
        value.then_some(SimValue::TRUE).unwrap_or(SimValue::FALSE)
    }
}

impl From<SimValue> for limbo_core::types::Value {
    fn from(value: SimValue) -> Self {
        value.0
    }
}

impl From<&SimValue> for limbo_core::types::Value {
    fn from(value: &SimValue) -> Self {
        value.0.clone()
    }
}

impl From<limbo_core::types::Value> for SimValue {
    fn from(value: limbo_core::types::Value) -> Self {
        Self(value)
    }
}

impl From<&limbo_core::types::Value> for SimValue {
    fn from(value: &limbo_core::types::Value) -> Self {
        Self(value.clone())
    }
}
