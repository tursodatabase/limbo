#![no_main]
use core::fmt;
use std::{num::NonZero, sync::Arc};

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use limbo_core::{OwnedValue, IO as _};

#[derive(Debug, Arbitrary)]
enum Binary {
    Equal,
    NotEqual,
}

impl fmt::Display for Binary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Binary::Equal => "=",
                Binary::NotEqual => "<>",
            }
        )
    }
}

#[derive(Debug, Arbitrary, Clone)]
enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl From<Value> for limbo_core::OwnedValue {
    fn from(value: Value) -> limbo_core::OwnedValue {
        match value {
            Value::Null => limbo_core::OwnedValue::Null,
            Value::Integer(v) => limbo_core::OwnedValue::Integer(v),
            Value::Real(v) => limbo_core::OwnedValue::Float(v),
            Value::Text(v) => limbo_core::OwnedValue::from_text(&v),
            Value::Blob(v) => limbo_core::OwnedValue::from_blob(v.to_owned()),
        }
    }
}

impl rusqlite::ToSql for Value {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        use rusqlite::types::ToSqlOutput;

        Ok(match self {
            Value::Null => ToSqlOutput::Owned(rusqlite::types::Value::Null),
            Value::Integer(v) => ToSqlOutput::Owned(rusqlite::types::Value::Integer(*v)),
            Value::Real(v) => ToSqlOutput::Owned(rusqlite::types::Value::Real(*v)),
            Value::Text(v) => ToSqlOutput::Owned(rusqlite::types::Value::Text(v.to_owned())),
            Value::Blob(v) => ToSqlOutput::Owned(rusqlite::types::Value::Blob(v.to_owned())),
        })
    }
}

impl rusqlite::types::FromSql for Value {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        Ok(match value {
            rusqlite::types::ValueRef::Null => Value::Null,
            rusqlite::types::ValueRef::Integer(v) => Value::Integer(v),
            rusqlite::types::ValueRef::Real(v) => Value::Real(v),
            rusqlite::types::ValueRef::Text(v) => {
                Value::Text(String::from_utf8_lossy(v).to_string())
            }
            rusqlite::types::ValueRef::Blob(v) => Value::Blob(v.to_vec()),
        })
    }
}

#[derive(Debug, Arbitrary)]
enum Expr {
    Value(Value),
    Binary(Binary, Box<Expr>, Box<Expr>),
}

fn to_sql(expr: &Expr) -> (String, Vec<Value>) {
    match expr {
        Expr::Value(value) => ("?".to_string(), vec![value.clone()]),
        Expr::Binary(op, lhs, rhs) => {
            let mut lhs = to_sql(lhs);
            let mut rhs = to_sql(rhs);
            lhs.1.append(&mut rhs.1);
            (format!("({}) {op} ({})", lhs.0, rhs.0), lhs.1)
        }
    }
}

fn do_fuzz(expr: Expr) {
    let (sql, params) = to_sql(&expr);
    let sql = format!("select {}", &sql);

    let sqlite = rusqlite::Connection::open_in_memory().unwrap();
    let io = Arc::new(limbo_core::MemoryIO::new());
    let memory = limbo_core::Database::open_file(io.clone(), ":memory:", true).unwrap();
    let limbo = memory.connect().unwrap();

    let expected = sqlite
        .query_row(&sql, rusqlite::params_from_iter(params.iter()), |row| {
            row.get::<_, Value>(0)
        })
        .unwrap();

    let mut stmt = limbo.prepare(sql).unwrap();
    for (idx, value) in params.into_iter().enumerate() {
        stmt.bind_at(NonZero::new(idx + 1).unwrap(), value.into())
    }

    let value = 'value: {
        loop {
            use limbo_core::StepResult;
            match stmt.step().unwrap() {
                StepResult::IO => io.run_once().unwrap(),
                StepResult::Row => {
                    let row = stmt.row().unwrap();
                    assert_eq!(row.count(), 1);
                    break 'value row.get_value(0).clone();
                }
                _ => unreachable!(),
            }
        }
    };

    assert_eq!(OwnedValue::from(expected), value);
}

fuzz_target!(|expr: Expr| do_fuzz(expr));
