use crate::common::TempDatabase;
use limbo_core::{StepResult, Value};

#[test]
fn test_statement_reset_bind() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let tmp_db = TempDatabase::new_with_rusqlite("create table test (i integer);");
    let conn = tmp_db.connect_limbo();

    let mut stmt = conn.prepare("select ?")?;

    stmt.bind_at(1.try_into()?, Value::Integer(1));

    loop {
        match stmt.step()? {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                assert_eq!(
                    *row.get::<&Value>(0).unwrap(),
                    limbo_core::Value::Integer(1)
                );
            }
            StepResult::IO => tmp_db.io.run_once()?,
            _ => break,
        }
    }

    stmt.reset();

    stmt.bind_at(1.try_into()?, Value::Integer(2));

    loop {
        match stmt.step()? {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                assert_eq!(
                    *row.get::<&Value>(0).unwrap(),
                    limbo_core::Value::Integer(2)
                );
            }
            StepResult::IO => tmp_db.io.run_once()?,
            _ => break,
        }
    }

    Ok(())
}

#[test]
fn test_statement_bind() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let tmp_db = TempDatabase::new_with_rusqlite("create table test (i integer);");
    let conn = tmp_db.connect_limbo();

    let mut stmt = conn.prepare("select ?, ?1, :named, ?3, ?4")?;

    stmt.bind_at(1.try_into()?, Value::build_text("hello"));

    let i = stmt.parameters().index(":named").unwrap();
    stmt.bind_at(i, Value::Integer(42));

    stmt.bind_at(3.try_into()?, Value::from_blob(vec![0x1, 0x2, 0x3]));

    stmt.bind_at(4.try_into()?, Value::Float(0.5));

    assert_eq!(stmt.parameters().count(), 4);

    loop {
        match stmt.step()? {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                if let limbo_core::Value::Text(s) = row.get::<&Value>(0).unwrap() {
                    assert_eq!(s.as_str(), "hello")
                }

                if let limbo_core::Value::Text(s) = row.get::<&Value>(1).unwrap() {
                    assert_eq!(s.as_str(), "hello")
                }

                if let limbo_core::Value::Integer(i) = row.get::<&Value>(2).unwrap() {
                    assert_eq!(*i, 42)
                }

                if let limbo_core::Value::Blob(v) = row.get::<&Value>(3).unwrap() {
                    assert_eq!(v.as_slice(), &vec![0x1 as u8, 0x2, 0x3])
                }

                if let limbo_core::Value::Float(f) = row.get::<&Value>(4).unwrap() {
                    assert_eq!(*f, 0.5)
                }
            }
            StepResult::IO => {
                tmp_db.io.run_once()?;
            }
            StepResult::Interrupt => break,
            StepResult::Done => break,
            StepResult::Busy => panic!("Database is busy"),
        };
    }
    Ok(())
}
