use crate::common::{self, maybe_setup_tracing};
use crate::common::{compare_string, do_flush, TempDatabase};
use limbo_core::{Connection, Row, Statement, StepResult, Value};
use log::debug;
use std::sync::Arc;

#[macro_export]
macro_rules! change_state {
    ($current:expr, $pattern:pat => $selector:expr) => {
        let state = match std::mem::replace($current, unsafe { std::mem::zeroed() }) {
            $pattern => $selector,
            _ => panic!("unexpected state"),
        };
        #[allow(clippy::forget_non_drop)]
        std::mem::forget(std::mem::replace($current, state));
    };
}

#[test]
#[ignore]
fn test_simple_overflow_page() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let tmp_db =
        TempDatabase::new_with_rusqlite("CREATE TABLE test (x INTEGER PRIMARY KEY, t TEXT);");
    let conn = tmp_db.connect_limbo();

    let mut huge_text = String::new();
    for i in 0..8192 {
        huge_text.push((b'A' + (i % 24) as u8) as char);
    }

    let list_query = "SELECT * FROM test LIMIT 1";
    let insert_query = format!("INSERT INTO test VALUES (1, '{}')", huge_text.as_str());

    match conn.query(insert_query) {
        Ok(Some(ref mut rows)) => loop {
            match rows.step()? {
                StepResult::IO => {
                    tmp_db.io.run_once()?;
                }
                StepResult::Done => break,
                _ => unreachable!(),
            }
        },
        Ok(None) => {}
        Err(err) => {
            eprintln!("{}", err);
        }
    };

    // this flush helped to review hex of test.db
    do_flush(&conn, &tmp_db)?;

    match conn.query(list_query) {
        Ok(Some(ref mut rows)) => loop {
            match rows.step()? {
                StepResult::Row => {
                    let row = rows.row().unwrap();
                    let id = row.get::<i64>(0).unwrap();
                    let text = row.get::<&str>(0).unwrap();
                    assert_eq!(1, id);
                    compare_string(&huge_text, text);
                }
                StepResult::IO => {
                    tmp_db.io.run_once()?;
                }
                StepResult::Interrupt => break,
                StepResult::Done => break,
                StepResult::Busy => unreachable!(),
            }
        },
        Ok(None) => {}
        Err(err) => {
            eprintln!("{}", err);
        }
    }
    do_flush(&conn, &tmp_db)?;
    Ok(())
}

#[test]
fn test_sequential_overflow_page() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    maybe_setup_tracing();
    let tmp_db =
        TempDatabase::new_with_rusqlite("CREATE TABLE test (x INTEGER PRIMARY KEY, t TEXT);");
    let conn = tmp_db.connect_limbo();
    let iterations = 10_usize;

    let mut huge_texts = Vec::new();
    for i in 0..iterations {
        let mut huge_text = String::new();
        for _j in 0..8192 {
            huge_text.push((b'A' + i as u8) as char);
        }
        huge_texts.push(huge_text);
    }

    for i in 0..iterations {
        let huge_text = &huge_texts[i];
        let insert_query = format!("INSERT INTO test VALUES ({}, '{}')", i, huge_text.as_str());
        match conn.query(insert_query) {
            Ok(Some(ref mut rows)) => loop {
                match rows.step()? {
                    StepResult::IO => {
                        tmp_db.io.run_once()?;
                    }
                    StepResult::Done => break,
                    _ => unreachable!(),
                }
            },
            Ok(None) => {}
            Err(err) => {
                eprintln!("{}", err);
            }
        };
    }

    let list_query = "SELECT * FROM test LIMIT 1";
    let mut current_index = 0;
    match conn.query(list_query) {
        Ok(Some(ref mut rows)) => loop {
            match rows.step()? {
                StepResult::Row => {
                    let row = rows.row().unwrap();
                    let id = row.get::<i64>(0).unwrap();
                    let text = row.get::<String>(1).unwrap();
                    let huge_text = &huge_texts[current_index];
                    compare_string(huge_text, text);
                    assert_eq!(current_index, id as usize);
                    current_index += 1;
                }
                StepResult::IO => {
                    tmp_db.io.run_once()?;
                }
                StepResult::Interrupt => break,
                StepResult::Done => break,
                StepResult::Busy => unreachable!(),
            }
        },
        Ok(None) => {}
        Err(err) => {
            eprintln!("{}", err);
        }
    }
    do_flush(&conn, &tmp_db)?;
    Ok(())
}

#[test_log::test]
#[ignore = "this takes too long :)"]
fn test_sequential_write() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    maybe_setup_tracing();

    let tmp_db = TempDatabase::new_with_rusqlite("CREATE TABLE test (x INTEGER PRIMARY KEY);");
    let conn = tmp_db.connect_limbo();

    let list_query = "SELECT * FROM test";
    let max_iterations = 10000;
    for i in 0..max_iterations {
        println!("inserting {} ", i);
        if (i % 100) == 0 {
            let progress = (i as f64 / max_iterations as f64) * 100.0;
            println!("progress {:.1}%", progress);
        }
        let insert_query = format!("INSERT INTO test VALUES ({})", i);
        run_query(&tmp_db, &conn, &insert_query)?;

        let mut current_read_index = 0;
        run_query_on_row(&tmp_db, &conn, &list_query, |row: &Row| {
            let first_value = row.get::<&Value>(0).expect("missing id");
            let id = match first_value {
                limbo_core::Value::Integer(i) => *i as i32,
                limbo_core::Value::Float(f) => *f as i32,
                _ => unreachable!(),
            };
            assert_eq!(current_read_index, id);
            current_read_index += 1;
        })?;
        common::do_flush(&conn, &tmp_db)?;
    }
    Ok(())
}

#[test]
/// There was a regression with inserting multiple rows with a column containing an unary operator :)
/// https://github.com/tursodatabase/limbo/pull/679
fn test_regression_multi_row_insert() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let tmp_db = TempDatabase::new_with_rusqlite("CREATE TABLE test (x REAL);");
    let conn = tmp_db.connect_limbo();

    let insert_query = "INSERT INTO test VALUES (-2), (-3), (-1)";
    let list_query = "SELECT * FROM test";

    run_query(&tmp_db, &conn, insert_query)?;

    common::do_flush(&conn, &tmp_db)?;

    let mut current_read_index = 1;
    let expected_ids = vec![-3, -2, -1];
    let mut actual_ids = Vec::new();
    run_query_on_row(&tmp_db, &conn, list_query, |row: &Row| {
        let first_value = row.get::<&Value>(0).expect("missing id");
        let id = match first_value {
            Value::Float(f) => *f as i32,
            _ => panic!("expected float"),
        };
        actual_ids.push(id);
        current_read_index += 1;
    })?;

    assert_eq!(current_read_index, 4); // Verify we read all rows
                                       // sort ids
    actual_ids.sort();
    assert_eq!(actual_ids, expected_ids);
    Ok(())
}

#[test]
fn test_statement_reset() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let tmp_db = TempDatabase::new_with_rusqlite("create table test (i integer);");
    let conn = tmp_db.connect_limbo();

    conn.execute("insert into test values (1)")?;
    conn.execute("insert into test values (2)")?;

    let mut stmt = conn.prepare("select * from test")?;

    loop {
        match stmt.step()? {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                assert_eq!(
                    *row.get::<&Value>(0).unwrap(),
                    limbo_core::Value::Integer(1)
                );
                break;
            }
            StepResult::IO => tmp_db.io.run_once()?,
            _ => break,
        }
    }

    stmt.reset();

    loop {
        match stmt.step()? {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                assert_eq!(
                    *row.get::<&Value>(0).unwrap(),
                    limbo_core::Value::Integer(1)
                );
                break;
            }
            StepResult::IO => tmp_db.io.run_once()?,
            _ => break,
        }
    }

    Ok(())
}

#[test]
#[ignore]
fn test_wal_checkpoint() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let tmp_db = TempDatabase::new_with_rusqlite("CREATE TABLE test (x INTEGER PRIMARY KEY);");
    // threshold is 1000 by default
    let iterations = 1001_usize;
    let conn = tmp_db.connect_limbo();

    for i in 0..iterations {
        let insert_query = format!("INSERT INTO test VALUES ({})", i);
        do_flush(&conn, &tmp_db)?;
        conn.checkpoint()?;
        run_query(&tmp_db, &conn, &insert_query)?;
    }

    do_flush(&conn, &tmp_db)?;
    let list_query = "SELECT * FROM test LIMIT 1";
    let mut current_index = 0;
    run_query_on_row(&tmp_db, &conn, list_query, |row: &Row| {
        let id = row.get::<i64>(0).unwrap();
        assert_eq!(current_index, id as usize);
        current_index += 1;
    })?;
    do_flush(&conn, &tmp_db)?;
    Ok(())
}

#[test]
fn test_wal_restart() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let tmp_db = TempDatabase::new_with_rusqlite("CREATE TABLE test (x INTEGER PRIMARY KEY);");
    // threshold is 1000 by default

    fn insert(i: usize, conn: &Arc<Connection>, tmp_db: &TempDatabase) -> anyhow::Result<()> {
        debug!("inserting {}", i);
        let insert_query = format!("INSERT INTO test VALUES ({})", i);
        run_query(tmp_db, conn, &insert_query)?;
        debug!("inserted {}", i);
        tmp_db.io.run_once()?;
        Ok(())
    }

    fn count(conn: &Arc<Connection>, tmp_db: &TempDatabase) -> anyhow::Result<usize> {
        debug!("counting");
        let list_query = "SELECT count(x) FROM test";
        let mut count = None;
        run_query_on_row(tmp_db, conn, list_query, |row: &Row| {
            assert!(count.is_none());
            count = Some(row.get::<i64>(0).unwrap() as usize);
            debug!("counted {:?}", count);
        })?;
        Ok(count.unwrap())
    }

    {
        let conn = tmp_db.connect_limbo();
        insert(1, &conn, &tmp_db)?;
        assert_eq!(count(&conn, &tmp_db)?, 1);
        conn.close()?;
    }
    {
        let conn = tmp_db.connect_limbo();
        assert_eq!(
            count(&conn, &tmp_db)?,
            1,
            "failed to read from wal from another connection"
        );
        conn.close()?;
    }
    Ok(())
}

#[test]
fn test_insert_after_big_blob() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let tmp_db = TempDatabase::new_with_rusqlite("CREATE TABLE temp (t1 BLOB, t2 INTEGER)");
    let conn = tmp_db.connect_limbo();

    conn.execute("insert into temp(t1) values (zeroblob (262144))")?;
    conn.execute("insert into temp(t2) values (1)")?;

    Ok(())
}

#[test_log::test]
#[ignore = "this takes too long :)"]
fn test_write_delete_with_index() -> anyhow::Result<()> {
    let _ = env_logger::try_init();

    maybe_setup_tracing();

    let tmp_db = TempDatabase::new_with_rusqlite("CREATE TABLE test (x PRIMARY KEY);");
    let conn = tmp_db.connect_limbo();

    let list_query = "SELECT * FROM test";
    let max_iterations = 1000;
    for i in 0..max_iterations {
        println!("inserting {} ", i);
        let insert_query = format!("INSERT INTO test VALUES ({})", i);
        run_query(&tmp_db, &conn, &insert_query)?;
    }
    for i in 0..max_iterations {
        println!("deleting {} ", i);
        let delete_query = format!("delete from test where x={}", i);
        run_query(&tmp_db, &conn, &delete_query)?;
        println!("listing after deleting {} ", i);
        let mut current_read_index = i + 1;
        run_query_on_row(&tmp_db, &conn, list_query, |row: &Row| {
            let first_value = row.get::<&Value>(0).expect("missing id");
            let id = match first_value {
                limbo_core::Value::Integer(i) => *i as i32,
                limbo_core::Value::Float(f) => *f as i32,
                _ => unreachable!(),
            };
            assert_eq!(current_read_index, id);
            current_read_index += 1;
        })?;
        for i in i + 1..max_iterations {
            // now test with seek
            run_query_on_row(
                &tmp_db,
                &conn,
                &format!("select * from test where x = {}", i),
                |row| {
                    let first_value = row.get::<&Value>(0).expect("missing id");
                    let id = match first_value {
                        limbo_core::Value::Integer(i) => *i as i32,
                        limbo_core::Value::Float(f) => *f as i32,
                        _ => unreachable!(),
                    };
                    assert_eq!(i, id);
                },
            )?;
        }
    }

    Ok(())
}

#[test]
#[cfg(feature = "index_experimental")]
fn test_update_with_index() -> anyhow::Result<()> {
    let _ = env_logger::try_init();

    maybe_setup_tracing();

    let tmp_db = TempDatabase::new_with_rusqlite("CREATE TABLE test (x REAL PRIMARY KEY, y TEXT);");
    let conn = tmp_db.connect_limbo();

    run_query(&tmp_db, &conn, "INSERT INTO test VALUES (1.0, 'foo')")?;
    run_query(&tmp_db, &conn, "INSERT INTO test VALUES (2.0, 'bar')")?;

    run_query_on_row(&tmp_db, &conn, "SELECT * from test WHERE x=10.0", |row| {
        assert_eq!(row.get::<f64>(0).unwrap(), 1.0);
    })?;
    run_query(&tmp_db, &conn, "UPDATE test SET x=10.0 WHERE x=1.0")?;
    run_query_on_row(&tmp_db, &conn, "SELECT * from test WHERE x=10.0", |row| {
        assert_eq!(row.get::<f64>(0).unwrap(), 10.0);
    })?;

    let mut count_1 = 0;
    let mut count_10 = 0;
    run_query_on_row(&tmp_db, &conn, "SELECT * from test", |row| {
        let v = row.get::<f64>(0).unwrap();
        if v == 1.0 {
            count_1 += 1;
        } else if v == 10.0 {
            count_10 += 1;
        }
    })?;
    assert_eq!(count_1, 0, "1.0 shouldn't be inside table");
    assert_eq!(count_10, 1, "10.0 should have existed");

    Ok(())
}

#[test]
fn test_delete_with_index() -> anyhow::Result<()> {
    let _ = env_logger::try_init();

    maybe_setup_tracing();

    let tmp_db = TempDatabase::new_with_rusqlite("CREATE TABLE t(x UNIQUE)");
    let conn = tmp_db.connect_limbo();

    run_query(&tmp_db, &conn, "INSERT INTO t VALUES (1), (2)")?;
    run_query(&tmp_db, &conn, "DELETE FROM t WHERE x >= 1")?;

    run_query_on_row(&tmp_db, &conn, "SELECT * FROM t", |_| {
        panic!("Delete should've deleted every row!");
    })?;

    Ok(())
}

enum ConnectionState {
    PrepareQuery { query_idx: usize },
    ExecuteQuery { query_idx: usize, stmt: Statement },
    Done,
}

struct ConnectionPlan {
    queries: Vec<String>,
    conn: Arc<Connection>,
    state: ConnectionState,
}

impl ConnectionPlan {
    pub fn step(&mut self) -> anyhow::Result<bool> {
        loop {
            match &mut self.state {
                ConnectionState::PrepareQuery { query_idx } => {
                    if *query_idx >= self.queries.len() {
                        self.state = ConnectionState::Done;
                        return Ok(true);
                    }
                    let query = &self.queries[*query_idx];
                    tracing::info!("preparing {}", query);
                    let stmt = self.conn.query(query)?.unwrap();
                    self.state = ConnectionState::ExecuteQuery {
                        query_idx: *query_idx,
                        stmt,
                    };
                }
                ConnectionState::ExecuteQuery { stmt, query_idx } => loop {
                    let query = &self.queries[*query_idx];
                    tracing::info!("stepping {}", query);
                    let current_query_idx = *query_idx;
                    let step_result = stmt.step()?;
                    match step_result {
                        StepResult::IO => {
                            return Ok(false);
                        }
                        StepResult::Done => {
                            change_state!(&mut self.state, ConnectionState::ExecuteQuery { .. } => ConnectionState::PrepareQuery { query_idx: current_query_idx + 1 });
                            return Ok(false);
                        }
                        StepResult::Row => {}
                        StepResult::Busy => {
                            return Ok(false);
                        }
                        _ => unreachable!(),
                    }
                },
                ConnectionState::Done => {
                    return Ok(true);
                }
            }
        }
    }

    pub fn is_finished(&self) -> bool {
        matches!(self.state, ConnectionState::Done)
    }
}

#[test]
fn test_write_concurrent_connections() -> anyhow::Result<()> {
    let _ = env_logger::try_init();

    maybe_setup_tracing();

    let tmp_db = TempDatabase::new_with_rusqlite("CREATE TABLE t(x)");
    let num_connections = 4;
    let num_inserts_per_connection = 100;
    let mut connections = vec![];
    for connection_idx in 0..num_connections {
        let conn = tmp_db.connect_limbo();
        let mut queries = Vec::with_capacity(num_inserts_per_connection);
        for query_idx in 0..num_inserts_per_connection {
            queries.push(format!(
                "INSERT INTO t VALUES({})",
                (connection_idx * num_inserts_per_connection) + query_idx
            ));
        }
        connections.push(ConnectionPlan {
            queries,
            conn,
            state: ConnectionState::PrepareQuery { query_idx: 0 },
        });
    }

    let mut connections_finished = 0;
    while connections_finished != num_connections {
        for conn in &mut connections {
            if conn.is_finished() {
                continue;
            }
            let finished = conn.step()?;
            if finished {
                connections_finished += 1;
            }
        }
    }

    let conn = tmp_db.connect_limbo();
    run_query_on_row(&tmp_db, &conn, "SELECT count(1) from t", |row: &Row| {
        let count = row.get::<i64>(0).unwrap();
        assert_eq!(
            count,
            (num_connections * num_inserts_per_connection) as i64,
            "received wrong number of rows"
        );
    })?;

    Ok(())
}

fn run_query(tmp_db: &TempDatabase, conn: &Arc<Connection>, query: &str) -> anyhow::Result<()> {
    run_query_core(tmp_db, conn, query, None::<fn(&Row)>)
}

fn run_query_on_row(
    tmp_db: &TempDatabase,
    conn: &Arc<Connection>,
    query: &str,
    on_row: impl FnMut(&Row),
) -> anyhow::Result<()> {
    run_query_core(tmp_db, conn, query, Some(on_row))
}

fn run_query_core(
    tmp_db: &TempDatabase,
    conn: &Arc<Connection>,
    query: &str,
    mut on_row: Option<impl FnMut(&Row)>,
) -> anyhow::Result<()> {
    match conn.query(query) {
        Ok(Some(ref mut rows)) => loop {
            match rows.step()? {
                StepResult::IO => {
                    tmp_db.io.run_once()?;
                }
                StepResult::Done => break,
                StepResult::Row => {
                    if let Some(on_row) = on_row.as_mut() {
                        let row = rows.row().unwrap();
                        on_row(row)
                    }
                }
                _ => unreachable!(),
            }
        },
        Ok(None) => {}
        Err(err) => {
            eprintln!("{}", err);
        }
    };
    Ok(())
}
