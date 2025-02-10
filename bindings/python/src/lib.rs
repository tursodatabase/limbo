use anyhow::Result;
use errors::*;
use pyo3::prelude::*;
use pyo3::types::PyList;
use pyo3::types::PyTuple;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

mod errors;

#[pyclass]
#[derive(Clone, Debug)]
struct Description {
    #[pyo3(get)]
    name: String,
    #[pyo3(get)]
    type_code: String,
    #[pyo3(get)]
    display_size: Option<String>,
    #[pyo3(get)]
    internal_size: Option<String>,
    #[pyo3(get)]
    precision: Option<String>,
    #[pyo3(get)]
    scale: Option<String>,
    #[pyo3(get)]
    null_ok: Option<String>,
}

impl IntoPy<Py<PyTuple>> for Description {
    fn into_py(self, py: Python<'_>) -> Py<PyTuple> {
        PyTuple::new_bound(
            py,
            vec![
                self.name.into_py(py),
                self.type_code.into_py(py),
                self.display_size.into_py(py),
                self.internal_size.into_py(py),
                self.precision.into_py(py),
                self.scale.into_py(py),
                self.null_ok.into_py(py),
            ],
        )
        .into()
    }
}

#[pyclass]
pub struct Cursor {
    /// This read/write attribute specifies the number of rows to fetch at a time with `.fetchmany()`.
    /// It defaults to `1`, meaning it fetches a single row at a time.
    #[pyo3(get)]
    arraysize: i64,

    conn: Connection,

    /// The `.description` attribute is a read-only sequence of 7-item, each describing a column in the result set:
    ///
    /// - `name`: The column's name (always present).
    /// - `type_code`: The data type code (always present).
    /// - `display_size`: Column's display size (optional).
    /// - `internal_size`: Column's internal size (optional).
    /// - `precision`: Numeric precision (optional).
    /// - `scale`: Numeric scale (optional).
    /// - `null_ok`: Indicates if null values are allowed (optional).
    ///
    /// The `name` and `type_code` fields are mandatory; others default to `None` if not applicable.
    ///
    /// This attribute is `None` for operations that do not return rows or if no `.execute*()` method has been invoked.
    #[pyo3(get)]
    description: Option<Description>,

    /// Read-only attribute that provides the number of modified rows for `INSERT`, `UPDATE`, `DELETE`,
    /// and `REPLACE` statements; it is `-1` for other statements, including CTE queries.
    /// It is only updated by the `execute()` and `executemany()` methods after the statement has run to completion.
    /// This means any resulting rows must be fetched for `rowcount` to be updated.
    #[pyo3(get)]
    rowcount: i64,

    smt: Option<Rc<RefCell<limbo_core::Statement>>>,
}

// SAFETY: The limbo_core crate guarantees that `Cursor` is thread-safe.
unsafe impl Send for Cursor {}

#[allow(unused_variables, clippy::arc_with_non_send_sync)]
#[pymethods]
impl Cursor {
    #[pyo3(signature = (sql, parameters=None))]
    pub fn execute(&mut self, sql: &str, parameters: Option<Py<PyTuple>>) -> Result<Self> {
        let stmt_is_dml = stmt_is_dml(sql);
        let stmt_is_ddl = stmt_is_ddl(sql);

        let statement = self.conn.conn.prepare(sql).map_err(|e| {
            PyErr::new::<ProgrammingError, _>(format!("Failed to prepare statement: {:?}", e))
        })?;

        let stmt = Rc::new(RefCell::new(statement));

        // For DDL and DML statements,
        // we need to execute the statement immediately
        if stmt_is_ddl || stmt_is_dml {
            loop {
                match stmt.borrow_mut().step().map_err(|e| {
                    PyErr::new::<OperationalError, _>(format!("Step error: {:?}", e))
                })? {
                    limbo_core::StepResult::IO => {
                        self.conn.io.run_once().map_err(|e| {
                            PyErr::new::<OperationalError, _>(format!("IO error: {:?}", e))
                        })?;
                    }
                    _ => break,
                }
            }
        }

        self.smt = Some(stmt);

        Ok(Cursor {
            smt: self.smt.clone(),
            conn: self.conn.clone(),
            description: self.description.clone(),
            rowcount: self.rowcount,
            arraysize: self.arraysize,
        })
    }

    pub fn fetchone(&mut self, py: Python) -> Result<Option<PyObject>> {
        if let Some(smt) = &self.smt {
            loop {
                let mut stmt = smt.borrow_mut();
                match stmt.step().map_err(|e| {
                    PyErr::new::<OperationalError, _>(format!("Step error: {:?}", e))
                })? {
                    limbo_core::StepResult::Row => {
                        let row = stmt.row().unwrap();
                        let py_row = row_to_py(py, &row);
                        return Ok(Some(py_row));
                    }
                    limbo_core::StepResult::IO => {
                        self.conn.io.run_once().map_err(|e| {
                            PyErr::new::<OperationalError, _>(format!("IO error: {:?}", e))
                        })?;
                    }
                    limbo_core::StepResult::Interrupt => {
                        return Ok(None);
                    }
                    limbo_core::StepResult::Done => {
                        return Ok(None);
                    }
                    limbo_core::StepResult::Busy => {
                        return Err(
                            PyErr::new::<OperationalError, _>("Busy error".to_string()).into()
                        );
                    }
                }
            }
        } else {
            Err(PyErr::new::<ProgrammingError, _>("No statement prepared for execution").into())
        }
    }

    pub fn fetchall(&mut self, py: Python) -> Result<Vec<PyObject>> {
        let mut results = Vec::new();
        if let Some(smt) = &self.smt {
            loop {
                let mut stmt = smt.borrow_mut();
                match stmt.step().map_err(|e| {
                    PyErr::new::<OperationalError, _>(format!("Step error: {:?}", e))
                })? {
                    limbo_core::StepResult::Row => {
                        let row = stmt.row().unwrap();
                        let py_row = row_to_py(py, &row);
                        results.push(py_row);
                    }
                    limbo_core::StepResult::IO => {
                        self.conn.io.run_once().map_err(|e| {
                            PyErr::new::<OperationalError, _>(format!("IO error: {:?}", e))
                        })?;
                    }
                    limbo_core::StepResult::Interrupt => {
                        return Ok(results);
                    }
                    limbo_core::StepResult::Done => {
                        return Ok(results);
                    }
                    limbo_core::StepResult::Busy => {
                        return Err(
                            PyErr::new::<OperationalError, _>("Busy error".to_string()).into()
                        );
                    }
                }
            }
        } else {
            Err(PyErr::new::<ProgrammingError, _>("No statement prepared for execution").into())
        }
    }

    pub fn close(&self) -> PyResult<()> {
        Err(PyErr::new::<NotSupportedError, _>(
            "close() is not supported in this version",
        ))
    }

    #[pyo3(signature = (sql, parameters=None))]
    pub fn executemany(&self, sql: &str, parameters: Option<Py<PyList>>) -> PyResult<()> {
        Err(PyErr::new::<NotSupportedError, _>(
            "executemany() is not supported in this version",
        ))
    }

    #[pyo3(signature = (size=None))]
    pub fn fetchmany(&self, size: Option<i64>) -> PyResult<Option<Vec<PyObject>>> {
        Err(PyErr::new::<NotSupportedError, _>(
            "fetchmany() is not supported in this version",
        ))
    }
}

fn stmt_is_dml(sql: &str) -> bool {
    let sql = sql.trim();
    let sql = sql.to_uppercase();
    sql.starts_with("INSERT") || sql.starts_with("UPDATE") || sql.starts_with("DELETE")
}

fn stmt_is_ddl(sql: &str) -> bool {
    let sql = sql.trim();
    let sql = sql.to_uppercase();
    sql.starts_with("CREATE") || sql.starts_with("ALTER") || sql.starts_with("DROP")
}

#[pyclass]
#[derive(Clone)]
pub struct Connection {
    conn: Rc<limbo_core::Connection>,
    io: Arc<dyn limbo_core::IO>,
}

// SAFETY: The limbo_core crate guarantees that `Connection` is thread-safe.
unsafe impl Send for Connection {}

#[pymethods]
impl Connection {
    pub fn cursor(&self) -> Result<Cursor> {
        Ok(Cursor {
            arraysize: 1,
            conn: self.clone(),
            description: None,
            rowcount: -1,
            smt: None,
        })
    }

    pub fn close(&self) {
        drop(self.conn.clone());
    }

    pub fn commit(&self) -> PyResult<()> {
        Err(PyErr::new::<NotSupportedError, _>(
            "Transactions are not supported in this version",
        ))
    }

    pub fn rollback(&self) -> PyResult<()> {
        Err(PyErr::new::<NotSupportedError, _>(
            "Transactions are not supported in this version",
        ))
    }
}

#[allow(clippy::arc_with_non_send_sync)]
#[pyfunction]
pub fn connect(path: &str) -> Result<Connection> {
    #[inline(always)]
    fn open_or(
        io: Arc<dyn limbo_core::IO>,
        path: &str,
    ) -> std::result::Result<Arc<limbo_core::Database>, PyErr> {
        limbo_core::Database::open_file(io, path).map_err(|e| {
            PyErr::new::<DatabaseError, _>(format!("Failed to open database: {:?}", e))
        })
    }

    match path {
        ":memory:" => {
            let io: Arc<dyn limbo_core::IO> = Arc::new(limbo_core::MemoryIO::new()?);
            let db = open_or(io.clone(), path)?;
            let conn: Rc<limbo_core::Connection> = db.connect();
            Ok(Connection { conn, io })
        }
        path => {
            let io: Arc<dyn limbo_core::IO> = Arc::new(limbo_core::PlatformIO::new()?);
            let db = open_or(io.clone(), path)?;
            let conn: Rc<limbo_core::Connection> = db.connect();
            Ok(Connection { conn, io })
        }
    }
}

fn row_to_py(py: Python, row: &limbo_core::Row) -> PyObject {
    let py_values: Vec<PyObject> = row
        .values
        .iter()
        .map(|value| match value.to_value() {
            limbo_core::Value::Null => py.None(),
            limbo_core::Value::Integer(i) => i.to_object(py),
            limbo_core::Value::Float(f) => f.to_object(py),
            limbo_core::Value::Text(s) => s.to_object(py),
            limbo_core::Value::Blob(b) => b.to_object(py),
        })
        .collect();

    PyTuple::new_bound(py, &py_values).to_object(py)
}

#[pymodule]
fn _limbo(m: &Bound<PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_class::<Connection>()?;
    m.add_class::<Cursor>()?;
    m.add_function(wrap_pyfunction!(connect, m)?)?;
    m.add("Warning", m.py().get_type_bound::<Warning>())?;
    m.add("Error", m.py().get_type_bound::<Error>())?;
    m.add("InterfaceError", m.py().get_type_bound::<InterfaceError>())?;
    m.add("DatabaseError", m.py().get_type_bound::<DatabaseError>())?;
    m.add("DataError", m.py().get_type_bound::<DataError>())?;
    m.add(
        "OperationalError",
        m.py().get_type_bound::<OperationalError>(),
    )?;
    m.add("IntegrityError", m.py().get_type_bound::<IntegrityError>())?;
    m.add("InternalError", m.py().get_type_bound::<InternalError>())?;
    m.add(
        "ProgrammingError",
        m.py().get_type_bound::<ProgrammingError>(),
    )?;
    m.add(
        "NotSupportedError",
        m.py().get_type_bound::<NotSupportedError>(),
    )?;
    Ok(())
}
