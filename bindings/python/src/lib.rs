use anyhow::Result;
use errors::*;
use limbo_core::types::Text;
use limbo_core::OwnedValue;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList, PyTuple};
use std::cell::RefCell;
use std::num::NonZeroUsize;
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

#[pyclass(unsendable)]
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

        Python::with_gil(|py| {
            if let Some(params) = parameters {
                let obj = params.into_bound(py);

                for (i, elem) in obj.iter().enumerate() {
                    let value = py_to_owned_value(&elem)?;
                    stmt.borrow_mut()
                        .bind_at(NonZeroUsize::new(i + 1).unwrap(), value);
                }
            }

            Ok::<(), anyhow::Error>(())
        })?;

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
                        let py_row = row_to_py(py, &row)?;
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
                        let py_row = row_to_py(py, &row)?;
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
        self.conn.close()?;

        Ok(())
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

#[pyclass(unsendable)]
#[derive(Clone)]
pub struct Connection {
    conn: Rc<limbo_core::Connection>,
    io: Arc<dyn limbo_core::IO>,
}

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

    pub fn close(&self) -> PyResult<()> {
        self.conn.close().map_err(|e| {
            PyErr::new::<OperationalError, _>(format!("Failed to close connection: {:?}", e))
        })?;

        Ok(())
    }

    pub fn commit(&self) -> PyResult<()> {
        if !self.conn.get_auto_commit() {
            self.conn.execute("COMMIT").map_err(|e| {
                PyErr::new::<OperationalError, _>(format!("Failed to commit: {:?}", e))
            })?;

            self.conn.execute("BEGIN").map_err(|e| {
                PyErr::new::<OperationalError, _>(format!("Failed to commit: {:?}", e))
            })?;
        }
        Ok(())
    }

    pub fn rollback(&self) -> PyResult<()> {
        Err(PyErr::new::<NotSupportedError, _>(
            "Transactions are not supported in this version",
        ))
    }

    fn __enter__(&self) -> PyResult<Self> {
        Ok(self.clone())
    }

    fn __exit__(
        &self,
        _exc_type: Option<&Bound<'_, PyAny>>,
        _exc_val: Option<&Bound<'_, PyAny>>,
        _exc_tb: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        self.close()
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.conn
            .close()
            .expect("Failed to drop (close) connection");
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
        limbo_core::Database::open_file(io, path, false).map_err(|e| {
            PyErr::new::<DatabaseError, _>(format!("Failed to open database: {:?}", e))
        })
    }

    match path {
        ":memory:" => {
            let io: Arc<dyn limbo_core::IO> = Arc::new(limbo_core::MemoryIO::new());
            let db = open_or(io.clone(), path)?;
            let conn: Rc<limbo_core::Connection> = db.connect().unwrap();
            Ok(Connection { conn, io })
        }
        path => {
            let io: Arc<dyn limbo_core::IO> = Arc::new(limbo_core::PlatformIO::new()?);
            let db = open_or(io.clone(), path)?;
            let conn: Rc<limbo_core::Connection> = db.connect().unwrap();
            Ok(Connection { conn, io })
        }
    }
}

fn row_to_py(py: Python, row: &limbo_core::Row) -> Result<PyObject> {
    let mut py_values = Vec::new();
    for value in row.get_values() {
        match value {
            limbo_core::OwnedValue::Null => py_values.push(py.None()),
            limbo_core::OwnedValue::Integer(i) => py_values.push(i.into_pyobject(py)?.into()),
            limbo_core::OwnedValue::Float(f) => py_values.push(f.into_pyobject(py)?.into()),
            limbo_core::OwnedValue::Text(s) => py_values.push(s.as_str().into_pyobject(py)?.into()),
            limbo_core::OwnedValue::Blob(b) => {
                py_values.push(PyBytes::new(py, b.as_slice()).into())
            }
        }
    }
    Ok(PyTuple::new(py, &py_values)
        .unwrap()
        .into_pyobject(py)?
        .into())
}

/// Converts a Python object to a Limbo OwnedValue
fn py_to_owned_value(obj: &Bound<PyAny>) -> Result<limbo_core::OwnedValue> {
    if obj.is_none() {
        return Ok(OwnedValue::Null);
    } else if let Ok(integer) = obj.extract::<i64>() {
        return Ok(OwnedValue::Integer(integer));
    } else if let Ok(float) = obj.extract::<f64>() {
        return Ok(OwnedValue::Float(float));
    } else if let Ok(string) = obj.extract::<String>() {
        return Ok(OwnedValue::Text(Text::from_str(string)));
    } else if let Ok(bytes) = obj.downcast::<PyBytes>() {
        return Ok(OwnedValue::Blob(bytes.as_bytes().to_vec()));
    } else {
        return Err(PyErr::new::<ProgrammingError, _>(format!(
            "Unsupported Python type: {}",
            obj.get_type().name()?
        ))
        .into());
    }
}

#[pymodule]
fn _limbo(m: &Bound<PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_class::<Connection>()?;
    m.add_class::<Cursor>()?;
    m.add_function(wrap_pyfunction!(connect, m)?)?;
    m.add("Warning", m.py().get_type::<Warning>())?;
    m.add("Error", m.py().get_type::<Error>())?;
    m.add("InterfaceError", m.py().get_type::<InterfaceError>())?;
    m.add("DatabaseError", m.py().get_type::<DatabaseError>())?;
    m.add("DataError", m.py().get_type::<DataError>())?;
    m.add("OperationalError", m.py().get_type::<OperationalError>())?;
    m.add("IntegrityError", m.py().get_type::<IntegrityError>())?;
    m.add("InternalError", m.py().get_type::<InternalError>())?;
    m.add("ProgrammingError", m.py().get_type::<ProgrammingError>())?;
    m.add("NotSupportedError", m.py().get_type::<NotSupportedError>())?;
    Ok(())
}
