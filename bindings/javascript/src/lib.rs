#![deny(clippy::all)]

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use napi::{Env, JsObject, JsUnknown, Result as NapiResult};
use napi_derive::napi;

#[napi(js_name = "Database")]
pub struct Database {
    #[napi(writable = false)]
    pub memory: bool,
    _db: Arc<limbo_core::Database>,
    conn: Rc<limbo_core::Connection>,
}

#[napi]
impl Database {
    #[napi(constructor)]
    pub fn new(path: String) -> Self {
        let memory = path == ":memory:";
        let io: Arc<dyn limbo_core::IO> = if memory {
            Arc::new(limbo_core::MemoryIO::new())
        } else {
            Arc::new(IO {})
        };
        let file = io
            .open_file(&path, limbo_core::OpenFlags::Create, false)
            .unwrap();
        limbo_core::maybe_init_database_file(&file, &io).unwrap();
        let db_file = Arc::new(DatabaseFile::new(file));
        let db_header = limbo_core::Pager::begin_open(db_file.clone()).unwrap();

        // ensure db header is there
        io.run_once().unwrap();

        let page_size = db_header.lock().page_size;

        let wal_path = format!("{}-wal", path);
        let wal_shared =
            limbo_core::WalFileShared::open_shared(&io, wal_path.as_str(), page_size).unwrap();

        let db = limbo_core::Database::open(io, db_file, wal_shared, false).unwrap();
        let conn = db.connect().unwrap();
        Self {
            memory,
            _db: db,
            conn,
        }
    }

    #[napi]
    pub fn prepare(&self, sql: String) -> Statement {
        let stmt = self.conn.prepare(&sql).unwrap();
        Statement::new(RefCell::new(stmt))
    }
}

#[napi(js_name = "Statement")]
pub struct Statement {
    inner: RefCell<limbo_core::Statement>,
}

#[napi]
impl Statement {
    pub fn new(inner: RefCell<limbo_core::Statement>) -> Self {
        Self { inner }
    }

    #[napi]
    pub fn get(&self, env: Env) -> NapiResult<JsObject> {
        let mut stmt = self.inner.borrow_mut();
        match stmt.step() {
            Ok(limbo_core::StepResult::Row) => {
                let row = stmt.row().unwrap();
                let mut obj = env.create_object()?;

                for (idx, value) in row.get_values().iter().enumerate() {
                    let key = (idx + 1).to_string();
                    let js_value = to_js_value(&env, value);
                    obj.set_named_property(&key, js_value)?;
                }
                Ok(obj)
            }
            Ok(limbo_core::StepResult::IO)
            | Ok(limbo_core::StepResult::Done)
            | Ok(limbo_core::StepResult::Interrupt)
            | Ok(limbo_core::StepResult::Busy) => Ok(env.create_object()?),
            Err(e) => Err(napi::Error::from_reason(format!("Database error: {:?}", e))),
        }
    }
}

fn to_js_value(env: &napi::Env, value: &limbo_core::RefValue) -> JsUnknown {
    match value {
        limbo_core::RefValue::Null => env.get_null().unwrap().into_unknown(),
        limbo_core::RefValue::Integer(i) => env.create_int64(*i).unwrap().into_unknown(),
        limbo_core::RefValue::Float(f) => env.create_double(*f).unwrap().into_unknown(),
        limbo_core::RefValue::Text(s) => env.create_string(s.as_str()).unwrap().into_unknown(),
        limbo_core::RefValue::Blob(b) => {
            env.create_buffer_copy(b.to_slice()).unwrap().into_unknown()
        }
    }
}

struct DatabaseFile {
    file: Arc<dyn limbo_core::File>,
}

unsafe impl Send for DatabaseFile {}
unsafe impl Sync for DatabaseFile {}

impl DatabaseFile {
    pub fn new(file: Arc<dyn limbo_core::File>) -> Self {
        Self { file }
    }
}

impl limbo_core::DatabaseStorage for DatabaseFile {
    fn read_page(&self, page_idx: usize, c: limbo_core::Completion) -> limbo_core::Result<()> {
        let r = match c {
            limbo_core::Completion::Read(ref r) => r,
            _ => unreachable!(),
        };
        let size = r.buf().len();
        assert!(page_idx > 0);
        if !(512..=65536).contains(&size) || size & (size - 1) != 0 {
            return Err(limbo_core::LimboError::NotADB);
        }
        let pos = (page_idx - 1) * size;
        self.file.pread(pos, c)?;
        Ok(())
    }

    fn write_page(
        &self,
        page_idx: usize,
        buffer: Arc<std::cell::RefCell<limbo_core::Buffer>>,
        c: limbo_core::Completion,
    ) -> limbo_core::Result<()> {
        let size = buffer.borrow().len();
        let pos = (page_idx - 1) * size;
        self.file.pwrite(pos, buffer, c)?;
        Ok(())
    }

    fn sync(&self, _c: limbo_core::Completion) -> limbo_core::Result<()> {
        todo!()
    }
}

struct IO {}

impl limbo_core::IO for IO {
    fn open_file(
        &self,
        _path: &str,
        _flags: limbo_core::OpenFlags,
        _direct: bool,
    ) -> limbo_core::Result<Arc<dyn limbo_core::File>> {
        todo!();
    }

    fn run_once(&self) -> limbo_core::Result<()> {
        todo!();
    }

    fn generate_random_number(&self) -> i64 {
        todo!();
    }

    fn get_current_time(&self) -> String {
        todo!();
    }
}
