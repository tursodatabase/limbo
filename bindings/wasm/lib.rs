use js_sys::{Array, Object};
use limbo_core::{maybe_init_database_file, Clock, Instant, OpenFlags, Result};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use wasm_bindgen::prelude::*;
#[allow(dead_code)]
#[wasm_bindgen]
pub struct Database {
    db: Arc<limbo_core::Database>,
    conn: Rc<limbo_core::Connection>,
}

#[allow(clippy::arc_with_non_send_sync)]
#[wasm_bindgen]
impl Database {
    #[wasm_bindgen(constructor)]
    pub fn new(path: &str) -> Database {
        let io: Arc<dyn limbo_core::IO> = Arc::new(PlatformIO { vfs: VFS::new() });
        let file = io.open_file(path, OpenFlags::Create, false).unwrap();
        maybe_init_database_file(&file, &io).unwrap();
        let db_file = Arc::new(DatabaseFile::new(file));
        let db = limbo_core::Database::open(io, path, db_file, false).unwrap();
        let conn = db.connect().unwrap();
        Database { db, conn }
    }

    #[wasm_bindgen]
    pub fn exec(&self, _sql: &str) {
        self.conn.execute(_sql).unwrap();
    }

    #[wasm_bindgen]
    pub fn prepare(&self, _sql: &str) -> Statement {
        let stmt = self.conn.prepare(_sql).unwrap();
        Statement::new(RefCell::new(stmt), false)
    }
}

#[wasm_bindgen]
pub struct RowIterator {
    inner: RefCell<limbo_core::Statement>,
}

#[wasm_bindgen]
impl RowIterator {
    fn new(inner: RefCell<limbo_core::Statement>) -> Self {
        Self { inner }
    }

    #[wasm_bindgen]
    pub fn next(&mut self) -> JsValue {
        let mut stmt = self.inner.borrow_mut();
        match stmt.step() {
            Ok(limbo_core::StepResult::Row) => {
                let row = stmt.row().unwrap();
                let row_array = Array::new();
                for value in row.get_values() {
                    let value = to_js_value(value);
                    row_array.push(&value);
                }
                JsValue::from(row_array)
            }
            Ok(limbo_core::StepResult::IO) => JsValue::UNDEFINED,
            Ok(limbo_core::StepResult::Done) | Ok(limbo_core::StepResult::Interrupt) => {
                JsValue::UNDEFINED
            }
            Ok(limbo_core::StepResult::Busy) => JsValue::UNDEFINED,
            Err(e) => panic!("Error: {:?}", e),
        }
    }
}

#[wasm_bindgen]
pub struct Statement {
    inner: RefCell<limbo_core::Statement>,
    raw: bool,
}

#[wasm_bindgen]
impl Statement {
    fn new(inner: RefCell<limbo_core::Statement>, raw: bool) -> Self {
        Self { inner, raw }
    }

    #[wasm_bindgen]
    pub fn raw(mut self, toggle: Option<bool>) -> Self {
        self.raw = toggle.unwrap_or(true);
        self
    }

    pub fn get(&self) -> JsValue {
        let mut stmt = self.inner.borrow_mut();
        match stmt.step() {
            Ok(limbo_core::StepResult::Row) => {
                let row = stmt.row().unwrap();
                let row_array = js_sys::Array::new();
                for value in row.get_values() {
                    let value = to_js_value(value);
                    row_array.push(&value);
                }
                JsValue::from(row_array)
            }
            Ok(limbo_core::StepResult::IO)
            | Ok(limbo_core::StepResult::Done)
            | Ok(limbo_core::StepResult::Interrupt)
            | Ok(limbo_core::StepResult::Busy) => JsValue::UNDEFINED,
            Err(e) => panic!("Error: {:?}", e),
        }
    }

    pub fn all(&self) -> js_sys::Array {
        let array = js_sys::Array::new();
        loop {
            let mut stmt = self.inner.borrow_mut();
            match stmt.step() {
                Ok(limbo_core::StepResult::Row) => {
                    let row = stmt.row().unwrap();
                    let row_array = js_sys::Array::new();
                    for value in row.get_values() {
                        let value = to_js_value(value);
                        row_array.push(&value);
                    }
                    array.push(&row_array);
                }
                Ok(limbo_core::StepResult::IO) => {}
                Ok(limbo_core::StepResult::Interrupt) => break,
                Ok(limbo_core::StepResult::Done) => break,
                Ok(limbo_core::StepResult::Busy) => break,
                Err(e) => panic!("Error: {:?}", e),
            }
        }
        array
    }

    #[wasm_bindgen]
    pub fn iterate(self) -> JsValue {
        let iterator = RowIterator::new(self.inner);
        let iterator_obj = Object::new();

        // Define the next method that will be called by JavaScript
        let next_fn = js_sys::Function::new_with_args(
            "",
            "const value = this.iterator.next();
             const done = value === undefined;
             return {
                value,
                done
             };",
        );

        js_sys::Reflect::set(&iterator_obj, &JsValue::from_str("next"), &next_fn).unwrap();

        js_sys::Reflect::set(
            &iterator_obj,
            &JsValue::from_str("iterator"),
            &JsValue::from(iterator),
        )
        .unwrap();

        let symbol_iterator = js_sys::Function::new_no_args("return this;");
        js_sys::Reflect::set(&iterator_obj, &js_sys::Symbol::iterator(), &symbol_iterator).unwrap();

        JsValue::from(iterator_obj)
    }
}

fn to_js_value(value: &limbo_core::Value) -> JsValue {
    match value {
        limbo_core::Value::Null => JsValue::null(),
        limbo_core::Value::Integer(i) => {
            let i = *i;
            if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
                JsValue::from(i as i32)
            } else {
                JsValue::from(i)
            }
        }
        limbo_core::Value::Float(f) => JsValue::from(*f),
        limbo_core::Value::Text(t) => JsValue::from_str(t.as_str()),
        limbo_core::Value::Blob(b) => js_sys::Uint8Array::from(b.as_slice()).into(),
    }
}

pub struct File {
    vfs: VFS,
    fd: i32,
}

unsafe impl Send for File {}
unsafe impl Sync for File {}

#[allow(dead_code)]
impl File {
    fn new(vfs: VFS, fd: i32) -> Self {
        Self { vfs, fd }
    }
}

impl limbo_core::File for File {
    fn lock_file(&self, _exclusive: bool) -> Result<()> {
        // TODO
        Ok(())
    }

    fn unlock_file(&self) -> Result<()> {
        // TODO
        Ok(())
    }

    fn pread(&self, pos: usize, c: limbo_core::Completion) -> Result<()> {
        let r = match &c {
            limbo_core::Completion::Read(ref r) => r,
            _ => unreachable!(),
        };
        {
            let mut buf = r.buf_mut();
            let buf: &mut [u8] = buf.as_mut_slice();
            let nr = self.vfs.pread(self.fd, buf, pos);
            assert!(nr >= 0);
        }
        r.complete();
        Ok(())
    }

    fn pwrite(
        &self,
        pos: usize,
        buffer: Arc<std::cell::RefCell<limbo_core::Buffer>>,
        c: limbo_core::Completion,
    ) -> Result<()> {
        let w = match &c {
            limbo_core::Completion::Write(ref w) => w,
            _ => unreachable!(),
        };
        let buf = buffer.borrow();
        let buf: &[u8] = buf.as_slice();
        self.vfs.pwrite(self.fd, buf, pos);
        w.complete(buf.len() as i32);
        Ok(())
    }

    fn sync(&self, c: limbo_core::Completion) -> Result<()> {
        self.vfs.sync(self.fd);
        c.complete(0);
        Ok(())
    }

    fn size(&self) -> Result<u64> {
        Ok(self.vfs.size(self.fd))
    }
}

pub struct PlatformIO {
    vfs: VFS,
}
unsafe impl Send for PlatformIO {}
unsafe impl Sync for PlatformIO {}

impl Clock for PlatformIO {
    fn now(&self) -> Instant {
        let date = Date::new();
        let ms_since_epoch = date.getTime();

        Instant {
            secs: (ms_since_epoch / 1000.0) as i64,
            micros: ((ms_since_epoch % 1000.0) * 1000.0) as u32,
        }
    }
}

impl limbo_core::IO for PlatformIO {
    fn open_file(
        &self,
        path: &str,
        _flags: OpenFlags,
        _direct: bool,
    ) -> Result<Arc<dyn limbo_core::File>> {
        let fd = self.vfs.open(path, "a+");
        Ok(Arc::new(File {
            vfs: VFS::new(),
            fd,
        }))
    }

    fn run_once(&self) -> Result<()> {
        Ok(())
    }

    fn generate_random_number(&self) -> i64 {
        let random_f64 = Math_random();
        (random_f64 * i64::MAX as f64) as i64
    }

    fn get_memory_io(&self) -> Arc<limbo_core::MemoryIO> {
        Arc::new(limbo_core::MemoryIO::new())
    }
}

#[wasm_bindgen]
extern "C" {
    fn Math_random() -> f64;
}

#[wasm_bindgen]
extern "C" {
    type Date;

    #[wasm_bindgen(constructor)]
    fn new() -> Date;

    #[wasm_bindgen(method, getter)]
    fn toISOString(this: &Date) -> String;

    #[wasm_bindgen(method)]
    fn getTime(this: &Date) -> f64;
}

pub struct DatabaseFile {
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
    fn read_page(&self, page_idx: usize, c: limbo_core::Completion) -> Result<()> {
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
    ) -> Result<()> {
        let size = buffer.borrow().len();
        let pos = (page_idx - 1) * size;
        self.file.pwrite(pos, buffer, c)?;
        Ok(())
    }

    fn sync(&self, _c: limbo_core::Completion) -> Result<()> {
        todo!()
    }
}

#[cfg(all(feature = "web", feature = "nodejs"))]
compile_error!("Features 'web' and 'nodejs' cannot be enabled at the same time");

#[cfg(feature = "web")]
#[wasm_bindgen(module = "/web/src/web-vfs.js")]
extern "C" {
    type VFS;
    #[wasm_bindgen(constructor)]
    fn new() -> VFS;

    #[wasm_bindgen(method)]
    fn open(this: &VFS, path: &str, flags: &str) -> i32;

    #[wasm_bindgen(method)]
    fn close(this: &VFS, fd: i32) -> bool;

    #[wasm_bindgen(method)]
    fn pwrite(this: &VFS, fd: i32, buffer: &[u8], offset: usize) -> i32;

    #[wasm_bindgen(method)]
    fn pread(this: &VFS, fd: i32, buffer: &mut [u8], offset: usize) -> i32;

    #[wasm_bindgen(method)]
    fn size(this: &VFS, fd: i32) -> u64;

    #[wasm_bindgen(method)]
    fn sync(this: &VFS, fd: i32);
}

#[cfg(feature = "nodejs")]
#[wasm_bindgen(module = "/node/src/vfs.cjs")]
extern "C" {
    type VFS;
    #[wasm_bindgen(constructor)]
    fn new() -> VFS;

    #[wasm_bindgen(method)]
    fn open(this: &VFS, path: &str, flags: &str) -> i32;

    #[wasm_bindgen(method)]
    fn close(this: &VFS, fd: i32) -> bool;

    #[wasm_bindgen(method)]
    fn pwrite(this: &VFS, fd: i32, buffer: &[u8], offset: usize) -> i32;

    #[wasm_bindgen(method)]
    fn pread(this: &VFS, fd: i32, buffer: &mut [u8], offset: usize) -> i32;

    #[wasm_bindgen(method)]
    fn size(this: &VFS, fd: i32) -> u64;

    #[wasm_bindgen(method)]
    fn sync(this: &VFS, fd: i32);
}

#[wasm_bindgen(start)]
pub fn init() {
    console_error_panic_hook::set_once();
}
