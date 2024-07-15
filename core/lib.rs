mod btree;
mod buffer_pool;
mod function;
mod io;
mod pager;
mod schema;
mod sorter;
mod sqlite3_ondisk;
mod storage;
mod translate;
mod types;
mod util;
mod vdbe;

#[cfg(not(target_family = "wasm"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use anyhow::Result;
use fallible_iterator::FallibleIterator;
use log::trace;
use pager::Pager;
use schema::Schema;
use sqlite3_ondisk::DatabaseHeader;
use sqlite3_parser::{ast::Cmd, lexer::sql::Parser};
use std::{cell::RefCell, rc::Rc};

#[cfg(feature = "fs")]
pub use io::PlatformIO;
pub use io::{Buffer, Completion, File, WriteCompletion, IO};
pub use storage::{PageIO, PageSource};
pub use types::Value;

pub struct Database {
    pager: Rc<Pager>,
    schema: Rc<RefCell<Schema>>,
    header: Rc<RefCell<DatabaseHeader>>,
}

impl Database {
    #[cfg(feature = "fs")]
    pub fn open_file(io: Rc<dyn crate::io::IO>, path: &str) -> Result<Database> {
        let file = io.open_file(path)?;
        let storage = storage::PageSource::from_file(file);
        Self::open(io, storage)
    }

    pub fn open(io: Rc<dyn crate::io::IO>, page_source: PageSource) -> Result<Database> {
        let db_header = Pager::begin_open(&page_source)?;
        io.run_once()?;
        let pager = Rc::new(Pager::finish_open(
            db_header.clone(),
            page_source,
            io.clone(),
        )?);
        let bootstrap_schema = Rc::new(RefCell::new(Schema::new()));
        let conn = Connection {
            pager: pager.clone(),
            schema: bootstrap_schema.clone(),
            header: db_header.clone(),
        };
        let mut schema = Schema::new();
        let rows = conn.query("SELECT * FROM sqlite_schema")?;
        if let Some(mut rows) = rows {
            loop {
                match rows.next_row()? {
                    RowResult::Row(row) => {
                        let ty = row.get::<&str>(0)?;
                        if ty != "table" {
                            continue;
                        }
                        let root_page: i64 = row.get::<i64>(3)?;
                        let sql: &str = row.get::<&str>(4)?;
                        let table = schema::BTreeTable::from_sql(sql, root_page as usize)?;
                        schema.add_table(Rc::new(table));
                    }
                    RowResult::IO => {
                        // TODO: How do we ensure that the I/O we submitted to
                        // read the schema is actually complete?
                        io.run_once()?;
                    }
                    RowResult::Done => break,
                }
            }
        }
        let schema = Rc::new(RefCell::new(schema));
        let header = db_header;
        Ok(Database {
            pager,
            schema,
            header,
        })
    }

    pub fn connect(&self) -> Connection {
        Connection {
            pager: self.pager.clone(),
            schema: self.schema.clone(),
            header: self.header.clone(),
        }
    }
}

pub struct Connection {
    pager: Rc<Pager>,
    schema: Rc<RefCell<Schema>>,
    header: Rc<RefCell<DatabaseHeader>>,
}

impl Connection {
    pub fn prepare(&self, sql: impl Into<String>) -> Result<Statement> {
        let sql = sql.into();
        trace!("Preparing: {}", sql);
        let mut parser = Parser::new(sql.as_bytes());
        let cmd = parser.next()?;
        if let Some(cmd) = cmd {
            match cmd {
                Cmd::Stmt(stmt) => {
                    let program = Rc::new(translate::translate(
                        self.schema.clone(),
                        stmt,
                        self.header.clone(),
                        self.pager.clone(),
                    )?);
                    Ok(Statement::new(program, self.pager.clone()))
                }
                Cmd::Explain(_stmt) => todo!(),
                Cmd::ExplainQueryPlan(_stmt) => todo!(),
            }
        } else {
            todo!()
        }
    }

    pub fn query(&self, sql: impl Into<String>) -> Result<Option<Rows>> {
        let sql = sql.into();
        trace!("Querying: {}", sql);
        let mut parser = Parser::new(sql.as_bytes());
        let cmd = parser.next()?;
        if let Some(cmd) = cmd {
            match cmd {
                Cmd::Stmt(stmt) => {
                    let program = Rc::new(translate::translate(
                        self.schema.clone(),
                        stmt,
                        self.header.clone(),
                        self.pager.clone(),
                    )?);
                    let stmt = Statement::new(program, self.pager.clone());
                    Ok(Some(Rows { stmt }))
                }
                Cmd::Explain(stmt) => {
                    let program = translate::translate(
                        self.schema.clone(),
                        stmt,
                        self.header.clone(),
                        self.pager.clone(),
                    )?;
                    program.explain();
                    Ok(None)
                }
                Cmd::ExplainQueryPlan(_stmt) => Ok(None),
            }
        } else {
            Ok(None)
        }
    }

    pub fn execute(&self, sql: impl Into<String>) -> Result<()> {
        let sql = sql.into();
        let mut parser = Parser::new(sql.as_bytes());
        let cmd = parser.next()?;
        if let Some(cmd) = cmd {
            match cmd {
                Cmd::Explain(stmt) => {
                    let program = translate::translate(
                        self.schema.clone(),
                        stmt,
                        self.header.clone(),
                        self.pager.clone(),
                    )?;
                    program.explain();
                }
                Cmd::ExplainQueryPlan(_stmt) => todo!(),
                Cmd::Stmt(stmt) => {
                    let program = translate::translate(
                        self.schema.clone(),
                        stmt,
                        self.header.clone(),
                        self.pager.clone(),
                    )?;
                    let mut state = vdbe::ProgramState::new(program.max_registers);
                    program.step(&mut state, self.pager.clone())?;
                }
            }
        }
        Ok(())
    }
}

pub struct Statement {
    program: Rc<vdbe::Program>,
    state: vdbe::ProgramState,
    pager: Rc<Pager>,
}

impl Statement {
    pub fn new(program: Rc<vdbe::Program>, pager: Rc<Pager>) -> Self {
        let state = vdbe::ProgramState::new(program.max_registers);
        Self {
            program,
            state,
            pager,
        }
    }

    pub fn step(&mut self) -> Result<RowResult<'_>> {
        let result = self.program.step(&mut self.state, self.pager.clone())?;
        match result {
            vdbe::StepResult::Row(row) => Ok(RowResult::Row(Row { values: row.values })),
            vdbe::StepResult::IO => Ok(RowResult::IO),
            vdbe::StepResult::Done => Ok(RowResult::Done),
        }
    }

    pub fn query(&mut self) -> Result<Rows> {
        let stmt = Statement::new(self.program.clone(), self.pager.clone());
        Ok(Rows::new(stmt))
    }

    pub fn reset(&self) {}
}

pub enum RowResult<'a> {
    Row(Row<'a>),
    IO,
    Done,
}

pub struct Row<'a> {
    pub values: Vec<Value<'a>>,
}

impl<'a> Row<'a> {
    pub fn get<T: crate::types::FromValue<'a> + 'a>(&self, idx: usize) -> Result<T> {
        let value = &self.values[idx];
        T::from_value(value)
    }
}

pub struct Rows {
    stmt: Statement,
}

impl Rows {
    pub fn new(stmt: Statement) -> Self {
        Self { stmt }
    }

    pub fn next_row(&mut self) -> Result<RowResult<'_>> {
        self.stmt.step()
    }
}
