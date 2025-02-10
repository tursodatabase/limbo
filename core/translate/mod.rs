//! The VDBE bytecode code generator.
//!
//! This module is responsible for translating the SQL AST into a sequence of
//! instructions for the VDBE. The VDBE is a register-based virtual machine that
//! executes bytecode instructions. This code generator is responsible for taking
//! the SQL AST and generating the corresponding VDBE instructions. For example,
//! a SELECT statement will be translated into a sequence of instructions that
//! will read rows from the database and filter them according to a WHERE clause.

pub(crate) mod aggregation;
pub(crate) mod delete;
pub(crate) mod emitter;
pub(crate) mod expr;
pub(crate) mod group_by;
pub(crate) mod insert;
pub(crate) mod main_loop;
pub(crate) mod optimizer;
pub(crate) mod order_by;
pub(crate) mod plan;
pub(crate) mod planner;
pub(crate) mod pragma;
pub(crate) mod result_row;
pub(crate) mod select;
pub(crate) mod subquery;

use crate::schema::Schema;
use crate::storage::pager::Pager;
use crate::storage::sqlite3_ondisk::DatabaseHeader;
use crate::translate::delete::translate_delete;
use crate::util::PRIMARY_KEY_AUTOMATIC_INDEX_NAME_PREFIX;
use crate::vdbe::builder::{CursorType, ProgramBuilderOpts, QueryMode};
use crate::vdbe::{builder::ProgramBuilder, insn::Insn, Program};
use crate::{bail_parse_error, Connection, LimboError, Result, SymbolTable};
use insert::translate_insert;
use select::translate_select;
use sqlite3_parser::ast::{self, fmt::ToTokens};
use sqlite3_parser::ast::{Delete, Insert};
use std::cell::RefCell;
use std::fmt::Display;
use std::rc::{Rc, Weak};

/// Translate SQL statement into bytecode program.
pub fn translate(
    schema: &Schema,
    stmt: ast::Stmt,
    database_header: Rc<RefCell<DatabaseHeader>>,
    pager: Rc<Pager>,
    connection: Weak<Connection>,
    syms: &SymbolTable,
    query_mode: QueryMode,
) -> Result<Program> {
    let mut change_cnt_on = false;

    let program = match stmt {
        ast::Stmt::AlterTable(_) => bail_parse_error!("ALTER TABLE not supported yet"),
        ast::Stmt::Analyze(_) => bail_parse_error!("ANALYZE not supported yet"),
        ast::Stmt::Attach { .. } => bail_parse_error!("ATTACH not supported yet"),
        ast::Stmt::Begin(_, _) => bail_parse_error!("BEGIN not supported yet"),
        ast::Stmt::Commit(_) => bail_parse_error!("COMMIT not supported yet"),
        ast::Stmt::CreateIndex { .. } => bail_parse_error!("CREATE INDEX not supported yet"),
        ast::Stmt::CreateTable {
            temporary,
            if_not_exists,
            tbl_name,
            body,
        } => {
            if temporary {
                bail_parse_error!("TEMPORARY table not supported yet");
            }

            translate_create_table(query_mode, tbl_name, *body, if_not_exists, schema)?
        }
        ast::Stmt::CreateTrigger { .. } => bail_parse_error!("CREATE TRIGGER not supported yet"),
        ast::Stmt::CreateView { .. } => bail_parse_error!("CREATE VIEW not supported yet"),
        ast::Stmt::CreateVirtualTable { .. } => {
            bail_parse_error!("CREATE VIRTUAL TABLE not supported yet")
        }
        ast::Stmt::Delete(delete) => {
            let Delete {
                tbl_name,
                where_clause,
                limit,
                ..
            } = *delete;
            change_cnt_on = true;
            translate_delete(query_mode, schema, &tbl_name, where_clause, limit, syms)?
        }
        ast::Stmt::Detach(_) => bail_parse_error!("DETACH not supported yet"),
        ast::Stmt::DropIndex { .. } => bail_parse_error!("DROP INDEX not supported yet"),
        ast::Stmt::DropTable { .. } => bail_parse_error!("DROP TABLE not supported yet"),
        ast::Stmt::DropTrigger { .. } => bail_parse_error!("DROP TRIGGER not supported yet"),
        ast::Stmt::DropView { .. } => bail_parse_error!("DROP VIEW not supported yet"),
        ast::Stmt::Pragma(name, body) => pragma::translate_pragma(
            query_mode,
            &schema,
            &name,
            body.map(|b| *b),
            database_header.clone(),
            pager,
        )?,
        ast::Stmt::Reindex { .. } => bail_parse_error!("REINDEX not supported yet"),
        ast::Stmt::Release(_) => bail_parse_error!("RELEASE not supported yet"),
        ast::Stmt::Rollback { .. } => bail_parse_error!("ROLLBACK not supported yet"),
        ast::Stmt::Savepoint(_) => bail_parse_error!("SAVEPOINT not supported yet"),
        ast::Stmt::Select(select) => translate_select(query_mode, schema, *select, syms)?,
        ast::Stmt::Update { .. } => bail_parse_error!("UPDATE not supported yet"),
        ast::Stmt::Vacuum(_, _) => bail_parse_error!("VACUUM not supported yet"),
        ast::Stmt::Insert(insert) => {
            let Insert {
                with,
                or_conflict,
                tbl_name,
                columns,
                body,
                returning,
            } = *insert;
            change_cnt_on = true;
            translate_insert(
                query_mode,
                schema,
                &with,
                &or_conflict,
                &tbl_name,
                &columns,
                &body,
                &returning,
                syms,
            )?
        }
    };

    Ok(program.build(database_header, connection, change_cnt_on))
}

/* Example:

sqlite> EXPLAIN CREATE TABLE users (id INT, email TEXT);;
addr  opcode         p1    p2    p3    p4             p5  comment
----  -------------  ----  ----  ----  -------------  --  -------------
0     Init           0     30    0                    0   Start at 30
1     ReadCookie     0     3     2                    0
2     If             3     5     0                    0
3     SetCookie      0     2     4                    0
4     SetCookie      0     5     1                    0
5     CreateBtree    0     2     1                    0   r[2]=root iDb=0 flags=1
6     OpenWrite      0     1     0     5              0   root=1 iDb=0
7     NewRowid       0     1     0                    0   r[1]=rowid
8     Blob           6     3     0                   0   r[3]= (len=6)
9     Insert         0     3     1                    8   intkey=r[1] data=r[3]
10    Close          0     0     0                    0
11    Close          0     0     0                    0
12    Null           0     4     5                    0   r[4..5]=NULL
13    Noop           2     0     4                    0
14    OpenWrite      1     1     0     5              0   root=1 iDb=0; sqlite_master
15    SeekRowid      1     17    1                    0   intkey=r[1]
16    Rowid          1     5     0                    0   r[5]= rowid of 1
17    IsNull         5     26    0                    0   if r[5]==NULL goto 26
18    String8        0     6     0     table          0   r[6]='table'
19    String8        0     7     0     users          0   r[7]='users'
20    String8        0     8     0     users          0   r[8]='users'
21    Copy           2     9     0                    0   r[9]=r[2]
22    String8        0     10    0     CREATE TABLE users (id INT, email TEXT) 0   r[10]='CREATE TABLE users (id INT, email TEXT)'
23    MakeRecord     6     5     4     BBBDB          0   r[4]=mkrec(r[6..10])
24    Delete         1     68    5                    0
25    Insert         1     4     5                    0   intkey=r[5] data=r[4]
26    SetCookie      0     1     1                    0
27    ParseSchema    0     0     0     tbl_name='users' AND type!='trigger' 0
28    SqlExec        1     0     0     PRAGMA "main".integrity_check('users') 0
29    Halt           0     0     0                    0
30    Transaction    0     1     0     0              1   usesStmtJournal=1
31    Goto           0     1     0                    0

*/
#[derive(Debug)]
enum SchemaEntryType {
    Table,
    Index,
}

impl SchemaEntryType {
    fn as_str(&self) -> &'static str {
        match self {
            SchemaEntryType::Table => "table",
            SchemaEntryType::Index => "index",
        }
    }
}

fn emit_schema_entry(
    program: &mut ProgramBuilder,
    sqlite_schema_cursor_id: usize,
    entry_type: SchemaEntryType,
    name: &str,
    tbl_name: &str,
    root_page_reg: usize,
    sql: Option<String>,
) {
    let rowid_reg = program.alloc_register();
    program.emit_insn(Insn::NewRowid {
        cursor: sqlite_schema_cursor_id,
        rowid_reg,
        prev_largest_reg: 0,
    });

    let type_reg = program.emit_string8_new_reg(entry_type.as_str().to_string());
    program.emit_string8_new_reg(name.to_string());
    program.emit_string8_new_reg(tbl_name.to_string());

    let rootpage_reg = program.alloc_register();
    program.emit_insn(Insn::Copy {
        src_reg: root_page_reg,
        dst_reg: rootpage_reg,
        amount: 1,
    });

    let sql_reg = program.alloc_register();
    if let Some(sql) = sql {
        program.emit_string8(sql, sql_reg);
    } else {
        program.emit_null(sql_reg);
    }

    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: type_reg,
        count: 5,
        dest_reg: record_reg,
    });

    program.emit_insn(Insn::InsertAsync {
        cursor: sqlite_schema_cursor_id,
        key_reg: rowid_reg,
        record_reg,
        flag: 0,
    });
    program.emit_insn(Insn::InsertAwait {
        cursor_id: sqlite_schema_cursor_id,
    });
}

struct PrimaryKeyColumnInfo<'a> {
    name: &'a String,
    is_descending: bool,
}

/// Check if an automatic PRIMARY KEY index is required for the table.
/// If so, create a register for the index root page and return it.
///
/// An automatic PRIMARY KEY index is not required if:
/// - The table has no PRIMARY KEY
/// - The table has a single-column PRIMARY KEY whose typename is _exactly_ "INTEGER" e.g. not "INT".
///   In this case, the PRIMARY KEY column becomes an alias for the rowid.
///
/// Otherwise, an automatic PRIMARY KEY index is required.
fn check_automatic_pk_index_required(
    body: &ast::CreateTableBody,
    program: &mut ProgramBuilder,
    tbl_name: &str,
) -> Result<Option<usize>> {
    match body {
        ast::CreateTableBody::ColumnsAndConstraints {
            columns,
            constraints,
            options,
        } => {
            let mut primary_key_definition = None;

            // Check table constraints for PRIMARY KEY
            if let Some(constraints) = constraints {
                for constraint in constraints {
                    if let ast::TableConstraint::PrimaryKey {
                        columns: pk_cols, ..
                    } = &constraint.constraint
                    {
                        let primary_key_column_results: Vec<Result<PrimaryKeyColumnInfo>> = pk_cols
                            .iter()
                            .map(|col| match &col.expr {
                                ast::Expr::Id(name) => Ok(PrimaryKeyColumnInfo {
                                    name: &name.0,
                                    is_descending: matches!(col.order, Some(ast::SortOrder::Desc)),
                                }),
                                _ => Err(LimboError::ParseError(
                                    "expressions prohibited in PRIMARY KEY and UNIQUE constraints"
                                        .to_string(),
                                )),
                            })
                            .collect();

                        for result in primary_key_column_results {
                            if let Err(e) = result {
                                bail_parse_error!("{}", e);
                            }
                            let pk_info = result?;

                            let column_name = pk_info.name;
                            let column_def = columns.get(&ast::Name(column_name.clone()));
                            if column_def.is_none() {
                                bail_parse_error!("No such column: {}", column_name);
                            }

                            if matches!(
                                primary_key_definition,
                                Some(PrimaryKeyDefinitionType::Simple { .. })
                            ) {
                                primary_key_definition = Some(PrimaryKeyDefinitionType::Composite);
                                continue;
                            }
                            if primary_key_definition.is_none() {
                                let column_def = column_def.unwrap();
                                let typename =
                                    column_def.col_type.as_ref().map(|t| t.name.as_str());
                                let is_descending = pk_info.is_descending;
                                primary_key_definition = Some(PrimaryKeyDefinitionType::Simple {
                                    typename,
                                    is_descending,
                                });
                            }
                        }
                    }
                }
            }

            // Check column constraints for PRIMARY KEY
            for (_, col_def) in columns.iter() {
                for constraint in &col_def.constraints {
                    if matches!(
                        constraint.constraint,
                        ast::ColumnConstraint::PrimaryKey { .. }
                    ) {
                        if primary_key_definition.is_some() {
                            bail_parse_error!("table {} has more than one primary key", tbl_name);
                        }
                        let typename = col_def.col_type.as_ref().map(|t| t.name.as_str());
                        primary_key_definition = Some(PrimaryKeyDefinitionType::Simple {
                            typename,
                            is_descending: false,
                        });
                    }
                }
            }

            // Check if table has rowid
            if options.contains(ast::TableOptions::WITHOUT_ROWID) {
                bail_parse_error!("WITHOUT ROWID tables are not supported yet");
            }

            // Check if we need an automatic index
            let needs_auto_index = if let Some(primary_key_definition) = &primary_key_definition {
                match primary_key_definition {
                    PrimaryKeyDefinitionType::Simple {
                        typename,
                        is_descending,
                    } => {
                        let is_integer =
                            typename.is_some() && typename.unwrap().to_uppercase() == "INTEGER";
                        !is_integer || *is_descending
                    }
                    PrimaryKeyDefinitionType::Composite => true,
                }
            } else {
                false
            };

            if needs_auto_index {
                let index_root_reg = program.alloc_register();
                Ok(Some(index_root_reg))
            } else {
                Ok(None)
            }
        }
        ast::CreateTableBody::AsSelect(_) => {
            bail_parse_error!("CREATE TABLE AS SELECT not supported yet")
        }
    }
}

fn translate_create_table(
    query_mode: QueryMode,
    tbl_name: ast::QualifiedName,
    body: ast::CreateTableBody,
    if_not_exists: bool,
    schema: &Schema,
) -> Result<ProgramBuilder> {
    let mut program = ProgramBuilder::new(ProgramBuilderOpts {
        query_mode,
        num_cursors: 1,
        approx_num_insns: 30,
        approx_num_labels: 1,
    });
    if schema.get_table(tbl_name.name.0.as_str()).is_some() {
        if if_not_exists {
            let init_label = program.emit_init();
            let start_offset = program.offset();
            program.emit_halt();
            program.resolve_label(init_label, program.offset());
            program.emit_transaction(true);
            program.emit_constant_insns();
            program.emit_goto(start_offset);

            return Ok(program);
        }
        bail_parse_error!("Table {} already exists", tbl_name);
    }

    let sql = create_table_body_to_str(&tbl_name, &body);

    let parse_schema_label = program.allocate_label();
    let init_label = program.emit_init();
    let start_offset = program.offset();
    // TODO: ReadCookie
    // TODO: If
    // TODO: SetCookie
    // TODO: SetCookie

    // Create the table B-tree
    let table_root_reg = program.alloc_register();
    program.emit_insn(Insn::CreateBtree {
        db: 0,
        root: table_root_reg,
        flags: 1, // Table leaf page
    });

    // Create an automatic index B-tree if needed
    //
    // NOTE: we are deviating from SQLite bytecode here. For some reason, SQLite first creates a placeholder entry
    // for the table in sqlite_schema, then writes the index to sqlite_schema, then UPDATEs the table placeholder entry
    // in sqlite_schema with actual data.
    //
    // What we do instead is:
    // 1. Create the table B-tree
    // 2. Create the index B-tree
    // 3. Add the table entry to sqlite_schema
    // 4. Add the index entry to sqlite_schema
    //
    // I.e. we skip the weird song and dance with the placeholder entry. Unclear why sqlite does this.
    // The sqlite code has this comment:
    //
    // "This just creates a place-holder record in the sqlite_schema table.
    // The record created does not contain anything yet.  It will be replaced
    // by the real entry in code generated at sqlite3EndTable()."
    //
    // References:
    // https://github.com/sqlite/sqlite/blob/95f6df5b8d55e67d1e34d2bff217305a2f21b1fb/src/build.c#L1355
    // https://github.com/sqlite/sqlite/blob/95f6df5b8d55e67d1e34d2bff217305a2f21b1fb/src/build.c#L2856-L2871
    // https://github.com/sqlite/sqlite/blob/95f6df5b8d55e67d1e34d2bff217305a2f21b1fb/src/build.c#L1334C5-L1336C65

    let index_root_reg = check_automatic_pk_index_required(&body, &mut program, &tbl_name.name.0)?;
    if let Some(index_root_reg) = index_root_reg {
        program.emit_insn(Insn::CreateBtree {
            db: 0,
            root: index_root_reg,
            flags: 2, // Index leaf page
        });
    }

    let table_id = "sqlite_schema".to_string();
    let table = schema.get_table(&table_id).unwrap();
    let sqlite_schema_cursor_id = program.alloc_cursor_id(
        Some(table_id.to_owned()),
        CursorType::BTreeTable(table.clone()),
    );
    program.emit_insn(Insn::OpenWriteAsync {
        cursor_id: sqlite_schema_cursor_id,
        root_page: 1,
    });
    program.emit_insn(Insn::OpenWriteAwait {});

    // Add the table entry to sqlite_schema
    emit_schema_entry(
        &mut program,
        sqlite_schema_cursor_id,
        SchemaEntryType::Table,
        &tbl_name.name.0,
        &tbl_name.name.0,
        table_root_reg,
        Some(sql),
    );

    // If we need an automatic index, add its entry to sqlite_schema
    if let Some(index_root_reg) = index_root_reg {
        let index_name = format!(
            "{}{}_1",
            PRIMARY_KEY_AUTOMATIC_INDEX_NAME_PREFIX, tbl_name.name.0
        );
        emit_schema_entry(
            &mut program,
            sqlite_schema_cursor_id,
            SchemaEntryType::Index,
            &index_name,
            &tbl_name.name.0,
            index_root_reg,
            None,
        );
    }

    program.resolve_label(parse_schema_label, program.offset());
    // TODO: SetCookie
    //
    // TODO: remove format, it sucks for performance but is convenient
    let parse_schema_where_clause = format!("tbl_name = '{}' AND type != 'trigger'", tbl_name);
    program.emit_insn(Insn::ParseSchema {
        db: sqlite_schema_cursor_id,
        where_clause: parse_schema_where_clause,
    });

    // TODO: SqlExec
    program.emit_halt();
    program.resolve_label(init_label, program.offset());
    program.emit_transaction(true);
    program.emit_constant_insns();
    program.emit_goto(start_offset);

    Ok(program)
}

enum PrimaryKeyDefinitionType<'a> {
    Simple {
        typename: Option<&'a str>,
        is_descending: bool,
    },
    Composite,
}

struct TableFormatter<'a> {
    body: &'a ast::CreateTableBody,
}
impl Display for TableFormatter<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.body.to_fmt(f)
    }
}

fn create_table_body_to_str(tbl_name: &ast::QualifiedName, body: &ast::CreateTableBody) -> String {
    let mut sql = String::new();
    let formatter = TableFormatter { body };
    sql.push_str(format!("CREATE TABLE {} {}", tbl_name.name.0, formatter).as_str());
    match body {
        ast::CreateTableBody::ColumnsAndConstraints {
            columns: _,
            constraints: _,
            options: _,
        } => {}
        ast::CreateTableBody::AsSelect(_select) => todo!("as select not yet supported"),
    }
    sql
}
