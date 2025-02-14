//! VDBE bytecode generation for pragma statements.
//! More info: https://www.sqlite.org/pragma.html.

use limbo_sqlite3_parser::ast;
use limbo_sqlite3_parser::ast::PragmaName;
use std::cell::RefCell;
use std::rc::Rc;

use crate::schema::Schema;
use crate::storage::sqlite3_ondisk::{DatabaseHeader, MIN_PAGE_CACHE_SIZE};
use crate::storage::wal::CheckpointMode;
use crate::util::normalize_ident;
use crate::vdbe::builder::{ProgramBuilder, ProgramBuilderOpts, QueryMode};
use crate::vdbe::insn::{Cookie, Insn};
use crate::vdbe::BranchOffset;
use crate::{bail_parse_error, Pager};
use std::str::FromStr;
use strum::IntoEnumIterator;

fn list_pragmas(
    program: &mut ProgramBuilder,
    init_label: BranchOffset,
    start_offset: BranchOffset,
) {
    for x in PragmaName::iter() {
        let register = program.emit_string8_new_reg(x.to_string());
        program.emit_result_row(register, 1);
    }

    program.emit_halt();
    program.resolve_label(init_label, program.offset());
    program.emit_constant_insns();
    program.emit_goto(start_offset);
}

pub fn translate_pragma(
    query_mode: QueryMode,
    schema: &Schema,
    name: &ast::QualifiedName,
    body: Option<ast::PragmaBody>,
    database_header: Rc<RefCell<DatabaseHeader>>,
    pager: Rc<Pager>,
) -> crate::Result<ProgramBuilder> {
    let mut program = ProgramBuilder::new(ProgramBuilderOpts {
        query_mode,
        num_cursors: 0,
        approx_num_insns: 20,
        approx_num_labels: 0,
    });
    let init_label = program.emit_init();
    let start_offset = program.offset();
    let mut write = false;

    if name.name.0.to_lowercase() == "pragma_list" {
        list_pragmas(&mut program, init_label, start_offset);
        return Ok(program);
    }

    let pragma = match PragmaName::from_str(&name.name.0) {
        Ok(pragma) => pragma,
        Err(_) => bail_parse_error!("Not a valid pragma name"),
    };

    match body {
        None => {
            query_pragma(pragma, schema, None, database_header.clone(), &mut program)?;
        }
        Some(ast::PragmaBody::Equals(value)) => match pragma {
            PragmaName::TableInfo | PragmaName::TableList => {
                query_pragma(
                    pragma,
                    schema,
                    Some(value),
                    database_header.clone(),
                    &mut program,
                )?;
            }
            _ => {
                write = true;
                update_pragma(
                    pragma,
                    schema,
                    value,
                    database_header.clone(),
                    pager,
                    &mut program,
                )?;
            }
        },
        Some(ast::PragmaBody::Call(value)) => match pragma {
            PragmaName::TableInfo | PragmaName::TableList => {
                query_pragma(
                    pragma,
                    schema,
                    Some(value),
                    database_header.clone(),
                    &mut program,
                )?;
            }
            _ => {
                todo!()
            }
        },
    };
    program.emit_halt();
    program.resolve_label(init_label, program.offset());
    program.emit_transaction(write);
    program.emit_constant_insns();
    program.emit_goto(start_offset);

    Ok(program)
}

fn update_pragma(
    pragma: PragmaName,
    schema: &Schema,
    value: ast::Expr,
    header: Rc<RefCell<DatabaseHeader>>,
    pager: Rc<Pager>,
    program: &mut ProgramBuilder,
) -> crate::Result<()> {
    match pragma {
        PragmaName::CacheSize => {
            let cache_size = match value {
                ast::Expr::Literal(ast::Literal::Numeric(numeric_value)) => {
                    numeric_value.parse::<i64>()?
                }
                ast::Expr::Unary(ast::UnaryOperator::Negative, expr) => match *expr {
                    ast::Expr::Literal(ast::Literal::Numeric(numeric_value)) => {
                        -numeric_value.parse::<i64>()?
                    }
                    _ => bail_parse_error!("Not a valid value"),
                },
                _ => bail_parse_error!("Not a valid value"),
            };
            update_cache_size(cache_size, header, pager);
            Ok(())
        }
        PragmaName::JournalMode => {
            query_pragma(PragmaName::JournalMode, schema, None, header, program)?;
            Ok(())
        }
        PragmaName::WalCheckpoint => {
            query_pragma(PragmaName::WalCheckpoint, schema, None, header, program)?;
            Ok(())
        }
        PragmaName::PageCount => {
            query_pragma(PragmaName::PageCount, schema, None, header, program)?;
            Ok(())
        }
        PragmaName::UserVersion => {
            // TODO: Implement updating user_version
            todo!("updating user_version not yet implemented")
        }
        PragmaName::TableInfo | PragmaName::TableList => {
            // because we need control over the write parameter for the transaction,
            // this should be unreachable. We have to force-call query_pragma before
            // getting here
            unreachable!();
        }
    }
}

fn query_pragma(
    pragma: PragmaName,
    schema: &Schema,
    value: Option<ast::Expr>,
    database_header: Rc<RefCell<DatabaseHeader>>,
    program: &mut ProgramBuilder,
) -> crate::Result<()> {
    let register = program.alloc_register();
    match pragma {
        PragmaName::CacheSize => {
            program.emit_int(
                database_header.borrow().default_page_cache_size.into(),
                register,
            );
            program.emit_result_row(register, 1);
        }
        PragmaName::JournalMode => {
            program.emit_string8("wal".into(), register);
            program.emit_result_row(register, 1);
        }
        PragmaName::WalCheckpoint => {
            // Checkpoint uses 3 registers: P1, P2, P3. Ref Insn::Checkpoint for more info.
            // Allocate two more here as one was allocated at the top.
            program.alloc_register();
            program.alloc_register();
            program.emit_insn(Insn::Checkpoint {
                database: 0,
                checkpoint_mode: CheckpointMode::Passive,
                dest: register,
            });
            program.emit_result_row(register, 3);
        }
        PragmaName::PageCount => {
            program.emit_insn(Insn::PageCount {
                db: 0,
                dest: register,
            });
            program.emit_result_row(register, 1);
        }
        PragmaName::TableInfo => {
            let table = match value {
                Some(ast::Expr::Name(name)) => {
                    let tbl = normalize_ident(&name.0);
                    schema.get_table(&tbl)
                }
                _ => None,
            };

            let base_reg = register;
            program.alloc_register();
            program.alloc_register();
            program.alloc_register();
            program.alloc_register();
            program.alloc_register();
            if let Some(table) = table {
                for (i, column) in table.columns.iter().enumerate() {
                    // cid
                    program.emit_int(i as i64, base_reg);
                    // name
                    program.emit_string8(column.name.clone().unwrap_or_default(), base_reg + 1);

                    // type
                    program.emit_string8(column.ty_str.clone(), base_reg + 2);

                    // notnull
                    program.emit_bool(column.notnull, base_reg + 3);

                    // dflt_value
                    match &column.default {
                        None => {
                            program.emit_null(base_reg + 4);
                        }
                        Some(expr) => {
                            program.emit_string8(expr.to_string(), base_reg + 4);
                        }
                    }

                    // pk
                    program.emit_bool(column.primary_key, base_reg + 5);

                    program.emit_result_row(base_reg, 6);
                }
            }
        }
        PragmaName::TableList => {
            let table_name_filter = match value {
                Some(ast::Expr::Name(name)) => Some(normalize_ident(&name.0)),
                _ => None,
            };

            let base_reg = register;
            program.alloc_register();
            program.alloc_register();
            program.alloc_register();
            program.alloc_register();
            program.alloc_register();

            // SQLite just queries the schema table, and then ordering comes
            // from the sequence numbers that we have there. But for us, querying
            // something here is a harder, as we would need to have the IO context.
            //
            // So we keep a creation timestamp and iterate over that.
            let mut sorted_tables: Vec<_> = schema
                .tables
                .iter()
                .filter(|(tbl_name, _)| match &table_name_filter {
                    None => true,
                    Some(name) => *tbl_name == name,
                })
                .collect();

            sorted_tables.sort_by(|(_, a), (_, b)| b.created_at.cmp(&a.created_at));

            for (_, table) in sorted_tables {
                // schema - currently we only support "main", not "temp"
                program.emit_string8("main".to_string(), base_reg);
                // name
                program.emit_string8(table.name.clone(), base_reg + 1);
                // type - always "table" for now since we don't support views
                program.emit_string8("table".to_string(), base_reg + 2);
                // ncol - number of columns
                program.emit_int(table.columns.len() as i64, base_reg + 3);
                // wr - without rowid flag
                program.emit_int((!table.has_rowid) as i64, base_reg + 4);
                // strict - we don't track this yet, so always 0
                program.emit_int(0, base_reg + 5);
                program.emit_result_row(base_reg, 6);
            }
        }
        PragmaName::UserVersion => {
            program.emit_transaction(false);
            program.emit_insn(Insn::ReadCookie {
                db: 0,
                dest: register,
                cookie: Cookie::UserVersion,
            });
            program.emit_result_row(register, 1);
        }
    }

    Ok(())
}

fn update_cache_size(value: i64, header: Rc<RefCell<DatabaseHeader>>, pager: Rc<Pager>) {
    let mut cache_size_unformatted: i64 = value;
    let mut cache_size = if cache_size_unformatted < 0 {
        let kb = cache_size_unformatted.abs() * 1024;
        kb / 512 // assume 512 page size for now
    } else {
        value
    } as usize;

    if cache_size < MIN_PAGE_CACHE_SIZE {
        // update both in memory and stored disk value
        cache_size = MIN_PAGE_CACHE_SIZE;
        cache_size_unformatted = MIN_PAGE_CACHE_SIZE as i64;
    }

    // update in-memory header
    header.borrow_mut().default_page_cache_size = cache_size_unformatted
        .try_into()
        .unwrap_or_else(|_| panic!("invalid value, too big for a i32 {}", value));

    // update in disk
    let header_copy = header.borrow().clone();
    pager.write_database_header(&header_copy);

    // update cache size
    pager.change_page_cache_size(cache_size);
}
