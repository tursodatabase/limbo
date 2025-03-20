use crate::translate::plan::Operation;
use crate::{
    bail_parse_error,
    schema::{Schema, Table},
    util::normalize_ident,
    vdbe::{
        builder::{CursorType, ProgramBuilder, ProgramBuilderOpts, QueryMode},
        insn::Insn,
    },
    SymbolTable,
};
use limbo_sqlite3_parser::ast::{self, Update};

use super::expr::translate_condition_expr;
use super::{emitter::Resolver, expr::translate_expr, plan::TableReference};

pub fn translate_update(
    query_mode: QueryMode,
    schema: &Schema,
    body: &Update,
    syms: &SymbolTable,
) -> crate::Result<ProgramBuilder> {
    // TODO: freestyling these numbers
    let mut program = ProgramBuilder::new(ProgramBuilderOpts {
        query_mode,
        num_cursors: 1,
        approx_num_insns: 20,
        approx_num_labels: 4,
    });

    if body.with.is_some() {
        bail_parse_error!("WITH clause is not supported");
    }
    if body.or_conflict.is_some() {
        bail_parse_error!("ON CONFLICT clause is not supported");
    }
    let table_name = &body.tbl_name.name;
    let table = match schema.get_table(table_name.0.as_str()) {
        Some(table) => table,
        None => bail_parse_error!("Parse error: no such table: {}", table_name),
    };
    if let Table::Virtual(_) = table.as_ref() {
        bail_parse_error!("vtable update not yet supported");
    }
    let resolver = Resolver::new(syms);

    let init_label = program.allocate_label();
    program.emit_insn(Insn::Init {
        target_pc: init_label,
    });
    let start_offset = program.offset();
    let Some(btree_table) = table.btree() else {
        crate::bail_corrupt_error!("Parse error: no such table: {}", table_name);
    };
    let cursor_id = program.alloc_cursor_id(
        Some(table_name.0.clone()),
        CursorType::BTreeTable(btree_table.clone()),
    );
    let root_page = btree_table.root_page;
    program.emit_insn(Insn::OpenWriteAsync {
        cursor_id,
        root_page,
    });
    program.emit_insn(Insn::OpenWriteAwait {});

    let end_label = program.allocate_label();
    program.emit_insn(Insn::RewindAsync { cursor_id });
    program.emit_insn(Insn::RewindAwait {
        cursor_id,
        pc_if_empty: end_label,
    });
    let first_col_reg = program.alloc_registers(btree_table.columns.len());
    let referenced_tables = vec![TableReference {
        table: Table::BTree(btree_table.clone()),
        identifier: table_name.0.clone(),
        op: Operation::Scan { iter_dir: None },
        join_info: None,
    }];

    // store the (col_index, Expr value) of each 'Set'
    // if a column declared here isn't found: error
    let mut update_idxs = Vec::with_capacity(body.sets.len());
    for s in body.sets.iter() {
        let ident = normalize_ident(s.col_names[0].0.as_str());
        if let Some((i, _)) = btree_table.columns.iter().enumerate().find(|(_, col)| {
            col.name
                .as_ref()
                .unwrap_or(&String::new())
                .eq_ignore_ascii_case(&ident)
        }) {
            update_idxs.push((i, &s.expr));
        } else {
            bail_parse_error!("column {} not found", ident);
        }
    }

    let loop_start = program.offset();
    let skip_label = program.allocate_label();
    if let Some(where_clause) = &body.where_clause {
        let expr = if let ast::Expr::Binary(lhs, op, rhs) = where_clause.as_ref() {
            // we don't support Expr::Id in translate_expr, so we rewrite to an Expr::Column
            if let ast::Expr::Id(col_name) = lhs.as_ref() {
                let Some((col_idx, col)) = btree_table.get_column(&col_name.0) else {
                    bail_parse_error!("column {} not found", col_name.0);
                };
                &ast::Expr::Binary(
                    Box::new(ast::Expr::Column {
                        table: 0, // one table in our [referenced_tables]
                        database: None,
                        column: col_idx,
                        is_rowid_alias: col.is_rowid_alias,
                    }),
                    *op,
                    rhs.clone(),
                )
            } else {
                where_clause
            }
        } else {
            where_clause
        };
        translate_condition_expr(
            &mut program,
            &referenced_tables,
            expr,
            super::expr::ConditionMetadata {
                jump_if_condition_is_true: false,
                jump_target_when_true: crate::vdbe::BranchOffset::Placeholder,
                jump_target_when_false: skip_label,
            },
            &resolver,
        )?;
    }
    let rowid_reg = program.alloc_register();
    program.emit_insn(Insn::RowId {
        cursor_id,
        dest: rowid_reg,
    });
    // if no rowid, we're done
    program.emit_insn(Insn::IsNull {
        reg: rowid_reg,
        target_pc: end_label,
    });

    // we scan a column at a time, loading either the column's values, or the new value
    // from the Set expression, into registers so we can emit a MakeRecord and update the row.
    for idx in 0..btree_table.columns.len() {
        if let Some((idx, expr)) = update_idxs.iter().find(|(i, _)| *i == idx) {
            let target_reg = first_col_reg + idx;
            translate_expr(&mut program, None, expr, target_reg, &resolver)?;
        } else {
            program.emit_insn(Insn::Column {
                cursor_id,
                column: idx,
                dest: first_col_reg + idx,
            });
        }
    }
    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: first_col_reg,
        count: btree_table.columns.len(),
        dest_reg: record_reg,
    });

    program.emit_insn(Insn::InsertAsync {
        cursor: cursor_id,
        key_reg: rowid_reg,
        record_reg,
        flag: 0,
    });
    program.emit_insn(Insn::InsertAwait { cursor_id });

    // label for false `WHERE` clause: proceed to next row
    program.resolve_label(skip_label, program.offset());
    program.emit_insn(Insn::NextAsync { cursor_id });
    program.emit_insn(Insn::NextAwait {
        cursor_id,
        pc_if_next: loop_start,
    });

    // cleanup/halt
    program.resolve_label(end_label, program.offset());
    program.emit_insn(Insn::Halt {
        err_code: 0,
        description: String::new(),
    });
    program.resolve_label(init_label, program.offset());
    program.emit_insn(Insn::Transaction { write: true });

    program.emit_constant_insns();
    program.emit_insn(Insn::Goto {
        target_pc: start_offset,
    });
    program.table_references = referenced_tables.clone();
    Ok(program)
}
