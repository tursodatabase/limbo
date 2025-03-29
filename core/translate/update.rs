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
use limbo_sqlite3_parser::ast::Update;

use super::planner::bind_column_references;
use super::{
    emitter::Resolver,
    expr::{translate_condition_expr, translate_expr, ConditionMetadata},
    plan::TableReference,
};

/*
* Update is simple. By default we scan the table, and for each row, we check the WHERE
* clause. If it evaluates to true, we build the new record with the updated value and insert.
*
* EXAMPLE:
*
sqlite> explain update t set a = 100 where b = 5;
addr  opcode         p1    p2    p3    p4             p5  comment
----  -------------  ----  ----  ----  -------------  --  -------------
0     Init           0     16    0                    0   Start at 16
1     Null           0     1     2                    0   r[1..2]=NULL
2     Noop           1     0     1                    0
3     OpenWrite      0     2     0     3              0   root=2 iDb=0; t
4     Rewind         0     15    0                    0
5       Column         0     1     6                    0   r[6]= cursor 0 column 1
6       Ne             7     14    6     BINARY-8       81  if r[6]!=r[7] goto 14
7       Rowid          0     2     0                    0   r[2]= rowid of 0
8       IsNull         2     15    0                    0   if r[2]==NULL goto 15
9       Integer        100   3     0                    0   r[3]=100
10      Column         0     1     4                    0   r[4]= cursor 0 column 1
11      Column         0     2     5                    0   r[5]= cursor 0 column 2
12      MakeRecord     3     3     1                    0   r[1]=mkrec(r[3..5])
13      Insert         0     1     2     t              7   intkey=r[2] data=r[1]
14    Next           0     5     0                    1
15    Halt           0     0     0                    0
16    Transaction    0     1     1     0              1   usesStmtJournal=0
17    Integer        5     7     0                    0   r[7]=5
18    Goto           0     1     0                    0
*/
pub fn translate_update(
    query_mode: QueryMode,
    schema: &Schema,
    body: &mut Update,
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
        op: Operation::Scan {
            iter_dir: None,
            index: None,
        },
        join_info: None,
    }];

    // store the (col_index, Expr value) of each 'Set'
    // if a column declared here isn't found: error
    let mut update_idxs = Vec::with_capacity(body.sets.len());
    for s in body.sets.iter_mut() {
        let ident = normalize_ident(s.col_names[0].0.as_str());
        if let Some((i, _)) = btree_table.columns.iter().enumerate().find(|(_, col)| {
            col.name
                .as_ref()
                .unwrap_or(&String::new())
                .eq_ignore_ascii_case(&ident)
        }) {
            bind_column_references(&mut s.expr, &referenced_tables, None)?;
            update_idxs.push((i, &s.expr));
        } else {
            bail_parse_error!("column {} not found", ident);
        }
    }

    let loop_start = program.offset();
    let skip_label = program.allocate_label();
    if let Some(where_clause) = body.where_clause.as_mut() {
        bind_column_references(where_clause, &referenced_tables, None)?;
        translate_condition_expr(
            &mut program,
            &referenced_tables,
            where_clause,
            ConditionMetadata {
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
            translate_expr(
                &mut program,
                Some(&referenced_tables),
                expr,
                target_reg,
                &resolver,
            )?;
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
