use limbo_ext::{OrderByInfo, VTabKind};
use limbo_sqlite3_parser::ast;

use crate::{
    schema::Table,
    translate::result_row::emit_select_result,
    vdbe::{
        builder::{CursorType, ProgramBuilder},
        insn::{CmpInsFlags, Insn},
        BranchOffset,
    },
    Result,
};

use super::{
    aggregation::translate_aggregation_step,
    emitter::{OperationMode, TranslateCtx},
    expr::{translate_condition_expr, translate_expr, ConditionMetadata},
    group_by::is_column_in_group_by,
    order_by::{order_by_sorter_insert, sorter_insert},
    plan::{
        try_convert_to_constraint_info, IterationDirection, Operation, Search, SelectPlan,
        SelectQueryType, TableReference, WhereTerm,
    },
};

// Metadata for handling LEFT JOIN operations
#[derive(Debug)]
pub struct LeftJoinMetadata {
    // integer register that holds a flag that is set to true if the current row has a match for the left join
    pub reg_match_flag: usize,
    // label for the instruction that sets the match flag to true
    pub label_match_flag_set_true: BranchOffset,
    // label for the instruction that checks if the match flag is true
    pub label_match_flag_check_value: BranchOffset,
}

/// Jump labels for each loop in the query's main execution loop
#[derive(Debug, Clone, Copy)]
pub struct LoopLabels {
    /// jump to the start of the loop body
    pub loop_start: BranchOffset,
    /// jump to the NextAsync instruction (or equivalent)
    pub next: BranchOffset,
    /// jump to the end of the loop, exiting it
    pub loop_end: BranchOffset,
}

impl LoopLabels {
    pub fn new(program: &mut ProgramBuilder) -> Self {
        Self {
            loop_start: program.allocate_label(),
            next: program.allocate_label(),
            loop_end: program.allocate_label(),
        }
    }
}

/// Initialize resources needed for the source operators (tables, joins, etc)
pub fn init_loop(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    tables: &[TableReference],
    mode: OperationMode,
) -> Result<()> {
    assert!(
        t_ctx.meta_left_joins.len() == tables.len(),
        "meta_left_joins length does not match tables length"
    );
    for (table_index, table) in tables.iter().enumerate() {
        // Initialize bookkeeping for OUTER JOIN
        if let Some(join_info) = table.join_info.as_ref() {
            if join_info.outer {
                let lj_metadata = LeftJoinMetadata {
                    reg_match_flag: program.alloc_register(),
                    label_match_flag_set_true: program.allocate_label(),
                    label_match_flag_check_value: program.allocate_label(),
                };
                t_ctx.meta_left_joins[table_index] = Some(lj_metadata);
            }
        }
        match &table.op {
            Operation::Scan { index, .. } => {
                let cursor_id = program.alloc_cursor_id(
                    Some(table.identifier.clone()),
                    match &table.table {
                        Table::BTree(_) => CursorType::BTreeTable(table.btree().unwrap().clone()),
                        Table::Virtual(_) => {
                            CursorType::VirtualTable(table.virtual_table().unwrap().clone())
                        }
                        other => panic!("Invalid table reference type in Scan: {:?}", other),
                    },
                );
                let index_cursor_id = index.as_ref().map(|i| {
                    program.alloc_cursor_id(Some(i.name.clone()), CursorType::BTreeIndex(i.clone()))
                });
                match (mode, &table.table) {
                    (OperationMode::SELECT, Table::BTree(btree)) => {
                        let root_page = btree.root_page;
                        program.emit_insn(Insn::OpenReadAsync {
                            cursor_id,
                            root_page,
                        });
                        program.emit_insn(Insn::OpenReadAwait {});
                        if let Some(index_cursor_id) = index_cursor_id {
                            program.emit_insn(Insn::OpenReadAsync {
                                cursor_id: index_cursor_id,
                                root_page: index.as_ref().unwrap().root_page,
                            });
                            program.emit_insn(Insn::OpenReadAwait {});
                        }
                    }
                    (OperationMode::DELETE, Table::BTree(btree)) => {
                        let root_page = btree.root_page;
                        program.emit_insn(Insn::OpenWriteAsync {
                            cursor_id,
                            root_page: root_page.into(),
                        });
                        program.emit_insn(Insn::OpenWriteAwait {});
                    }
                    (OperationMode::UPDATE, Table::BTree(btree)) => {
                        let root_page = btree.root_page;
                        program.emit_insn(Insn::OpenWriteAsync {
                            cursor_id,
                            root_page: root_page.into(),
                        });
                        if let Some(index_cursor_id) = index_cursor_id {
                            program.emit_insn(Insn::OpenWriteAsync {
                                cursor_id: index_cursor_id,
                                root_page: index.as_ref().unwrap().root_page.into(),
                            });
                        }
                        program.emit_insn(Insn::OpenWriteAwait {});
                    }
                    (_, Table::Virtual(_)) => {
                        program.emit_insn(Insn::VOpenAsync { cursor_id });
                        program.emit_insn(Insn::VOpenAwait {});
                    }
                    _ => {
                        unimplemented!()
                    }
                }
            }
            Operation::Search(search) => {
                let table_cursor_id = program.alloc_cursor_id(
                    Some(table.identifier.clone()),
                    CursorType::BTreeTable(table.btree().unwrap().clone()),
                );

                match mode {
                    OperationMode::SELECT => {
                        program.emit_insn(Insn::OpenReadAsync {
                            cursor_id: table_cursor_id,
                            root_page: table.table.get_root_page(),
                        });
                        program.emit_insn(Insn::OpenReadAwait {});
                    }
                    OperationMode::DELETE | OperationMode::UPDATE => {
                        program.emit_insn(Insn::OpenWriteAsync {
                            cursor_id: table_cursor_id,
                            root_page: table.table.get_root_page().into(),
                        });
                        program.emit_insn(Insn::OpenWriteAwait {});
                    }
                    _ => {
                        unimplemented!()
                    }
                }

                if let Search::IndexSearch { index, .. } = search {
                    let index_cursor_id = program.alloc_cursor_id(
                        Some(index.name.clone()),
                        CursorType::BTreeIndex(index.clone()),
                    );

                    match mode {
                        OperationMode::SELECT => {
                            program.emit_insn(Insn::OpenReadAsync {
                                cursor_id: index_cursor_id,
                                root_page: index.root_page,
                            });
                            program.emit_insn(Insn::OpenReadAwait);
                        }
                        OperationMode::UPDATE | OperationMode::DELETE => {
                            program.emit_insn(Insn::OpenWriteAsync {
                                cursor_id: index_cursor_id,
                                root_page: index.root_page.into(),
                            });
                            program.emit_insn(Insn::OpenWriteAwait {});
                        }
                        _ => {
                            unimplemented!()
                        }
                    }
                }
            }
            _ => {}
        }
    }

    Ok(())
}

/// Set up the main query execution loop
/// For example in the case of a nested table scan, this means emitting the RewindAsync instruction
/// for all tables involved, outermost first.
pub fn open_loop(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    tables: &[TableReference],
    predicates: &[WhereTerm],
) -> Result<()> {
    for (table_index, table) in tables.iter().enumerate() {
        let LoopLabels {
            loop_start,
            loop_end,
            next,
        } = *t_ctx
            .labels_main_loop
            .get(table_index)
            .expect("table has no loop labels");

        // Each OUTER JOIN has a "match flag" that is initially set to false,
        // and is set to true when a match is found for the OUTER JOIN.
        // This is used to determine whether to emit actual columns or NULLs for the columns of the right table.
        if let Some(join_info) = table.join_info.as_ref() {
            if join_info.outer {
                let lj_meta = t_ctx.meta_left_joins[table_index].as_ref().unwrap();
                program.emit_insn(Insn::Integer {
                    value: 0,
                    dest: lj_meta.reg_match_flag,
                });
            }
        }

        match &table.op {
            Operation::Subquery { plan, .. } => {
                let (yield_reg, coroutine_implementation_start) = match &plan.query_type {
                    SelectQueryType::Subquery {
                        yield_reg,
                        coroutine_implementation_start,
                    } => (*yield_reg, *coroutine_implementation_start),
                    _ => unreachable!("Subquery operator with non-subquery query type"),
                };
                // In case the subquery is an inner loop, it needs to be reinitialized on each iteration of the outer loop.
                program.emit_insn(Insn::InitCoroutine {
                    yield_reg,
                    jump_on_definition: BranchOffset::Offset(0),
                    start_offset: coroutine_implementation_start,
                });
                program.resolve_label(loop_start, program.offset());
                // A subquery within the main loop of a parent query has no cursor, so instead of advancing the cursor,
                // it emits a Yield which jumps back to the main loop of the subquery itself to retrieve the next row.
                // When the subquery coroutine completes, this instruction jumps to the label at the top of the termination_label_stack,
                // which in this case is the end of the Yield-Goto loop in the parent query.
                program.emit_insn(Insn::Yield {
                    yield_reg,
                    end_offset: loop_end,
                });

                for cond in predicates
                    .iter()
                    .filter(|cond| cond.should_eval_at_loop(table_index))
                {
                    let jump_target_when_true = program.allocate_label();
                    let condition_metadata = ConditionMetadata {
                        jump_if_condition_is_true: false,
                        jump_target_when_true,
                        jump_target_when_false: next,
                    };
                    translate_condition_expr(
                        program,
                        tables,
                        &cond.expr,
                        condition_metadata,
                        &t_ctx.resolver,
                    )?;
                    program.resolve_label(jump_target_when_true, program.offset());
                }
            }
            Operation::Scan { iter_dir, index } => {
                let cursor_id = program.resolve_cursor_id(&table.identifier);
                let index_cursor_id = index.as_ref().map(|i| program.resolve_cursor_id(&i.name));
                let iteration_cursor_id = index_cursor_id.unwrap_or(cursor_id);
                if !matches!(&table.table, Table::Virtual(_)) {
                    if *iter_dir == IterationDirection::Backwards {
                        program.emit_insn(Insn::LastAsync {
                            cursor_id: iteration_cursor_id,
                        });
                    } else {
                        program.emit_insn(Insn::RewindAsync {
                            cursor_id: iteration_cursor_id,
                        });
                    }
                }
                match &table.table {
                    Table::BTree(_) => {
                        program.emit_insn(if *iter_dir == IterationDirection::Backwards {
                            Insn::LastAwait {
                                cursor_id: iteration_cursor_id,
                                pc_if_empty: loop_end,
                            }
                        } else {
                            Insn::RewindAwait {
                                cursor_id: iteration_cursor_id,
                                pc_if_empty: loop_end,
                            }
                        });
                    }
                    Table::Virtual(ref vtab) => {
                        // Virtual tables may be used either as VTab or TVF, distinguished by vtab.name.
                        let (start_reg, count, maybe_idx_str, maybe_idx_int) = if vtab
                            .kind
                            .eq(&VTabKind::VirtualTable)
                        {
                            // Build converted constraints from the predicates.
                            let mut converted_constraints = Vec::with_capacity(predicates.len());
                            for (i, pred) in predicates.iter().enumerate() {
                                if let Some(cinfo) =
                                    try_convert_to_constraint_info(pred, table_index, i)
                                {
                                    converted_constraints.push((cinfo, pred));
                                }
                            }
                            let constraints: Vec<_> =
                                converted_constraints.iter().map(|(c, _)| *c).collect();
                            let order_by = vec![OrderByInfo {
                                column_index: *t_ctx
                                    .result_column_indexes_in_orderby_sorter
                                    .first()
                                    .unwrap_or(&0)
                                    as u32,
                                desc: matches!(iter_dir, IterationDirection::Backwards),
                            }];
                            let index_info = vtab.best_index(&constraints, &order_by);

                            // Determine the number of VFilter arguments (constraints with an argv_index).
                            let args_needed = index_info
                                .constraint_usages
                                .iter()
                                .filter(|u| u.argv_index.is_some())
                                .count();
                            let start_reg = program.alloc_registers(args_needed);

                            // For each constraint used by best_index, translate the literal side.
                            for (i, usage) in index_info.constraint_usages.iter().enumerate() {
                                if let Some(argv_index) = usage.argv_index {
                                    if let Some((_, pred)) = converted_constraints.get(i) {
                                        if let ast::Expr::Binary(lhs, _, rhs) = &pred.expr {
                                            let literal_expr = match (&**lhs, &**rhs) {
                                                (ast::Expr::Column { .. }, lit) => lit,
                                                (lit, ast::Expr::Column { .. }) => lit,
                                                _ => continue,
                                            };
                                            // argv_index is 1-based; adjust to get the proper register offset.
                                            let target_reg = start_reg + (argv_index - 1) as usize;
                                            translate_expr(
                                                program,
                                                Some(tables),
                                                literal_expr,
                                                target_reg,
                                                &t_ctx.resolver,
                                            )?;
                                        }
                                    }
                                }
                            }

                            // If best_index provided an idx_str, translate it.
                            let maybe_idx_str = if let Some(idx_str) = index_info.idx_str {
                                let reg = program.alloc_register();
                                program.emit_insn(Insn::String8 {
                                    dest: reg,
                                    value: idx_str,
                                });
                                Some(reg)
                            } else {
                                None
                            };

                            // Record (in t_ctx) the indices of predicates that best_index tells us to omit.
                            // Here we insert directly into t_ctx.omit_predicates (assumed to be a HashSet).
                            for (j, usage) in index_info.constraint_usages.iter().enumerate() {
                                if usage.argv_index.is_some() && usage.omit {
                                    if let Some(constraint) = constraints.get(j) {
                                        t_ctx.omit_predicates.push(constraint.pred_idx);
                                    }
                                }
                            }
                            (
                                start_reg,
                                args_needed,
                                maybe_idx_str,
                                Some(index_info.idx_num),
                            )
                        } else {
                            // For table-valued functions (TVFs): translate the table args.
                            let args = match vtab.args.as_ref() {
                                Some(args) => args,
                                None => &vec![],
                            };
                            let start_reg = program.alloc_registers(args.len());
                            let mut cur_reg = start_reg;
                            for arg in args {
                                let reg = cur_reg;
                                cur_reg += 1;
                                let _ = translate_expr(
                                    program,
                                    Some(tables),
                                    arg,
                                    reg,
                                    &t_ctx.resolver,
                                )?;
                            }
                            (start_reg, args.len(), None, None)
                        };

                        // Emit VFilter with the computed arguments.
                        program.emit_insn(Insn::VFilter {
                            cursor_id,
                            pc_if_empty: loop_end,
                            arg_count: count,
                            args_reg: start_reg,
                            idx_str: maybe_idx_str,
                            idx_num: maybe_idx_int.unwrap_or(0) as usize,
                        });
                    }
                    other => panic!("Unsupported table reference type: {:?}", other),
                }
                program.resolve_label(loop_start, program.offset());

                if let Some(index_cursor_id) = index_cursor_id {
                    program.emit_insn(Insn::DeferredSeek {
                        index_cursor_id,
                        table_cursor_id: cursor_id,
                    });
                }

                for (_, cond) in predicates.iter().enumerate().filter(|(i, cond)| {
                    cond.should_eval_at_loop(table_index) && !t_ctx.omit_predicates.contains(i)
                }) {
                    let jump_target_when_true = program.allocate_label();
                    let condition_metadata = ConditionMetadata {
                        jump_if_condition_is_true: false,
                        jump_target_when_true,
                        jump_target_when_false: next,
                    };
                    translate_condition_expr(
                        program,
                        tables,
                        &cond.expr,
                        condition_metadata,
                        &t_ctx.resolver,
                    )?;
                    program.resolve_label(jump_target_when_true, program.offset());
                }
            }
            Operation::Search(search) => {
                let table_cursor_id = program.resolve_cursor_id(&table.identifier);
                // Open the loop for the index search.
                // Rowid equality point lookups are handled with a SeekRowid instruction which does not loop, since it is a single row lookup.
                if let Search::RowidEq { cmp_expr } = search {
                    let src_reg = program.alloc_register();
                    translate_expr(
                        program,
                        Some(tables),
                        &cmp_expr.expr,
                        src_reg,
                        &t_ctx.resolver,
                    )?;
                    program.emit_insn(Insn::SeekRowid {
                        cursor_id: table_cursor_id,
                        src_reg,
                        target_pc: next,
                    });
                } else {
                    // Otherwise, it's an index/rowid scan, i.e. first a seek is performed and then a scan until the comparison expression is not satisfied anymore.
                    let index_cursor_id = if let Search::IndexSearch { index, .. } = search {
                        Some(program.resolve_cursor_id(&index.name))
                    } else {
                        None
                    };
                    let (cmp_expr, cmp_op, iter_dir) = match search {
                        Search::IndexSearch {
                            cmp_expr,
                            cmp_op,
                            iter_dir,
                            ..
                        } => (cmp_expr, cmp_op, iter_dir),
                        Search::RowidSearch {
                            cmp_expr,
                            cmp_op,
                            iter_dir,
                        } => (cmp_expr, cmp_op, iter_dir),
                        Search::RowidEq { .. } => unreachable!(),
                    };

                    // There are a few steps in an index seek:
                    // 1. Emit the comparison expression for the rowid/index seek. For example, if we a clause 'WHERE index_key >= 10', we emit the comparison expression 10 into cmp_reg.
                    //
                    // 2. Emit the seek instruction. SeekGE and SeekGT are used in forwards iteration, SeekLT and SeekLE are used in backwards iteration.
                    //    All of the examples below assume an ascending index, because we do not support descending indexes yet.
                    //    If we are scanning the ascending index:
                    //    - Forwards, and have a GT/GE/EQ comparison, the comparison expression from step 1 is used as the value to seek to, because that is the lowest possible value that satisfies the clause.
                    //    - Forwards, and have a LT/LE comparison, NULL is used as the comparison expression because we actually want to start scanning from the beginning of the index.
                    //    - Backwards, and have a GT/GE comparison, no Seek instruction is emitted and we emit LastAsync instead, because we want to start scanning from the end of the index.
                    //    - Backwards, and have a LT/LE/EQ comparison, we emit a Seek instruction with the comparison expression from step 1 as the value to seek to, since that is the highest possible
                    //      value that satisfies the clause.
                    let seek_cmp_reg = program.alloc_register();
                    let mut comparison_expr_translated = false;
                    match (cmp_op, iter_dir) {
                        // Forwards, GT/GE/EQ -> use the comparison expression (i.e. seek to the first key where the cmp expr is satisfied, and then scan forwards)
                        (
                            ast::Operator::Equals
                            | ast::Operator::Greater
                            | ast::Operator::GreaterEquals,
                            IterationDirection::Forwards,
                        ) => {
                            translate_expr(
                                program,
                                Some(tables),
                                &cmp_expr.expr,
                                seek_cmp_reg,
                                &t_ctx.resolver,
                            )?;
                            comparison_expr_translated = true;
                            match cmp_op {
                                ast::Operator::Equals | ast::Operator::GreaterEquals => {
                                    program.emit_insn(Insn::SeekGE {
                                        is_index: index_cursor_id.is_some(),
                                        cursor_id: index_cursor_id.unwrap_or(table_cursor_id),
                                        start_reg: seek_cmp_reg,
                                        num_regs: 1,
                                        target_pc: loop_end,
                                    });
                                }
                                ast::Operator::Greater => {
                                    program.emit_insn(Insn::SeekGT {
                                        is_index: index_cursor_id.is_some(),
                                        cursor_id: index_cursor_id.unwrap_or(table_cursor_id),
                                        start_reg: seek_cmp_reg,
                                        num_regs: 1,
                                        target_pc: loop_end,
                                    });
                                }
                                _ => unreachable!(),
                            }
                        }
                        // Forwards, LT/LE -> use NULL (i.e. start from the beginning of the index)
                        (
                            ast::Operator::Less | ast::Operator::LessEquals,
                            IterationDirection::Forwards,
                        ) => {
                            program.emit_insn(Insn::Null {
                                dest: seek_cmp_reg,
                                dest_end: None,
                            });
                            program.emit_insn(Insn::SeekGT {
                                is_index: index_cursor_id.is_some(),
                                cursor_id: index_cursor_id.unwrap_or(table_cursor_id),
                                start_reg: seek_cmp_reg,
                                num_regs: 1,
                                target_pc: loop_end,
                            });
                        }
                        // Backwards, GT/GE -> no seek, emit LastAsync (i.e. start from the end of the index)
                        (
                            ast::Operator::Greater | ast::Operator::GreaterEquals,
                            IterationDirection::Backwards,
                        ) => {
                            program.emit_insn(Insn::LastAsync {
                                cursor_id: index_cursor_id.unwrap_or(table_cursor_id),
                            });
                            program.emit_insn(Insn::LastAwait {
                                cursor_id: index_cursor_id.unwrap_or(table_cursor_id),
                                pc_if_empty: loop_end,
                            });
                        }
                        // Backwards, LT/LE/EQ -> use the comparison expression (i.e. seek from the end of the index until the cmp expr is satisfied, and then scan backwards)
                        (
                            ast::Operator::Less | ast::Operator::LessEquals | ast::Operator::Equals,
                            IterationDirection::Backwards,
                        ) => {
                            translate_expr(
                                program,
                                Some(tables),
                                &cmp_expr.expr,
                                seek_cmp_reg,
                                &t_ctx.resolver,
                            )?;
                            comparison_expr_translated = true;
                            match cmp_op {
                                ast::Operator::Less => {
                                    program.emit_insn(Insn::SeekLT {
                                        is_index: index_cursor_id.is_some(),
                                        cursor_id: index_cursor_id.unwrap_or(table_cursor_id),
                                        start_reg: seek_cmp_reg,
                                        num_regs: 1,
                                        target_pc: loop_end,
                                    });
                                }
                                ast::Operator::LessEquals | ast::Operator::Equals => {
                                    program.emit_insn(Insn::SeekLE {
                                        is_index: index_cursor_id.is_some(),
                                        cursor_id: index_cursor_id.unwrap_or(table_cursor_id),
                                        start_reg: seek_cmp_reg,
                                        num_regs: 1,
                                        target_pc: loop_end,
                                    });
                                }
                                _ => unreachable!(),
                            }
                        }
                        _ => unreachable!(),
                    };

                    program.resolve_label(loop_start, program.offset());

                    let scan_terminating_cmp_reg = if comparison_expr_translated {
                        seek_cmp_reg
                    } else {
                        let reg = program.alloc_register();
                        translate_expr(
                            program,
                            Some(tables),
                            &cmp_expr.expr,
                            reg,
                            &t_ctx.resolver,
                        )?;
                        reg
                    };

                    // 3. Emit a scan-terminating comparison instruction (IdxGT, IdxGE, IdxLT, IdxLE if index; GT, GE, LT, LE if btree rowid scan).
                    //    Here the comparison expression from step 1 is compared to the current index key and the loop is exited if the comparison is true.
                    //    The comparison operator used in the Idx__ instruction is the inverse of the WHERE clause comparison operator.
                    //    For example, if we are scanning forwards and have a clause 'WHERE index_key < 10', we emit IdxGE(10) since >=10 is the first key where our condition is not satisfied anymore.
                    match (cmp_op, iter_dir) {
                        // Forwards, <= -> terminate if >
                        (
                            ast::Operator::Equals | ast::Operator::LessEquals,
                            IterationDirection::Forwards,
                        ) => {
                            if let Some(index_cursor_id) = index_cursor_id {
                                program.emit_insn(Insn::IdxGT {
                                    cursor_id: index_cursor_id,
                                    start_reg: scan_terminating_cmp_reg,
                                    num_regs: 1,
                                    target_pc: loop_end,
                                });
                            } else {
                                let rowid_reg = program.alloc_register();
                                program.emit_insn(Insn::RowId {
                                    cursor_id: table_cursor_id,
                                    dest: rowid_reg,
                                });
                                program.emit_insn(Insn::Gt {
                                    lhs: rowid_reg,
                                    rhs: scan_terminating_cmp_reg,
                                    target_pc: loop_end,
                                    flags: CmpInsFlags::default(),
                                });
                            }
                        }
                        // Forwards, < -> terminate if >=
                        (ast::Operator::Less, IterationDirection::Forwards) => {
                            if let Some(index_cursor_id) = index_cursor_id {
                                program.emit_insn(Insn::IdxGE {
                                    cursor_id: index_cursor_id,
                                    start_reg: scan_terminating_cmp_reg,
                                    num_regs: 1,
                                    target_pc: loop_end,
                                });
                            } else {
                                let rowid_reg = program.alloc_register();
                                program.emit_insn(Insn::RowId {
                                    cursor_id: table_cursor_id,
                                    dest: rowid_reg,
                                });
                                program.emit_insn(Insn::Ge {
                                    lhs: rowid_reg,
                                    rhs: scan_terminating_cmp_reg,
                                    target_pc: loop_end,
                                    flags: CmpInsFlags::default(),
                                });
                            }
                        }
                        // Backwards, >= -> terminate if <
                        (
                            ast::Operator::Equals | ast::Operator::GreaterEquals,
                            IterationDirection::Backwards,
                        ) => {
                            if let Some(index_cursor_id) = index_cursor_id {
                                program.emit_insn(Insn::IdxLT {
                                    cursor_id: index_cursor_id,
                                    start_reg: scan_terminating_cmp_reg,
                                    num_regs: 1,
                                    target_pc: loop_end,
                                });
                            } else {
                                let rowid_reg = program.alloc_register();
                                program.emit_insn(Insn::RowId {
                                    cursor_id: table_cursor_id,
                                    dest: rowid_reg,
                                });
                                program.emit_insn(Insn::Lt {
                                    lhs: rowid_reg,
                                    rhs: scan_terminating_cmp_reg,
                                    target_pc: loop_end,
                                    flags: CmpInsFlags::default(),
                                });
                            }
                        }
                        // Backwards, > -> terminate if <=
                        (ast::Operator::Greater, IterationDirection::Backwards) => {
                            if let Some(index_cursor_id) = index_cursor_id {
                                program.emit_insn(Insn::IdxLE {
                                    cursor_id: index_cursor_id,
                                    start_reg: scan_terminating_cmp_reg,
                                    num_regs: 1,
                                    target_pc: loop_end,
                                });
                            } else {
                                let rowid_reg = program.alloc_register();
                                program.emit_insn(Insn::RowId {
                                    cursor_id: table_cursor_id,
                                    dest: rowid_reg,
                                });
                                program.emit_insn(Insn::Le {
                                    lhs: rowid_reg,
                                    rhs: scan_terminating_cmp_reg,
                                    target_pc: loop_end,
                                    flags: CmpInsFlags::default(),
                                });
                            }
                        }
                        // Forwards, > and >= -> we already did a seek to the first key where the cmp expr is satisfied, so we dont have a terminating condition
                        // Backwards, < and <= -> we already did a seek to the last key where the cmp expr is satisfied, so we dont have a terminating condition
                        _ => {}
                    }

                    if let Some(index_cursor_id) = index_cursor_id {
                        // Don't do a btree table seek until it's actually necessary to read from the table.
                        program.emit_insn(Insn::DeferredSeek {
                            index_cursor_id,
                            table_cursor_id,
                        });
                    }
                }

                for cond in predicates
                    .iter()
                    .filter(|cond| cond.should_eval_at_loop(table_index))
                {
                    let jump_target_when_true = program.allocate_label();
                    let condition_metadata = ConditionMetadata {
                        jump_if_condition_is_true: false,
                        jump_target_when_true,
                        jump_target_when_false: next,
                    };
                    translate_condition_expr(
                        program,
                        tables,
                        &cond.expr,
                        condition_metadata,
                        &t_ctx.resolver,
                    )?;
                    program.resolve_label(jump_target_when_true, program.offset());
                }
            }
        }

        // Set the match flag to true if this is a LEFT JOIN.
        // At this point of execution we are going to emit columns for the left table,
        // and either emit columns or NULLs for the right table, depending on whether the null_flag is set
        // for the right table's cursor.
        if let Some(join_info) = table.join_info.as_ref() {
            if join_info.outer {
                let lj_meta = t_ctx.meta_left_joins[table_index].as_ref().unwrap();
                program.resolve_label(lj_meta.label_match_flag_set_true, program.offset());
                program.emit_insn(Insn::Integer {
                    value: 1,
                    dest: lj_meta.reg_match_flag,
                });
            }
        }
    }

    Ok(())
}

/// SQLite (and so Limbo) processes joins as a nested loop.
/// The loop may emit rows to various destinations depending on the query:
/// - a GROUP BY sorter (grouping is done by sorting based on the GROUP BY keys and aggregating while the GROUP BY keys match)
/// - an ORDER BY sorter (when there is no GROUP BY, but there is an ORDER BY)
/// - an AggStep (the columns are collected for aggregation, which is finished later)
/// - a QueryResult (there is none of the above, so the loop either emits a ResultRow, or if it's a subquery, yields to the parent query)
enum LoopEmitTarget {
    GroupBySorter,
    OrderBySorter,
    AggStep,
    QueryResult,
}

/// Emits the bytecode for the inner loop of a query.
/// At this point the cursors for all tables have been opened and rewound.
pub fn emit_loop(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    plan: &mut SelectPlan,
) -> Result<()> {
    // if we have a group by, we emit a record into the group by sorter.
    if plan.group_by.is_some() {
        return emit_loop_source(program, t_ctx, plan, LoopEmitTarget::GroupBySorter);
    }
    // if we DONT have a group by, but we have aggregates, we emit without ResultRow.
    // we also do not need to sort because we are emitting a single row.
    if !plan.aggregates.is_empty() {
        return emit_loop_source(program, t_ctx, plan, LoopEmitTarget::AggStep);
    }
    // if we DONT have a group by, but we have an order by, we emit a record into the order by sorter.
    if plan.order_by.is_some() {
        return emit_loop_source(program, t_ctx, plan, LoopEmitTarget::OrderBySorter);
    }
    // if we have neither, we emit a ResultRow. In that case, if we have a Limit, we handle that with DecrJumpZero.
    emit_loop_source(program, t_ctx, plan, LoopEmitTarget::QueryResult)
}

/// This is a helper function for inner_loop_emit,
/// which does a different thing depending on the emit target.
/// See the InnerLoopEmitTarget enum for more details.
fn emit_loop_source(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    plan: &SelectPlan,
    emit_target: LoopEmitTarget,
) -> Result<()> {
    match emit_target {
        LoopEmitTarget::GroupBySorter => {
            // This function creates a sorter for GROUP BY operations by allocating registers and
            // translating expressions for three types of columns:
            // 1) GROUP BY columns (used as sorting keys)
            // 2) non-aggregate, non-GROUP BY columns
            // 3) aggregate function arguments
            let group_by = plan.group_by.as_ref().unwrap();
            let aggregates = &plan.aggregates;

            // Identify columns in the result set that are neither in GROUP BY nor contain aggregates
            let non_group_by_non_agg_expr = plan
                .result_columns
                .iter()
                .filter(|rc| {
                    !rc.contains_aggregates && !is_column_in_group_by(&rc.expr, &group_by.exprs)
                })
                .map(|rc| &rc.expr);
            let non_agg_count = non_group_by_non_agg_expr.clone().count();
            // Store the count of non-GROUP BY, non-aggregate columns in the metadata
            // This will be used later during aggregation processing
            t_ctx.meta_group_by.as_mut().map(|meta| {
                meta.non_group_by_non_agg_column_count = Some(non_agg_count);
                meta
            });

            // Calculate the total number of arguments used across all aggregate functions
            let aggregate_arguments_count = plan
                .aggregates
                .iter()
                .map(|agg| agg.args.len())
                .sum::<usize>();

            // Calculate total number of registers needed for all columns in the sorter
            let column_count = group_by.exprs.len() + aggregate_arguments_count + non_agg_count;

            // Allocate a contiguous block of registers for all columns
            let start_reg = program.alloc_registers(column_count);
            let mut cur_reg = start_reg;

            // Step 1: Process GROUP BY columns first
            // These will be the first columns in the sorter and serve as sort keys
            for expr in group_by.exprs.iter() {
                let key_reg = cur_reg;
                cur_reg += 1;
                translate_expr(
                    program,
                    Some(&plan.table_references),
                    expr,
                    key_reg,
                    &t_ctx.resolver,
                )?;
            }

            // Step 2: Process columns that aren't part of GROUP BY and don't contain aggregates
            // Example: SELECT col1, col2, SUM(col3) FROM table GROUP BY col1
            // Here col2 would be processed in this loop if it's in the result set
            for expr in non_group_by_non_agg_expr {
                let key_reg = cur_reg;
                cur_reg += 1;
                translate_expr(
                    program,
                    Some(&plan.table_references),
                    expr,
                    key_reg,
                    &t_ctx.resolver,
                )?;
            }

            // Step 3: Process arguments for all aggregate functions
            // For each aggregate, translate all its argument expressions
            for agg in aggregates.iter() {
                // For a query like: SELECT group_col, SUM(val1), AVG(val2) FROM table GROUP BY group_col
                // we'll process val1 and val2 here, storing them in the sorter so they're available
                // when computing the aggregates after sorting by group_col
                for expr in agg.args.iter() {
                    let agg_reg = cur_reg;
                    cur_reg += 1;
                    translate_expr(
                        program,
                        Some(&plan.table_references),
                        expr,
                        agg_reg,
                        &t_ctx.resolver,
                    )?;
                }
            }

            let group_by_metadata = t_ctx.meta_group_by.as_ref().unwrap();

            sorter_insert(
                program,
                start_reg,
                column_count,
                group_by_metadata.sort_cursor,
                group_by_metadata.reg_sorter_key,
            );

            Ok(())
        }
        LoopEmitTarget::OrderBySorter => order_by_sorter_insert(program, t_ctx, plan),
        LoopEmitTarget::AggStep => {
            let num_aggs = plan.aggregates.len();
            let start_reg = program.alloc_registers(num_aggs);
            t_ctx.reg_agg_start = Some(start_reg);

            // In planner.rs, we have collected all aggregates from the SELECT clause, including ones where the aggregate is embedded inside
            // a more complex expression. Some examples: length(sum(x)), sum(x) + avg(y), sum(x) + 1, etc.
            // The result of those more complex expressions depends on the final result of the aggregate, so we don't translate the complete expressions here.
            // Instead, we accumulate the intermediate results of all aggreagates, and evaluate any expressions that do not contain aggregates.
            for (i, agg) in plan.aggregates.iter().enumerate() {
                let reg = start_reg + i;
                translate_aggregation_step(
                    program,
                    &plan.table_references,
                    agg,
                    reg,
                    &t_ctx.resolver,
                )?;
            }

            let label_emit_nonagg_only_once = if let Some(flag) = t_ctx.reg_nonagg_emit_once_flag {
                let if_label = program.allocate_label();
                program.emit_insn(Insn::If {
                    reg: flag,
                    target_pc: if_label,
                    jump_if_null: false,
                });
                Some(if_label)
            } else {
                None
            };

            let col_start = t_ctx.reg_result_cols_start.unwrap();

            // Process only non-aggregate columns
            let non_agg_columns = plan
                .result_columns
                .iter()
                .enumerate()
                .filter(|(_, rc)| !rc.contains_aggregates);

            for (i, rc) in non_agg_columns {
                let reg = col_start + i;

                translate_expr(
                    program,
                    Some(&plan.table_references),
                    &rc.expr,
                    reg,
                    &t_ctx.resolver,
                )?;
            }
            if let Some(label) = label_emit_nonagg_only_once {
                program.resolve_label(label, program.offset());
                let flag = t_ctx.reg_nonagg_emit_once_flag.unwrap();
                program.emit_int(1, flag);
            }

            Ok(())
        }
        LoopEmitTarget::QueryResult => {
            assert!(
                plan.aggregates.is_empty(),
                "We should not get here with aggregates"
            );
            let offset_jump_to = t_ctx
                .labels_main_loop
                .first()
                .map(|l| l.next)
                .or(t_ctx.label_main_loop_end);
            emit_select_result(
                program,
                t_ctx,
                plan,
                t_ctx.label_main_loop_end,
                offset_jump_to,
            )?;

            Ok(())
        }
    }
}

/// Closes the loop for a given source operator.
/// For example in the case of a nested table scan, this means emitting the NextAsync instruction
/// for all tables involved, innermost first.
pub fn close_loop(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    tables: &[TableReference],
) -> Result<()> {
    // We close the loops for all tables in reverse order, i.e. innermost first.
    // OPEN t1
    //   OPEN t2
    //     OPEN t3
    //       <do stuff>
    //     CLOSE t3
    //   CLOSE t2
    // CLOSE t1
    for (idx, table) in tables.iter().rev().enumerate() {
        let table_index = tables.len() - idx - 1;
        let loop_labels = *t_ctx
            .labels_main_loop
            .get(table_index)
            .expect("source has no loop labels");

        match &table.op {
            Operation::Subquery { .. } => {
                program.resolve_label(loop_labels.next, program.offset());
                // A subquery has no cursor to call NextAsync on, so it just emits a Goto
                // to the Yield instruction, which in turn jumps back to the main loop of the subquery,
                // so that the next row from the subquery can be read.
                program.emit_insn(Insn::Goto {
                    target_pc: loop_labels.loop_start,
                });
            }
            Operation::Scan {
                index, iter_dir, ..
            } => {
                program.resolve_label(loop_labels.next, program.offset());

                let cursor_id = program.resolve_cursor_id(&table.identifier);
                let index_cursor_id = index.as_ref().map(|i| program.resolve_cursor_id(&i.name));
                let iteration_cursor_id = index_cursor_id.unwrap_or(cursor_id);
                match &table.table {
                    Table::BTree(_) => {
                        if *iter_dir == IterationDirection::Backwards {
                            program.emit_insn(Insn::PrevAsync {
                                cursor_id: iteration_cursor_id,
                            });
                        } else {
                            program.emit_insn(Insn::NextAsync {
                                cursor_id: iteration_cursor_id,
                            });
                        }
                        if *iter_dir == IterationDirection::Backwards {
                            program.emit_insn(Insn::PrevAwait {
                                cursor_id: iteration_cursor_id,
                                pc_if_next: loop_labels.loop_start,
                            });
                        } else {
                            program.emit_insn(Insn::NextAwait {
                                cursor_id: iteration_cursor_id,
                                pc_if_next: loop_labels.loop_start,
                            });
                        }
                    }
                    Table::Virtual(_) => {
                        program.emit_insn(Insn::VNext {
                            cursor_id,
                            pc_if_next: loop_labels.loop_start,
                        });
                    }
                    other => unreachable!("Unsupported table reference type: {:?}", other),
                }
            }
            Operation::Search(search) => {
                program.resolve_label(loop_labels.next, program.offset());
                // Rowid equality point lookups are handled with a SeekRowid instruction which does not loop, so there is no need to emit a NextAsync instruction.
                if !matches!(search, Search::RowidEq { .. }) {
                    let (cursor_id, iter_dir) = match search {
                        Search::IndexSearch {
                            index, iter_dir, ..
                        } => (program.resolve_cursor_id(&index.name), *iter_dir),
                        Search::RowidSearch { iter_dir, .. } => {
                            (program.resolve_cursor_id(&table.identifier), *iter_dir)
                        }
                        Search::RowidEq { .. } => unreachable!(),
                    };

                    if iter_dir == IterationDirection::Backwards {
                        program.emit_insn(Insn::PrevAsync { cursor_id });
                        program.emit_insn(Insn::PrevAwait {
                            cursor_id,
                            pc_if_next: loop_labels.loop_start,
                        });
                    } else {
                        program.emit_insn(Insn::NextAsync { cursor_id });
                        program.emit_insn(Insn::NextAwait {
                            cursor_id,
                            pc_if_next: loop_labels.loop_start,
                        });
                    }
                }
            }
        }

        program.resolve_label(loop_labels.loop_end, program.offset());

        // Handle OUTER JOIN logic. The reason this comes after the "loop end" mark is that we may need to still jump back
        // and emit a row with NULLs for the right table, and then jump back to the next row of the left table.
        if let Some(join_info) = table.join_info.as_ref() {
            if join_info.outer {
                let lj_meta = t_ctx.meta_left_joins[table_index].as_ref().unwrap();
                // The left join match flag is set to 1 when there is any match on the right table
                // (e.g. SELECT * FROM t1 LEFT JOIN t2 ON t1.a = t2.a).
                // If the left join match flag has been set to 1, we jump to the next row on the outer table,
                // i.e. continue to the next row of t1 in our example.
                program.resolve_label(lj_meta.label_match_flag_check_value, program.offset());
                let jump_offset = program.offset().add(3u32);
                program.emit_insn(Insn::IfPos {
                    reg: lj_meta.reg_match_flag,
                    target_pc: jump_offset,
                    decrement_by: 0,
                });
                // If the left join match flag is still 0, it means there was no match on the right table,
                // but since it's a LEFT JOIN, we still need to emit a row with NULLs for the right table.
                // In that case, we now enter the routine that does exactly that.
                // First we set the right table cursor's "pseudo null bit" on, which means any Insn::Column will return NULL
                let right_cursor_id = match &table.op {
                    Operation::Scan { .. } => program.resolve_cursor_id(&table.identifier),
                    Operation::Search { .. } => program.resolve_cursor_id(&table.identifier),
                    _ => unreachable!(),
                };
                program.emit_insn(Insn::NullRow {
                    cursor_id: right_cursor_id,
                });
                // Then we jump to setting the left join match flag to 1 again,
                // but this time the right table cursor will set everything to null.
                // This leads to emitting a row with cols from the left + nulls from the right,
                // and we will end up back in the IfPos instruction above, which will then
                // check the match flag again, and since it is now 1, we will jump to the
                // next row in the left table.
                program.emit_insn(Insn::Goto {
                    target_pc: lj_meta.label_match_flag_set_true,
                });

                assert_eq!(program.offset(), jump_offset);
            }
        }
    }
    Ok(())
}
