// This module contains code for emitting bytecode instructions for SQL query execution.
// It handles translating high-level SQL operations into low-level bytecode that can be executed by the virtual machine.

use std::rc::Rc;
use std::sync::Arc;

use limbo_sqlite3_parser::ast::{self};

use crate::function::Func;
use crate::parameters::UpdatePos;
use crate::schema::Index;
use crate::translate::plan::{DeletePlan, Plan, Search};
use crate::util::exprs_are_equivalent;
use crate::vdbe::builder::{CursorType, ProgramBuilder};
use crate::vdbe::insn::{IdxInsertFlags, RegisterOrLiteral};
use crate::vdbe::{insn::Insn, BranchOffset};
use crate::{Result, SymbolTable};

use super::aggregation::emit_ungrouped_aggregation;
use super::expr::{translate_condition_expr, translate_expr, ConditionMetadata};
use super::group_by::{
    group_by_agg_phase, group_by_emit_row_phase, init_group_by, GroupByMetadata, GroupByRowSource,
};
use super::main_loop::{close_loop, emit_loop, init_loop, open_loop, LeftJoinMetadata, LoopLabels};
use super::order_by::{emit_order_by, init_order_by, SortMetadata};
use super::plan::{JoinOrderMember, Operation, SelectPlan, TableReference, UpdatePlan};
use super::schema::ParseSchema;
use super::subquery::emit_subqueries;

#[derive(Debug)]
pub struct Resolver<'a> {
    pub symbol_table: &'a SymbolTable,
    pub expr_to_reg_cache: Vec<(&'a ast::Expr, usize)>,
}

impl<'a> Resolver<'a> {
    pub fn new(symbol_table: &'a SymbolTable) -> Self {
        Self {
            symbol_table,
            expr_to_reg_cache: Vec::new(),
        }
    }

    pub fn resolve_function(&self, func_name: &str, arg_count: usize) -> Option<Func> {
        match Func::resolve_function(func_name, arg_count).ok() {
            Some(func) => Some(func),
            None => self
                .symbol_table
                .resolve_function(func_name, arg_count)
                .map(|arg| Func::External(arg.clone())),
        }
    }

    pub fn resolve_cached_expr_reg(&self, expr: &ast::Expr) -> Option<usize> {
        self.expr_to_reg_cache
            .iter()
            .find(|(e, _)| exprs_are_equivalent(expr, e))
            .map(|(_, reg)| *reg)
    }
}

/// The TranslateCtx struct holds various information and labels used during bytecode generation.
/// It is used for maintaining state and control flow during the bytecode
/// generation process.
#[derive(Debug)]
pub struct TranslateCtx<'a> {
    // A typical query plan is a nested loop. Each loop has its own LoopLabels (see the definition of LoopLabels for more details)
    pub labels_main_loop: Vec<LoopLabels>,
    // label for the instruction that jumps to the next phase of the query after the main loop
    // we don't know ahead of time what that is (GROUP BY, ORDER BY, etc.)
    pub label_main_loop_end: Option<BranchOffset>,
    // First register of the aggregation results
    pub reg_agg_start: Option<usize>,
    // In non-group-by statements with aggregations (e.g. SELECT foo, bar, sum(baz) FROM t),
    // we want to emit the non-aggregate columns (foo and bar) only once.
    // This register is a flag that tracks whether we have already done that.
    pub reg_nonagg_emit_once_flag: Option<usize>,
    // First register of the result columns of the query
    pub reg_result_cols_start: Option<usize>,
    // The register holding the limit value, if any.
    pub reg_limit: Option<usize>,
    // The register holding the offset value, if any.
    pub reg_offset: Option<usize>,
    // The register holding the limit+offset value, if any.
    pub reg_limit_offset_sum: Option<usize>,
    // metadata for the group by operator
    pub meta_group_by: Option<GroupByMetadata>,
    // metadata for the order by operator
    pub meta_sort: Option<SortMetadata>,
    /// mapping between table loop index and associated metadata (for left joins only)
    /// this metadata exists for the right table in a given left join
    pub meta_left_joins: Vec<Option<LeftJoinMetadata>>,
    // We need to emit result columns in the order they are present in the SELECT, but they may not be in the same order in the ORDER BY sorter.
    // This vector holds the indexes of the result columns in the ORDER BY sorter.
    pub result_column_indexes_in_orderby_sorter: Vec<usize>,
    // We might skip adding a SELECT result column into the ORDER BY sorter if it is an exact match in the ORDER BY keys.
    // This vector holds the indexes of the result columns that we need to skip.
    pub result_columns_to_skip_in_orderby_sorter: Option<Vec<usize>>,
    pub resolver: Resolver<'a>,
    pub omit_predicates: Vec<usize>,
}

/// Used to distinguish database operations
#[allow(clippy::upper_case_acronyms, dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationMode {
    SELECT,
    INSERT,
    UPDATE,
    DELETE,
}

/// Initialize the program with basic setup and return initial metadata and labels
fn prologue<'a>(
    program: &mut ProgramBuilder,
    syms: &'a SymbolTable,
    table_count: usize,
    result_column_count: usize,
) -> Result<(TranslateCtx<'a>, BranchOffset, BranchOffset)> {
    let init_label = program.allocate_label();

    program.emit_insn(Insn::Init {
        target_pc: init_label,
    });

    let start_offset = program.offset();

    let t_ctx = TranslateCtx {
        labels_main_loop: (0..table_count).map(|_| LoopLabels::new(program)).collect(),
        label_main_loop_end: None,
        reg_agg_start: None,
        reg_nonagg_emit_once_flag: None,
        reg_limit: None,
        reg_offset: None,
        reg_limit_offset_sum: None,
        reg_result_cols_start: None,
        meta_group_by: None,
        meta_left_joins: (0..table_count).map(|_| None).collect(),
        meta_sort: None,
        result_column_indexes_in_orderby_sorter: (0..result_column_count).collect(),
        result_columns_to_skip_in_orderby_sorter: None,
        resolver: Resolver::new(syms),
        omit_predicates: Vec::new(),
    };

    Ok((t_ctx, init_label, start_offset))
}

#[derive(Clone, Copy, Debug)]
pub enum TransactionMode {
    None,
    Read,
    Write,
}

/// Clean up and finalize the program, resolving any remaining labels
/// Note that although these are the final instructions, typically an SQLite
/// query will jump to the Transaction instruction via init_label.
fn epilogue(
    program: &mut ProgramBuilder,
    init_label: BranchOffset,
    start_offset: BranchOffset,
    txn_mode: TransactionMode,
) -> Result<()> {
    program.emit_insn(Insn::Halt {
        err_code: 0,
        description: String::new(),
    });
    program.preassign_label_to_next_insn(init_label);

    match txn_mode {
        TransactionMode::Read => program.emit_insn(Insn::Transaction { write: false }),
        TransactionMode::Write => program.emit_insn(Insn::Transaction { write: true }),
        TransactionMode::None => {}
    }

    program.emit_constant_insns();
    program.emit_insn(Insn::Goto {
        target_pc: start_offset,
    });

    Ok(())
}

/// Main entry point for emitting bytecode for a SQL query
/// Takes a query plan and generates the corresponding bytecode program
pub fn emit_program(program: &mut ProgramBuilder, plan: Plan, syms: &SymbolTable) -> Result<()> {
    match plan {
        Plan::Select(plan) => emit_program_for_select(program, plan, syms),
        Plan::Delete(plan) => emit_program_for_delete(program, plan, syms),
        Plan::Update(plan) => emit_program_for_update(program, plan, syms),
    }
}

fn emit_program_for_select(
    program: &mut ProgramBuilder,
    mut plan: SelectPlan,
    syms: &SymbolTable,
) -> Result<()> {
    let (mut t_ctx, init_label, start_offset) = prologue(
        program,
        syms,
        plan.table_references.len(),
        plan.result_columns.len(),
    )?;

    // Trivial exit on LIMIT 0
    if let Some(limit) = plan.limit {
        if limit == 0 {
            epilogue(program, init_label, start_offset, TransactionMode::Read)?;
            program.result_columns = plan.result_columns;
            program.table_references = plan.table_references;
            return Ok(());
        }
    }
    // Emit main parts of query
    emit_query(program, &mut plan, &mut t_ctx)?;

    // Finalize program
    if plan.table_references.is_empty() {
        epilogue(program, init_label, start_offset, TransactionMode::None)?;
    } else {
        epilogue(program, init_label, start_offset, TransactionMode::Read)?;
    }

    program.result_columns = plan.result_columns;
    program.table_references = plan.table_references;
    Ok(())
}

pub fn emit_query<'a>(
    program: &'a mut ProgramBuilder,
    plan: &'a mut SelectPlan,
    t_ctx: &'a mut TranslateCtx<'a>,
) -> Result<usize> {
    // Emit subqueries first so the results can be read in the main query loop.
    emit_subqueries(program, t_ctx, &mut plan.table_references)?;

    if t_ctx.reg_limit.is_none() {
        t_ctx.reg_limit = plan.limit.map(|_| program.alloc_register());
    }

    if t_ctx.reg_offset.is_none() {
        t_ctx.reg_offset = plan.offset.map(|_| program.alloc_register());
    }

    if t_ctx.reg_limit_offset_sum.is_none() {
        t_ctx.reg_limit_offset_sum = plan.offset.map(|_| program.alloc_register());
    }

    // No rows will be read from source table loops if there is a constant false condition eg. WHERE 0
    // however an aggregation might still happen,
    // e.g. SELECT COUNT(*) WHERE 0 returns a row with 0, not an empty result set
    let after_main_loop_label = program.allocate_label();
    t_ctx.label_main_loop_end = Some(after_main_loop_label);
    if plan.contains_constant_false_condition {
        program.emit_insn(Insn::Goto {
            target_pc: after_main_loop_label,
        });
    }

    // For non-grouped aggregation queries that also have non-aggregate columns,
    // we need to ensure non-aggregate columns are only emitted once.
    // This flag helps track whether we've already emitted these columns.
    if !plan.aggregates.is_empty()
        && plan.group_by.is_none()
        && plan.result_columns.iter().any(|c| !c.contains_aggregates)
    {
        let flag = program.alloc_register();
        program.emit_int(0, flag); // Initialize flag to 0 (not yet emitted)
        t_ctx.reg_nonagg_emit_once_flag = Some(flag);
    }

    // Allocate registers for result columns
    t_ctx.reg_result_cols_start = Some(program.alloc_registers(plan.result_columns.len()));

    // Initialize cursors and other resources needed for query execution
    if let Some(ref mut order_by) = plan.order_by {
        init_order_by(program, t_ctx, order_by)?;
    }

    if let Some(ref group_by) = plan.group_by {
        init_group_by(program, t_ctx, group_by, &plan)?;
    }
    init_loop(
        program,
        t_ctx,
        &plan.table_references,
        OperationMode::SELECT,
    )?;

    for where_term in plan
        .where_clause
        .iter()
        .filter(|wt| wt.is_constant(&plan.join_order))
    {
        let jump_target_when_true = program.allocate_label();
        let condition_metadata = ConditionMetadata {
            jump_if_condition_is_true: false,
            jump_target_when_false: after_main_loop_label,
            jump_target_when_true,
        };
        translate_condition_expr(
            program,
            &plan.table_references,
            &where_term.expr,
            condition_metadata,
            &t_ctx.resolver,
        )?;
        program.preassign_label_to_next_insn(jump_target_when_true);
    }

    // Set up main query execution loop
    open_loop(
        program,
        t_ctx,
        &plan.table_references,
        &plan.join_order,
        &plan.where_clause,
    )?;

    // Process result columns and expressions in the inner loop
    emit_loop(program, t_ctx, plan)?;

    // Clean up and close the main execution loop
    close_loop(program, t_ctx, &plan.table_references, &plan.join_order)?;
    program.preassign_label_to_next_insn(after_main_loop_label);

    let mut order_by_necessary = plan.order_by.is_some() && !plan.contains_constant_false_condition;
    let order_by = plan.order_by.as_ref();

    // Handle GROUP BY and aggregation processing
    if plan.group_by.is_some() {
        let row_source = &t_ctx
            .meta_group_by
            .as_ref()
            .expect("group by metadata not found")
            .row_source;
        if matches!(row_source, GroupByRowSource::Sorter { .. }) {
            group_by_agg_phase(program, t_ctx, plan)?;
        }
        group_by_emit_row_phase(program, t_ctx, plan)?;
    } else if !plan.aggregates.is_empty() {
        // Handle aggregation without GROUP BY
        emit_ungrouped_aggregation(program, t_ctx, plan)?;
        // Single row result for aggregates without GROUP BY, so ORDER BY not needed
        order_by_necessary = false;
    }

    // Process ORDER BY results if needed
    if order_by.is_some() && order_by_necessary {
        emit_order_by(program, t_ctx, plan)?;
    }

    Ok(t_ctx.reg_result_cols_start.unwrap())
}

fn emit_program_for_delete(
    program: &mut ProgramBuilder,
    plan: DeletePlan,
    syms: &SymbolTable,
) -> Result<()> {
    let (mut t_ctx, init_label, start_offset) = prologue(
        program,
        syms,
        plan.table_references.len(),
        plan.result_columns.len(),
    )?;

    // exit early if LIMIT 0
    if let Some(0) = plan.limit {
        epilogue(program, init_label, start_offset, TransactionMode::Write)?;
        program.result_columns = plan.result_columns;
        program.table_references = plan.table_references;
        return Ok(());
    }

    // No rows will be read from source table loops if there is a constant false condition eg. WHERE 0
    let after_main_loop_label = program.allocate_label();
    t_ctx.label_main_loop_end = Some(after_main_loop_label);
    if plan.contains_constant_false_condition {
        program.emit_insn(Insn::Goto {
            target_pc: after_main_loop_label,
        });
    }

    // Initialize cursors and other resources needed for query execution
    init_loop(
        program,
        &mut t_ctx,
        &plan.table_references,
        OperationMode::DELETE,
    )?;

    // Set up main query execution loop
    open_loop(
        program,
        &mut t_ctx,
        &plan.table_references,
        &[JoinOrderMember::default()],
        &plan.where_clause,
    )?;
    emit_delete_insns(
        program,
        &mut t_ctx,
        &plan.table_references,
        &plan.indexes,
        &plan.limit,
    )?;

    // Clean up and close the main execution loop
    close_loop(
        program,
        &mut t_ctx,
        &plan.table_references,
        &[JoinOrderMember::default()],
    )?;
    program.preassign_label_to_next_insn(after_main_loop_label);

    // Finalize program
    epilogue(program, init_label, start_offset, TransactionMode::Write)?;
    program.result_columns = plan.result_columns;
    program.table_references = plan.table_references;
    Ok(())
}

fn emit_delete_insns(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    table_references: &[TableReference],
    index_references: &[Arc<Index>],
    limit: &Option<isize>,
) -> Result<()> {
    let table_reference = table_references.first().unwrap();
    let cursor_id = match &table_reference.op {
        Operation::Scan { .. } => program.resolve_cursor_id(&table_reference.identifier),
        Operation::Search(search) => match search {
            Search::RowidEq { .. } | Search::Seek { index: None, .. } => {
                program.resolve_cursor_id(&table_reference.identifier)
            }
            Search::Seek {
                index: Some(index), ..
            } => program.resolve_cursor_id(&index.name),
        },
        _ => return Ok(()),
    };
    let main_table_cursor_id = program.resolve_cursor_id(table_reference.table.get_name());

    // Emit the instructions to delete the row
    let key_reg = program.alloc_register();
    program.emit_insn(Insn::RowId {
        cursor_id: main_table_cursor_id,
        dest: key_reg,
    });

    if let Some(vtab) = table_reference.virtual_table() {
        let conflict_action = 0u16;
        let start_reg = key_reg;

        let new_rowid_reg = program.alloc_register();
        program.emit_insn(Insn::Null {
            dest: new_rowid_reg,
            dest_end: None,
        });
        program.emit_insn(Insn::VUpdate {
            cursor_id,
            arg_count: 2,
            start_reg,
            vtab_ptr: vtab.implementation.as_ref().ctx as usize,
            conflict_action,
        });
    } else {
        for index in index_references {
            let index_cursor_id = program.alloc_cursor_id(
                Some(index.name.clone()),
                crate::vdbe::builder::CursorType::BTreeIndex(index.clone()),
            );

            program.emit_insn(Insn::OpenWrite {
                cursor_id: index_cursor_id,
                root_page: RegisterOrLiteral::Literal(index.root_page),
            });
            let num_regs = index.columns.len() + 1;
            let start_reg = program.alloc_registers(num_regs);
            // Emit columns that are part of the index
            index
                .columns
                .iter()
                .enumerate()
                .for_each(|(reg_offset, column_index)| {
                    program.emit_insn(Insn::Column {
                        cursor_id: main_table_cursor_id,
                        column: column_index.pos_in_table,
                        dest: start_reg + reg_offset,
                    });
                });
            program.emit_insn(Insn::RowId {
                cursor_id: main_table_cursor_id,
                dest: start_reg + num_regs - 1,
            });
            program.emit_insn(Insn::IdxDelete {
                start_reg,
                num_regs,
                cursor_id: index_cursor_id,
            });
        }
        program.emit_insn(Insn::Delete {
            cursor_id: main_table_cursor_id,
        });
    }
    if let Some(limit) = limit {
        let limit_reg = program.alloc_register();
        program.emit_insn(Insn::Integer {
            value: *limit as i64,
            dest: limit_reg,
        });
        program.mark_last_insn_constant();
        program.emit_insn(Insn::DecrJumpZero {
            reg: limit_reg,
            target_pc: t_ctx.label_main_loop_end.unwrap(),
        })
    }

    Ok(())
}

fn emit_program_for_update(
    program: &mut ProgramBuilder,
    plan: UpdatePlan,
    syms: &SymbolTable,
) -> Result<()> {
    let (mut t_ctx, init_label, start_offset) = prologue(
        program,
        syms,
        plan.table_references.len(),
        plan.returning.as_ref().map_or(0, |r| r.len()),
    )?;

    // Exit on LIMIT 0
    if let Some(0) = plan.limit {
        epilogue(program, init_label, start_offset, TransactionMode::None)?;
        program.result_columns = plan.returning.unwrap_or_default();
        program.table_references = plan.table_references;
        return Ok(());
    }
    if t_ctx.reg_limit.is_none() && plan.limit.is_some() {
        let reg = program.alloc_register();
        t_ctx.reg_limit = Some(reg);
        program.emit_insn(Insn::Integer {
            value: plan.limit.unwrap() as i64,
            dest: reg,
        });
        program.mark_last_insn_constant();
        if t_ctx.reg_offset.is_none() && plan.offset.is_some_and(|n| n.ne(&0)) {
            let reg = program.alloc_register();
            t_ctx.reg_offset = Some(reg);
            program.emit_insn(Insn::Integer {
                value: plan.offset.unwrap() as i64,
                dest: reg,
            });
            program.mark_last_insn_constant();
            let combined_reg = program.alloc_register();
            t_ctx.reg_limit_offset_sum = Some(combined_reg);
            program.emit_insn(Insn::OffsetLimit {
                limit_reg: t_ctx.reg_limit.unwrap(),
                offset_reg: reg,
                combined_reg,
            });
        }
    }
    let after_main_loop_label = program.allocate_label();
    t_ctx.label_main_loop_end = Some(after_main_loop_label);
    if plan.contains_constant_false_condition {
        program.emit_insn(Insn::Goto {
            target_pc: after_main_loop_label,
        });
    }

    init_loop(
        program,
        &mut t_ctx,
        &plan.table_references,
        OperationMode::UPDATE,
    )?;
    // Open indexes for update.
    let mut index_cursors = Vec::with_capacity(plan.indexes_to_update.len());
    // TODO: do not reopen if there is table reference using it.
    for index in &plan.indexes_to_update {
        let index_cursor = program.alloc_cursor_id(
            Some(index.table_name.clone()),
            CursorType::BTreeIndex(index.clone()),
        );
        program.emit_insn(Insn::OpenWrite {
            cursor_id: index_cursor,
            root_page: RegisterOrLiteral::Literal(index.root_page),
        });
        index_cursors.push(index_cursor);
    }
    open_loop(
        program,
        &mut t_ctx,
        &plan.table_references,
        &[JoinOrderMember::default()],
        &plan.where_clause,
    )?;
    emit_update_insns(&plan, &t_ctx, program, index_cursors)?;

    match plan.parse_schema {
        ParseSchema::None => {}
        ParseSchema::Reload => {
            program.emit_insn(crate::vdbe::insn::Insn::ParseSchema {
                db: usize::MAX, // TODO: This value is unused, change when we do something with it
                where_clause: None,
            });
        }
    }

    close_loop(
        program,
        &mut t_ctx,
        &plan.table_references,
        &[JoinOrderMember::default()],
    )?;

    program.preassign_label_to_next_insn(after_main_loop_label);

    // Finalize program
    epilogue(program, init_label, start_offset, TransactionMode::Write)?;
    program.result_columns = plan.returning.unwrap_or_default();
    program.table_references = plan.table_references;
    Ok(())
}

fn emit_update_insns(
    plan: &UpdatePlan,
    t_ctx: &TranslateCtx,
    program: &mut ProgramBuilder,
    index_cursors: Vec<usize>,
) -> crate::Result<()> {
    let table_ref = &plan.table_references.first().unwrap();
    let loop_labels = t_ctx.labels_main_loop.first().unwrap();
    let (cursor_id, index, is_virtual) = match &table_ref.op {
        Operation::Scan { .. } => (
            program.resolve_cursor_id(&table_ref.identifier),
            None,
            table_ref.virtual_table().is_some(),
        ),
        Operation::Search(search) => match search {
            &Search::RowidEq { .. } | Search::Seek { index: None, .. } => (
                program.resolve_cursor_id(&table_ref.identifier),
                None,
                false,
            ),
            Search::Seek {
                index: Some(index), ..
            } => (
                program.resolve_cursor_id(&table_ref.identifier),
                Some((index.clone(), program.resolve_cursor_id(&index.name))),
                false,
            ),
        },
        _ => return Ok(()),
    };

    for cond in plan
        .where_clause
        .iter()
        .filter(|c| c.is_constant(&[JoinOrderMember::default()]))
    {
        let jump_target = program.allocate_label();
        let meta = ConditionMetadata {
            jump_if_condition_is_true: false,
            jump_target_when_true: jump_target,
            jump_target_when_false: t_ctx.label_main_loop_end.unwrap(),
        };
        program.parameters.set_update_position(UpdatePos::Where);
        translate_condition_expr(
            program,
            &plan.table_references,
            &cond.expr,
            meta,
            &t_ctx.resolver,
        )?;
        program.preassign_label_to_next_insn(jump_target);
    }
    let beg = program.alloc_registers(
        table_ref.table.columns().len()
            + if is_virtual {
                2 // two args before the relevant columns for VUpdate
            } else {
                1 // rowid reg
            },
    );
    program.emit_insn(Insn::RowId {
        cursor_id,
        dest: beg,
    });
    // if no rowid, we're done
    program.emit_insn(Insn::IsNull {
        reg: beg,
        target_pc: t_ctx.label_main_loop_end.unwrap(),
    });
    if is_virtual {
        program.emit_insn(Insn::Copy {
            src_reg: beg,
            dst_reg: beg + 1,
            amount: 0,
        })
    }

    if let Some(offset) = t_ctx.reg_offset {
        program.emit_insn(Insn::IfPos {
            reg: offset,
            target_pc: loop_labels.next,
            decrement_by: 1,
        });
    }

    for cond in plan
        .where_clause
        .iter()
        .filter(|c| c.is_constant(&[JoinOrderMember::default()]))
    {
        let meta = ConditionMetadata {
            jump_if_condition_is_true: false,
            jump_target_when_true: BranchOffset::Placeholder,
            jump_target_when_false: loop_labels.next,
        };
        program.parameters.set_update_position(UpdatePos::Where);
        translate_condition_expr(
            program,
            &plan.table_references,
            &cond.expr,
            meta,
            &t_ctx.resolver,
        )?;
    }

    // can we cache the translated expressions by saving their register values so we can copy them
    // to the result registers later instead of re-evaluating them every time?
    let mut new_value_registers = Vec::with_capacity(plan.set_clauses.len());
    // Update indexes first. Columns that are updated will be translated from an expression and those who aren't modified will be
    // read from table. Mutiple value index key could be updated partially.
    for (index, index_cursor) in plan.indexes_to_update.iter().zip(index_cursors) {
        let index_record_reg_count = index.columns.len() + 1;
        let index_record_reg_start = program.alloc_registers(index_record_reg_count);
        for (idx, column) in index.columns.iter().enumerate() {
            if let Some((_, expr)) = plan.set_clauses.iter().find(|(i, _)| *i == idx) {
                new_value_registers.push(translate_expr(
                    program,
                    Some(&plan.table_references),
                    expr,
                    index_record_reg_start + idx,
                    &t_ctx.resolver,
                )?);
            } else {
                program.emit_insn(Insn::Column {
                    cursor_id,
                    column: column.pos_in_table,
                    dest: index_record_reg_start + idx,
                });
            }
        }
        program.emit_insn(Insn::RowId {
            cursor_id,
            dest: index_record_reg_start + index.columns.len(),
        });
        let index_record_reg = program.alloc_register();
        program.emit_insn(Insn::MakeRecord {
            start_reg: index_record_reg_start,
            count: index_record_reg_count,
            dest_reg: index_record_reg,
        });
        program.emit_insn(Insn::IdxInsert {
            cursor_id: index_cursor,
            record_reg: index_record_reg,
            unpacked_start: Some(index_record_reg_start),
            unpacked_count: Some(index_record_reg_count as u16),
            flags: IdxInsertFlags::new(),
        });
    }
    // we scan a column at a time, loading either the column's values, or the new value
    // from the Set expression, into registers so we can emit a MakeRecord and update the row.
    let start = if is_virtual { beg + 2 } else { beg + 1 };
    for idx in 0..table_ref.columns().len() {
        let target_reg = start + idx;
        if let Some((idx, (_, expr))) = plan
            .set_clauses
            .iter()
            .enumerate()
            .find(|(_, (i, _))| *i == idx)
        {
            // set the parameter's update context so it can map it to the proper param index
            program.parameters.set_update_position(UpdatePos::Set(idx));
            translate_expr(
                program,
                Some(&plan.table_references),
                expr,
                target_reg,
                &t_ctx.resolver,
            )?;
        } else {
            let table_column = table_ref.table.columns().get(idx).unwrap();
            let column_idx_in_index = index.as_ref().and_then(|(idx, _)| {
                idx.columns
                    .iter()
                    .position(|c| Some(&c.name) == table_column.name.as_ref())
            });

            // don't emit null for pkey of virtual tables. they require first two args
            // before the 'record' to be explicitly non-null
            if table_column.is_rowid_alias && !is_virtual {
                program.emit_null(target_reg, None);
            } else if is_virtual {
                program.emit_insn(Insn::VColumn {
                    cursor_id,
                    column: idx,
                    dest: target_reg,
                });
            } else {
                program.emit_insn(Insn::Column {
                    cursor_id: *index
                        .as_ref()
                        .and_then(|(_, id)| {
                            if column_idx_in_index.is_some() {
                                Some(id)
                            } else {
                                None
                            }
                        })
                        .unwrap_or(&cursor_id),
                    column: column_idx_in_index.unwrap_or(idx),
                    dest: target_reg,
                });
            }
        }
    }
    if let Some(btree_table) = table_ref.btree() {
        if btree_table.is_strict {
            program.emit_insn(Insn::TypeCheck {
                start_reg: start,
                count: table_ref.columns().len(),
                check_generated: true,
                table_reference: Rc::clone(&btree_table),
            });
        }
        let record_reg = program.alloc_register();
        program.emit_insn(Insn::MakeRecord {
            start_reg: start,
            count: table_ref.columns().len(),
            dest_reg: record_reg,
        });
        program.emit_insn(Insn::Insert {
            cursor: cursor_id,
            key_reg: beg,
            record_reg,
            flag: 0,
            table_name: table_ref.identifier.clone(),
        });
    } else if let Some(vtab) = table_ref.virtual_table() {
        let arg_count = table_ref.columns().len() + 2;
        program.emit_insn(Insn::VUpdate {
            cursor_id,
            arg_count,
            start_reg: beg,
            vtab_ptr: vtab.implementation.as_ref().ctx as usize,
            conflict_action: 0u16,
        });
    }

    if let Some(limit_reg) = t_ctx.reg_limit {
        program.emit_insn(Insn::DecrJumpZero {
            reg: limit_reg,
            target_pc: t_ctx.label_main_loop_end.unwrap(),
        })
    }
    // TODO(pthorpe): handle RETURNING clause
    Ok(())
}
