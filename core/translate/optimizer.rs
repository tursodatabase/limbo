use std::{cell::RefCell, cmp::Ordering, collections::HashMap, sync::Arc};

use limbo_sqlite3_parser::ast::{self, Expr, SortOrder};

use crate::{
    schema::{Index, IndexColumn, Schema, Table},
    translate::plan::TerminationKey,
    types::SeekOp,
    util::exprs_are_equivalent,
    Result,
};

use super::{
    emitter::Resolver,
    plan::{
        DeletePlan, EvalAt, GroupBy, IterationDirection, JoinOrderMember, Operation, Plan, Search,
        SeekDef, SeekKey, SelectPlan, TableReference, UpdatePlan, WhereTerm,
    },
    planner::{determine_where_to_eval_expr, table_mask_from_expr, TableMask},
};

pub fn optimize_plan(plan: &mut Plan, schema: &Schema) -> Result<()> {
    match plan {
        Plan::Select(plan) => optimize_select_plan(plan, schema),
        Plan::Delete(plan) => optimize_delete_plan(plan, schema),
        Plan::Update(plan) => optimize_update_plan(plan, schema),
    }
}

/**
 * Make a few passes over the plan to optimize it.
 * TODO: these could probably be done in less passes,
 * but having them separate makes them easier to understand
 */
fn optimize_select_plan(plan: &mut SelectPlan, schema: &Schema) -> Result<()> {
    optimize_subqueries(plan, schema)?;
    rewrite_exprs_select(plan)?;
    if let ConstantConditionEliminationResult::ImpossibleCondition =
        eliminate_constant_conditions(&mut plan.where_clause)?
    {
        plan.contains_constant_false_condition = true;
        return Ok(());
    }

    let best_join_order = use_indexes(
        &mut plan.table_references,
        &schema.indexes,
        &mut plan.where_clause,
        &mut plan.order_by,
        &mut plan.group_by,
    )?;

    if let Some(best_join_order) = best_join_order {
        plan.join_order = best_join_order;
    }

    Ok(())
}

fn optimize_delete_plan(plan: &mut DeletePlan, schema: &Schema) -> Result<()> {
    rewrite_exprs_delete(plan)?;
    if let ConstantConditionEliminationResult::ImpossibleCondition =
        eliminate_constant_conditions(&mut plan.where_clause)?
    {
        plan.contains_constant_false_condition = true;
        return Ok(());
    }

    let _ = use_indexes(
        &mut plan.table_references,
        &schema.indexes,
        &mut plan.where_clause,
        &mut plan.order_by,
        &mut None,
    )?;

    Ok(())
}

fn optimize_update_plan(plan: &mut UpdatePlan, schema: &Schema) -> Result<()> {
    rewrite_exprs_update(plan)?;
    if let ConstantConditionEliminationResult::ImpossibleCondition =
        eliminate_constant_conditions(&mut plan.where_clause)?
    {
        plan.contains_constant_false_condition = true;
        return Ok(());
    }
    let _ = use_indexes(
        &mut plan.table_references,
        &schema.indexes,
        &mut plan.where_clause,
        &mut plan.order_by,
        &mut None,
    )?;
    Ok(())
}

fn optimize_subqueries(plan: &mut SelectPlan, schema: &Schema) -> Result<()> {
    for table in plan.table_references.iter_mut() {
        if let Operation::Subquery { plan, .. } = &mut table.op {
            optimize_select_plan(&mut *plan, schema)?;
        }
    }

    Ok(())
}

/// Represents an n-ary join, anywhere from 1 table to N tables.
#[derive(Debug, Clone)]
struct JoinN {
    /// Identifiers of the tables in the best_plan
    pub table_numbers: Vec<usize>,
    /// The best access methods for the best_plans
    pub best_access_methods: Vec<usize>,
    /// The estimated number of rows returned by joining these n tables together.
    pub output_cardinality: usize,
    /// Estimated execution cost of this N-ary join.
    pub cost: Cost,
}

const SELECTIVITY_EQ: f64 = 0.01;
const SELECTIVITY_RANGE: f64 = 0.4;
const SELECTIVITY_OTHER: f64 = 0.9;

fn join_lhs_tables_to_rhs_table<'a>(
    lhs: Option<&JoinN>,
    rhs_table_number: usize,
    rhs_table_reference: &TableReference,
    where_clause: &Vec<WhereTerm>,
    constraints: &'a [Constraints],
    join_order: &[JoinOrderMember],
    maybe_order_target: Option<&OrderTarget>,
    access_methods_arena: &'a RefCell<Vec<AccessMethod<'a>>>,
) -> Result<JoinN> {
    let loop_idx = lhs.map_or(0, |l| l.table_numbers.len());
    // Estimate based on the WHERE clause terms how much the different filters will reduce the output set.
    let output_cardinality_multiplier = where_clause
        .iter()
        .filter(|term| is_potential_index_constraint(term, loop_idx, &join_order))
        .map(|term| {
            let ast::Expr::Binary(lhs, op, rhs) = &term.expr else {
                return 1.0;
            };
            let mut column = if let ast::Expr::Column { table, column, .. } = lhs.as_ref() {
                if *table != rhs_table_number {
                    None
                } else {
                    let columns = rhs_table_reference.columns();
                    Some(&columns[*column])
                }
            } else {
                None
            };
            if column.is_none() {
                column = if let ast::Expr::Column { table, column, .. } = rhs.as_ref() {
                    if *table != rhs_table_number {
                        None
                    } else {
                        let columns = rhs_table_reference.columns();
                        Some(&columns[*column])
                    }
                } else {
                    None
                }
            };
            let Some(column) = column else {
                return 1.0;
            };
            match op {
                ast::Operator::Equals => {
                    if column.is_rowid_alias || column.primary_key {
                        1.0 / ESTIMATED_HARDCODED_ROWS_PER_TABLE as f64
                    } else {
                        SELECTIVITY_EQ
                    }
                }
                ast::Operator::Greater => SELECTIVITY_RANGE,
                ast::Operator::GreaterEquals => SELECTIVITY_RANGE,
                ast::Operator::Less => SELECTIVITY_RANGE,
                ast::Operator::LessEquals => SELECTIVITY_RANGE,
                _ => SELECTIVITY_OTHER,
            }
        })
        .product::<f64>();

    // Produce a number of rows estimated to be returned when this table is filtered by the WHERE clause.
    // If there is an input best_plan on the left, we multiply the input cardinality by the estimated number of rows per table.
    let input_cardinality = lhs.map_or(1, |l| l.output_cardinality);
    let output_cardinality = (input_cardinality as f64
        * ESTIMATED_HARDCODED_ROWS_PER_TABLE as f64
        * output_cardinality_multiplier)
        .ceil() as usize;

    let best_access_method = find_best_access_method_for_join_order(
        rhs_table_number,
        rhs_table_reference,
        constraints,
        &join_order,
        maybe_order_target,
        input_cardinality as f64,
    )?;

    let lhs_cost = lhs.map_or(Cost(0.0), |l| l.cost);
    let cost = lhs_cost + best_access_method.cost.total();

    let new_numbers = lhs.map_or(vec![rhs_table_number], |l| {
        let mut numbers = l.table_numbers.clone();
        numbers.push(rhs_table_number);
        numbers
    });

    access_methods_arena.borrow_mut().push(best_access_method);
    let mut best_access_methods = lhs.map_or(vec![], |l| l.best_access_methods.clone());
    best_access_methods.push(access_methods_arena.borrow().len() - 1);
    Ok(JoinN {
        table_numbers: new_numbers,
        best_access_methods,
        output_cardinality,
        cost,
    })
}

#[derive(Debug, Clone)]
pub struct AccessMethod<'a> {
    // The estimated number of page fetches.
    // We are ignoring CPU cost for now.
    pub cost: ScanCost,
    pub kind: AccessMethodKind<'a>,
}

impl<'a> AccessMethod<'a> {
    pub fn set_iter_dir(&mut self, new_dir: IterationDirection) {
        match &mut self.kind {
            AccessMethodKind::Scan { iter_dir, .. } => *iter_dir = new_dir,
            AccessMethodKind::Search { iter_dir, .. } => *iter_dir = new_dir,
        }
    }

    pub fn set_constraints(&mut self, index: Option<Arc<Index>>, constraints: &'a [Constraint]) {
        match (&mut self.kind, constraints.is_empty()) {
            (
                AccessMethodKind::Search {
                    constraints,
                    index: i,
                    ..
                },
                false,
            ) => {
                *constraints = constraints;
                *i = index;
            }
            (AccessMethodKind::Search { iter_dir, .. }, true) => {
                self.kind = AccessMethodKind::Scan {
                    index,
                    iter_dir: *iter_dir,
                };
            }
            (AccessMethodKind::Scan { iter_dir, .. }, false) => {
                self.kind = AccessMethodKind::Search {
                    index,
                    iter_dir: *iter_dir,
                    constraints,
                };
            }
            (AccessMethodKind::Scan { index: i, .. }, true) => {
                *i = index;
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum AccessMethodKind<'a> {
    /// A full scan, which can be an index scan or a table scan.
    Scan {
        index: Option<Arc<Index>>,
        iter_dir: IterationDirection,
    },
    /// A search, which can be an index seek or a rowid-based search.
    Search {
        index: Option<Arc<Index>>,
        iter_dir: IterationDirection,
        constraints: &'a [Constraint],
    },
}

/// Iterator that generates all possible size k bitmasks for a given number of tables.
/// For example, given: 3 tables and k=2, the bitmasks are:
/// - 0b011 (tables 0, 1)
/// - 0b101 (tables 0, 2)
/// - 0b110 (tables 1, 2)
///
/// This is used in the dynamic programming approach to finding the best way to join a subset of N tables.
struct JoinBitmaskIter {
    current: u128,
    max_exclusive: u128,
}

impl JoinBitmaskIter {
    fn new(table_number_max_exclusive: usize, how_many: usize) -> Self {
        Self {
            current: (1 << how_many) - 1, // Start with smallest k-bit number (e.g., 000111 for k=3)
            max_exclusive: 1 << table_number_max_exclusive,
        }
    }
}

impl Iterator for JoinBitmaskIter {
    type Item = TableMask;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current >= self.max_exclusive {
            return None;
        }

        let result = TableMask::from_bits(self.current);

        // Gosper's hack: compute next k-bit combination in lexicographic order
        let c = self.current & (!self.current + 1); // rightmost set bit
        let r = self.current + c; // add it to get a carry
        let ones = self.current ^ r; // changed bits
        let ones = (ones >> 2) / c; // right-adjust shifted bits
        self.current = r | ones; // form the next combination

        Some(result)
    }
}

/// Generate all possible bitmasks of size `how_many` for a given number of tables.
fn generate_join_bitmasks(table_number_max_exclusive: usize, how_many: usize) -> JoinBitmaskIter {
    JoinBitmaskIter::new(table_number_max_exclusive, how_many)
}

/// Check if the plan's row iteration order matches the [OrderTarget]'s column order
fn plan_satisfies_order_target(
    plan: &JoinN,
    access_methods_arena: &RefCell<Vec<AccessMethod>>,
    table_references: &[TableReference],
    order_target: &OrderTarget,
) -> bool {
    let mut target_col_idx = 0;
    for (i, table_no) in plan.table_numbers.iter().enumerate() {
        let table_ref = &table_references[*table_no];
        // Check if this table has an access method that provides ordering
        let access_method = &access_methods_arena.borrow()[plan.best_access_methods[i]];
        match &access_method.kind {
            AccessMethodKind::Scan {
                index: None,
                iter_dir,
            } => {
                let rowid_alias_col = table_ref
                    .table
                    .columns()
                    .iter()
                    .position(|c| c.is_rowid_alias);
                let Some(rowid_alias_col) = rowid_alias_col else {
                    return false;
                };
                let target_col = &order_target.0[target_col_idx];
                let order_matches = if *iter_dir == IterationDirection::Forwards {
                    target_col.order == SortOrder::Asc
                } else {
                    target_col.order == SortOrder::Desc
                };
                if target_col.table_no != *table_no
                    || target_col.column_no != rowid_alias_col
                    || !order_matches
                {
                    return false;
                }
                target_col_idx += 1;
                if target_col_idx == order_target.0.len() {
                    return true;
                }
            }
            AccessMethodKind::Scan {
                index: Some(index),
                iter_dir,
            } => {
                // The index columns must match the order target columns for this table
                for index_col in index.columns.iter() {
                    let target_col = &order_target.0[target_col_idx];
                    let order_matches = if *iter_dir == IterationDirection::Forwards {
                        target_col.order == index_col.order
                    } else {
                        target_col.order != index_col.order
                    };
                    if target_col.table_no != *table_no
                        || target_col.column_no != index_col.pos_in_table
                        || !order_matches
                    {
                        return false;
                    }
                    target_col_idx += 1;
                    if target_col_idx == order_target.0.len() {
                        return true;
                    }
                }
            }
            AccessMethodKind::Search {
                index, iter_dir, ..
            } => {
                if let Some(index) = index {
                    for index_col in index.columns.iter() {
                        let target_col = &order_target.0[target_col_idx];
                        let order_matches = if *iter_dir == IterationDirection::Forwards {
                            target_col.order == index_col.order
                        } else {
                            target_col.order != index_col.order
                        };
                        if target_col.table_no != *table_no
                            || target_col.column_no != index_col.pos_in_table
                            || !order_matches
                        {
                            return false;
                        }
                        target_col_idx += 1;
                        if target_col_idx == order_target.0.len() {
                            return true;
                        }
                    }
                } else {
                    let rowid_alias_col = table_ref
                        .table
                        .columns()
                        .iter()
                        .position(|c| c.is_rowid_alias);
                    let Some(rowid_alias_col) = rowid_alias_col else {
                        return false;
                    };
                    let target_col = &order_target.0[target_col_idx];
                    let order_matches = if *iter_dir == IterationDirection::Forwards {
                        target_col.order == SortOrder::Asc
                    } else {
                        target_col.order == SortOrder::Desc
                    };
                    if target_col.table_no != *table_no
                        || target_col.column_no != rowid_alias_col
                        || !order_matches
                    {
                        return false;
                    }
                    target_col_idx += 1;
                    if target_col_idx == order_target.0.len() {
                        return true;
                    }
                }
            }
        }
    }
    false
}

/// The result of [compute_best_join_order].
#[derive(Debug)]
struct BestJoinOrderResult {
    /// The best plan overall.
    best_plan: JoinN,
    /// The best plan for the given order target, if it isn't the overall best.
    best_ordered_plan: Option<JoinN>,
}

/// Compute the best way to join a given set of tables.
/// Returns the best [JoinN] if one exists, otherwise returns None.
fn compute_best_join_order<'a>(
    table_references: &[TableReference],
    where_clause: &Vec<WhereTerm>,
    maybe_order_target: Option<&OrderTarget>,
    constraints: &'a [Constraints],
    access_methods_arena: &'a RefCell<Vec<AccessMethod<'a>>>,
) -> Result<Option<BestJoinOrderResult>> {
    // Skip work if we have no tables to consider.
    if table_references.is_empty() {
        return Ok(None);
    }

    let num_tables = table_references.len();

    // Compute naive left-to-right plan to use as pruning threshold
    let naive_plan = compute_naive_left_deep_plan(
        table_references,
        where_clause,
        maybe_order_target,
        access_methods_arena,
        &constraints,
    )?;

    // Keep track of both 1. the best plan overall (not considering sorting), and 2. the best ordered plan (which might not be the same).
    // We assign Some Cost (tm) to any required sort operation, so the best ordered plan may end up being
    // the one we choose, if the cost reduction from avoiding sorting brings it below the cost of the overall best one.
    let mut best_ordered_plan: Option<JoinN> = None;
    let mut best_plan_is_also_ordered = if let Some(ref order_target) = maybe_order_target {
        plan_satisfies_order_target(
            &naive_plan,
            &access_methods_arena,
            table_references,
            order_target,
        )
    } else {
        false
    };

    // If we have one table, then the "naive left-to-right plan" is always the best.
    if table_references.len() == 1 {
        return Ok(Some(BestJoinOrderResult {
            best_plan: naive_plan,
            best_ordered_plan: None,
        }));
    }
    let mut best_plan = naive_plan;

    // Reuse a single mutable join order to avoid allocating join orders per permutation.
    let mut join_order = Vec::with_capacity(num_tables);
    join_order.push(JoinOrderMember {
        table_no: 0,
        is_outer: false,
    });

    // Keep track of the current best cost so we can short-circuit planning for subplans
    // that already exceed the cost of the current best plan.
    let cost_upper_bound = best_plan.cost;
    let cost_upper_bound_ordered = {
        if best_plan_is_also_ordered {
            cost_upper_bound
        } else {
            Cost(f64::MAX)
        }
    };

    // Keep track of the best plan for a given subset of tables.
    // Consider this example: we have tables a,b,c,d to join.
    // if we find that 'b JOIN a' is better than 'a JOIN b', then we don't need to even try
    // to do 'a JOIN b JOIN c', because we know 'b JOIN a JOIN c' is going to be better.
    // This is due to the commutativity and associativity of inner joins.
    let mut best_plan_memo: HashMap<TableMask, JoinN> = HashMap::new();

    // Dynamic programming base case: calculate the best way to access each single table, as if
    // there were no other tables.
    for i in 0..num_tables {
        let mut mask = TableMask::new();
        mask.add_table(i);
        let table_ref = &table_references[i];
        join_order[0] = JoinOrderMember {
            table_no: i,
            is_outer: false,
        };
        assert!(join_order.len() == 1);
        let rel = join_lhs_tables_to_rhs_table(
            None,
            i,
            table_ref,
            where_clause,
            &constraints,
            &join_order,
            maybe_order_target,
            access_methods_arena,
        )?;
        best_plan_memo.insert(mask, rel);
    }
    join_order.clear();

    // As mentioned, inner joins are commutative. Outer joins are NOT.
    // Example:
    // "a LEFT JOIN b" can NOT be reordered as "b LEFT JOIN a".
    // If there are outer joins in the plan, ensure correct ordering.
    let left_join_illegal_map = {
        let left_join_count = table_references
            .iter()
            .filter(|t| t.join_info.as_ref().map_or(false, |j| j.outer))
            .count();
        if left_join_count == 0 {
            None
        } else {
            // map from rhs table index to lhs table index
            let mut left_join_illegal_map: HashMap<usize, TableMask> =
                HashMap::with_capacity(left_join_count);
            for (i, _) in table_references.iter().enumerate() {
                for j in i + 1..table_references.len() {
                    if table_references[j]
                        .join_info
                        .as_ref()
                        .map_or(false, |j| j.outer)
                    {
                        // bitwise OR the masks
                        if let Some(illegal_lhs) = left_join_illegal_map.get_mut(&i) {
                            illegal_lhs.add_table(j);
                        } else {
                            let mut mask = TableMask::new();
                            mask.add_table(j);
                            left_join_illegal_map.insert(i, mask);
                        }
                    }
                }
            }
            Some(left_join_illegal_map)
        }
    };

    // Now that we have our single-table base cases, we can start considering join subsets of 2 tables and more.
    // Try to join each single table to each other table.
    for subset_size in 2..=num_tables {
        for mask in generate_join_bitmasks(num_tables, subset_size) {
            // Keep track of the best way to join this subset of tables.
            // Take the (a,b,c,d) example from above:
            // E.g. for "a JOIN b JOIN c", the possibilities are (a,b,c), (a,c,b), (b,a,c) and so on.
            // If we find out (b,a,c) is the best way to join these three, then we ONLY need to compute
            // the cost of (b,a,c,d) in the final step, because (a,b,c,d) (and all others) are guaranteed to be worse.
            let mut best_for_mask: Option<JoinN> = None;
            // also keep track of the best plan for this subset that orders the rows in an Interesting Way (tm),
            // i.e. allows us to eliminate sort operations downstream.
            let (mut best_ordered_for_mask, mut best_for_mask_is_also_ordered) = (None, false);

            // Try to join all subsets (masks) with all other tables.
            // In this block, LHS is always (n-1) tables, and RHS is a single table.
            for rhs_idx in 0..num_tables {
                // If the RHS table isn't a member of this join subset, skip.
                if !mask.contains_table(rhs_idx) {
                    continue;
                }

                // If there are no other tables except RHS, skip.
                let lhs_mask = mask.without_table(rhs_idx);
                if lhs_mask.is_empty() {
                    continue;
                }

                // If this join ordering would violate LEFT JOIN ordering restrictions, skip.
                if let Some(illegal_lhs) = left_join_illegal_map
                    .as_ref()
                    .and_then(|deps| deps.get(&rhs_idx))
                {
                    let legal = !lhs_mask.intersects(illegal_lhs);
                    if !legal {
                        continue; // Don't allow RHS before its LEFT in LEFT JOIN
                    }
                }

                // If the already cached plan for this subset was too crappy to consider,
                // then joining it with RHS won't help. Skip.
                let Some(lhs) = best_plan_memo.get(&lhs_mask) else {
                    continue;
                };

                // Build a JoinOrder out of the table bitmask we are now considering.
                for table_no in lhs.table_numbers.iter() {
                    join_order.push(JoinOrderMember {
                        table_no: *table_no,
                        is_outer: table_references[*table_no]
                            .join_info
                            .as_ref()
                            .map_or(false, |j| j.outer),
                    });
                }
                join_order.push(JoinOrderMember {
                    table_no: rhs_idx,
                    is_outer: table_references[rhs_idx]
                        .join_info
                        .as_ref()
                        .map_or(false, |j| j.outer),
                });
                assert!(join_order.len() == subset_size);

                // Calculate the best way to join LHS with RHS.
                let rel = join_lhs_tables_to_rhs_table(
                    Some(lhs),
                    rhs_idx,
                    &table_references[rhs_idx],
                    where_clause,
                    &constraints,
                    &join_order,
                    maybe_order_target,
                    access_methods_arena,
                )?;
                join_order.clear();

                // Since cost_upper_bound_ordered is always >= to cost_upper_bound,
                // if the cost we calculated for this plan is worse than cost_upper_bound_ordered,
                // this join subset is already worse than our best plan for the ENTIRE query, so skip.
                if rel.cost >= cost_upper_bound_ordered {
                    continue;
                }

                let satisfies_order_target = if let Some(ref order_target) = maybe_order_target {
                    plan_satisfies_order_target(
                        &rel,
                        &access_methods_arena,
                        table_references,
                        order_target,
                    )
                } else {
                    false
                };

                // If this plan is worse than our overall best, it might still be the best ordered plan.
                if rel.cost >= cost_upper_bound {
                    // But if it isn't, skip.
                    if !satisfies_order_target {
                        continue;
                    }
                    let existing_ordered_cost: Cost = best_ordered_for_mask
                        .as_ref()
                        .map_or(Cost(f64::MAX), |p: &JoinN| p.cost);
                    if rel.cost < existing_ordered_cost {
                        best_ordered_for_mask = Some(rel);
                    }
                } else if best_for_mask.is_none() || rel.cost < best_for_mask.as_ref().unwrap().cost
                {
                    best_for_mask = Some(rel);
                    best_for_mask_is_also_ordered = satisfies_order_target;
                }
            }

            if let Some(rel) = best_ordered_for_mask.take() {
                let cost = rel.cost;
                let has_all_tables = mask.table_count() == num_tables;
                if has_all_tables && cost_upper_bound_ordered > cost {
                    best_ordered_plan = Some(rel);
                }
            }

            if let Some(rel) = best_for_mask.take() {
                let cost = rel.cost;
                let has_all_tables = mask.table_count() == num_tables;
                if has_all_tables {
                    if cost_upper_bound > cost {
                        best_plan = rel;
                        best_plan_is_also_ordered = best_for_mask_is_also_ordered;
                    }
                } else {
                    best_plan_memo.insert(mask, rel);
                }
            }
        }
    }

    Ok(Some(BestJoinOrderResult {
        best_plan,
        best_ordered_plan: if best_plan_is_also_ordered {
            None
        } else {
            best_ordered_plan
        },
    }))
}

/// Specialized version of [compute_best_join_order] that just joins tables in the order they are given
/// in the SQL query. This is used as an upper bound for any other plans -- we can give up enumerating
/// permutations if they exceed this cost during enumeration.
fn compute_naive_left_deep_plan<'a>(
    table_references: &[TableReference],
    where_clause: &Vec<WhereTerm>,
    maybe_order_target: Option<&OrderTarget>,
    access_methods_arena: &'a RefCell<Vec<AccessMethod<'a>>>,
    constraints: &'a [Constraints],
) -> Result<JoinN> {
    let n = table_references.len();
    assert!(n > 0);

    let join_order = table_references
        .iter()
        .enumerate()
        .map(|(i, t)| JoinOrderMember {
            table_no: i,
            is_outer: t.join_info.as_ref().map_or(false, |j| j.outer),
        })
        .collect::<Vec<_>>();

    // Start with first table
    let mut best_plan = join_lhs_tables_to_rhs_table(
        None,
        0,
        &table_references[0],
        where_clause,
        constraints,
        &join_order[..1],
        maybe_order_target,
        access_methods_arena,
    )?;

    // Add remaining tables one at a time from left to right
    for i in 1..n {
        best_plan = join_lhs_tables_to_rhs_table(
            Some(&best_plan),
            i,
            &table_references[i],
            where_clause,
            constraints,
            &join_order[..i + 1],
            maybe_order_target,
            access_methods_arena,
        )?;
    }

    Ok(best_plan)
}

#[derive(Debug, PartialEq, Clone)]
struct ColumnOrder {
    table_no: usize,
    column_no: usize,
    order: SortOrder,
}

#[derive(Debug, PartialEq, Clone)]
enum EliminatesSort {
    GroupBy,
    OrderBy,
    GroupByAndOrderBy,
}

#[derive(Debug, PartialEq, Clone)]
pub struct OrderTarget(Vec<ColumnOrder>, EliminatesSort);

impl OrderTarget {
    fn maybe_from_iterator<'a>(
        list: impl Iterator<Item = (&'a ast::Expr, SortOrder)> + Clone,
        eliminates_sort: EliminatesSort,
    ) -> Option<Self> {
        if list.clone().count() == 0 {
            return None;
        }
        if list
            .clone()
            .any(|(expr, _)| !matches!(expr, ast::Expr::Column { .. }))
        {
            return None;
        }
        Some(OrderTarget(
            list.map(|(expr, order)| {
                let ast::Expr::Column { table, column, .. } = expr else {
                    unreachable!();
                };
                ColumnOrder {
                    table_no: *table,
                    column_no: *column,
                    order,
                }
            })
            .collect(),
            eliminates_sort,
        ))
    }
}

/// Compute an [OrderTarget] for the join optimizer to use.
/// Ideally, a join order is both efficient in joining the tables
/// but also returns the results in an order that minimizes the amount of
/// sorting that needs to be done later (either in GROUP BY, ORDER BY, or both).
///
/// TODO: this does not currently handle the case where we definitely cannot eliminate
/// the ORDER BY sorter, but we could still eliminate the GROUP BY sorter.
fn compute_order_target(
    order_by: &Option<Vec<(ast::Expr, SortOrder)>>,
    group_by: Option<&mut GroupBy>,
) -> Option<OrderTarget> {
    match (order_by, group_by) {
        // No ordering demands - we don't care what order the joined result rows are in
        (None, None) => None,
        // Only ORDER BY - we would like the joined result rows to be in the order specified by the ORDER BY
        (Some(order_by), None) => OrderTarget::maybe_from_iterator(
            order_by.iter().map(|(expr, order)| (expr, *order)),
            EliminatesSort::OrderBy,
        ),
        // Only GROUP BY - we would like the joined result rows to be in the order specified by the GROUP BY
        (None, Some(group_by)) => OrderTarget::maybe_from_iterator(
            group_by.exprs.iter().map(|expr| (expr, SortOrder::Asc)),
            EliminatesSort::GroupBy,
        ),
        // Both ORDER BY and GROUP BY:
        // If the GROUP BY does not contain all the expressions in the ORDER BY,
        // then we must separately sort the result rows for ORDER BY anyway.
        // However, in that case we can use the GROUP BY expressions as the target order for the join,
        // so that we don't have to sort twice.
        //
        // If the GROUP BY contains all the expressions in the ORDER BY,
        // then we again can use the GROUP BY expressions as the target order for the join;
        // however in this case we must take the ASC/DESC from ORDER BY into account.
        (Some(order_by), Some(group_by)) => {
            // Does the group by contain all expressions in the order by?
            let group_by_contains_all = group_by.exprs.iter().all(|expr| {
                order_by
                    .iter()
                    .any(|(order_by_expr, _)| exprs_are_equivalent(expr, order_by_expr))
            });
            // If not, let's try to target an ordering that matches the group by -- we don't care about ASC/DESC
            if !group_by_contains_all {
                return OrderTarget::maybe_from_iterator(
                    group_by.exprs.iter().map(|expr| (expr, SortOrder::Asc)),
                    EliminatesSort::GroupBy,
                );
            }
            // If yes, let's try to target an ordering that matches the GROUP BY columns,
            // but the ORDER BY orderings. First, we need to reorder the GROUP BY columns to match the ORDER BY columns.
            group_by.exprs.sort_by_key(|expr| {
                order_by
                    .iter()
                    .position(|(order_by_expr, _)| exprs_are_equivalent(expr, order_by_expr))
                    .map_or(usize::MAX, |i| i)
            });
            // Iterate over GROUP BY, but take the ORDER BY orderings into account.
            OrderTarget::maybe_from_iterator(
                group_by
                    .exprs
                    .iter()
                    .zip(
                        order_by
                            .iter()
                            .map(|(_, dir)| dir)
                            .chain(std::iter::repeat(&SortOrder::Asc)),
                    )
                    .map(|(expr, dir)| (expr, *dir)),
                EliminatesSort::GroupByAndOrderBy,
            )
        }
    }
}

fn use_indexes(
    table_references: &mut [TableReference],
    available_indexes: &HashMap<String, Vec<Arc<Index>>>,
    where_clause: &mut Vec<WhereTerm>,
    order_by: &mut Option<Vec<(ast::Expr, SortOrder)>>,
    group_by: &mut Option<GroupBy>,
) -> Result<Option<Vec<JoinOrderMember>>> {
    let access_methods_arena = RefCell::new(Vec::new());
    let maybe_order_target = compute_order_target(order_by, group_by.as_mut());
    let constraints =
        constraints_from_where_clause(where_clause, table_references, available_indexes)?;
    let Some(best_join_order_result) = compute_best_join_order(
        table_references,
        where_clause,
        maybe_order_target.as_ref(),
        &constraints,
        &access_methods_arena,
    )?
    else {
        return Ok(None);
    };

    let BestJoinOrderResult {
        best_plan,
        best_ordered_plan,
    } = best_join_order_result;

    let best_plan = if let Some(best_ordered_plan) = best_ordered_plan {
        let best_unordered_plan_cost = best_plan.cost;
        let best_ordered_plan_cost = best_ordered_plan.cost;
        const SORT_COST_PER_ROW_MULTIPLIER: f64 = 0.001;
        let sorting_penalty =
            Cost(best_plan.output_cardinality as f64 * SORT_COST_PER_ROW_MULTIPLIER);
        if best_unordered_plan_cost + sorting_penalty > best_ordered_plan_cost {
            best_ordered_plan
        } else {
            best_plan
        }
    } else {
        best_plan
    };

    if let Some(order_target) = maybe_order_target {
        let satisfies_order_target = plan_satisfies_order_target(
            &best_plan,
            &access_methods_arena,
            table_references,
            &order_target,
        );
        if satisfies_order_target {
            match order_target.1 {
                EliminatesSort::GroupBy => {
                    let _ = group_by.as_mut().and_then(|g| g.sort_order.take());
                }
                EliminatesSort::OrderBy => {
                    let _ = order_by.take();
                }
                EliminatesSort::GroupByAndOrderBy => {
                    let _ = group_by.as_mut().and_then(|g| g.sort_order.take());
                    let _ = order_by.take();
                }
            }
        }
    }

    let (best_access_methods, best_table_numbers) =
        (best_plan.best_access_methods, best_plan.table_numbers);
    let mut to_remove_from_where_clause = vec![];
    for (i, table_number) in best_table_numbers.iter().enumerate() {
        let access_method_kind = access_methods_arena.borrow()[best_access_methods[i]]
            .kind
            .clone();
        if matches!(
            table_references[*table_number].op,
            Operation::Subquery { .. }
        ) {
            // FIXME: Operation::Subquery shouldn't exist. It's not an operation, it's a kind of temporary table.
            assert!(
                matches!(access_method_kind, AccessMethodKind::Scan { index: None, .. }),
                "nothing in the current optimizer should be able to optimize subqueries, but got {:?} for table {}",
                access_method_kind,
                table_references[*table_number].table.get_name()
            );
            continue;
        }
        table_references[*table_number].op = match access_method_kind {
            AccessMethodKind::Scan { iter_dir, index } => Operation::Scan { iter_dir, index },
            AccessMethodKind::Search {
                index,
                constraints,
                iter_dir,
            } => {
                assert!(!constraints.is_empty());
                for constraint in constraints.iter() {
                    to_remove_from_where_clause.push(constraint.where_clause_position.0);
                }
                if let Some(index) = index {
                    Operation::Search(Search::Seek {
                        index: Some(index),
                        seek_def: build_seek_def_from_constraints(
                            constraints,
                            iter_dir,
                            where_clause,
                        )?,
                    })
                } else {
                    assert!(
                        constraints.len() == 1,
                        "expected exactly one constraint for rowid seek, got {:?}",
                        constraints
                    );
                    match constraints[0].operator {
                        ast::Operator::Equals => Operation::Search(Search::RowidEq {
                            cmp_expr: {
                                let (idx, side) = constraints[0].where_clause_position;
                                let ast::Expr::Binary(lhs, _, rhs) =
                                    unwrap_parens(&where_clause[idx].expr)?
                                else {
                                    panic!("Expected a binary expression");
                                };
                                let where_term = WhereTerm {
                                    expr: match side {
                                        BinaryExprSide::Lhs => lhs.as_ref().clone(),
                                        BinaryExprSide::Rhs => rhs.as_ref().clone(),
                                    },
                                    from_outer_join: where_clause[idx].from_outer_join.clone(),
                                };
                                where_term
                            },
                        }),
                        _ => Operation::Search(Search::Seek {
                            index: None,
                            seek_def: build_seek_def_from_constraints(
                                constraints,
                                iter_dir,
                                where_clause,
                            )?,
                        }),
                    }
                }
            }
        };
    }
    to_remove_from_where_clause.sort_by_key(|c| *c);
    for position in to_remove_from_where_clause.iter().rev() {
        where_clause.remove(*position);
    }
    let best_join_order = best_table_numbers
        .into_iter()
        .map(|table_number| JoinOrderMember {
            table_no: table_number,
            is_outer: table_references[table_number]
                .join_info
                .as_ref()
                .map_or(false, |join_info| join_info.outer),
        })
        .collect();
    Ok(Some(best_join_order))
}

#[derive(Debug, PartialEq, Clone)]
enum ConstantConditionEliminationResult {
    Continue,
    ImpossibleCondition,
}

/// Removes predicates that are always true.
/// Returns a ConstantEliminationResult indicating whether any predicates are always false.
/// This is used to determine whether the query can be aborted early.
fn eliminate_constant_conditions(
    where_clause: &mut Vec<WhereTerm>,
) -> Result<ConstantConditionEliminationResult> {
    let mut i = 0;
    while i < where_clause.len() {
        let predicate = &where_clause[i];
        if predicate.expr.is_always_true()? {
            // true predicates can be removed since they don't affect the result
            where_clause.remove(i);
        } else if predicate.expr.is_always_false()? {
            // any false predicate in a list of conjuncts (AND-ed predicates) will make the whole list false,
            // except an outer join condition, because that just results in NULLs, not skipping the whole loop
            if predicate.from_outer_join.is_some() {
                i += 1;
                continue;
            }
            where_clause.truncate(0);
            return Ok(ConstantConditionEliminationResult::ImpossibleCondition);
        } else {
            i += 1;
        }
    }

    Ok(ConstantConditionEliminationResult::Continue)
}

fn rewrite_exprs_select(plan: &mut SelectPlan) -> Result<()> {
    for rc in plan.result_columns.iter_mut() {
        rewrite_expr(&mut rc.expr)?;
    }
    for agg in plan.aggregates.iter_mut() {
        rewrite_expr(&mut agg.original_expr)?;
    }
    for cond in plan.where_clause.iter_mut() {
        rewrite_expr(&mut cond.expr)?;
    }
    if let Some(group_by) = &mut plan.group_by {
        for expr in group_by.exprs.iter_mut() {
            rewrite_expr(expr)?;
        }
    }
    if let Some(order_by) = &mut plan.order_by {
        for (expr, _) in order_by.iter_mut() {
            rewrite_expr(expr)?;
        }
    }

    Ok(())
}

fn rewrite_exprs_delete(plan: &mut DeletePlan) -> Result<()> {
    for cond in plan.where_clause.iter_mut() {
        rewrite_expr(&mut cond.expr)?;
    }
    Ok(())
}

fn rewrite_exprs_update(plan: &mut UpdatePlan) -> Result<()> {
    if let Some(rc) = plan.returning.as_mut() {
        for rc in rc.iter_mut() {
            rewrite_expr(&mut rc.expr)?;
        }
    }
    for (_, expr) in plan.set_clauses.iter_mut() {
        rewrite_expr(expr)?;
    }
    for cond in plan.where_clause.iter_mut() {
        rewrite_expr(&mut cond.expr)?;
    }
    if let Some(order_by) = &mut plan.order_by {
        for (expr, _) in order_by.iter_mut() {
            rewrite_expr(expr)?;
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlwaysTrueOrFalse {
    AlwaysTrue,
    AlwaysFalse,
}

/**
  Helper trait for expressions that can be optimized
  Implemented for ast::Expr
*/
pub trait Optimizable {
    // if the expression is a constant expression that, when evaluated as a condition, is always true or false
    // return a [ConstantPredicate].
    fn check_always_true_or_false(&self) -> Result<Option<AlwaysTrueOrFalse>>;
    fn is_always_true(&self) -> Result<bool> {
        Ok(self
            .check_always_true_or_false()?
            .map_or(false, |c| c == AlwaysTrueOrFalse::AlwaysTrue))
    }
    fn is_always_false(&self) -> Result<bool> {
        Ok(self
            .check_always_true_or_false()?
            .map_or(false, |c| c == AlwaysTrueOrFalse::AlwaysFalse))
    }
    fn is_constant(&self, resolver: &Resolver<'_>) -> bool;
    fn is_nonnull(&self, tables: &[TableReference]) -> bool;
}

impl Optimizable for ast::Expr {
    /// Returns true if the expressions is (verifiably) non-NULL.
    /// It might still be non-NULL even if we return false; we just
    /// weren't able to prove it.
    /// This function is currently very conservative, and will return false
    /// for any expression where we aren't sure and didn't bother to find out
    /// by writing more complex code.
    fn is_nonnull(&self, tables: &[TableReference]) -> bool {
        match self {
            Expr::Between {
                lhs, start, end, ..
            } => lhs.is_nonnull(tables) && start.is_nonnull(tables) && end.is_nonnull(tables),
            Expr::Binary(expr, _, expr1) => expr.is_nonnull(tables) && expr1.is_nonnull(tables),
            Expr::Case {
                base,
                when_then_pairs,
                else_expr,
                ..
            } => {
                base.as_ref().map_or(true, |base| base.is_nonnull(tables))
                    && when_then_pairs
                        .iter()
                        .all(|(_, then)| then.is_nonnull(tables))
                    && else_expr
                        .as_ref()
                        .map_or(true, |else_expr| else_expr.is_nonnull(tables))
            }
            Expr::Cast { expr, .. } => expr.is_nonnull(tables),
            Expr::Collate(expr, _) => expr.is_nonnull(tables),
            Expr::DoublyQualified(..) => {
                panic!("Do not call is_nonnull before DoublyQualified has been rewritten as Column")
            }
            Expr::Exists(..) => false,
            Expr::FunctionCall { .. } => false,
            Expr::FunctionCallStar { .. } => false,
            Expr::Id(..) => panic!("Do not call is_nonnull before Id has been rewritten as Column"),
            Expr::Column {
                table,
                column,
                is_rowid_alias,
                ..
            } => {
                if *is_rowid_alias {
                    return true;
                }

                let table_ref = &tables[*table];
                let columns = table_ref.columns();
                let column = &columns[*column];
                return column.primary_key || column.notnull;
            }
            Expr::RowId { .. } => true,
            Expr::InList { lhs, rhs, .. } => {
                lhs.is_nonnull(tables)
                    && rhs
                        .as_ref()
                        .map_or(true, |rhs| rhs.iter().all(|rhs| rhs.is_nonnull(tables)))
            }
            Expr::InSelect { .. } => false,
            Expr::InTable { .. } => false,
            Expr::IsNull(..) => true,
            Expr::Like { lhs, rhs, .. } => lhs.is_nonnull(tables) && rhs.is_nonnull(tables),
            Expr::Literal(literal) => match literal {
                ast::Literal::Numeric(_) => true,
                ast::Literal::String(_) => true,
                ast::Literal::Blob(_) => true,
                ast::Literal::Keyword(_) => true,
                ast::Literal::Null => false,
                ast::Literal::CurrentDate => true,
                ast::Literal::CurrentTime => true,
                ast::Literal::CurrentTimestamp => true,
            },
            Expr::Name(..) => false,
            Expr::NotNull(..) => true,
            Expr::Parenthesized(exprs) => exprs.iter().all(|expr| expr.is_nonnull(tables)),
            Expr::Qualified(..) => {
                panic!("Do not call is_nonnull before Qualified has been rewritten as Column")
            }
            Expr::Raise(..) => false,
            Expr::Subquery(..) => false,
            Expr::Unary(_, expr) => expr.is_nonnull(tables),
            Expr::Variable(..) => false,
        }
    }
    /// Returns true if the expression is a constant i.e. does not depend on variables or columns etc.
    fn is_constant(&self, resolver: &Resolver<'_>) -> bool {
        match self {
            Expr::Between {
                lhs, start, end, ..
            } => {
                lhs.is_constant(resolver)
                    && start.is_constant(resolver)
                    && end.is_constant(resolver)
            }
            Expr::Binary(expr, _, expr1) => {
                expr.is_constant(resolver) && expr1.is_constant(resolver)
            }
            Expr::Case {
                base,
                when_then_pairs,
                else_expr,
            } => {
                base.as_ref()
                    .map_or(true, |base| base.is_constant(resolver))
                    && when_then_pairs.iter().all(|(when, then)| {
                        when.is_constant(resolver) && then.is_constant(resolver)
                    })
                    && else_expr
                        .as_ref()
                        .map_or(true, |else_expr| else_expr.is_constant(resolver))
            }
            Expr::Cast { expr, .. } => expr.is_constant(resolver),
            Expr::Collate(expr, _) => expr.is_constant(resolver),
            Expr::DoublyQualified(_, _, _) => {
                panic!("DoublyQualified should have been rewritten as Column")
            }
            Expr::Exists(_) => false,
            Expr::FunctionCall { args, name, .. } => {
                let Some(func) =
                    resolver.resolve_function(&name.0, args.as_ref().map_or(0, |args| args.len()))
                else {
                    return false;
                };
                func.is_deterministic()
                    && args.as_ref().map_or(true, |args| {
                        args.iter().all(|arg| arg.is_constant(resolver))
                    })
            }
            Expr::FunctionCallStar { .. } => false,
            Expr::Id(_) => panic!("Id should have been rewritten as Column"),
            Expr::Column { .. } => false,
            Expr::RowId { .. } => false,
            Expr::InList { lhs, rhs, .. } => {
                lhs.is_constant(resolver)
                    && rhs
                        .as_ref()
                        .map_or(true, |rhs| rhs.iter().all(|rhs| rhs.is_constant(resolver)))
            }
            Expr::InSelect { .. } => {
                false // might be constant, too annoying to check subqueries etc. implement later
            }
            Expr::InTable { .. } => false,
            Expr::IsNull(expr) => expr.is_constant(resolver),
            Expr::Like {
                lhs, rhs, escape, ..
            } => {
                lhs.is_constant(resolver)
                    && rhs.is_constant(resolver)
                    && escape
                        .as_ref()
                        .map_or(true, |escape| escape.is_constant(resolver))
            }
            Expr::Literal(_) => true,
            Expr::Name(_) => false,
            Expr::NotNull(expr) => expr.is_constant(resolver),
            Expr::Parenthesized(exprs) => exprs.iter().all(|expr| expr.is_constant(resolver)),
            Expr::Qualified(_, _) => {
                panic!("Qualified should have been rewritten as Column")
            }
            Expr::Raise(_, expr) => expr
                .as_ref()
                .map_or(true, |expr| expr.is_constant(resolver)),
            Expr::Subquery(_) => false,
            Expr::Unary(_, expr) => expr.is_constant(resolver),
            Expr::Variable(_) => false,
        }
    }
    /// Returns true if the expression is a constant expression that, when evaluated as a condition, is always true or false
    fn check_always_true_or_false(&self) -> Result<Option<AlwaysTrueOrFalse>> {
        match self {
            Self::Literal(lit) => match lit {
                ast::Literal::Numeric(b) => {
                    if let Ok(int_value) = b.parse::<i64>() {
                        return Ok(Some(if int_value == 0 {
                            AlwaysTrueOrFalse::AlwaysFalse
                        } else {
                            AlwaysTrueOrFalse::AlwaysTrue
                        }));
                    }
                    if let Ok(float_value) = b.parse::<f64>() {
                        return Ok(Some(if float_value == 0.0 {
                            AlwaysTrueOrFalse::AlwaysFalse
                        } else {
                            AlwaysTrueOrFalse::AlwaysTrue
                        }));
                    }

                    Ok(None)
                }
                ast::Literal::String(s) => {
                    let without_quotes = s.trim_matches('\'');
                    if let Ok(int_value) = without_quotes.parse::<i64>() {
                        return Ok(Some(if int_value == 0 {
                            AlwaysTrueOrFalse::AlwaysFalse
                        } else {
                            AlwaysTrueOrFalse::AlwaysTrue
                        }));
                    }

                    if let Ok(float_value) = without_quotes.parse::<f64>() {
                        return Ok(Some(if float_value == 0.0 {
                            AlwaysTrueOrFalse::AlwaysFalse
                        } else {
                            AlwaysTrueOrFalse::AlwaysTrue
                        }));
                    }

                    Ok(Some(AlwaysTrueOrFalse::AlwaysFalse))
                }
                _ => Ok(None),
            },
            Self::Unary(op, expr) => {
                if *op == ast::UnaryOperator::Not {
                    let trivial = expr.check_always_true_or_false()?;
                    return Ok(trivial.map(|t| match t {
                        AlwaysTrueOrFalse::AlwaysTrue => AlwaysTrueOrFalse::AlwaysFalse,
                        AlwaysTrueOrFalse::AlwaysFalse => AlwaysTrueOrFalse::AlwaysTrue,
                    }));
                }

                if *op == ast::UnaryOperator::Negative {
                    let trivial = expr.check_always_true_or_false()?;
                    return Ok(trivial);
                }

                Ok(None)
            }
            Self::InList { lhs: _, not, rhs } => {
                if rhs.is_none() {
                    return Ok(Some(if *not {
                        AlwaysTrueOrFalse::AlwaysTrue
                    } else {
                        AlwaysTrueOrFalse::AlwaysFalse
                    }));
                }
                let rhs = rhs.as_ref().unwrap();
                if rhs.is_empty() {
                    return Ok(Some(if *not {
                        AlwaysTrueOrFalse::AlwaysTrue
                    } else {
                        AlwaysTrueOrFalse::AlwaysFalse
                    }));
                }

                Ok(None)
            }
            Self::Binary(lhs, op, rhs) => {
                let lhs_trivial = lhs.check_always_true_or_false()?;
                let rhs_trivial = rhs.check_always_true_or_false()?;
                match op {
                    ast::Operator::And => {
                        if lhs_trivial == Some(AlwaysTrueOrFalse::AlwaysFalse)
                            || rhs_trivial == Some(AlwaysTrueOrFalse::AlwaysFalse)
                        {
                            return Ok(Some(AlwaysTrueOrFalse::AlwaysFalse));
                        }
                        if lhs_trivial == Some(AlwaysTrueOrFalse::AlwaysTrue)
                            && rhs_trivial == Some(AlwaysTrueOrFalse::AlwaysTrue)
                        {
                            return Ok(Some(AlwaysTrueOrFalse::AlwaysTrue));
                        }

                        Ok(None)
                    }
                    ast::Operator::Or => {
                        if lhs_trivial == Some(AlwaysTrueOrFalse::AlwaysTrue)
                            || rhs_trivial == Some(AlwaysTrueOrFalse::AlwaysTrue)
                        {
                            return Ok(Some(AlwaysTrueOrFalse::AlwaysTrue));
                        }
                        if lhs_trivial == Some(AlwaysTrueOrFalse::AlwaysFalse)
                            && rhs_trivial == Some(AlwaysTrueOrFalse::AlwaysFalse)
                        {
                            return Ok(Some(AlwaysTrueOrFalse::AlwaysFalse));
                        }

                        Ok(None)
                    }
                    _ => Ok(None),
                }
            }
            _ => Ok(None),
        }
    }
}

fn opposite_cmp_op(op: ast::Operator) -> ast::Operator {
    match op {
        ast::Operator::Equals => ast::Operator::Equals,
        ast::Operator::Greater => ast::Operator::Less,
        ast::Operator::GreaterEquals => ast::Operator::LessEquals,
        ast::Operator::Less => ast::Operator::Greater,
        ast::Operator::LessEquals => ast::Operator::GreaterEquals,
        _ => panic!("unexpected operator: {:?}", op),
    }
}

/// A simple newtype wrapper over a f64 that represents the cost of an operation.
///
/// This is used to estimate the cost of scans, seeks, and joins.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct Cost(pub f64);

impl std::ops::Add for Cost {
    type Output = Cost;

    fn add(self, other: Cost) -> Cost {
        Cost(self.0 + other.0)
    }
}

impl std::ops::Deref for Cost {
    type Target = f64;

    fn deref(&self) -> &f64 {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct IndexInfo {
    unique: bool,
    column_count: usize,
    covering: bool,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ScanCost {
    run_cost: Cost,
    build_cost: Cost,
}

impl std::ops::Add for ScanCost {
    type Output = ScanCost;

    fn add(self, other: ScanCost) -> ScanCost {
        ScanCost {
            run_cost: self.run_cost + other.run_cost,
            build_cost: self.build_cost + other.build_cost,
        }
    }
}

impl ScanCost {
    pub fn total(&self) -> Cost {
        self.run_cost + self.build_cost
    }
}

const ESTIMATED_HARDCODED_ROWS_PER_TABLE: usize = 1000000;
const ESTIMATED_HARDCODED_ROWS_PER_PAGE: usize = 50; // roughly 80 bytes per 4096 byte page

fn estimate_page_io_cost(rowcount: f64) -> Cost {
    Cost((rowcount as f64 / ESTIMATED_HARDCODED_ROWS_PER_PAGE as f64).ceil())
}

/// Estimate the cost of a scan or seek operation.
///
/// This is a very simple model that estimates the number of pages read
/// based on the number of rows read, ignoring any CPU costs.
fn estimate_cost_for_scan_or_seek(
    index_info: Option<IndexInfo>,
    constraints: &[Constraint],
    is_ephemeral: bool,
    input_cardinality: f64,
) -> ScanCost {
    let build_cost = if is_ephemeral {
        estimate_page_io_cost(2.0 * ESTIMATED_HARDCODED_ROWS_PER_TABLE as f64)
    } else {
        Cost(0.0)
    };
    let Some(index_info) = index_info else {
        return ScanCost {
            run_cost: estimate_page_io_cost(
                input_cardinality * ESTIMATED_HARDCODED_ROWS_PER_TABLE as f64,
            ),
            build_cost,
        };
    };

    let final_constraint_is_range = constraints
        .last()
        .map_or(false, |c| c.operator != ast::Operator::Equals);
    let equalities_count = constraints
        .iter()
        .take(if final_constraint_is_range {
            constraints.len() - 1
        } else {
            constraints.len()
        })
        .count() as f64;

    let cost_multiplier = match (
        index_info.unique,
        index_info.column_count as f64,
        equalities_count,
    ) {
        // no equalities: let's assume range query selectivity is 0.4. if final constraint is not range and there are no equalities, it means full table scan incoming
        (_, _, 0.0) => {
            if final_constraint_is_range {
                0.4
            } else {
                1.0
            }
        }
        // on an unique index if we have equalities across all index columns, assume very high selectivity
        (true, index_cols, eq_count) if eq_count == index_cols => 0.01,
        (false, index_cols, eq_count) if eq_count == index_cols => 0.1,
        // some equalities: let's assume each equality has a selectivity of 0.1 and range query selectivity is 0.4
        (_, _, eq_count) => {
            let mut multiplier = 1.0;
            for _ in 0..(eq_count as usize) {
                multiplier *= 0.1;
            }
            multiplier * if final_constraint_is_range { 4.0 } else { 1.0 }
        }
    };

    // little bonus for covering indexes
    let covering_multiplier = if index_info.covering { 0.9 } else { 1.0 };

    let cost = estimate_page_io_cost(
        cost_multiplier
            * ESTIMATED_HARDCODED_ROWS_PER_TABLE as f64
            * input_cardinality
            * covering_multiplier,
    );
    ScanCost {
        run_cost: cost,
        build_cost,
    }
}

fn usable_constraints_for_join_order<'a>(
    cs: &'a [Constraint],
    table_index: usize,
    join_order: &[JoinOrderMember],
) -> &'a [Constraint] {
    let mut usable_until = 0;
    for constraint in cs.iter() {
        let other_side_refers_to_self = constraint.lhs_mask.contains_table(table_index);
        if other_side_refers_to_self {
            break;
        }
        let lhs_mask = TableMask::from_iter(
            join_order
                .iter()
                .take(join_order.len() - 1)
                .map(|j| j.table_no),
        );
        let all_required_tables_are_on_left_side = lhs_mask.contains_all(&constraint.lhs_mask);
        if !all_required_tables_are_on_left_side {
            break;
        }
        usable_until += 1;
    }
    &cs[..usable_until]
}

/// Return the best [AccessMethod] for a given join order.
/// table_index and table_reference refer to the rightmost table in the join order.
pub fn find_best_access_method_for_join_order<'a>(
    table_index: usize,
    table_reference: &TableReference,
    constraints: &'a [Constraints],
    join_order: &[JoinOrderMember],
    maybe_order_target: Option<&OrderTarget>,
    input_cardinality: f64,
) -> Result<AccessMethod<'a>> {
    let cost_of_full_table_scan =
        estimate_cost_for_scan_or_seek(None, &[], false, input_cardinality);
    let mut best_access_method = AccessMethod {
        cost: cost_of_full_table_scan,
        kind: AccessMethodKind::Scan {
            index: None,
            iter_dir: IterationDirection::Forwards,
        },
    };
    let rowid_column_idx = table_reference
        .columns()
        .iter()
        .position(|c| c.is_rowid_alias);
    for csmap in constraints
        .iter()
        .filter(|csmap| csmap.table_no == table_index)
    {
        let index_info = match csmap.index.as_ref() {
            Some(index) => IndexInfo {
                unique: index.unique,
                covering: table_reference.index_is_covering(index),
                column_count: index.columns.len(),
            },
            None => IndexInfo {
                unique: true, // rowids are always unique
                covering: false,
                column_count: 1,
            },
        };
        let usable_constraints =
            usable_constraints_for_join_order(&csmap.constraints, table_index, join_order);
        let cost = estimate_cost_for_scan_or_seek(
            Some(index_info),
            &usable_constraints,
            false,
            input_cardinality,
        );

        let order_satisfiability_bonus = if let Some(order_target) = maybe_order_target {
            let mut all_same_direction = true;
            let mut all_opposite_direction = true;
            for i in 0..order_target.0.len().min(index_info.column_count) {
                let correct_table = order_target.0[i].table_no == table_index;
                let correct_column = {
                    match csmap.index.as_ref() {
                        Some(index) => index.columns[i].pos_in_table == order_target.0[i].column_no,
                        None => {
                            rowid_column_idx.map_or(false, |idx| idx == order_target.0[i].column_no)
                        }
                    }
                };
                if !correct_table || !correct_column {
                    all_same_direction = false;
                    all_opposite_direction = false;
                    break;
                }
                let correct_order = {
                    match csmap.index.as_ref() {
                        Some(index) => order_target.0[i].order == index.columns[i].order,
                        None => order_target.0[i].order == SortOrder::Asc,
                    }
                };
                if correct_order {
                    all_opposite_direction = false;
                } else {
                    all_same_direction = false;
                }
            }
            if all_same_direction || all_opposite_direction {
                Cost(1.0)
            } else {
                Cost(0.0)
            }
        } else {
            Cost(0.0)
        };
        if cost.total() < best_access_method.cost.total() + order_satisfiability_bonus {
            best_access_method.cost = cost;
            best_access_method.set_constraints(csmap.index.clone(), &usable_constraints);
        }
    }

    // FIXME: ephemeral indexes are disabled for now.
    // These constraints need to be computed ad hoc.
    // // We haven't found a persistent btree index that is any better than a full table scan;
    // // let's see if building an ephemeral index would be better.
    // if best_index.index.is_none() && matches!(table_reference.table, Table::BTree(_)) {
    //     let (ephemeral_cost, constraints_with_col_idx) = ephemeral_index_estimate_cost(
    //         where_clause,
    //         table_reference,
    //         loop_index,
    //         table_index,
    //         join_order,
    //         input_cardinality,
    //     )?;
    //     if ephemeral_cost.total() < best_index.cost.total() {
    //         // ephemeral index makes sense, so let's build it now.
    //         // ephemeral columns are: columns from the table_reference, constraints first, then the rest
    //         let ephemeral_index =
    //             ephemeral_index_build(table_reference, table_index, &constraints_with_col_idx);
    //         best_index.index = Some(Arc::new(ephemeral_index));
    //         best_index.cost = ephemeral_cost;
    //     }
    // }

    let iter_dir = if let Some(order_target) = maybe_order_target {
        // if index columns match the order target columns in the exact reverse directions, then we should use IterationDirection::Backwards
        let index = match &best_access_method.kind {
            AccessMethodKind::Scan { index, .. } => index.as_ref(),
            AccessMethodKind::Search { index, .. } => index.as_ref(),
        };
        let mut should_use_backwards = true;
        let num_cols = index.map_or(1, |i| i.columns.len());
        for i in 0..order_target.0.len().min(num_cols) {
            let correct_table = order_target.0[i].table_no == table_index;
            let correct_column = {
                match index {
                    Some(index) => index.columns[i].pos_in_table == order_target.0[i].column_no,
                    None => {
                        rowid_column_idx.map_or(false, |idx| idx == order_target.0[i].column_no)
                    }
                }
            };
            if !correct_table || !correct_column {
                should_use_backwards = false;
                break;
            }
            let correct_order = {
                match index {
                    Some(index) => order_target.0[i].order == index.columns[i].order,
                    None => order_target.0[i].order == SortOrder::Asc,
                }
            };
            if correct_order {
                should_use_backwards = false;
                break;
            }
        }
        if should_use_backwards {
            IterationDirection::Backwards
        } else {
            IterationDirection::Forwards
        }
    } else {
        IterationDirection::Forwards
    };
    best_access_method.set_iter_dir(iter_dir);

    Ok(best_access_method)
}

// TODO get rid of doing this every time
fn ephemeral_index_estimate_cost(
    where_clause: &[WhereTerm],
    table_reference: &TableReference,
    loop_index: usize,
    table_index: usize,
    join_order: &[JoinOrderMember],
    input_cardinality: f64,
) -> Result<(ScanCost, Vec<(usize, Constraint)>)> {
    let mut constraints_with_col_idx: Vec<(usize, Constraint)> = where_clause
        .iter()
        .enumerate()
        .filter(|(_, term)| is_potential_index_constraint(term, loop_index, join_order))
        .filter_map(|(i, term)| {
            let Ok(ast::Expr::Binary(lhs, operator, rhs)) = unwrap_parens(&term.expr) else {
                panic!("expected binary expression");
            };
            if let ast::Expr::Column { table, column, .. } = lhs.as_ref() {
                if *table == table_index {
                    let Ok(constraining_table_mask) = table_mask_from_expr(rhs) else {
                        return None;
                    };
                    return Some((
                        *column,
                        Constraint {
                            where_clause_position: (i, BinaryExprSide::Rhs),
                            operator: *operator,
                            key_position: *column,
                            sort_order: SortOrder::Asc,
                            lhs_mask: constraining_table_mask,
                        },
                    ));
                }
            }
            if let ast::Expr::Column { table, column, .. } = rhs.as_ref() {
                if *table == table_index {
                    let Ok(constraining_table_mask) = table_mask_from_expr(lhs) else {
                        return None;
                    };
                    return Some((
                        *column,
                        Constraint {
                            where_clause_position: (i, BinaryExprSide::Lhs),
                            operator: opposite_cmp_op(*operator),
                            key_position: *column,
                            sort_order: SortOrder::Asc,
                            lhs_mask: constraining_table_mask,
                        },
                    ));
                }
            }
            None
        })
        .collect();
    // sort equalities first
    constraints_with_col_idx.sort_by(|a, _| {
        if a.1.operator == ast::Operator::Equals {
            Ordering::Less
        } else {
            Ordering::Equal
        }
    });
    // drop everything after the first inequality
    constraints_with_col_idx.truncate(
        constraints_with_col_idx
            .iter()
            .position(|c| c.1.operator != ast::Operator::Equals)
            .unwrap_or(constraints_with_col_idx.len()),
    );
    if constraints_with_col_idx.is_empty() {
        return Ok((
            ScanCost {
                run_cost: Cost(0.0),
                build_cost: Cost(f64::MAX),
            },
            vec![],
        ));
    }

    let ephemeral_column_count = table_reference
        .columns()
        .iter()
        .enumerate()
        .filter(|(i, _)| table_reference.column_is_used(*i))
        .count();

    let constraints_without_col_idx = constraints_with_col_idx
        .iter()
        .cloned()
        .map(|(_, c)| c)
        .collect::<Vec<Constraint>>();
    let ephemeral_cost = estimate_cost_for_scan_or_seek(
        Some(IndexInfo {
            unique: false,
            column_count: ephemeral_column_count,
            covering: false,
        }),
        &constraints_without_col_idx,
        true,
        input_cardinality,
    );
    Ok((ephemeral_cost, constraints_with_col_idx))
}

fn ephemeral_index_build(
    table_reference: &TableReference,
    table_index: usize,
    constraints: &[(usize, Constraint)],
) -> Index {
    let mut ephemeral_columns: Vec<IndexColumn> = table_reference
        .columns()
        .iter()
        .enumerate()
        .map(|(i, c)| IndexColumn {
            name: c.name.clone().unwrap(),
            order: SortOrder::Asc,
            pos_in_table: i,
        })
        // only include columns that are used in the query
        .filter(|c| table_reference.column_is_used(c.pos_in_table))
        .collect();
    // sort so that constraints first, then rest in whatever order they were in in the table
    ephemeral_columns.sort_by(|a, b| {
        let a_constraint = constraints
            .iter()
            .enumerate()
            .find(|(_, c)| c.0 == a.pos_in_table);
        let b_constraint = constraints
            .iter()
            .enumerate()
            .find(|(_, c)| c.0 == b.pos_in_table);
        match (a_constraint, b_constraint) {
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (Some((a_idx, _)), Some((b_idx, _))) => a_idx.cmp(&b_idx),
            (None, None) => Ordering::Equal,
        }
    });
    let ephemeral_index = Index {
        name: format!(
            "ephemeral_{}_{}",
            table_reference.table.get_name(),
            table_index
        ),
        columns: ephemeral_columns,
        unique: false,
        ephemeral: true,
        table_name: table_reference.table.get_name().to_string(),
        root_page: 0,
    };

    ephemeral_index
}

#[derive(Debug, Clone)]
pub struct Constraint {
    /// The position of the constraint in the WHERE clause, e.g. in SELECT * FROM t WHERE true AND t.x = 10, the position is (1, BinaryExprSide::Rhs),
    /// since the RHS '10' is the constraining expression and it's part of the second term in the WHERE clause.
    where_clause_position: (usize, BinaryExprSide),
    /// The operator of the constraint, e.g. =, >, <
    operator: ast::Operator,
    /// The position of the index column in the index, e.g. if the index is (a,b,c) and the constraint is on b, then index_column_pos is 1.
    /// For Rowid constraints this is always 0.
    key_position: usize,
    /// The sort order of the index column, ASC or DESC. For Rowid constraints this is always ASC.
    sort_order: SortOrder,
    /// Bitmask of tables that are required to be on the left side of the constrained table,
    /// e.g. in SELECT * FROM t1,t2,t3 WHERE t1.x = t2.x + t3.x, the lhs_mask contains t2 and t3.
    lhs_mask: TableMask,
}

#[derive(Debug)]
/// A collection of [Constraint]s for a given (table, index) pair.
pub struct Constraints {
    index: Option<Arc<Index>>,
    table_no: usize,
    constraints: Vec<Constraint>,
}

/// Precompute all potentially usable constraints from a WHERE clause.
/// The resulting list of [Constraints] is then used to evaluate the best access methods for various join orders.
pub fn constraints_from_where_clause(
    where_clause: &[WhereTerm],
    table_references: &[TableReference],
    available_indexes: &HashMap<String, Vec<Arc<Index>>>,
) -> Result<Vec<Constraints>> {
    let mut constraints = Vec::new();
    for (table_no, table_reference) in table_references.iter().enumerate() {
        let rowid_alias_column = table_reference
            .columns()
            .iter()
            .position(|c| c.is_rowid_alias);

        if let Some(rowid_alias_column) = rowid_alias_column {
            let mut cs = Constraints {
                index: None,
                table_no,
                constraints: Vec::new(),
            };
            for (i, term) in where_clause.iter().enumerate() {
                let ast::Expr::Binary(lhs, operator, rhs) = unwrap_parens(&term.expr)? else {
                    continue;
                };
                if !matches!(
                    operator,
                    ast::Operator::Equals
                        | ast::Operator::Greater
                        | ast::Operator::Less
                        | ast::Operator::GreaterEquals
                        | ast::Operator::LessEquals
                ) {
                    continue;
                }
                if let Some(outer_join_tbl) = term.from_outer_join {
                    if outer_join_tbl != table_no {
                        continue;
                    }
                }
                match lhs.as_ref() {
                    ast::Expr::Column { table, column, .. } => {
                        if *table == table_no && *column == rowid_alias_column {
                            cs.constraints.push(Constraint {
                                where_clause_position: (i, BinaryExprSide::Rhs),
                                operator: *operator,
                                key_position: 0,
                                sort_order: SortOrder::Asc,
                                lhs_mask: table_mask_from_expr(rhs)?,
                            });
                        }
                    }
                    ast::Expr::RowId { table, .. } => {
                        if *table == table_no {
                            cs.constraints.push(Constraint {
                                where_clause_position: (i, BinaryExprSide::Rhs),
                                operator: *operator,
                                key_position: 0,
                                sort_order: SortOrder::Asc,
                                lhs_mask: table_mask_from_expr(rhs)?,
                            });
                        }
                    }
                    _ => {}
                };
                match rhs.as_ref() {
                    ast::Expr::Column { table, column, .. } => {
                        if *table == table_no && *column == rowid_alias_column {
                            cs.constraints.push(Constraint {
                                where_clause_position: (i, BinaryExprSide::Lhs),
                                operator: opposite_cmp_op(*operator),
                                key_position: 0,
                                sort_order: SortOrder::Asc,
                                lhs_mask: table_mask_from_expr(lhs)?,
                            });
                        }
                    }
                    ast::Expr::RowId { table, .. } => {
                        if *table == table_no {
                            cs.constraints.push(Constraint {
                                where_clause_position: (i, BinaryExprSide::Lhs),
                                operator: opposite_cmp_op(*operator),
                                key_position: 0,
                                sort_order: SortOrder::Asc,
                                lhs_mask: table_mask_from_expr(lhs)?,
                            });
                        }
                    }
                    _ => {}
                };
            }
            // First sort by position, with equalities first within each position
            cs.constraints.sort_by(|a, b| {
                let pos_cmp = a.key_position.cmp(&b.key_position);
                if pos_cmp == Ordering::Equal {
                    // If same position, sort equalities first
                    if a.operator == ast::Operator::Equals {
                        Ordering::Less
                    } else if b.operator == ast::Operator::Equals {
                        Ordering::Greater
                    } else {
                        Ordering::Equal
                    }
                } else {
                    pos_cmp
                }
            });

            // Deduplicate by position, keeping first occurrence (which will be equality if one exists)
            cs.constraints.dedup_by_key(|c| c.key_position);

            // Truncate at first gap in positions
            let mut last_pos = 0;
            let mut i = 0;
            for constraint in cs.constraints.iter() {
                if constraint.key_position != last_pos {
                    if constraint.key_position != last_pos + 1 {
                        break;
                    }
                    last_pos = constraint.key_position;
                }
                i += 1;
            }
            cs.constraints.truncate(i);

            // Truncate after the first inequality
            if let Some(first_inequality) = cs
                .constraints
                .iter()
                .position(|c| c.operator != ast::Operator::Equals)
            {
                cs.constraints.truncate(first_inequality + 1);
            }
            constraints.push(cs);
        }
        let indexes = available_indexes.get(table_reference.table.get_name());
        if let Some(indexes) = indexes {
            for index in indexes {
                let mut cs = Constraints {
                    index: Some(index.clone()),
                    table_no,
                    constraints: Vec::new(),
                };
                for (i, term) in where_clause.iter().enumerate() {
                    let ast::Expr::Binary(lhs, operator, rhs) = unwrap_parens(&term.expr)? else {
                        continue;
                    };
                    if !matches!(
                        operator,
                        ast::Operator::Equals
                            | ast::Operator::Greater
                            | ast::Operator::Less
                            | ast::Operator::GreaterEquals
                            | ast::Operator::LessEquals
                    ) {
                        continue;
                    }
                    if let Some(outer_join_tbl) = term.from_outer_join {
                        if outer_join_tbl != table_no {
                            continue;
                        }
                    }
                    if let Some(position_in_index) =
                        get_column_position_in_index(lhs, table_no, index)?
                    {
                        cs.constraints.push(Constraint {
                            where_clause_position: (i, BinaryExprSide::Rhs),
                            operator: *operator,
                            key_position: position_in_index,
                            sort_order: index.columns[position_in_index].order,
                            lhs_mask: table_mask_from_expr(rhs)?,
                        });
                    }
                    if let Some(position_in_index) =
                        get_column_position_in_index(rhs, table_no, index)?
                    {
                        cs.constraints.push(Constraint {
                            where_clause_position: (i, BinaryExprSide::Lhs),
                            operator: opposite_cmp_op(*operator),
                            key_position: position_in_index,
                            sort_order: index.columns[position_in_index].order,
                            lhs_mask: table_mask_from_expr(lhs)?,
                        });
                    }
                }
                // First sort by position, with equalities first within each position
                cs.constraints.sort_by(|a, b| {
                    let pos_cmp = a.key_position.cmp(&b.key_position);
                    if pos_cmp == Ordering::Equal {
                        // If same position, sort equalities first
                        if a.operator == ast::Operator::Equals {
                            Ordering::Less
                        } else if b.operator == ast::Operator::Equals {
                            Ordering::Greater
                        } else {
                            Ordering::Equal
                        }
                    } else {
                        pos_cmp
                    }
                });

                // Deduplicate by position, keeping first occurrence (which will be equality if one exists)
                cs.constraints.dedup_by_key(|c| c.key_position);

                // Truncate at first gap in positions
                let mut last_pos = 0;
                let mut i = 0;
                for constraint in cs.constraints.iter() {
                    if constraint.key_position != last_pos {
                        if constraint.key_position != last_pos + 1 {
                            break;
                        }
                        last_pos = constraint.key_position;
                    }
                    i += 1;
                }
                cs.constraints.truncate(i);

                // Truncate after the first inequality
                if let Some(first_inequality) = cs
                    .constraints
                    .iter()
                    .position(|c| c.operator != ast::Operator::Equals)
                {
                    cs.constraints.truncate(first_inequality + 1);
                }
                constraints.push(cs);
            }
        }
    }
    Ok(constraints)
}

/// Helper enum for [IndexConstraint] to indicate which side of a binary comparison expression is being compared to the index column.
/// For example, if the where clause is "WHERE x = 10" and there's an index on x,
/// the [IndexConstraint] for the where clause term "x = 10" will have a [BinaryExprSide::Rhs]
/// because the right hand side expression "10" is being compared to the index column "x".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BinaryExprSide {
    Lhs,
    Rhs,
}

/// Recursively unwrap parentheses from an expression
/// e.g. (((t.x > 5))) -> t.x > 5
fn unwrap_parens<T>(expr: T) -> Result<T>
where
    T: UnwrapParens,
{
    expr.unwrap_parens()
}

trait UnwrapParens {
    fn unwrap_parens(self) -> Result<Self>
    where
        Self: Sized;
}

impl UnwrapParens for &ast::Expr {
    fn unwrap_parens(self) -> Result<Self> {
        match self {
            ast::Expr::Column { .. } => Ok(self),
            ast::Expr::Parenthesized(exprs) => match exprs.len() {
                1 => unwrap_parens(exprs.first().unwrap()),
                _ => crate::bail_parse_error!("expected single expression in parentheses"),
            },
            _ => Ok(self),
        }
    }
}

impl UnwrapParens for ast::Expr {
    fn unwrap_parens(self) -> Result<Self> {
        match self {
            ast::Expr::Column { .. } => Ok(self),
            ast::Expr::Parenthesized(mut exprs) => match exprs.len() {
                1 => unwrap_parens(exprs.pop().unwrap()),
                _ => crate::bail_parse_error!("expected single expression in parentheses"),
            },
            _ => Ok(self),
        }
    }
}

/// Get the position of a column in an index
/// For example, if there is an index on table T(x,y) then y's position in the index is 1.
fn get_column_position_in_index(
    expr: &ast::Expr,
    table_index: usize,
    index: &Arc<Index>,
) -> Result<Option<usize>> {
    let ast::Expr::Column { table, column, .. } = unwrap_parens(expr)? else {
        return Ok(None);
    };
    if *table != table_index {
        return Ok(None);
    }
    Ok(index.column_table_pos_to_index_pos(*column))
}

fn is_potential_index_constraint(
    term: &WhereTerm,
    loop_index: usize,
    join_order: &[JoinOrderMember],
) -> bool {
    // Skip terms that cannot be evaluated at this table's loop level
    if !term.should_eval_at_loop(loop_index, join_order) {
        return false;
    }
    // Skip terms that are not binary comparisons
    let Ok(ast::Expr::Binary(lhs, operator, rhs)) = unwrap_parens(&term.expr) else {
        return false;
    };
    // Only consider index scans for binary ops that are comparisons
    if !matches!(
        *operator,
        ast::Operator::Equals
            | ast::Operator::Greater
            | ast::Operator::GreaterEquals
            | ast::Operator::Less
            | ast::Operator::LessEquals
    ) {
        return false;
    }

    // If both lhs and rhs refer to columns from this table, we can't use this constraint
    // because we can't use the index to satisfy the condition.
    // Examples:
    // - WHERE t.x > t.y
    // - WHERE t.x + 1 > t.y - 5
    // - WHERE t.x = (t.x)
    let Ok(eval_at_left) = determine_where_to_eval_expr(&lhs, join_order) else {
        return false;
    };
    let Ok(eval_at_right) = determine_where_to_eval_expr(&rhs, join_order) else {
        return false;
    };
    if eval_at_left == EvalAt::Loop(loop_index) && eval_at_right == EvalAt::Loop(loop_index) {
        return false;
    }
    true
}

/// Build a [SeekDef] for a given list of [Constraint]s
pub fn build_seek_def_from_constraints(
    constraints: &[Constraint],
    iter_dir: IterationDirection,
    where_clause: &[WhereTerm],
) -> Result<SeekDef> {
    assert!(
        !constraints.is_empty(),
        "cannot build seek def from empty list of constraints"
    );
    // Extract the key values and operators
    let mut key = Vec::with_capacity(constraints.len());

    for constraint in constraints {
        // Extract the other expression from the binary WhereTerm (i.e. the one being compared to the index column)
        let (idx, side) = constraint.where_clause_position;
        let where_term = &where_clause[idx];
        let ast::Expr::Binary(lhs, _, rhs) = unwrap_parens(where_term.expr.clone())? else {
            crate::bail_parse_error!("expected binary expression");
        };
        let cmp_expr = if side == BinaryExprSide::Lhs {
            *lhs
        } else {
            *rhs
        };
        key.push((cmp_expr, constraint.sort_order));
    }

    // We know all but potentially the last term is an equality, so we can use the operator of the last term
    // to form the SeekOp
    let op = constraints.last().unwrap().operator;

    let seek_def = build_seek_def(op, iter_dir, key)?;
    Ok(seek_def)
}

/// Build a [SeekDef] for a given comparison operator and index key.
/// To be usable as a seek key, all but potentially the last term must be equalities.
/// The last term can be a nonequality.
/// The comparison operator referred to by `op` is the operator of the last term.
///
/// There are two parts to the seek definition:
/// 1. The [SeekKey], which specifies the key that we will use to seek to the first row that matches the index key.
/// 2. The [TerminationKey], which specifies the key that we will use to terminate the index scan that follows the seek.
///
/// There are some nuances to how, and which parts of, the index key can be used in the [SeekKey] and [TerminationKey],
/// depending on the operator and iteration order. This function explains those nuances inline when dealing with
/// each case.
///
/// But to illustrate the general idea, consider the following examples:
///
/// 1. For example, having two conditions like (x>10 AND y>20) cannot be used as a valid [SeekKey] GT(x:10, y:20)
/// because the first row greater than (x:10, y:20) might be (x:10, y:21), which does not satisfy the where clause.
/// In this case, only GT(x:10) must be used as the [SeekKey], and rows with y <= 20 must be filtered as a regular condition expression for each value of x.
///
/// 2. In contrast, having (x=10 AND y>20) forms a valid index key GT(x:10, y:20) because after the seek, we can simply terminate as soon as x > 10,
/// i.e. use GT(x:10, y:20) as the [SeekKey] and GT(x:10) as the [TerminationKey].
///
/// The preceding examples are for an ascending index. The logic is similar for descending indexes, but an important distinction is that
/// since a descending index is laid out in reverse order, the comparison operators are reversed, e.g. LT becomes GT, LE becomes GE, etc.
/// So when you see e.g. a SeekOp::GT below for a descending index, it actually means that we are seeking the first row where the index key is LESS than the seek key.
///
fn build_seek_def(
    op: ast::Operator,
    iter_dir: IterationDirection,
    key: Vec<(ast::Expr, SortOrder)>,
) -> Result<SeekDef> {
    let key_len = key.len();
    let sort_order_of_last_key = key.last().unwrap().1;

    // For the commented examples below, keep in mind that since a descending index is laid out in reverse order, the comparison operators are reversed, e.g. LT becomes GT, LE becomes GE, etc.
    // Also keep in mind that index keys are compared based on the number of columns given, so for example:
    // - if key is GT(x:10), then (x=10, y=usize::MAX) is not GT because only X is compared. (x=11, y=<any>) is GT.
    // - if key is GT(x:10, y:20), then (x=10, y=21) is GT because both X and Y are compared.
    // - if key is GT(x:10, y:NULL), then (x=10, y=0) is GT because NULL is always LT in index key comparisons.
    Ok(match (iter_dir, op) {
        // Forwards, EQ:
        // Example: (x=10 AND y=20)
        // Seek key: start from the first GE(x:10, y:20)
        // Termination key: end at the first GT(x:10, y:20)
        // Ascending vs descending doesn't matter because all the comparisons are equalities.
        (IterationDirection::Forwards, ast::Operator::Equals) => SeekDef {
            key,
            iter_dir,
            seek: Some(SeekKey {
                len: key_len,
                null_pad: false,
                op: SeekOp::GE,
            }),
            termination: Some(TerminationKey {
                len: key_len,
                null_pad: false,
                op: SeekOp::GT,
            }),
        },
        // Forwards, GT:
        // Ascending index example: (x=10 AND y>20)
        // Seek key: start from the first GT(x:10, y:20), e.g. (x=10, y=21)
        // Termination key: end at the first GT(x:10), e.g. (x=11, y=0)
        //
        // Descending index example: (x=10 AND y>20)
        // Seek key: start from the first LE(x:10), e.g. (x=10, y=usize::MAX), so reversed -> GE(x:10)
        // Termination key: end at the first LE(x:10, y:20), e.g. (x=10, y=20) so reversed -> GE(x:10, y:20)
        (IterationDirection::Forwards, ast::Operator::Greater) => {
            let (seek_key_len, termination_key_len, seek_op, termination_op) =
                if sort_order_of_last_key == SortOrder::Asc {
                    (key_len, key_len - 1, SeekOp::GT, SeekOp::GT)
                } else {
                    (
                        key_len - 1,
                        key_len,
                        SeekOp::LE.reverse(),
                        SeekOp::LE.reverse(),
                    )
                };
            SeekDef {
                key,
                iter_dir,
                seek: if seek_key_len > 0 {
                    Some(SeekKey {
                        len: seek_key_len,
                        op: seek_op,
                        null_pad: false,
                    })
                } else {
                    None
                },
                termination: if termination_key_len > 0 {
                    Some(TerminationKey {
                        len: termination_key_len,
                        op: termination_op,
                        null_pad: false,
                    })
                } else {
                    None
                },
            }
        }
        // Forwards, GE:
        // Ascending index example: (x=10 AND y>=20)
        // Seek key: start from the first GE(x:10, y:20), e.g. (x=10, y=20)
        // Termination key: end at the first GT(x:10), e.g. (x=11, y=0)
        //
        // Descending index example: (x=10 AND y>=20)
        // Seek key: start from the first LE(x:10), e.g. (x=10, y=usize::MAX), so reversed -> GE(x:10)
        // Termination key: end at the first LT(x:10, y:20), e.g. (x=10, y=19), so reversed -> GT(x:10, y:20)
        (IterationDirection::Forwards, ast::Operator::GreaterEquals) => {
            let (seek_key_len, termination_key_len, seek_op, termination_op) =
                if sort_order_of_last_key == SortOrder::Asc {
                    (key_len, key_len - 1, SeekOp::GE, SeekOp::GT)
                } else {
                    (
                        key_len - 1,
                        key_len,
                        SeekOp::LE.reverse(),
                        SeekOp::LT.reverse(),
                    )
                };
            SeekDef {
                key,
                iter_dir,
                seek: if seek_key_len > 0 {
                    Some(SeekKey {
                        len: seek_key_len,
                        op: seek_op,
                        null_pad: false,
                    })
                } else {
                    None
                },
                termination: if termination_key_len > 0 {
                    Some(TerminationKey {
                        len: termination_key_len,
                        op: termination_op,
                        null_pad: false,
                    })
                } else {
                    None
                },
            }
        }
        // Forwards, LT:
        // Ascending index example: (x=10 AND y<20)
        // Seek key: start from the first GT(x:10, y: NULL), e.g. (x=10, y=0)
        // Termination key: end at the first GE(x:10, y:20), e.g. (x=10, y=20)
        //
        // Descending index example: (x=10 AND y<20)
        // Seek key: start from the first LT(x:10, y:20), e.g. (x=10, y=19) so reversed -> GT(x:10, y:20)
        // Termination key: end at the first LT(x:10), e.g. (x=9, y=usize::MAX), so reversed -> GE(x:10, NULL); i.e. GE the smallest possible (x=10, y) combination (NULL is always LT)
        (IterationDirection::Forwards, ast::Operator::Less) => {
            let (seek_key_len, termination_key_len, seek_op, termination_op) =
                if sort_order_of_last_key == SortOrder::Asc {
                    (key_len - 1, key_len, SeekOp::GT, SeekOp::GE)
                } else {
                    (key_len, key_len - 1, SeekOp::GT, SeekOp::GE)
                };
            SeekDef {
                key,
                iter_dir,
                seek: if seek_key_len > 0 {
                    Some(SeekKey {
                        len: seek_key_len,
                        op: seek_op,
                        null_pad: sort_order_of_last_key == SortOrder::Asc,
                    })
                } else {
                    None
                },
                termination: if termination_key_len > 0 {
                    Some(TerminationKey {
                        len: termination_key_len,
                        op: termination_op,
                        null_pad: sort_order_of_last_key == SortOrder::Desc,
                    })
                } else {
                    None
                },
            }
        }
        // Forwards, LE:
        // Ascending index example: (x=10 AND y<=20)
        // Seek key: start from the first GE(x:10, y:NULL), e.g. (x=10, y=0)
        // Termination key: end at the first GT(x:10, y:20), e.g. (x=10, y=21)
        //
        // Descending index example: (x=10 AND y<=20)
        // Seek key: start from the first LE(x:10, y:20), e.g. (x=10, y=20) so reversed -> GE(x:10, y:20)
        // Termination key: end at the first LT(x:10), e.g. (x=9, y=usize::MAX), so reversed -> GE(x:10, NULL); i.e. GE the smallest possible (x=10, y) combination (NULL is always LT)
        (IterationDirection::Forwards, ast::Operator::LessEquals) => {
            let (seek_key_len, termination_key_len, seek_op, termination_op) =
                if sort_order_of_last_key == SortOrder::Asc {
                    (key_len - 1, key_len, SeekOp::GT, SeekOp::GT)
                } else {
                    (
                        key_len,
                        key_len - 1,
                        SeekOp::LE.reverse(),
                        SeekOp::LE.reverse(),
                    )
                };
            SeekDef {
                key,
                iter_dir,
                seek: if seek_key_len > 0 {
                    Some(SeekKey {
                        len: seek_key_len,
                        op: seek_op,
                        null_pad: sort_order_of_last_key == SortOrder::Asc,
                    })
                } else {
                    None
                },
                termination: if termination_key_len > 0 {
                    Some(TerminationKey {
                        len: termination_key_len,
                        op: termination_op,
                        null_pad: sort_order_of_last_key == SortOrder::Desc,
                    })
                } else {
                    None
                },
            }
        }
        // Backwards, EQ:
        // Example: (x=10 AND y=20)
        // Seek key: start from the last LE(x:10, y:20)
        // Termination key: end at the first LT(x:10, y:20)
        // Ascending vs descending doesn't matter because all the comparisons are equalities.
        (IterationDirection::Backwards, ast::Operator::Equals) => SeekDef {
            key,
            iter_dir,
            seek: Some(SeekKey {
                len: key_len,
                op: SeekOp::LE,
                null_pad: false,
            }),
            termination: Some(TerminationKey {
                len: key_len,
                op: SeekOp::LT,
                null_pad: false,
            }),
        },
        // Backwards, LT:
        // Ascending index example: (x=10 AND y<20)
        // Seek key: start from the last LT(x:10, y:20), e.g. (x=10, y=19)
        // Termination key: end at the first LE(x:10, NULL), e.g. (x=9, y=usize::MAX)
        //
        // Descending index example: (x=10 AND y<20)
        // Seek key: start from the last GT(x:10, y:NULL), e.g. (x=10, y=0) so reversed -> LT(x:10, NULL)
        // Termination key: end at the first GE(x:10, y:20), e.g. (x=10, y=20) so reversed -> LE(x:10, y:20)
        (IterationDirection::Backwards, ast::Operator::Less) => {
            let (seek_key_len, termination_key_len, seek_op, termination_op) =
                if sort_order_of_last_key == SortOrder::Asc {
                    (key_len, key_len - 1, SeekOp::LT, SeekOp::LE)
                } else {
                    (
                        key_len - 1,
                        key_len,
                        SeekOp::GT.reverse(),
                        SeekOp::GE.reverse(),
                    )
                };
            SeekDef {
                key,
                iter_dir,
                seek: if seek_key_len > 0 {
                    Some(SeekKey {
                        len: seek_key_len,
                        op: seek_op,
                        null_pad: sort_order_of_last_key == SortOrder::Desc,
                    })
                } else {
                    None
                },
                termination: if termination_key_len > 0 {
                    Some(TerminationKey {
                        len: termination_key_len,
                        op: termination_op,
                        null_pad: sort_order_of_last_key == SortOrder::Asc,
                    })
                } else {
                    None
                },
            }
        }
        // Backwards, LE:
        // Ascending index example: (x=10 AND y<=20)
        // Seek key: start from the last LE(x:10, y:20), e.g. (x=10, y=20)
        // Termination key: end at the first LT(x:10, NULL), e.g. (x=9, y=usize::MAX)
        //
        // Descending index example: (x=10 AND y<=20)
        // Seek key: start from the last GT(x:10, NULL), e.g. (x=10, y=0) so reversed -> LT(x:10, NULL)
        // Termination key: end at the first GT(x:10, y:20), e.g. (x=10, y=21) so reversed -> LT(x:10, y:20)
        (IterationDirection::Backwards, ast::Operator::LessEquals) => {
            let (seek_key_len, termination_key_len, seek_op, termination_op) =
                if sort_order_of_last_key == SortOrder::Asc {
                    (key_len, key_len - 1, SeekOp::LE, SeekOp::LE)
                } else {
                    (
                        key_len - 1,
                        key_len,
                        SeekOp::GT.reverse(),
                        SeekOp::GT.reverse(),
                    )
                };
            SeekDef {
                key,
                iter_dir,
                seek: if seek_key_len > 0 {
                    Some(SeekKey {
                        len: seek_key_len,
                        op: seek_op,
                        null_pad: sort_order_of_last_key == SortOrder::Desc,
                    })
                } else {
                    None
                },
                termination: if termination_key_len > 0 {
                    Some(TerminationKey {
                        len: termination_key_len,
                        op: termination_op,
                        null_pad: sort_order_of_last_key == SortOrder::Asc,
                    })
                } else {
                    None
                },
            }
        }
        // Backwards, GT:
        // Ascending index example: (x=10 AND y>20)
        // Seek key: start from the last LE(x:10), e.g. (x=10, y=usize::MAX)
        // Termination key: end at the first LE(x:10, y:20), e.g. (x=10, y=20)
        //
        // Descending index example: (x=10 AND y>20)
        // Seek key: start from the last GT(x:10, y:20), e.g. (x=10, y=21) so reversed -> LT(x:10, y:20)
        // Termination key: end at the first GT(x:10), e.g. (x=11, y=0) so reversed -> LT(x:10)
        (IterationDirection::Backwards, ast::Operator::Greater) => {
            let (seek_key_len, termination_key_len, seek_op, termination_op) =
                if sort_order_of_last_key == SortOrder::Asc {
                    (key_len - 1, key_len, SeekOp::LE, SeekOp::LE)
                } else {
                    (
                        key_len,
                        key_len - 1,
                        SeekOp::GT.reverse(),
                        SeekOp::GT.reverse(),
                    )
                };
            SeekDef {
                key,
                iter_dir,
                seek: if seek_key_len > 0 {
                    Some(SeekKey {
                        len: seek_key_len,
                        op: seek_op,
                        null_pad: false,
                    })
                } else {
                    None
                },
                termination: if termination_key_len > 0 {
                    Some(TerminationKey {
                        len: termination_key_len,
                        op: termination_op,
                        null_pad: false,
                    })
                } else {
                    None
                },
            }
        }
        // Backwards, GE:
        // Ascending index example: (x=10 AND y>=20)
        // Seek key: start from the last LE(x:10), e.g. (x=10, y=usize::MAX)
        // Termination key: end at the first LT(x:10, y:20), e.g. (x=10, y=19)
        //
        // Descending index example: (x=10 AND y>=20)
        // Seek key: start from the last GE(x:10, y:20), e.g. (x=10, y=20) so reversed -> LE(x:10, y:20)
        // Termination key: end at the first GT(x:10), e.g. (x=11, y=0) so reversed -> LT(x:10)
        (IterationDirection::Backwards, ast::Operator::GreaterEquals) => {
            let (seek_key_len, termination_key_len, seek_op, termination_op) =
                if sort_order_of_last_key == SortOrder::Asc {
                    (key_len - 1, key_len, SeekOp::LE, SeekOp::LT)
                } else {
                    (
                        key_len,
                        key_len - 1,
                        SeekOp::GE.reverse(),
                        SeekOp::GT.reverse(),
                    )
                };
            SeekDef {
                key,
                iter_dir,
                seek: if seek_key_len > 0 {
                    Some(SeekKey {
                        len: seek_key_len,
                        op: seek_op,
                        null_pad: false,
                    })
                } else {
                    None
                },
                termination: if termination_key_len > 0 {
                    Some(TerminationKey {
                        len: termination_key_len,
                        op: termination_op,
                        null_pad: false,
                    })
                } else {
                    None
                },
            }
        }
        (_, op) => {
            crate::bail_parse_error!("build_seek_def: invalid operator: {:?}", op,)
        }
    })
}

fn rewrite_expr(expr: &mut ast::Expr) -> Result<()> {
    match expr {
        ast::Expr::Id(id) => {
            // Convert "true" and "false" to 1 and 0
            if id.0.eq_ignore_ascii_case("true") {
                *expr = ast::Expr::Literal(ast::Literal::Numeric(1.to_string()));
                return Ok(());
            }
            if id.0.eq_ignore_ascii_case("false") {
                *expr = ast::Expr::Literal(ast::Literal::Numeric(0.to_string()));
                return Ok(());
            }
            Ok(())
        }
        ast::Expr::Between {
            lhs,
            not,
            start,
            end,
        } => {
            // Convert `y NOT BETWEEN x AND z` to `x > y OR y > z`
            let (lower_op, upper_op) = if *not {
                (ast::Operator::Greater, ast::Operator::Greater)
            } else {
                // Convert `y BETWEEN x AND z` to `x <= y AND y <= z`
                (ast::Operator::LessEquals, ast::Operator::LessEquals)
            };

            rewrite_expr(start)?;
            rewrite_expr(lhs)?;
            rewrite_expr(end)?;

            let start = start.take_ownership();
            let lhs = lhs.take_ownership();
            let end = end.take_ownership();

            let lower_bound = ast::Expr::Binary(Box::new(start), lower_op, Box::new(lhs.clone()));
            let upper_bound = ast::Expr::Binary(Box::new(lhs), upper_op, Box::new(end));

            if *not {
                *expr = ast::Expr::Binary(
                    Box::new(lower_bound),
                    ast::Operator::Or,
                    Box::new(upper_bound),
                );
            } else {
                *expr = ast::Expr::Binary(
                    Box::new(lower_bound),
                    ast::Operator::And,
                    Box::new(upper_bound),
                );
            }
            Ok(())
        }
        ast::Expr::Parenthesized(ref mut exprs) => {
            for subexpr in exprs.iter_mut() {
                rewrite_expr(subexpr)?;
            }
            let exprs = std::mem::take(exprs);
            *expr = ast::Expr::Parenthesized(exprs);
            Ok(())
        }
        // Process other expressions recursively
        ast::Expr::Binary(lhs, _, rhs) => {
            rewrite_expr(lhs)?;
            rewrite_expr(rhs)?;
            Ok(())
        }
        ast::Expr::FunctionCall { args, .. } => {
            if let Some(args) = args {
                for arg in args.iter_mut() {
                    rewrite_expr(arg)?;
                }
            }
            Ok(())
        }
        ast::Expr::Unary(_, arg) => {
            rewrite_expr(arg)?;
            Ok(())
        }
        _ => Ok(()),
    }
}

trait TakeOwnership {
    fn take_ownership(&mut self) -> Self;
}

impl TakeOwnership for ast::Expr {
    fn take_ownership(&mut self) -> Self {
        std::mem::replace(self, ast::Expr::Literal(ast::Literal::Null))
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use limbo_sqlite3_parser::ast::Operator;

    use super::*;
    use crate::{
        schema::{BTreeTable, Column, Table, Type},
        translate::plan::{ColumnUsedMask, JoinInfo},
        translate::planner::TableMask,
    };

    #[test]
    fn test_generate_bitmasks() {
        let bitmasks = generate_join_bitmasks(4, 2).collect::<Vec<_>>();
        assert!(bitmasks.contains(&TableMask(0b110))); // {0,1} -- first bit is always set to 0 so that a Mask with value 0 means "no tables are referenced".
        assert!(bitmasks.contains(&TableMask(0b1010))); // {0,2}
        assert!(bitmasks.contains(&TableMask(0b1100))); // {1,2}
        assert!(bitmasks.contains(&TableMask(0b10010))); // {0,3}
        assert!(bitmasks.contains(&TableMask(0b10100))); // {1,3}
        assert!(bitmasks.contains(&TableMask(0b11000))); // {2,3}
    }

    #[test]
    /// Test that [compute_best_join_order] returns None when there are no table references.
    fn test_compute_best_join_order_empty() {
        let table_references = vec![];
        let available_indexes = HashMap::new();
        let where_clause = vec![];

        let access_methods_arena = RefCell::new(Vec::new());
        let constraints =
            constraints_from_where_clause(&where_clause, &table_references, &available_indexes)
                .unwrap();

        let result = compute_best_join_order(
            &table_references,
            &where_clause,
            None,
            &constraints,
            &access_methods_arena,
        )
        .unwrap();
        assert!(result.is_none());
    }

    #[test]
    /// Test that [compute_best_join_order] returns a table scan access method when the where clause is empty.
    fn test_compute_best_join_order_single_table_no_indexes() {
        let t1 = _create_btree_table("test_table", _create_column_list(&["id"], Type::Integer));
        let table_references = vec![_create_table_reference(t1.clone(), None)];
        let available_indexes = HashMap::new();
        let where_clause = vec![];

        let access_methods_arena = RefCell::new(Vec::new());
        let constraints =
            constraints_from_where_clause(&where_clause, &table_references, &available_indexes)
                .unwrap();

        // SELECT * from test_table
        // expecting best_best_plan() not to do any work due to empty where clause.
        let BestJoinOrderResult { best_plan, .. } = compute_best_join_order(
            &table_references,
            &where_clause,
            None,
            &constraints,
            &access_methods_arena,
        )
        .unwrap()
        .unwrap();
        // Should just be a table scan access method
        assert!(matches!(
            access_methods_arena.borrow()[best_plan.best_access_methods[0]].kind,
            AccessMethodKind::Scan { index: None, iter_dir }
            if iter_dir == IterationDirection::Forwards
        ));
    }

    #[test]
    /// Test that [compute_best_join_order] returns a RowidEq access method when the where clause has an EQ constraint on the rowid alias.
    fn test_compute_best_join_order_single_table_rowid_eq() {
        let t1 = _create_btree_table("test_table", vec![_create_column_rowid_alias("id")]);
        let table_references = vec![_create_table_reference(t1.clone(), None)];

        let where_clause = vec![_create_binary_expr(
            _create_column_expr(0, 0, true), // table 0, column 0 (rowid)
            ast::Operator::Equals,
            _create_numeric_literal("42"),
        )];

        let access_methods_arena = RefCell::new(Vec::new());
        let available_indexes = HashMap::new();
        let constraints =
            constraints_from_where_clause(&where_clause, &table_references, &available_indexes)
                .unwrap();

        // SELECT * FROM test_table WHERE id = 42
        // expecting a RowidEq access method because id is a rowid alias.
        let result = compute_best_join_order(
            &table_references,
            &where_clause,
            None,
            &constraints,
            &access_methods_arena,
        )
        .unwrap();
        assert!(result.is_some());
        let BestJoinOrderResult { best_plan, .. } = result.unwrap();
        assert_eq!(best_plan.table_numbers, vec![0]);
        assert!(
            matches!(
                &access_methods_arena.borrow()[best_plan.best_access_methods[0]].kind,
                AccessMethodKind::Search {
                    index: None,
                    iter_dir,
                    constraints,
                }
                if *iter_dir == IterationDirection::Forwards && constraints.len() == 1 && constraints[0].where_clause_position == (0, BinaryExprSide::Rhs),
            ),
            "expected rowid eq access method, got {:?}",
            access_methods_arena.borrow()[best_plan.best_access_methods[0]].kind
        );
    }

    #[test]
    /// Test that [compute_best_join_order] returns an IndexScan access method when the where clause has an EQ constraint on a primary key.
    fn test_compute_best_join_order_single_table_pk_eq() {
        let t1 = _create_btree_table(
            "test_table",
            vec![_create_column_of_type("id", Type::Integer)],
        );
        let table_references = vec![_create_table_reference(t1.clone(), None)];

        let where_clause = vec![_create_binary_expr(
            _create_column_expr(0, 0, false), // table 0, column 0 (id)
            ast::Operator::Equals,
            _create_numeric_literal("42"),
        )];

        let access_methods_arena = RefCell::new(Vec::new());
        let mut available_indexes = HashMap::new();
        let index = Arc::new(Index {
            name: "sqlite_autoindex_test_table_1".to_string(),
            table_name: "test_table".to_string(),
            columns: vec![IndexColumn {
                name: "id".to_string(),
                order: SortOrder::Asc,
                pos_in_table: 0,
            }],
            unique: true,
            ephemeral: false,
            root_page: 1,
        });
        available_indexes.insert("test_table".to_string(), vec![index]);

        let constraints =
            constraints_from_where_clause(&where_clause, &table_references, &available_indexes)
                .unwrap();
        // SELECT * FROM test_table WHERE id = 42
        // expecting an IndexScan access method because id is a primary key with an index
        let result = compute_best_join_order(
            &table_references,
            &where_clause,
            None,
            &constraints,
            &access_methods_arena,
        )
        .unwrap();
        assert!(result.is_some());
        let BestJoinOrderResult { best_plan, .. } = result.unwrap();
        assert_eq!(best_plan.table_numbers, vec![0]);
        assert!(
            matches!(
                &access_methods_arena.borrow()[best_plan.best_access_methods[0]].kind,
                AccessMethodKind::Search {
                    index: Some(index),
                    iter_dir,
                    constraints,
                }
                if *iter_dir == IterationDirection::Forwards && constraints.len() == 1 && constraints[0].where_clause_position == (0, BinaryExprSide::Rhs),
            ),
            "expected index search access method, got {:?}",
            access_methods_arena.borrow()[best_plan.best_access_methods[0]].kind
        );
    }

    #[test]
    /// Test that [compute_best_join_order] moves the outer table to the inner position when an index can be used on it, but not the original inner table.
    fn test_compute_best_join_order_two_tables() {
        let t1 = _create_btree_table("table1", _create_column_list(&["id"], Type::Integer));
        let t2 = _create_btree_table("table2", _create_column_list(&["id"], Type::Integer));

        let mut table_references = vec![
            _create_table_reference(t1.clone(), None),
            _create_table_reference(
                t2.clone(),
                Some(JoinInfo {
                    outer: false,
                    using: None,
                }),
            ),
        ];

        let mut available_indexes = HashMap::new();
        // Index on the outer table (table1)
        let index1 = Arc::new(Index {
            name: "index1".to_string(),
            table_name: "table1".to_string(),
            columns: vec![IndexColumn {
                name: "id".to_string(),
                order: SortOrder::Asc,
                pos_in_table: 0,
            }],
            unique: true,
            ephemeral: false,
            root_page: 1,
        });
        available_indexes.insert("table1".to_string(), vec![index1]);

        // SELECT * FROM table1 JOIN table2 WHERE table1.id = table2.id
        // expecting table2 to be chosen first due to the index on table1.id
        let where_clause = vec![_create_binary_expr(
            _create_column_expr(0, 0, false), // table1.id
            ast::Operator::Equals,
            _create_column_expr(1, 0, false), // table2.id
        )];

        let access_methods_arena = RefCell::new(Vec::new());
        let constraints =
            constraints_from_where_clause(&where_clause, &table_references, &available_indexes)
                .unwrap();

        let result = compute_best_join_order(
            &mut table_references,
            &where_clause,
            None,
            &constraints,
            &access_methods_arena,
        )
        .unwrap();
        assert!(result.is_some());
        let BestJoinOrderResult { best_plan, .. } = result.unwrap();
        assert_eq!(best_plan.table_numbers, vec![1, 0]);
        assert!(
            matches!(
                &access_methods_arena.borrow()[best_plan.best_access_methods[0]].kind,
                AccessMethodKind::Scan { index: None, iter_dir }
                if *iter_dir == IterationDirection::Forwards
            ),
            "expected TableScan access method, got {:?}",
            access_methods_arena.borrow()[best_plan.best_access_methods[0]].kind
        );
        assert!(
            matches!(
                &access_methods_arena.borrow()[best_plan.best_access_methods[1]].kind,
                AccessMethodKind::Search {
                    index: Some(index),
                    iter_dir,
                    constraints,
                }
                if *iter_dir == IterationDirection::Forwards && constraints.len() == 1 && constraints[0].where_clause_position == (0, BinaryExprSide::Rhs) && index.name == "index1",
            ),
            "expected Search access method, got {:?}",
            access_methods_arena.borrow()[best_plan.best_access_methods[1]].kind
        );
    }

    #[test]
    /// Test that [compute_best_join_order] returns a sensible order and plan for three tables, each with indexes.
    fn test_compute_best_join_order_three_tables_indexed() {
        let table_orders = _create_btree_table(
            "orders",
            vec![
                _create_column_of_type("id", Type::Integer),
                _create_column_of_type("customer_id", Type::Integer),
                _create_column_of_type("total", Type::Integer),
            ],
        );
        let table_customers = _create_btree_table(
            "customers",
            vec![
                _create_column_of_type("id", Type::Integer),
                _create_column_of_type("name", Type::Integer),
            ],
        );
        let table_order_items = _create_btree_table(
            "order_items",
            vec![
                _create_column_of_type("id", Type::Integer),
                _create_column_of_type("order_id", Type::Integer),
                _create_column_of_type("product_id", Type::Integer),
                _create_column_of_type("quantity", Type::Integer),
            ],
        );

        let table_references = vec![
            _create_table_reference(table_orders.clone(), None),
            _create_table_reference(
                table_customers.clone(),
                Some(JoinInfo {
                    outer: false,
                    using: None,
                }),
            ),
            _create_table_reference(
                table_order_items.clone(),
                Some(JoinInfo {
                    outer: false,
                    using: None,
                }),
            ),
        ];

        const TABLE_NO_ORDERS: usize = 0;
        const TABLE_NO_CUSTOMERS: usize = 1;
        const TABLE_NO_ORDER_ITEMS: usize = 2;

        let mut available_indexes = HashMap::new();
        ["orders", "customers", "order_items"]
            .iter()
            .for_each(|table_name| {
                // add primary key index called sqlite_autoindex_<tablename>_1
                let index_name = format!("sqlite_autoindex_{}_1", table_name);
                let index = Arc::new(Index {
                    name: index_name,
                    table_name: table_name.to_string(),
                    columns: vec![IndexColumn {
                        name: "id".to_string(),
                        order: SortOrder::Asc,
                        pos_in_table: 0,
                    }],
                    unique: true,
                    ephemeral: false,
                    root_page: 1,
                });
                available_indexes.insert(table_name.to_string(), vec![index]);
            });
        let customer_id_idx = Arc::new(Index {
            name: "orders_customer_id_idx".to_string(),
            table_name: "orders".to_string(),
            columns: vec![IndexColumn {
                name: "customer_id".to_string(),
                order: SortOrder::Asc,
                pos_in_table: 1,
            }],
            unique: false,
            ephemeral: false,
            root_page: 1,
        });
        let order_id_idx = Arc::new(Index {
            name: "order_items_order_id_idx".to_string(),
            table_name: "order_items".to_string(),
            columns: vec![IndexColumn {
                name: "order_id".to_string(),
                order: SortOrder::Asc,
                pos_in_table: 1,
            }],
            unique: false,
            ephemeral: false,
            root_page: 1,
        });

        available_indexes
            .entry("orders".to_string())
            .and_modify(|v| v.push(customer_id_idx));
        available_indexes
            .entry("order_items".to_string())
            .and_modify(|v| v.push(order_id_idx));

        // SELECT * FROM orders JOIN customers JOIN order_items
        // WHERE orders.customer_id = customers.id AND orders.id = order_items.order_id AND customers.id = 42
        // expecting customers to be chosen first due to the index on customers.id and it having a selective filter (=42)
        // then orders to be chosen next due to the index on orders.customer_id
        // then order_items to be chosen last due to the index on order_items.order_id
        let where_clause = vec![
            // orders.customer_id = customers.id
            _create_binary_expr(
                _create_column_expr(TABLE_NO_ORDERS, 1, false), // orders.customer_id
                ast::Operator::Equals,
                _create_column_expr(TABLE_NO_CUSTOMERS, 0, false), // customers.id
            ),
            // orders.id = order_items.order_id
            _create_binary_expr(
                _create_column_expr(TABLE_NO_ORDERS, 0, false), // orders.id
                ast::Operator::Equals,
                _create_column_expr(TABLE_NO_ORDER_ITEMS, 1, false), // order_items.order_id
            ),
            // customers.id = 42
            _create_binary_expr(
                _create_column_expr(TABLE_NO_CUSTOMERS, 0, false), // customers.id
                ast::Operator::Equals,
                _create_numeric_literal("42"),
            ),
        ];

        let access_methods_arena = RefCell::new(Vec::new());
        let constraints =
            constraints_from_where_clause(&where_clause, &table_references, &available_indexes)
                .unwrap();

        let result = compute_best_join_order(
            &table_references,
            &where_clause,
            None,
            &constraints,
            &access_methods_arena,
        )
        .unwrap();
        assert!(result.is_some());
        let BestJoinOrderResult { best_plan, .. } = result.unwrap();

        // Customers (due to =42 filter) -> Orders (due to index on customer_id) -> Order_items (due to index on order_id)
        assert_eq!(
            best_plan.table_numbers,
            vec![TABLE_NO_CUSTOMERS, TABLE_NO_ORDERS, TABLE_NO_ORDER_ITEMS]
        );

        assert!(
            matches!(
                &access_methods_arena.borrow()[best_plan.best_access_methods[0]].kind,
                AccessMethodKind::Search {
                    index: Some(index),
                    iter_dir,
                    constraints,
                }
                if *iter_dir == IterationDirection::Forwards && constraints.len() == 1 && constraints[0].lhs_mask.is_empty() && index.name == "sqlite_autoindex_customers_1",
            ),
            "expected Search access method, got {:?}",
            access_methods_arena.borrow()[best_plan.best_access_methods[0]].kind
        );

        assert!(
            matches!(
                &access_methods_arena.borrow()[best_plan.best_access_methods[1]].kind,
                AccessMethodKind::Search {
                    index: Some(index),
                    iter_dir,
                    constraints,
                }
                if *iter_dir == IterationDirection::Forwards && constraints.len() == 1 && constraints[0].lhs_mask.contains_table(TABLE_NO_CUSTOMERS) && index.name == "orders_customer_id_idx",
            ),
            "expected Search access method, got {:?}",
            access_methods_arena.borrow()[best_plan.best_access_methods[1]].kind
        );

        assert!(
            matches!(
                &access_methods_arena.borrow()[best_plan.best_access_methods[2]].kind,
                AccessMethodKind::Search {
                    index: Some(index),
                    iter_dir,
                    constraints,
                }
                if *iter_dir == IterationDirection::Forwards && constraints.len() == 1 && constraints[0].lhs_mask.contains_table(TABLE_NO_ORDERS) && index.name == "order_items_order_id_idx",
            ),
            "expected Search access method, got {:?}",
            access_methods_arena.borrow()[best_plan.best_access_methods[2]].kind
        );
    }

    struct TestColumn {
        name: String,
        ty: Type,
        is_rowid_alias: bool,
    }

    impl Default for TestColumn {
        fn default() -> Self {
            Self {
                name: "a".to_string(),
                ty: Type::Integer,
                is_rowid_alias: false,
            }
        }
    }

    #[test]
    /// Test that [compute_best_join_order] faces a query with no indexes,
    /// it chooses the outer table based on the most restrictive filter,
    /// and builds an ephemeral index on the inner table.
    fn test_join_order_no_indexes_inner_ephemeral() {
        let t1 = _create_btree_table("t1", _create_column_list(&["id", "foo"], Type::Integer));
        let t2 = _create_btree_table("t2", _create_column_list(&["id", "foo"], Type::Integer));

        let mut table_references = vec![
            _create_table_reference(t1.clone(), None),
            _create_table_reference(
                t2.clone(),
                Some(JoinInfo {
                    outer: false,
                    using: None,
                }),
            ),
        ];

        // SELECT * FROM t1 JOIN t2 ON t1.id = t2.id WHERE t2.foo > 10
        let where_clause = vec![
            // t2.foo > 10
            // this should make the optimizer choose t2 as the outer table despite being inner in the query,
            // because it restricts the output of t2 to a smaller set of rows, resulting in a cheaper plan.
            _create_binary_expr(
                _create_column_expr(1, 1, false), // table 1, column 1 (foo)
                ast::Operator::Greater,
                _create_numeric_literal("10"),
            ),
            // t1.id = t2.id
            // this should make the optimizer choose to create an ephemeral index on t1
            // because it is cheaper than a table scan, despite the cost of building the index.
            _create_binary_expr(
                _create_column_expr(0, 0, false), // table 0, column 0 (id)
                ast::Operator::Equals,
                _create_column_expr(1, 0, false), // table 1, column 0 (id)
            ),
        ];

        let available_indexes = HashMap::new();
        let access_methods_arena = RefCell::new(Vec::new());
        let constraints =
            constraints_from_where_clause(&where_clause, &table_references, &available_indexes)
                .unwrap();
        let BestJoinOrderResult { best_plan, .. } = compute_best_join_order(
            &mut table_references,
            &where_clause,
            None,
            &constraints,
            &access_methods_arena,
        )
        .unwrap()
        .unwrap();

        // Verify that t2 is chosen first due to its filter
        assert_eq!(best_plan.table_numbers[0], 1, "best_plan: {:?}", best_plan);
        // Verify table scan is used since there are no indexes
        assert!(matches!(
            access_methods_arena.borrow()[best_plan.best_access_methods[0]].kind,
            AccessMethodKind::Scan { index: None, iter_dir }
            if iter_dir == IterationDirection::Forwards
        ));
        // Verify that an ephemeral index was built on t1
        assert!(
            matches!(
                &access_methods_arena.borrow()[best_plan.best_access_methods[1]].kind,
                AccessMethodKind::Search {
                    index: Some(index),
                    iter_dir,
                    constraints,
                }
                if *iter_dir == IterationDirection::Forwards && constraints.len() == 1 && constraints[0].where_clause_position == (0, BinaryExprSide::Rhs) && index.ephemeral
            ),
            "expected ephemeral index, got {:?}",
            access_methods_arena.borrow()[best_plan.best_access_methods[1]].kind
        );
    }

    #[test]
    fn test_join_order_three_tables_no_indexes() {
        let t1 = _create_btree_table("t1", _create_column_list(&["id", "foo"], Type::Integer));
        let t2 = _create_btree_table("t2", _create_column_list(&["id", "foo"], Type::Integer));
        let t3 = _create_btree_table("t3", _create_column_list(&["id", "foo"], Type::Integer));

        let mut table_references = vec![
            _create_table_reference(t1.clone(), None),
            _create_table_reference(
                t2.clone(),
                Some(JoinInfo {
                    outer: false,
                    using: None,
                }),
            ),
            _create_table_reference(
                t3.clone(),
                Some(JoinInfo {
                    outer: false,
                    using: None,
                }),
            ),
        ];

        let where_clause = vec![
            // t2.foo = 42 (equality filter, more selective)
            _create_binary_expr(
                _create_column_expr(1, 1, false), // table 1, column 1 (foo)
                ast::Operator::Equals,
                _create_numeric_literal("42"),
            ),
            // t1.foo > 10 (inequality filter, less selective)
            _create_binary_expr(
                _create_column_expr(0, 1, false), // table 0, column 1 (foo)
                ast::Operator::Greater,
                _create_numeric_literal("10"),
            ),
        ];

        let available_indexes = HashMap::new();
        let access_methods_arena = RefCell::new(Vec::new());
        let constraints =
            constraints_from_where_clause(&where_clause, &table_references, &available_indexes)
                .unwrap();

        let BestJoinOrderResult { best_plan, .. } = compute_best_join_order(
            &mut table_references,
            &where_clause,
            None,
            &constraints,
            &access_methods_arena,
        )
        .unwrap()
        .unwrap();

        // Verify that t2 is chosen first due to its equality filter
        assert_eq!(best_plan.table_numbers[0], 1);
        // Verify table scan is used since there are no indexes
        assert!(matches!(
            access_methods_arena.borrow()[best_plan.best_access_methods[0]].kind,
            AccessMethodKind::Scan { index: None, iter_dir }
            if iter_dir == IterationDirection::Forwards
        ));
        // Verify that t1 is chosen next due to its inequality filter
        assert!(matches!(
            access_methods_arena.borrow()[best_plan.best_access_methods[1]].kind,
            AccessMethodKind::Scan { index: None, iter_dir }
            if iter_dir == IterationDirection::Forwards
        ));
        // Verify that t3 is chosen last due to no filters
        assert!(matches!(
            access_methods_arena.borrow()[best_plan.best_access_methods[2]].kind,
            AccessMethodKind::Scan { index: None, iter_dir }
            if iter_dir == IterationDirection::Forwards
        ));
    }

    #[test]
    /// Test that [compute_best_join_order] chooses a "fact table" as the outer table,
    /// when it has a foreign key to all dimension tables.
    fn test_compute_best_join_order_star_schema() {
        const NUM_DIM_TABLES: usize = 9;
        const FACT_TABLE_IDX: usize = 9;

        // Create fact table with foreign keys to all dimension tables
        let mut fact_columns = vec![_create_column_rowid_alias("id")];
        for i in 0..NUM_DIM_TABLES {
            fact_columns.push(_create_column_of_type(
                &format!("dim{}_id", i),
                Type::Integer,
            ));
        }
        let fact_table = _create_btree_table("fact", fact_columns);

        // Create dimension tables, each with an id and value column
        let dim_tables: Vec<_> = (0..NUM_DIM_TABLES)
            .map(|i| {
                _create_btree_table(
                    &format!("dim{}", i),
                    vec![
                        _create_column_rowid_alias("id"),
                        _create_column_of_type("value", Type::Integer),
                    ],
                )
            })
            .collect();

        let mut where_clause = vec![];

        // Add join conditions between fact and each dimension table
        for i in 0..NUM_DIM_TABLES {
            where_clause.push(_create_binary_expr(
                _create_column_expr(FACT_TABLE_IDX, i + 1, false), // fact.dimX_id
                ast::Operator::Equals,
                _create_column_expr(i, 0, true), // dimX.id
            ));
        }

        let table_references = {
            let mut refs = vec![_create_table_reference(dim_tables[0].clone(), None)];
            refs.extend(dim_tables.iter().skip(1).map(|t| {
                _create_table_reference(
                    t.clone(),
                    Some(JoinInfo {
                        outer: false,
                        using: None,
                    }),
                )
            }));
            refs.push(_create_table_reference(
                fact_table.clone(),
                Some(JoinInfo {
                    outer: false,
                    using: None,
                }),
            ));
            refs
        };

        let access_methods_arena = RefCell::new(Vec::new());
        let available_indexes = HashMap::new();
        let constraints =
            constraints_from_where_clause(&where_clause, &table_references, &available_indexes)
                .unwrap();

        let result = compute_best_join_order(
            &table_references,
            &where_clause,
            None,
            &constraints,
            &access_methods_arena,
        )
        .unwrap();
        assert!(result.is_some());
        let BestJoinOrderResult { best_plan, .. } = result.unwrap();

        // Expected optimal order: fact table as outer, with rowid seeks in any order on each dimension table
        // Verify fact table is selected as the outer table as all the other tables can use SeekRowid
        assert_eq!(
            best_plan.table_numbers[0], FACT_TABLE_IDX,
            "First table should be fact (table {}) due to available index, got table {} instead",
            FACT_TABLE_IDX, best_plan.table_numbers[0]
        );

        // Verify access methods
        assert!(
            matches!(
                &access_methods_arena.borrow()[best_plan.best_access_methods[0]].kind,
                AccessMethodKind::Scan { index: None, iter_dir }
                if *iter_dir == IterationDirection::Forwards
            ),
            "First table (fact) should use table scan due to column filter"
        );

        for i in 1..best_plan.table_numbers.len() {
            assert!(
                matches!(
                    &access_methods_arena.borrow()[best_plan.best_access_methods[i]].kind,
                    AccessMethodKind::Search {
                        index: None,
                        iter_dir,
                        constraints,
                    }
                    if *iter_dir == IterationDirection::Forwards && constraints.len() == 1 && constraints[0].lhs_mask.contains_table(FACT_TABLE_IDX)
                ),
                "Table {} should use Search access method, got {:?}",
                i + 1,
                &access_methods_arena.borrow()[best_plan.best_access_methods[i]].kind
            );
        }
    }

    #[test]
    /// Test that [compute_best_join_order] figures out that the tables form a "linked list" pattern
    /// where a column in each table points to an indexed column in the next table,
    /// and chooses the best order based on that.
    fn test_compute_best_join_order_linked_list() {
        const NUM_TABLES: usize = 5;

        // Create tables t1 -> t2 -> t3 -> t4 -> t5 where there is a foreign key from each table to the next
        let mut tables = Vec::with_capacity(NUM_TABLES);
        for i in 0..NUM_TABLES {
            let mut columns = vec![_create_column_rowid_alias("id")];
            if i < NUM_TABLES - 1 {
                columns.push(_create_column_of_type(&format!("next_id"), Type::Integer));
            }
            tables.push(_create_btree_table(&format!("t{}", i + 1), columns));
        }

        let available_indexes = HashMap::new();

        // Create table references
        let table_references: Vec<_> = tables
            .iter()
            .map(|t| _create_table_reference(t.clone(), None))
            .collect();

        // Create where clause linking each table to the next
        let mut where_clause = Vec::new();
        for i in 0..NUM_TABLES - 1 {
            where_clause.push(_create_binary_expr(
                _create_column_expr(i, 1, false), // ti.next_id
                ast::Operator::Equals,
                _create_column_expr(i + 1, 0, true), // t(i+1).id
            ));
        }

        let access_methods_arena = RefCell::new(Vec::new());
        let constraints =
            constraints_from_where_clause(&where_clause, &table_references, &available_indexes)
                .unwrap();

        // Run the optimizer
        let BestJoinOrderResult { best_plan, .. } = compute_best_join_order(
            &table_references,
            &where_clause,
            None,
            &constraints,
            &access_methods_arena,
        )
        .unwrap()
        .unwrap();

        // Verify the join order is exactly t1 -> t2 -> t3 -> t4 -> t5
        for i in 0..NUM_TABLES {
            assert_eq!(
                best_plan.table_numbers[i], i,
                "Expected table {} at position {}, got table {} instead",
                i, i, best_plan.table_numbers[i]
            );
        }

        // Verify access methods:
        // - First table should use Table scan
        assert!(
            matches!(
                &access_methods_arena.borrow()[best_plan.best_access_methods[0]].kind,
                AccessMethodKind::Scan { index: None, iter_dir }
                if *iter_dir == IterationDirection::Forwards
            ),
            "First table should use Table scan"
        );

        // all of the rest should use rowid equality
        for i in 1..NUM_TABLES {
            let method = &access_methods_arena.borrow()[best_plan.best_access_methods[i]].kind;
            assert!(
                matches!(
                    method,
                    AccessMethodKind::Search {
                        index: None,
                        iter_dir,
                        constraints,
                    }
                    if *iter_dir == IterationDirection::Forwards && constraints.len() == 1 && constraints[0].lhs_mask.contains_table(i-1)
                ),
                "Table {} should use Search access method, got {:?}",
                i + 1,
                method
            );
        }
    }

    fn _create_column(c: &TestColumn) -> Column {
        Column {
            name: Some(c.name.clone()),
            ty: c.ty,
            ty_str: c.ty.to_string(),
            is_rowid_alias: c.is_rowid_alias,
            primary_key: false,
            notnull: false,
            default: None,
        }
    }
    fn _create_column_of_type(name: &str, ty: Type) -> Column {
        _create_column(&TestColumn {
            name: name.to_string(),
            ty,
            is_rowid_alias: false,
        })
    }

    fn _create_column_list(names: &[&str], ty: Type) -> Vec<Column> {
        names
            .iter()
            .map(|name| _create_column_of_type(name, ty))
            .collect()
    }

    fn _create_column_rowid_alias(name: &str) -> Column {
        _create_column(&TestColumn {
            name: name.to_string(),
            ty: Type::Integer,
            is_rowid_alias: true,
        })
    }

    /// Creates a BTreeTable with the given name and columns
    fn _create_btree_table(name: &str, columns: Vec<Column>) -> Rc<BTreeTable> {
        Rc::new(BTreeTable {
            root_page: 1, // Page number doesn't matter for tests
            name: name.to_string(),
            primary_key_columns: vec![],
            columns,
            has_rowid: true,
            is_strict: false,
        })
    }

    /// Creates a TableReference for a BTreeTable
    fn _create_table_reference(
        table: Rc<BTreeTable>,
        join_info: Option<JoinInfo>,
    ) -> TableReference {
        let name = table.name.clone();
        TableReference {
            table: Table::BTree(table),
            op: Operation::Scan {
                iter_dir: IterationDirection::Forwards,
                index: None,
            },
            identifier: name,
            join_info,
            col_used_mask: ColumnUsedMask::new(),
        }
    }

    /// Creates a column expression
    fn _create_column_expr(table: usize, column: usize, is_rowid_alias: bool) -> Expr {
        Expr::Column {
            database: None,
            table,
            column,
            is_rowid_alias,
        }
    }

    /// Creates a binary expression for a WHERE clause
    fn _create_binary_expr(lhs: Expr, op: Operator, rhs: Expr) -> WhereTerm {
        WhereTerm {
            expr: Expr::Binary(Box::new(lhs), op, Box::new(rhs)),
            from_outer_join: None,
        }
    }

    /// Creates a numeric literal expression
    fn _create_numeric_literal(value: &str) -> Expr {
        Expr::Literal(ast::Literal::Numeric(value.to_string()))
    }
}
