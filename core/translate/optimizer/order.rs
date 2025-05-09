use std::cell::RefCell;

use limbo_sqlite3_parser::ast::{self, SortOrder};

use crate::{
    translate::plan::{GroupBy, IterationDirection, TableReference},
    util::exprs_are_equivalent,
};

use super::{
    access_method::{AccessMethod, AccessMethodKind},
    join::JoinN,
};

#[derive(Debug, PartialEq, Clone)]
pub struct ColumnOrder {
    pub table_no: usize,
    pub column_no: usize,
    pub order: SortOrder,
}

#[derive(Debug, PartialEq, Clone)]
pub enum EliminatesSort {
    GroupBy,
    OrderBy,
    GroupByAndOrderBy,
}

#[derive(Debug, PartialEq, Clone)]
pub struct OrderTarget(pub Vec<ColumnOrder>, pub EliminatesSort);

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
pub fn compute_order_target(
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

/// Check if the plan's row iteration order matches the [OrderTarget]'s column order
pub fn plan_satisfies_order_target(
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
