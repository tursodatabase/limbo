use std::sync::Arc;

use limbo_sqlite3_parser::ast::SortOrder;

use crate::{
    schema::Index,
    translate::plan::{IterationDirection, JoinOrderMember, TableReference},
    Result,
};

use super::{
    constraints::{usable_constraints_for_join_order, ConstraintRef, TableConstraints},
    cost::{estimate_cost_for_scan_or_seek, Cost, IndexInfo},
    order::OrderTarget,
};

#[derive(Debug, Clone)]
/// Represents a way to access a table.
pub struct AccessMethod<'a> {
    /// The estimated number of page fetches.
    /// We are ignoring CPU cost for now.
    pub cost: Cost,
    pub kind: AccessMethodKind<'a>,
}

impl<'a> AccessMethod<'a> {
    pub fn set_iter_dir(&mut self, new_dir: IterationDirection) {
        match &mut self.kind {
            AccessMethodKind::Scan { iter_dir, .. } => *iter_dir = new_dir,
            AccessMethodKind::Search { iter_dir, .. } => *iter_dir = new_dir,
        }
    }

    pub fn set_constraint_refs(
        &mut self,
        new_index: Option<Arc<Index>>,
        new_constraint_refs: &'a [ConstraintRef],
    ) {
        match (&mut self.kind, new_constraint_refs.is_empty()) {
            (
                AccessMethodKind::Search {
                    constraint_refs,
                    index,
                    ..
                },
                false,
            ) => {
                *constraint_refs = new_constraint_refs;
                *index = new_index;
            }
            (AccessMethodKind::Search { iter_dir, .. }, true) => {
                self.kind = AccessMethodKind::Scan {
                    index: new_index,
                    iter_dir: *iter_dir,
                };
            }
            (AccessMethodKind::Scan { iter_dir, .. }, false) => {
                self.kind = AccessMethodKind::Search {
                    index: new_index,
                    iter_dir: *iter_dir,
                    constraint_refs: new_constraint_refs,
                };
            }
            (AccessMethodKind::Scan { index, .. }, true) => {
                *index = new_index;
            }
        }
    }
}

#[derive(Debug, Clone)]
/// Represents the kind of access method.
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
        constraint_refs: &'a [ConstraintRef],
    },
}

/// Return the best [AccessMethod] for a given join order.
pub fn find_best_access_method_for_join_order<'a>(
    rhs_table: &TableReference,
    rhs_constraints: &'a TableConstraints,
    join_order: &[JoinOrderMember],
    maybe_order_target: Option<&OrderTarget>,
    input_cardinality: f64,
) -> Result<AccessMethod<'a>> {
    let table_no = join_order.last().unwrap().table_no;
    let cost_of_full_table_scan = estimate_cost_for_scan_or_seek(None, &[], &[], input_cardinality);
    let mut best_access_method = AccessMethod {
        cost: cost_of_full_table_scan,
        kind: AccessMethodKind::Scan {
            index: None,
            iter_dir: IterationDirection::Forwards,
        },
    };
    let rowid_column_idx = rhs_table.columns().iter().position(|c| c.is_rowid_alias);
    for candidate in rhs_constraints.candidates.iter() {
        let index_info = match candidate.index.as_ref() {
            Some(index) => IndexInfo {
                unique: index.unique,
                covering: rhs_table.index_is_covering(index),
                column_count: index.columns.len(),
            },
            None => IndexInfo {
                unique: true, // rowids are always unique
                covering: false,
                column_count: 1,
            },
        };
        let usable_constraint_refs = usable_constraints_for_join_order(
            &rhs_constraints.constraints,
            &candidate.refs,
            join_order,
        );
        let cost = estimate_cost_for_scan_or_seek(
            Some(index_info),
            &rhs_constraints.constraints,
            &usable_constraint_refs,
            input_cardinality,
        );

        let order_satisfiability_bonus = if let Some(order_target) = maybe_order_target {
            let mut all_same_direction = true;
            let mut all_opposite_direction = true;
            for i in 0..order_target.0.len().min(index_info.column_count) {
                let correct_table = order_target.0[i].table_no == table_no;
                let correct_column = {
                    match &candidate.index {
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
                    match &candidate.index {
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
        if cost < best_access_method.cost + order_satisfiability_bonus {
            best_access_method.cost = cost;
            best_access_method
                .set_constraint_refs(candidate.index.clone(), &usable_constraint_refs);
        }
    }

    let iter_dir = if let Some(order_target) = maybe_order_target {
        // if index columns match the order target columns in the exact reverse directions, then we should use IterationDirection::Backwards
        let index = match &best_access_method.kind {
            AccessMethodKind::Scan { index, .. } => index.as_ref(),
            AccessMethodKind::Search { index, .. } => index.as_ref(),
        };
        let mut should_use_backwards = true;
        let num_cols = index.map_or(1, |i| i.columns.len());
        for i in 0..order_target.0.len().min(num_cols) {
            let correct_table = order_target.0[i].table_no == table_no;
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
