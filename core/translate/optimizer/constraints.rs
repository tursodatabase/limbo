use std::{cmp::Ordering, collections::HashMap, sync::Arc};

use crate::{
    schema::{Column, Index},
    translate::{
        expr::as_binary_components,
        plan::{JoinOrderMember, TableReference, WhereTerm},
        planner::{table_mask_from_expr, TableMask},
    },
    Result,
};
use limbo_sqlite3_parser::ast::{self, SortOrder};

use super::cost::ESTIMATED_HARDCODED_ROWS_PER_TABLE;

#[derive(Debug, Clone)]
pub struct Constraint {
    /// The position of the constraint in the WHERE clause, e.g. in SELECT * FROM t WHERE true AND t.x = 10, the position is (1, BinaryExprSide::Rhs),
    /// since the RHS '10' is the constraining expression and it's part of the second term in the WHERE clause.
    pub where_clause_pos: (usize, BinaryExprSide),
    /// The operator of the constraint, e.g. =, >, <
    pub operator: ast::Operator,
    /// The position of the constrained column in the table.
    pub table_col_pos: usize,
    /// Bitmask of tables that are required to be on the left side of the constrained table,
    /// e.g. in SELECT * FROM t1,t2,t3 WHERE t1.x = t2.x + t3.x, the lhs_mask contains t2 and t3.
    pub lhs_mask: TableMask,
    /// The selectivity of the constraint, i.e. the fraction of rows that will match the constraint.
    pub selectivity: f64,
}

#[derive(Debug, Clone)]
/// A reference to a [Constraint] in a [TableConstraints].
pub struct ConstraintRef {
    /// The position of the constraint in the [TableConstraints::constraints] vector.
    pub constraint_vec_pos: usize,
    /// The position of the constrained column in the index. Always 0 for rowid indices.
    pub index_col_pos: usize,
    /// The sort order of the constrained column in the index. Always ascending for rowid indices.
    pub sort_order: SortOrder,
}
#[derive(Debug, Clone)]
/// A collection of [ConstraintRef]s for a given index, or if index is None, for the table's rowid index.
pub struct ConstraintUseCandidate {
    /// The index that may be used to satisfy the constraints. If none, the table's rowid index is used.
    pub index: Option<Arc<Index>>,
    /// References to the constraints that may be used as an access path for the index.
    pub refs: Vec<ConstraintRef>,
}

#[derive(Debug)]
/// A collection of [Constraint]s and their potential [ConstraintUseCandidate]s for a given table.
pub struct TableConstraints {
    pub table_no: usize,
    /// The constraints for the table, i.e. any [WhereTerm]s that reference columns from this table.
    pub constraints: Vec<Constraint>,
    /// Candidates for indexes that may use the constraints to perform a lookup.
    pub candidates: Vec<ConstraintUseCandidate>,
}

/// Helper enum for [Constraint] to indicate which side of a binary comparison expression is being compared to the index column.
/// For example, if the where clause is "WHERE x = 10" and there's an index on x,
/// the [Constraint] for the where clause term "x = 10" will have a [BinaryExprSide::Rhs]
/// because the right hand side expression "10" is being compared to the index column "x".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryExprSide {
    Lhs,
    Rhs,
}

/// In lieu of statistics, we estimate that an equality filter will reduce the output set to 1% of its size.
const SELECTIVITY_EQ: f64 = 0.01;
/// In lieu of statistics, we estimate that a range filter will reduce the output set to 40% of its size.
const SELECTIVITY_RANGE: f64 = 0.4;
/// In lieu of statistics, we estimate that other filters will reduce the output set to 90% of its size.
const SELECTIVITY_OTHER: f64 = 0.9;

const SELECTIVITY_UNIQUE_EQUALITY: f64 = 1.0 / ESTIMATED_HARDCODED_ROWS_PER_TABLE as f64;

/// Estimate the selectivity of a constraint based on the operator and the column type.
fn estimate_selectivity(column: &Column, op: ast::Operator) -> f64 {
    match op {
        ast::Operator::Equals => {
            if column.is_rowid_alias || column.primary_key {
                SELECTIVITY_UNIQUE_EQUALITY
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
}

/// Precompute all potentially usable [Constraints] from a WHERE clause.
/// The resulting list of [TableConstraints] is then used to evaluate the best access methods for various join orders.
pub fn constraints_from_where_clause(
    where_clause: &[WhereTerm],
    table_references: &[TableReference],
    available_indexes: &HashMap<String, Vec<Arc<Index>>>,
) -> Result<Vec<TableConstraints>> {
    let mut constraints = Vec::new();

    // For each table, collect all the Constraints and all potential index candidates that may use them.
    for (table_no, table_reference) in table_references.iter().enumerate() {
        let rowid_alias_column = table_reference
            .columns()
            .iter()
            .position(|c| c.is_rowid_alias);

        let mut cs = TableConstraints {
            table_no,
            constraints: Vec::new(),
            candidates: available_indexes
                .get(table_reference.table.get_name())
                .map_or(Vec::new(), |indexes| {
                    indexes
                        .iter()
                        .map(|index| ConstraintUseCandidate {
                            index: Some(index.clone()),
                            refs: Vec::new(),
                        })
                        .collect()
                }),
        };
        // Add a candidate for the rowid index, which is always available when the table has a rowid alias.
        cs.candidates.push(ConstraintUseCandidate {
            index: None,
            refs: Vec::new(),
        });

        for (i, term) in where_clause.iter().enumerate() {
            let Some((lhs, operator, rhs)) = as_binary_components(&term.expr)? else {
                continue;
            };

            // Constraints originating from a LEFT JOIN must always be evaluated in that join's RHS table's loop,
            // regardless of which tables the constraint references.
            if let Some(outer_join_tbl) = term.from_outer_join {
                if outer_join_tbl != table_no {
                    continue;
                }
            }

            // If either the LHS or RHS of the constraint is a column from the table, add the constraint.
            match lhs {
                ast::Expr::Column { table, column, .. } => {
                    if *table == table_no {
                        let table_column = &table_reference.table.columns()[*column];
                        cs.constraints.push(Constraint {
                            where_clause_pos: (i, BinaryExprSide::Rhs),
                            operator,
                            table_col_pos: *column,
                            lhs_mask: table_mask_from_expr(rhs)?,
                            selectivity: estimate_selectivity(table_column, operator),
                        });
                    }
                }
                ast::Expr::RowId { table, .. } => {
                    // A rowid alias column must exist for the 'rowid' keyword to be considered a valid reference.
                    // This should be a parse error at an earlier stage of the query compilation, but nevertheless,
                    // we check it here.
                    if *table == table_no && rowid_alias_column.is_some() {
                        let table_column =
                            &table_reference.table.columns()[rowid_alias_column.unwrap()];
                        cs.constraints.push(Constraint {
                            where_clause_pos: (i, BinaryExprSide::Rhs),
                            operator,
                            table_col_pos: rowid_alias_column.unwrap(),
                            lhs_mask: table_mask_from_expr(rhs)?,
                            selectivity: estimate_selectivity(table_column, operator),
                        });
                    }
                }
                _ => {}
            };
            match rhs {
                ast::Expr::Column { table, column, .. } => {
                    if *table == table_no {
                        let table_column = &table_reference.table.columns()[*column];
                        cs.constraints.push(Constraint {
                            where_clause_pos: (i, BinaryExprSide::Lhs),
                            operator: opposite_cmp_op(operator),
                            table_col_pos: *column,
                            lhs_mask: table_mask_from_expr(lhs)?,
                            selectivity: estimate_selectivity(table_column, operator),
                        });
                    }
                }
                ast::Expr::RowId { table, .. } => {
                    if *table == table_no && rowid_alias_column.is_some() {
                        let table_column =
                            &table_reference.table.columns()[rowid_alias_column.unwrap()];
                        cs.constraints.push(Constraint {
                            where_clause_pos: (i, BinaryExprSide::Lhs),
                            operator: opposite_cmp_op(operator),
                            table_col_pos: rowid_alias_column.unwrap(),
                            lhs_mask: table_mask_from_expr(lhs)?,
                            selectivity: estimate_selectivity(table_column, operator),
                        });
                    }
                }
                _ => {}
            };
        }
        // sort equalities first so that index keys will be properly constructed.
        // see e.g.: https://www.solarwinds.com/blog/the-left-prefix-index-rule
        cs.constraints.sort_by(|a, b| {
            if a.operator == ast::Operator::Equals {
                Ordering::Less
            } else if b.operator == ast::Operator::Equals {
                Ordering::Greater
            } else {
                Ordering::Equal
            }
        });

        // For each constraint we found, add a reference to it for each index that may be able to use it.
        for (i, constraint) in cs.constraints.iter().enumerate() {
            if rowid_alias_column.map_or(false, |idx| constraint.table_col_pos == idx) {
                let rowid_candidate = cs
                    .candidates
                    .iter_mut()
                    .find_map(|candidate| {
                        if candidate.index.is_none() {
                            Some(candidate)
                        } else {
                            None
                        }
                    })
                    .unwrap();
                rowid_candidate.refs.push(ConstraintRef {
                    constraint_vec_pos: i,
                    index_col_pos: 0,
                    sort_order: SortOrder::Asc,
                });
            }
            for index in available_indexes
                .get(table_reference.table.get_name())
                .unwrap_or(&Vec::new())
            {
                if let Some(position_in_index) =
                    index.column_table_pos_to_index_pos(constraint.table_col_pos)
                {
                    let index_candidate = cs
                        .candidates
                        .iter_mut()
                        .find_map(|candidate| {
                            if candidate
                                .index
                                .as_ref()
                                .map_or(false, |i| Arc::ptr_eq(index, i))
                            {
                                Some(candidate)
                            } else {
                                None
                            }
                        })
                        .unwrap();
                    index_candidate.refs.push(ConstraintRef {
                        constraint_vec_pos: i,
                        index_col_pos: position_in_index,
                        sort_order: index.columns[position_in_index].order,
                    });
                }
            }
        }

        for candidate in cs.candidates.iter_mut() {
            // Deduplicate by position, keeping first occurrence (which will be equality if one exists, since the constraints vec is sorted that way)
            candidate.refs.dedup_by_key(|cref| cref.index_col_pos);

            // Truncate at first gap in positions -- index columns must be consumed in contiguous order.
            let mut last_pos = 0;
            let mut i = 0;
            for cref in candidate.refs.iter() {
                if cref.index_col_pos != last_pos {
                    if cref.index_col_pos != last_pos + 1 {
                        break;
                    }
                    last_pos = cref.index_col_pos;
                }
                i += 1;
            }
            candidate.refs.truncate(i);

            // Truncate after the first inequality, since the left-prefix rule of indexes requires that all constraints but the last one must be equalities;
            // again see: https://www.solarwinds.com/blog/the-left-prefix-index-rule
            if let Some(first_inequality) = candidate.refs.iter().position(|cref| {
                cs.constraints[cref.constraint_vec_pos].operator != ast::Operator::Equals
            }) {
                candidate.refs.truncate(first_inequality + 1);
            }
        }
        constraints.push(cs);
    }

    Ok(constraints)
}

/// Find which [Constraint]s are usable for a given join order.
/// Returns a slice of the references to the constraints that are usable.
/// A constraint is considered usable for a given table if all of the other tables referenced by the constraint
/// are on the left side in the join order relative to the table.
pub fn usable_constraints_for_join_order<'a>(
    constraints: &'a [Constraint],
    refs: &'a [ConstraintRef],
    table_index: usize,
    join_order: &[JoinOrderMember],
) -> &'a [ConstraintRef] {
    let mut usable_until = 0;
    for cref in refs.iter() {
        let constraint = &constraints[cref.constraint_vec_pos];
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
    &refs[..usable_until]
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
