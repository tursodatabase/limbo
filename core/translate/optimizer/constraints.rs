use std::{cmp::Ordering, collections::HashMap, sync::Arc};

use crate::{
    schema::Index,
    translate::{
        expr::{as_binary_components, unwrap_parens},
        plan::{JoinOrderMember, TableReference, WhereTerm},
        planner::{table_mask_from_expr, TableMask},
    },
    Result,
};
use limbo_sqlite3_parser::ast::{self, SortOrder};
#[derive(Debug, Clone)]
pub struct Constraint {
    /// The position of the constraint in the WHERE clause, e.g. in SELECT * FROM t WHERE true AND t.x = 10, the position is (1, BinaryExprSide::Rhs),
    /// since the RHS '10' is the constraining expression and it's part of the second term in the WHERE clause.
    pub where_clause_pos: (usize, BinaryExprSide),
    /// The operator of the constraint, e.g. =, >, <
    pub operator: ast::Operator,
    /// The position of the index column in the index, e.g. if the index is (a,b,c) and the constraint is on b, then index_column_pos is 1.
    /// For Rowid constraints this is always 0.
    pub index_col_pos: usize,
    /// The position of the constrained column in the table.
    pub table_col_pos: usize,
    /// The sort order of the index column, ASC or DESC. For Rowid constraints this is always ASC.
    pub sort_order: SortOrder,
    /// Bitmask of tables that are required to be on the left side of the constrained table,
    /// e.g. in SELECT * FROM t1,t2,t3 WHERE t1.x = t2.x + t3.x, the lhs_mask contains t2 and t3.
    pub lhs_mask: TableMask,
}

#[derive(Debug, Clone)]
/// Lookup denotes how a given set of [Constraint]s can be used to access a table.
///
/// Lookup::Index(index) means that the constraints can be used to access the table using the given index.
/// Lookup::Rowid means that the constraints can be used to access the table using the table's rowid column.
/// Lookup::EphemeralIndex means that the constraints are not useful for accessing the table,
/// but an ephemeral index can be built ad-hoc to use them.
pub enum ConstraintLookup {
    Index(Arc<Index>),
    Rowid,
    EphemeralIndex,
}

#[derive(Debug)]
/// A collection of [Constraint]s for a given (table, index) pair.
pub struct Constraints {
    pub lookup: ConstraintLookup,
    pub table_no: usize,
    pub constraints: Vec<Constraint>,
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

/// Precompute all potentially usable [Constraints] from a WHERE clause.
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

        let mut cs = Constraints {
            lookup: ConstraintLookup::Rowid,
            table_no,
            constraints: Vec::new(),
        };
        let mut cs_ephemeral = Constraints {
            lookup: ConstraintLookup::EphemeralIndex,
            table_no,
            constraints: Vec::new(),
        };
        for (i, term) in where_clause.iter().enumerate() {
            let Some((lhs, operator, rhs)) = as_binary_components(&term.expr)? else {
                continue;
            };
            if let Some(outer_join_tbl) = term.from_outer_join {
                if outer_join_tbl != table_no {
                    continue;
                }
            }
            match lhs {
                ast::Expr::Column { table, column, .. } => {
                    if *table == table_no {
                        if rowid_alias_column.map_or(false, |idx| *column == idx) {
                            cs.constraints.push(Constraint {
                                where_clause_pos: (i, BinaryExprSide::Rhs),
                                operator,
                                index_col_pos: 0,
                                table_col_pos: rowid_alias_column.unwrap(),
                                sort_order: SortOrder::Asc,
                                lhs_mask: table_mask_from_expr(rhs)?,
                            });
                        } else {
                            cs_ephemeral.constraints.push(Constraint {
                                where_clause_pos: (i, BinaryExprSide::Rhs),
                                operator,
                                index_col_pos: 0,
                                table_col_pos: *column,
                                sort_order: SortOrder::Asc,
                                lhs_mask: table_mask_from_expr(rhs)?,
                            });
                        }
                    }
                }
                ast::Expr::RowId { table, .. } => {
                    if *table == table_no && rowid_alias_column.is_some() {
                        cs.constraints.push(Constraint {
                            where_clause_pos: (i, BinaryExprSide::Rhs),
                            operator,
                            index_col_pos: 0,
                            table_col_pos: rowid_alias_column.unwrap(),
                            sort_order: SortOrder::Asc,
                            lhs_mask: table_mask_from_expr(rhs)?,
                        });
                    }
                }
                _ => {}
            };
            match rhs {
                ast::Expr::Column { table, column, .. } => {
                    if *table == table_no {
                        if rowid_alias_column.map_or(false, |idx| *column == idx) {
                            cs.constraints.push(Constraint {
                                where_clause_pos: (i, BinaryExprSide::Lhs),
                                operator: opposite_cmp_op(operator),
                                index_col_pos: 0,
                                table_col_pos: rowid_alias_column.unwrap(),
                                sort_order: SortOrder::Asc,
                                lhs_mask: table_mask_from_expr(lhs)?,
                            });
                        } else {
                            cs_ephemeral.constraints.push(Constraint {
                                where_clause_pos: (i, BinaryExprSide::Lhs),
                                operator: opposite_cmp_op(operator),
                                index_col_pos: 0,
                                table_col_pos: *column,
                                sort_order: SortOrder::Asc,
                                lhs_mask: table_mask_from_expr(lhs)?,
                            });
                        }
                    }
                }
                ast::Expr::RowId { table, .. } => {
                    if *table == table_no && rowid_alias_column.is_some() {
                        cs.constraints.push(Constraint {
                            where_clause_pos: (i, BinaryExprSide::Lhs),
                            operator: opposite_cmp_op(operator),
                            index_col_pos: 0,
                            table_col_pos: rowid_alias_column.unwrap(),
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
            let pos_cmp = a.index_col_pos.cmp(&b.index_col_pos);
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
        cs_ephemeral.constraints.sort_by(|a, b| {
            if a.operator == ast::Operator::Equals {
                Ordering::Less
            } else if b.operator == ast::Operator::Equals {
                Ordering::Greater
            } else {
                Ordering::Equal
            }
        });

        // Deduplicate by position, keeping first occurrence (which will be equality if one exists)
        cs.constraints.dedup_by_key(|c| c.index_col_pos);

        // Truncate at first gap in positions
        let mut last_pos = 0;
        let mut i = 0;
        for constraint in cs.constraints.iter() {
            if constraint.index_col_pos != last_pos {
                if constraint.index_col_pos != last_pos + 1 {
                    break;
                }
                last_pos = constraint.index_col_pos;
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
        if rowid_alias_column.is_some() {
            constraints.push(cs);
        }
        constraints.push(cs_ephemeral);

        let indexes = available_indexes.get(table_reference.table.get_name());
        if let Some(indexes) = indexes {
            for index in indexes {
                let mut cs = Constraints {
                    lookup: ConstraintLookup::Index(index.clone()),
                    table_no,
                    constraints: Vec::new(),
                };
                for (i, term) in where_clause.iter().enumerate() {
                    let Some((lhs, operator, rhs)) = as_binary_components(&term.expr)? else {
                        continue;
                    };
                    if let Some(outer_join_tbl) = term.from_outer_join {
                        if outer_join_tbl != table_no {
                            continue;
                        }
                    }
                    if let Some(position_in_index) =
                        get_column_position_in_index(lhs, table_no, index)?
                    {
                        cs.constraints.push(Constraint {
                            where_clause_pos: (i, BinaryExprSide::Rhs),
                            operator,
                            index_col_pos: position_in_index,
                            table_col_pos: {
                                let ast::Expr::Column { column, .. } = unwrap_parens(lhs)? else {
                                    crate::bail_parse_error!("expected column in index constraint");
                                };
                                *column
                            },
                            sort_order: index.columns[position_in_index].order,
                            lhs_mask: table_mask_from_expr(rhs)?,
                        });
                    }
                    if let Some(position_in_index) =
                        get_column_position_in_index(rhs, table_no, index)?
                    {
                        cs.constraints.push(Constraint {
                            where_clause_pos: (i, BinaryExprSide::Lhs),
                            operator: opposite_cmp_op(operator),
                            index_col_pos: position_in_index,
                            table_col_pos: {
                                let ast::Expr::Column { column, .. } = unwrap_parens(rhs)? else {
                                    crate::bail_parse_error!("expected column in index constraint");
                                };
                                *column
                            },
                            sort_order: index.columns[position_in_index].order,
                            lhs_mask: table_mask_from_expr(lhs)?,
                        });
                    }
                }
                // First sort by position, with equalities first within each position
                cs.constraints.sort_by(|a, b| {
                    let pos_cmp = a.index_col_pos.cmp(&b.index_col_pos);
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
                cs.constraints.dedup_by_key(|c| c.index_col_pos);

                // Truncate at first gap in positions
                let mut last_pos = 0;
                let mut i = 0;
                for constraint in cs.constraints.iter() {
                    if constraint.index_col_pos != last_pos {
                        if constraint.index_col_pos != last_pos + 1 {
                            break;
                        }
                        last_pos = constraint.index_col_pos;
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

pub fn usable_constraints_for_join_order<'a>(
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
