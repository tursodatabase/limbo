use limbo_sqlite3_parser::ast;

use super::constraints::Constraint;

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
pub struct IndexInfo {
    pub unique: bool,
    pub column_count: usize,
    pub covering: bool,
}

pub const ESTIMATED_HARDCODED_ROWS_PER_TABLE: usize = 1000000;
pub const ESTIMATED_HARDCODED_ROWS_PER_PAGE: usize = 50; // roughly 80 bytes per 4096 byte page

pub fn estimate_page_io_cost(rowcount: f64) -> Cost {
    Cost((rowcount as f64 / ESTIMATED_HARDCODED_ROWS_PER_PAGE as f64).ceil())
}

/// Estimate the cost of a scan or seek operation.
///
/// This is a very simple model that estimates the number of pages read
/// based on the number of rows read, ignoring any CPU costs.
pub fn estimate_cost_for_scan_or_seek(
    index_info: Option<IndexInfo>,
    constraints: &[Constraint],
    input_cardinality: f64,
) -> Cost {
    let Some(index_info) = index_info else {
        return estimate_page_io_cost(
            input_cardinality * ESTIMATED_HARDCODED_ROWS_PER_TABLE as f64,
        );
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

    estimate_page_io_cost(
        cost_multiplier
            * ESTIMATED_HARDCODED_ROWS_PER_TABLE as f64
            * input_cardinality
            * covering_multiplier,
    )
}
