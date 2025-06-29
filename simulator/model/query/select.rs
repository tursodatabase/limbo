use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::model::{table::SimValue, Shadow, SimulatorEnv};

use super::predicate::Predicate;

/// `SELECT` distinctness
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Distinctness {
    /// `DISTINCT`
    Distinct,
    /// `ALL`
    All,
}

/// `SELECT` or `RETURNING` result column
// https://sqlite.org/syntax/result-column.html
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ResultColumn {
    /// expression
    Expr(Predicate),
    /// `*`
    Star,
    /// column name
    Column(String),
}

impl Display for ResultColumn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResultColumn::Expr(expr) => write!(f, "({})", expr),
            ResultColumn::Star => write!(f, "*"),
            ResultColumn::Column(name) => write!(f, "{}", name),
        }
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Select {
    pub table: String,
    pub result_columns: Vec<ResultColumn>,
    pub predicate: Predicate,
    pub distinct: Distinctness,
    pub limit: Option<usize>,
}

impl Shadow for Select {
    fn shadow<E: SimulatorEnv>(&self, env: &mut E) -> Vec<Vec<SimValue>> {
        let table = env.tables().iter().find(|t| t.name == self.table.as_str());
        if let Some(table) = table {
            table
                .rows
                .iter()
                .filter(|row| self.predicate.test(row, table))
                .cloned()
                .collect()
        } else {
            vec![]
        }
    }
}

impl Display for Select {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SELECT {} FROM {} WHERE {}{}",
            self.result_columns
                .iter()
                .map(ResultColumn::to_string)
                .collect::<Vec<_>>()
                .join(", "),
            self.table,
            self.predicate,
            self.limit
                .map_or("".to_string(), |l| format!(" LIMIT {}", l))
        )
    }
}
