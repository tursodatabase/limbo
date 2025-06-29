use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::model::{table::SimValue, Shadow, SimulatorEnv};

use super::select::Select;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Insert {
    Values {
        table: String,
        values: Vec<Vec<SimValue>>,
    },
    Select {
        table: String,
        select: Box<Select>,
    },
}

impl Insert {
    pub(crate) fn table(&self) -> &str {
        match self {
            Insert::Values { table, .. } | Insert::Select { table, .. } => table,
        }
    }
}

impl Shadow for Insert {
    fn shadow<E: SimulatorEnv>(&self, env: &mut E) -> Vec<Vec<SimValue>> {
        match self {
            Insert::Values { table, values } => {
                if let Some(t) = env.tables_mut().iter_mut().find(|t| &t.name == table) {
                    t.rows.extend(values.clone());
                }
            }
            Insert::Select { table, select } => {
                let rows = select.shadow(env);
                if let Some(t) = env.tables_mut().iter_mut().find(|t| &t.name == table) {
                    t.rows.extend(rows);
                }
            }
        }

        vec![]
    }
}

impl Display for Insert {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Insert::Values { table, values } => {
                write!(f, "INSERT INTO {} VALUES ", table)?;
                for (i, row) in values.iter().enumerate() {
                    if i != 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "(")?;
                    for (j, value) in row.iter().enumerate() {
                        if j != 0 {
                            write!(f, ", ")?;
                        }
                        write!(f, "{}", value)?;
                    }
                    write!(f, ")")?;
                }
                Ok(())
            }
            Insert::Select { table, select } => {
                write!(f, "INSERT INTO {} ", table)?;
                write!(f, "{}", select)
            }
        }
    }
}
