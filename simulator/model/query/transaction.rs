use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::{model::table::SimValue, runner::env::SimulatorEnv};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Begin {
    pub(crate) immediate: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Commit;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Rollback;

impl Begin {
    pub(crate) fn shadow(&self, _env: &mut SimulatorEnv) -> Vec<Vec<SimValue>> {
        vec![]
    }
}

impl Commit {
    pub(crate) fn shadow(&self, _env: &mut SimulatorEnv) -> Vec<Vec<SimValue>> {
        vec![]
    }
}

impl Rollback {
    pub(crate) fn shadow(&self, _env: &mut SimulatorEnv) -> Vec<Vec<SimValue>> {
        vec![]
    }
}

impl Display for Begin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BEGIN {}", if self.immediate { "IMMEDIATE" } else { "" })
    }
}

impl Display for Commit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "COMMIT")
    }
}

impl Display for Rollback {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ROLLBACK")
    }
}
