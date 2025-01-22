use std::{cell::RefCell, rc::Rc};

use crate::schema::EphemeralTable;

pub struct EphemeralCursor {
    table: Rc<RefCell<EphemeralTable>>,
    rowid: Option<u64>,
}

impl EphemeralCursor {
    pub fn new() -> Self {
        Self {
            table: todo!(),
            rowid: todo!(),
        }
    }
}
