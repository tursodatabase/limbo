use std::{
    cell::{Ref, RefCell},
    rc::Rc,
};

use crate::{
    schema::EphemeralTable,
    types::{CursorResult, OwnedRecord},
    LimboError,
};
use crate::{types::OwnedValue, Result};

pub struct EphemeralCursor {
    table: Rc<RefCell<EphemeralTable>>,
    rowid: Option<u64>,
    current: Option<OwnedRecord>,
    null_flag: bool,
}

impl EphemeralCursor {
    pub fn new() -> Self {
        let table = Rc::new(RefCell::new(EphemeralTable::new()));
        Self {
            table,
            rowid: None,
            current: None,
            null_flag: false,
        }
    }

    pub fn rewind(&mut self) -> Result<CursorResult<()>> {
        todo!()
    }
    pub fn last(&mut self) -> Result<CursorResult<()>> {
        todo!()
    }

    pub fn wait_for_completion(&mut self) -> Result<()> {
        // TODO: Wait for pager I/O to complete
        Ok(())
    }

    pub fn record(&self) -> Option<&OwnedRecord> {
        self.current.as_ref()
    }

    pub fn is_empty(&self) -> bool {
        self.current.is_none()
    }

    pub fn get_null_flag(&self) -> bool {
        self.null_flag
    }

    pub fn set_null_flag(&mut self, flag: bool) {
        self.null_flag = flag;
    }

    pub fn next(&mut self) -> Result<CursorResult<()>> {
        // TODO
        Ok(CursorResult::Ok(()))
    }
    pub fn prev(&mut self) -> Result<CursorResult<()>> {
        // TODO
        Ok(CursorResult::Ok(()))
    }

    pub fn exists(&mut self, key: &OwnedValue) -> Result<CursorResult<bool>> {
        todo!()
    }
}
