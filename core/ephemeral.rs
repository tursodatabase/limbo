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

    pub fn insert(
        &mut self,
        key: &OwnedValue,
        record: &OwnedRecord,
        moved_before: bool,
    ) -> Result<CursorResult<()>> {
        let mut table = self.table.borrow_mut();

        // Generate a new row ID if necessary
        let rowid = if moved_before {
            // Traverse to find the correct position (here, just use `key` as rowid for simplicity)
            if let OwnedValue::Integer(rowid) = key {
                *rowid as u64
            } else {
                return Err(LimboError::InternalError(
                    "Invalid key type for rowid".to_string(),
                ));
            }
        } else {
            // Use the next available rowid
            let rowid = table.next_rowid;
            table.next_rowid += 1;
            rowid
        };

        // Insert the record into the table
        if table.rows.insert(rowid, record.values.clone()).is_some() {
            // If a row already exists with the same rowid, overwrite it
            self.rowid = Some(rowid);
            self.current = Some(record.clone());
            self.null_flag = false;
            return Ok(CursorResult::Ok(()));
        }

        // Update cursor state
        self.rowid = Some(rowid);
        self.current = Some(record.clone());
        self.null_flag = false;

        Ok(CursorResult::Ok(()))
    }

    pub fn rewind(&mut self) -> Result<CursorResult<()>> {
        let table = self.table.borrow();
        let rows = &table.rows;

        if let Some((&first_rowid, row_data)) = rows.iter().next() {
            self.rowid = Some(first_rowid);
            self.current = Some(OwnedRecord {
                values: row_data.clone(),
            });
            self.null_flag = false;
            Ok(CursorResult::Ok(()))
        } else {
            self.rowid = None;
            self.current = None;
            self.null_flag = true;
            Ok(CursorResult::Ok(()))
        }
    }

    pub fn last(&mut self) -> Result<CursorResult<()>> {
        let table = self.table.borrow();
        let rows = &table.rows;

        if let Some((&last_rowid, row_data)) = rows.iter().next_back() {
            self.rowid = Some(last_rowid);
            self.current = Some(OwnedRecord {
                values: row_data.clone(),
            });
            self.null_flag = false;
            Ok(CursorResult::Ok(()))
        } else {
            self.rowid = None;
            self.current = None;
            self.null_flag = true;
            Ok(CursorResult::Ok(()))
        }
    }

    pub fn wait_for_completion(&mut self) -> Result<()> {
        // Ephemeral operations should be sync
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
        let table = self.table.borrow();
        let rows = &table.rows;

        if self.rowid.is_none() {
            if let Some((&first_rowid, row_data)) = rows.iter().next() {
                self.rowid = Some(first_rowid);
                self.current = Some(OwnedRecord {
                    values: row_data.clone(),
                });
                self.null_flag = false;
                return Ok(CursorResult::Ok(()));
            }
        } else if let Some(current_rowid) = self.rowid {
            if let Some((&next_rowid, row_data)) = rows.range((current_rowid + 1)..).next() {
                self.rowid = Some(next_rowid);
                self.current = Some(OwnedRecord {
                    values: row_data.clone(),
                });
                self.null_flag = false;
                return Ok(CursorResult::Ok(()));
            }
        }

        // No more rows
        self.null_flag = true;
        self.rowid = None;
        self.current = None;
        Ok(CursorResult::Ok(()))
    }
    pub fn prev(&mut self) -> Result<CursorResult<()>> {
        let table = self.table.borrow();
        let rows = &table.rows;

        if self.rowid.is_none() {
            if let Some((&first_rowid, row_data)) = rows.iter().next_back() {
                self.rowid = Some(first_rowid);
                self.current = Some(OwnedRecord {
                    values: row_data.clone(),
                });
                self.null_flag = false;
                return Ok(CursorResult::Ok(()));
            }
        } else if let Some(current_rowid) = self.rowid {
            if let Some((&next_rowid, row_data)) = rows.range(..current_rowid).next_back() {
                self.rowid = Some(next_rowid);
                self.current = Some(OwnedRecord {
                    values: row_data.clone(),
                });
                self.null_flag = false;
                return Ok(CursorResult::Ok(()));
            }
        }

        // No more rows
        self.null_flag = true;
        self.rowid = None;
        self.current = None;
        Ok(CursorResult::Ok(()))
    }

    pub fn exists(&mut self, key: &OwnedValue) -> Result<CursorResult<bool>> {
        let table = self.table.borrow();
        let rows = &table.rows;

        for (rowid, row_data) in rows.iter() {
            if row_data.contains(key) {
                self.rowid = Some(*rowid);
                self.current = Some(OwnedRecord {
                    values: row_data.clone(),
                });
                self.null_flag = false;
                return Ok(CursorResult::Ok(true));
            }
        }

        // Key not found
        self.rowid = None;
        self.current = None;
        self.null_flag = true;
        Ok(CursorResult::Ok(false))
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, collections::BTreeMap, rc::Rc};

    use crate::{
        schema::EphemeralTable,
        types::{CursorResult, LimboText, OwnedRecord, OwnedValue},
    };

    use super::EphemeralCursor;

    #[test]
    fn test_next() {
        let mut table = EphemeralTable {
            rows: BTreeMap::new(),
            next_rowid: 1,
            columns: vec![],
        };
        let val1 = vec![OwnedValue::Integer(42)];
        let val2 = vec![OwnedValue::Text(LimboText::new(Rc::new(
            "Hello".to_string(),
        )))];
        table.rows.insert(1, val1.clone());
        table.rows.insert(2, val2.clone());

        let mut cursor = EphemeralCursor {
            table: Rc::new(RefCell::new(table)),
            rowid: None,
            current: None,
            null_flag: true,
        };

        cursor.next().unwrap(); // Move to the first row
        assert_eq!(
            cursor.current,
            Some(OwnedRecord {
                values: val1.clone()
            })
        );

        cursor.next().unwrap(); // Move to the second row
        assert_eq!(
            cursor.current,
            Some(OwnedRecord {
                values: val2.clone()
            })
        );
    }

    #[test]
    fn test_prev() {
        let mut table = EphemeralTable {
            rows: BTreeMap::new(),
            next_rowid: 1,
            columns: vec![],
        };

        let val1 = vec![OwnedValue::Integer(42)];
        let val2 = vec![OwnedValue::Text(LimboText::new(Rc::new(
            "Hello".to_string(),
        )))];
        table.rows.insert(1, val1.clone());
        table.rows.insert(2, val2.clone());

        let mut cursor = EphemeralCursor {
            table: Rc::new(RefCell::new(table)),
            rowid: None,
            current: None,
            null_flag: true,
        };

        cursor.prev().unwrap(); // Should move to row 2
        assert_eq!(
            cursor.current,
            Some(OwnedRecord {
                values: val2.clone()
            })
        );

        cursor.prev().unwrap(); // Should move to row 1
        assert_eq!(
            cursor.current,
            Some(OwnedRecord {
                values: val1.clone()
            })
        );

        cursor.prev().unwrap(); // Should go out of bounds
        assert!(cursor.current.is_none());
        assert!(cursor.null_flag);
    }

    #[test]
    fn test_last() {
        let mut table = EphemeralTable {
            rows: BTreeMap::new(),
            next_rowid: 1,
            columns: vec![],
        };

        let val1 = vec![OwnedValue::Integer(42)];
        let val2 = vec![OwnedValue::Text(LimboText::new(Rc::new(
            "Hello".to_string(),
        )))];
        table.rows.insert(1, val1.clone());
        table.rows.insert(2, val2.clone());

        let mut cursor = EphemeralCursor {
            table: Rc::new(RefCell::new(table)),
            rowid: None,
            current: None,
            null_flag: true,
        };

        cursor.last().unwrap(); // Move to the last row
        assert_eq!(
            cursor.current,
            Some(OwnedRecord {
                values: val2.clone()
            })
        );
        assert_eq!(cursor.rowid, Some(2));
        assert!(!cursor.null_flag);
    }

    #[test]
    fn test_last_empty_table() {
        let table = EphemeralTable {
            rows: BTreeMap::new(),
            next_rowid: 1,
            columns: vec![],
        };

        let mut cursor = EphemeralCursor {
            table: Rc::new(RefCell::new(table)),
            rowid: None,
            current: None,
            null_flag: true,
        };

        cursor.last().unwrap(); // Calling last on an empty table
        assert!(cursor.current.is_none());
        assert!(cursor.null_flag);
        assert!(cursor.rowid.is_none());
    }

    #[test]
    fn test_rewind() {
        let mut table = EphemeralTable {
            rows: BTreeMap::new(),
            next_rowid: 1,
            columns: vec![],
        };

        let val1 = vec![OwnedValue::Integer(42)];
        let val2 = vec![OwnedValue::Text(LimboText::new(Rc::new(
            "Hello".to_string(),
        )))];
        table.rows.insert(1, val1.clone());
        table.rows.insert(2, val2.clone());

        let mut cursor = EphemeralCursor {
            table: Rc::new(RefCell::new(table)),
            rowid: None,
            current: None,
            null_flag: true,
        };

        cursor.rewind().unwrap(); // Move to the first row
        assert_eq!(
            cursor.current,
            Some(OwnedRecord {
                values: val1.clone()
            })
        );
        assert_eq!(cursor.rowid, Some(1));
        assert!(!cursor.null_flag);
    }

    #[test]
    fn test_rewind_empty_table() {
        let table = EphemeralTable {
            rows: BTreeMap::new(),
            next_rowid: 1,
            columns: vec![],
        };

        let mut cursor = EphemeralCursor {
            table: Rc::new(RefCell::new(table)),
            rowid: None,
            current: None,
            null_flag: true,
        };

        cursor.rewind().unwrap(); // Calling rewind on an empty table
        assert!(cursor.current.is_none());
        assert!(cursor.null_flag);
        assert!(cursor.rowid.is_none());
    }

    #[test]
    fn test_exists_key_found() {
        let mut table = EphemeralTable {
            rows: BTreeMap::new(),
            next_rowid: 1,
            columns: vec![],
        };

        let val1 = OwnedValue::Integer(42);
        let val2 = OwnedValue::Text(LimboText::new(Rc::new("Hello".to_string())));
        table.rows.insert(1, vec![val1.clone()]);
        table.rows.insert(2, vec![val2.clone()]);

        let mut cursor = EphemeralCursor {
            table: Rc::new(RefCell::new(table)),
            rowid: None,
            current: None,
            null_flag: true,
        };

        let result = cursor.exists(&val1).unwrap();
        assert_eq!(result, CursorResult::Ok(true));
        assert_eq!(cursor.rowid, Some(1));
        assert_eq!(
            cursor.current,
            Some(OwnedRecord {
                values: vec![val1.clone()]
            })
        );
        assert!(!cursor.null_flag);
    }

    #[test]
    fn test_exists_key_not_found() {
        let mut table = EphemeralTable {
            rows: BTreeMap::new(),
            next_rowid: 1,
            columns: vec![],
        };

        let val1 = OwnedValue::Integer(42);
        let val2 = OwnedValue::Text(LimboText::new(Rc::new("Hello".to_string())));
        table.rows.insert(1, vec![val1.clone()]);
        table.rows.insert(2, vec![val2.clone()]);

        let mut cursor = EphemeralCursor {
            table: Rc::new(RefCell::new(table)),
            rowid: None,
            current: None,
            null_flag: true,
        };

        let result = cursor.exists(&OwnedValue::Integer(99)).unwrap();
        assert_eq!(result, CursorResult::Ok(false));
        assert!(cursor.rowid.is_none());
        assert!(cursor.current.is_none());
        assert!(cursor.null_flag);
    }

    #[test]
    fn test_insert_new_row() {
        let table = EphemeralTable {
            rows: BTreeMap::new(),
            next_rowid: 1,
            columns: vec![],
        };

        let mut cursor = EphemeralCursor {
            table: Rc::new(RefCell::new(table)),
            rowid: None,
            current: None,
            null_flag: true,
        };

        let key = OwnedValue::Integer(1);
        let record = OwnedRecord {
            values: vec![OwnedValue::Text(LimboText::new(Rc::new(
                "Hello".to_string(),
            )))],
        };

        cursor.insert(&key, &record, false).unwrap();

        let table = cursor.table.borrow();
        assert_eq!(table.rows.len(), 1);
        assert_eq!(table.rows.get(&1), Some(&record.values));
        assert_eq!(cursor.rowid, Some(1));
        assert_eq!(cursor.current, Some(record));
        assert!(!cursor.null_flag);
    }

    #[test]
    fn test_insert_overwrite_row() {
        let table = EphemeralTable {
            rows: BTreeMap::new(),
            next_rowid: 1,
            columns: vec![],
        };

        let mut cursor = EphemeralCursor {
            table: Rc::new(RefCell::new(table)),
            rowid: None,
            current: None,
            null_flag: true,
        };

        let key = OwnedValue::Integer(1);
        let record1 = OwnedRecord {
            values: vec![OwnedValue::Text(LimboText::new(Rc::new(
                "First".to_string(),
            )))],
        };
        let record2 = OwnedRecord {
            values: vec![OwnedValue::Text(LimboText::new(Rc::new(
                "Second".to_string(),
            )))],
        };

        cursor.insert(&key, &record1, false).unwrap();
        cursor.insert(&key, &record2, true).unwrap();

        let table = cursor.table.borrow();
        assert_eq!(table.rows.len(), 1);
        assert_eq!(table.rows.get(&1), Some(&record2.values));
        assert_eq!(cursor.rowid, Some(1));
        assert_eq!(cursor.current, Some(record2));
        assert!(!cursor.null_flag);
    }
}
