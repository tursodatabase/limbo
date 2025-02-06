use crate::{
    schema::{EphemeralIndex, EphemeralTable},
    types::{CursorResult, OwnedRecord, SeekKey, SeekOp},
    LimboError,
};
use crate::{types::OwnedValue, Result};
pub struct EphemeralCursor {
    source: Ephemeral,
    rowid: Option<u64>,
    current: Option<OwnedRecord>,
    null_flag: bool,
}

enum Ephemeral {
    Table(EphemeralTable),
    Index(EphemeralIndex),
}

#[allow(dead_code)]
impl EphemeralCursor {
    pub fn new_with_table() -> Self {
        Self {
            source: Ephemeral::Table(EphemeralTable::new()),
            current: None,
            rowid: None,
            null_flag: false,
        }
    }

    pub fn new_with_index() -> Self {
        Self {
            source: Ephemeral::Index(EphemeralIndex::new()),
            current: None,
            rowid: None,
            null_flag: false,
        }
    }

    pub fn do_seek(
        &mut self,
        key: SeekKey<'_>,
        op: SeekOp,
    ) -> Result<CursorResult<(Option<u64>, Option<OwnedRecord>)>> {
        match &mut self.source {
            Ephemeral::Table(table) => {
                let SeekKey::TableRowId(rowid) = key else {
                    unreachable!("table seek key should be a rowid");
                };
                let rows = &table.rows;

                // Seek by row ID
                let entry = match op {
                    SeekOp::EQ => rows.get(&rowid).map(|values| (rowid, values.clone())),
                    SeekOp::GE => rows
                        .range(rowid..)
                        .next()
                        .map(|(&id, values)| (id, values.clone())),
                    SeekOp::GT => rows
                        .range((rowid + 1)..)
                        .next()
                        .map(|(&id, values)| (id, values.clone())),
                };

                if let Some((id, values)) = entry {
                    self.rowid = Some(id);
                    self.current = Some(OwnedRecord { values });
                    self.null_flag = false;
                    return Ok(CursorResult::Ok((Some(id), self.current.clone())));
                }
            }
            Ephemeral::Index(index) => {
                let SeekKey::IndexKey(index_key) = key else {
                    unreachable!("index seek key should be a record");
                };

                let search_key = index_key.clone();

                // Exclude the last value for comparison
                let trimmed_key = OwnedRecord {
                    values: search_key.values[..search_key.values.len() - 1].to_vec(),
                };

                let rows = &index.rows;
                let mut range = match op {
                    SeekOp::EQ => rows.range(trimmed_key.clone()..=trimmed_key.clone()),
                    SeekOp::GE => rows.range(trimmed_key.clone()..),
                    SeekOp::GT => rows.range(trimmed_key.clone()..),
                };

                // this is an obvious makeshift but I didn't find any better way to do it
                if matches!(op, SeekOp::GT) {
                    range.next();
                }

                if let Some(record) = range.next() {
                    let rowid = match record.values.last() {
                        Some(OwnedValue::Integer(rowid)) => *rowid as u64,
                        _ => unreachable!("index records should have an integer rowid"),
                    };

                    self.rowid = Some(rowid);
                    self.current = Some(record.clone());
                    self.null_flag = false;
                    return Ok(CursorResult::Ok((Some(rowid), self.current.clone())));
                }
            }
        }

        // No matching record found
        self.rowid = None;
        self.current = None;
        self.null_flag = true;
        Ok(CursorResult::Ok((None, None)))
    }

    pub fn insert(
        &mut self,
        key: &OwnedValue,
        record: &OwnedRecord,
        moved_before: bool,
    ) -> Result<CursorResult<()>> {
        match &mut self.source {
            Ephemeral::Table(table) => {
                let rowid = if moved_before {
                    if let OwnedValue::Integer(rowid) = key {
                        *rowid as u64
                    } else {
                        return Err(LimboError::InternalError(
                            "Invalid key type for rowid".to_string(),
                        ));
                    }
                } else {
                    let rowid = table.next_rowid;
                    table.next_rowid += 1;
                    rowid
                };

                table.rows.insert(rowid, record.values.clone());

                self.rowid = Some(rowid);
                self.current = Some(record.clone());
                self.null_flag = false;

                return Ok(CursorResult::Ok(()));
            }
            Ephemeral::Index(index) => {
                let OwnedValue::Integer(rowid) = key else {
                    return Err(LimboError::InternalError(
                        "Invalid key type for rowid".to_string(),
                    ));
                };
                let mut record = record.clone();

                index
                    .rows
                    .retain(|r| !r.values.contains(&OwnedValue::Integer(*rowid)));

                record.values.push(OwnedValue::Integer(*rowid));
                index.rows.insert(record.clone());

                self.rowid = Some(*rowid as u64);
                self.current = Some(record.clone());
                self.null_flag = false;

                return Ok(CursorResult::Ok(()));
            }
        }
    }

    pub fn rewind(&mut self) -> Result<CursorResult<()>> {
        match &self.source {
            Ephemeral::Table(table) => {
                if let Some((&first_rowid, row_data)) = table.rows.iter().next() {
                    self.rowid = Some(first_rowid);
                    self.current = Some(OwnedRecord {
                        values: row_data.clone(),
                    });
                    self.null_flag = false;
                    return Ok(CursorResult::Ok(()));
                }
            }
            Ephemeral::Index(index) => {
                if let Some(row) = index.rows.iter().next() {
                    let OwnedValue::Integer(rowid) = row.values.last().unwrap() else {
                        return Err(LimboError::InternalError(
                            "Invalid key type for rowid".to_string(),
                        ));
                    };
                    self.rowid = Some(*rowid as u64);
                    self.current = Some(row.clone());
                    self.null_flag = false;
                    return Ok(CursorResult::Ok(()));
                }
            }
        }

        self.rowid = None;
        self.current = None;
        self.null_flag = true;
        Ok(CursorResult::Ok(()))
    }

    pub fn last(&mut self) -> Result<CursorResult<()>> {
        match &self.source {
            Ephemeral::Table(table) => {
                if let Some((&last_rowid, row_data)) = table.rows.iter().next_back() {
                    self.rowid = Some(last_rowid);
                    self.current = Some(OwnedRecord {
                        values: row_data.clone(),
                    });
                    self.null_flag = false;
                    return Ok(CursorResult::Ok(()));
                }
            }
            Ephemeral::Index(index) => {
                if let Some(row) = index.rows.iter().next_back() {
                    let OwnedValue::Integer(rowid) = row.values.last().unwrap() else {
                        return Err(LimboError::InternalError(
                            "Invalid key type for rowid".to_string(),
                        ));
                    };

                    self.rowid = Some(*rowid as u64);
                    self.current = Some(row.clone());
                    self.null_flag = false;
                    return Ok(CursorResult::Ok(()));
                }
            }
        };

        self.rowid = None;
        self.current = None;
        self.null_flag = true;
        Ok(CursorResult::Ok(()))
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
        match &mut self.source {
            Ephemeral::Table(table) => {
                if self.rowid.is_none() {
                    if let Some((&first_rowid, row_data)) = table.rows.iter().next() {
                        self.rowid = Some(first_rowid);
                        self.current = Some(OwnedRecord {
                            values: row_data.clone(),
                        });
                        self.null_flag = false;
                        return Ok(CursorResult::Ok(()));
                    }
                } else if let Some(current_rowid) = self.rowid {
                    if let Some((&next_rowid, row_data)) =
                        table.rows.range((current_rowid + 1)..).next()
                    {
                        self.rowid = Some(next_rowid);
                        self.current = Some(OwnedRecord {
                            values: row_data.clone(),
                        });
                        self.null_flag = false;
                        return Ok(CursorResult::Ok(()));
                    }
                }
            }
            Ephemeral::Index(index) => {
                if self.rowid.is_none() {
                    if let Some(row) = index.rows.iter().next() {
                        let OwnedValue::Integer(rowid) = row.values.last().unwrap() else {
                            return Err(LimboError::InternalError(
                                "Invalid key type for rowid".to_string(),
                            ));
                        };
                        self.rowid = Some(*rowid as u64);
                        self.current = Some(row.clone());
                        self.null_flag = false;
                        return Ok(CursorResult::Ok(()));
                    }
                } else if let Some(current) = &self.current {
                    let mut iter = index.rows.range(current..);
                    iter.next(); // ignore first result since we don't support Exclude in OwnedRecord. That would require the impl of RangeBound
                    if let Some(row) = iter.next() {
                        let OwnedValue::Integer(rowid) = row.values.last().unwrap() else {
                            return Err(LimboError::InternalError(
                                "Invalid key type for rowid".to_string(),
                            ));
                        };
                        self.rowid = Some(*rowid as u64);
                        self.current = Some(row.clone());
                        self.null_flag = false;
                        return Ok(CursorResult::Ok(()));
                    }
                }
            }
        }

        // No more rows
        self.null_flag = true;
        self.rowid = None;
        self.current = None;
        Ok(CursorResult::Ok(()))
    }
    pub fn prev(&mut self) -> Result<CursorResult<()>> {
        match &self.source {
            Ephemeral::Table(table) => {
                if self.rowid.is_none() {
                    if let Some((&first_rowid, row_data)) = table.rows.iter().next_back() {
                        self.rowid = Some(first_rowid);
                        self.current = Some(OwnedRecord {
                            values: row_data.clone(),
                        });
                        self.null_flag = false;
                        return Ok(CursorResult::Ok(()));
                    }
                } else if let Some(current_rowid) = self.rowid {
                    if let Some((&next_rowid, row_data)) =
                        table.rows.range(..current_rowid).next_back()
                    {
                        self.rowid = Some(next_rowid);
                        self.current = Some(OwnedRecord {
                            values: row_data.clone(),
                        });
                        self.null_flag = false;
                        return Ok(CursorResult::Ok(()));
                    }
                }
            }
            Ephemeral::Index(index) => {
                if self.rowid.is_none() {
                    if let Some(row) = index.rows.iter().next_back() {
                        let OwnedValue::Integer(rowid) = row.values.last().unwrap() else {
                            return Err(LimboError::InternalError(
                                "Invalid key type for rowid".to_string(),
                            ));
                        };
                        self.rowid = Some(*rowid as u64);
                        self.current = Some(row.clone());
                        self.null_flag = false;
                        return Ok(CursorResult::Ok(()));
                    }
                } else if let Some(current) = &self.current {
                    if let Some(prev_record) = index.rows.range(..current).next_back() {
                        let OwnedValue::Integer(prev_rowid) = prev_record.values.last().unwrap()
                        else {
                            return Err(LimboError::InternalError(
                                "Invalid key type for rowid".to_string(),
                            ));
                        };

                        self.rowid = Some(*prev_rowid as u64);
                        self.current = Some(prev_record.clone());
                        self.null_flag = false;
                        return Ok(CursorResult::Ok(()));
                    }
                }
            }
        }

        // No more rows
        self.null_flag = true;
        self.rowid = None;
        self.current = None;
        Ok(CursorResult::Ok(()))
    }

    pub fn exists(&mut self, key: &OwnedValue) -> Result<CursorResult<bool>> {
        match &self.source {
            Ephemeral::Table(table) => {
                let OwnedValue::Integer(key) = key else {
                    return Err(LimboError::InternalError(
                        "btree tables are indexed by integers!".to_string(),
                    ));
                };

                if let Some(row) = table.rows.get(&(*key as u64)) {
                    self.rowid = Some(*key as u64);
                    self.current = Some(OwnedRecord {
                        values: row.clone(),
                    });
                    self.null_flag = false;
                    return Ok(CursorResult::Ok(true));
                }
            }
            Ephemeral::Index(index) => {
                let search_key = match key {
                    OwnedValue::Record(record) => record.clone(),
                    _ => OwnedRecord {
                        values: vec![key.clone()],
                    },
                };

                let mut iter = index.rows.range(search_key.clone()..);
                if let Some(record) = iter.next() {
                    if record.values.contains(&key) {
                        let OwnedValue::Integer(rowid) = record.values.last().unwrap() else {
                            return Err(LimboError::InternalError(
                                "Invalid key type for rowid".to_string(),
                            ));
                        };

                        self.rowid = Some(*rowid as u64);
                        self.current = Some(record.clone());
                        self.null_flag = false;
                        return Ok(CursorResult::Ok(true));
                    }
                }
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

    mod test_table {
        use std::{collections::BTreeMap, rc::Rc};

        use crate::{
            ephemeral::{Ephemeral, EphemeralCursor},
            schema::EphemeralTable,
            types::{CursorResult, LimboText, OwnedRecord, OwnedValue, SeekKey, SeekOp},
        };

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
                source: Ephemeral::Table(table),
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
                source: Ephemeral::Table(table),
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
                source: Ephemeral::Table(table),
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
                source: Ephemeral::Table(table),
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
                source: Ephemeral::Table(table),
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
                source: Ephemeral::Table(table),
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
                source: Ephemeral::Table(table),
                rowid: None,
                current: None,
                null_flag: true,
            };

            let result = cursor.exists(&OwnedValue::Integer(1)).unwrap();
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
                source: Ephemeral::Table(table),
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
                source: Ephemeral::Table(table),
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
            if let Ephemeral::Table(table) = cursor.source {
                assert_eq!(table.rows.len(), 1);
                assert_eq!(table.rows.get(&1), Some(&record.values));
                assert_eq!(cursor.rowid, Some(1));
                assert_eq!(cursor.current, Some(record));
                assert!(!cursor.null_flag);
            }
        }

        #[test]
        fn test_insert_overwrite_row() {
            let table = EphemeralTable {
                rows: BTreeMap::new(),
                next_rowid: 1,
                columns: vec![],
            };

            let mut cursor = EphemeralCursor {
                source: Ephemeral::Table(table),
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

            if let Ephemeral::Table(table) = cursor.source {
                assert_eq!(table.rows.len(), 1);
                assert_eq!(table.rows.get(&1), Some(&record2.values));
                assert_eq!(cursor.rowid, Some(1));
                assert_eq!(cursor.current, Some(record2));
                assert!(!cursor.null_flag);
            }
        }

        #[test]
        fn test_do_seek_by_rowid_eq() {
            let mut table = EphemeralTable {
                rows: BTreeMap::new(),
                next_rowid: 1,
                columns: vec![],
            };

            table.rows.insert(1, vec![OwnedValue::Integer(10)]);
            table.rows.insert(2, vec![OwnedValue::Integer(20)]);
            table.rows.insert(3, vec![OwnedValue::Integer(30)]);

            let mut cursor = EphemeralCursor {
                source: Ephemeral::Table(table),
                rowid: None,
                current: None,
                null_flag: true,
            };

            let result = cursor.do_seek(SeekKey::TableRowId(2), SeekOp::EQ).unwrap();
            assert_eq!(
                result,
                CursorResult::Ok((
                    Some(2),
                    Some(OwnedRecord {
                        values: vec![OwnedValue::Integer(20)]
                    })
                ))
            );
            assert_eq!(cursor.rowid, Some(2));
            assert!(!cursor.null_flag);
        }
    }

    mod test_index {
        use std::{collections::BTreeSet, rc::Rc};

        use crate::{
            ephemeral::{Ephemeral, EphemeralCursor},
            schema::EphemeralIndex,
            types::{CursorResult, LimboText, OwnedRecord, OwnedValue, SeekKey, SeekOp},
        };

        #[test]
        fn test_next() {
            let mut table = EphemeralIndex {
                rows: BTreeSet::new(),
                columns: vec![],
            };
            let val1 = OwnedRecord {
                values: vec![OwnedValue::Integer(42), OwnedValue::Integer(47)],
            };

            let val2 = OwnedRecord {
                values: vec![
                    OwnedValue::Text(LimboText::new(std::rc::Rc::new("Hello".to_string()))),
                    OwnedValue::Integer(44),
                ],
            };

            table.rows.insert(val1.clone());
            table.rows.insert(val2.clone());

            let mut cursor = EphemeralCursor {
                source: Ephemeral::Index(table),
                rowid: None,
                current: None,
                null_flag: true,
            };

            cursor.next().unwrap(); // Move to the first row
            assert_eq!(cursor.current, Some(val1));

            cursor.next().unwrap(); // Move to the second row
            assert_eq!(cursor.current, Some(val2));
        }

        #[test]
        fn test_prev() {
            let mut table = EphemeralIndex {
                rows: BTreeSet::new(),
                columns: vec![],
            };
            let val1 = OwnedRecord {
                values: vec![OwnedValue::Integer(42), OwnedValue::Integer(47)],
            };

            let val2 = OwnedRecord {
                values: vec![
                    OwnedValue::Text(LimboText::new(std::rc::Rc::new("Hello".to_string()))),
                    OwnedValue::Integer(44),
                ],
            };

            table.rows.insert(val1.clone());
            table.rows.insert(val2.clone());
            let mut cursor = EphemeralCursor {
                source: Ephemeral::Index(table),
                rowid: None,
                current: None,
                null_flag: true,
            };

            cursor.prev().unwrap(); // Should move to row 2
            assert_eq!(cursor.current, Some(val2));

            cursor.prev().unwrap(); // Should move to row 1
            assert_eq!(cursor.current, Some(val1));

            cursor.prev().unwrap(); // Should go out of bounds
            assert!(cursor.null_flag);
            assert!(cursor.current.is_none());
        }

        #[test]
        fn test_last() {
            let mut table = EphemeralIndex {
                rows: BTreeSet::new(),
                columns: vec![],
            };
            let val1 = OwnedRecord {
                values: vec![OwnedValue::Integer(42), OwnedValue::Integer(47)],
            };

            let val2 = OwnedRecord {
                values: vec![
                    OwnedValue::Text(LimboText::new(std::rc::Rc::new("Hello".to_string()))),
                    OwnedValue::Integer(44),
                ],
            };

            table.rows.insert(val1.clone());
            table.rows.insert(val2.clone());
            let mut cursor = EphemeralCursor {
                source: Ephemeral::Index(table),
                rowid: None,
                current: None,
                null_flag: true,
            };

            cursor.last().unwrap(); // Move to the last row
            assert_eq!(cursor.current, Some(val2));
            assert_eq!(cursor.rowid, Some(44));
            assert!(!cursor.null_flag);
        }

        #[test]
        fn test_last_empty_table() {
            let table = EphemeralIndex {
                rows: BTreeSet::new(),
                columns: vec![],
            };

            let mut cursor = EphemeralCursor {
                source: Ephemeral::Index(table),
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
            let mut table = EphemeralIndex {
                rows: BTreeSet::new(),
                columns: vec![],
            };

            let val1 = OwnedRecord {
                values: vec![OwnedValue::Integer(42), OwnedValue::Integer(47)],
            };

            let val2 = OwnedRecord {
                values: vec![
                    OwnedValue::Text(LimboText::new(std::rc::Rc::new("Hello".to_string()))),
                    OwnedValue::Integer(44),
                ],
            };
            table.rows.insert(val1.clone());
            table.rows.insert(val2.clone());

            let mut cursor = EphemeralCursor {
                source: Ephemeral::Index(table),
                rowid: None,
                current: None,
                null_flag: true,
            };

            cursor.rewind().unwrap(); // Move to the first row
            assert_eq!(cursor.current, Some(val1));
            assert_eq!(cursor.rowid, Some(47));
            assert!(!cursor.null_flag);
        }

        #[test]
        fn test_rewind_empty_table() {
            let table = EphemeralIndex {
                rows: BTreeSet::new(),
                columns: vec![],
            };

            let mut cursor = EphemeralCursor {
                source: Ephemeral::Index(table),
                rowid: None,
                current: None,
                null_flag: true,
            };

            cursor.rewind().unwrap();
            assert!(cursor.current.is_none());
            assert!(cursor.null_flag);
            assert!(cursor.rowid.is_none());
        }

        #[test]
        fn test_exists_key_found() {
            let mut table = EphemeralIndex {
                rows: BTreeSet::new(),
                columns: vec![],
            };
            let val1 = OwnedRecord {
                values: vec![OwnedValue::Integer(42), OwnedValue::Integer(47)],
            };
            let val2 = OwnedRecord {
                values: vec![
                    OwnedValue::Text(LimboText::new(std::rc::Rc::new("Hello".to_string()))),
                    OwnedValue::Integer(44),
                ],
            };

            table.rows.insert(val1.clone());
            table.rows.insert(val2.clone());

            let mut cursor = EphemeralCursor {
                source: Ephemeral::Index(table),
                rowid: None,
                current: None,
                null_flag: true,
            };

            let result = cursor.exists(&OwnedValue::Integer(42)).unwrap();
            assert_eq!(result, CursorResult::Ok(true));
            assert_eq!(cursor.rowid, Some(47));
            assert_eq!(cursor.current, Some(val1));
            assert!(!cursor.null_flag);
        }

        #[test]
        fn test_exists_key_not_found() {
            let mut table = EphemeralIndex {
                rows: BTreeSet::new(),
                columns: vec![],
            };
            let val1 = OwnedRecord {
                values: vec![OwnedValue::Integer(42), OwnedValue::Integer(47)],
            };

            let val2 = OwnedRecord {
                values: vec![
                    OwnedValue::Text(LimboText::new(std::rc::Rc::new("Hello".to_string()))),
                    OwnedValue::Integer(44),
                ],
            };

            table.rows.insert(val1.clone());
            table.rows.insert(val2.clone());

            let mut cursor = EphemeralCursor {
                source: Ephemeral::Index(table),
                rowid: None,
                current: None,
                null_flag: true,
            };

            let result = cursor.exists(&OwnedValue::Integer(23)).unwrap();
            assert_eq!(result, CursorResult::Ok(false));
            assert!(cursor.rowid.is_none());
            assert!(cursor.current.is_none());
            assert!(cursor.null_flag);
        }

        #[test]
        fn test_insert_new_row() {
            let table = EphemeralIndex {
                rows: BTreeSet::new(),
                columns: vec![],
            };
            let record = OwnedRecord {
                values: vec![OwnedValue::Integer(42), OwnedValue::Integer(47)],
            };

            let mut cursor = EphemeralCursor {
                source: Ephemeral::Index(table),
                rowid: None,
                current: None,
                null_flag: true,
            };

            cursor
                .insert(&OwnedValue::Integer(1), &record, false)
                .unwrap();

            let mut values = record.values.clone();
            values.push(OwnedValue::Integer(1));
            let result = OwnedRecord { values };

            if let Ephemeral::Index(ref table) = cursor.source {
                assert_eq!(table.rows.len(), 1);
                assert_eq!(cursor.rowid, Some(1));
                assert_eq!(cursor.current, Some(result));
                assert!(!cursor.null_flag);
            }
        }

        #[test]
        fn test_insert_overwrite_row() {
            let table = EphemeralIndex {
                rows: BTreeSet::new(),
                columns: vec![],
            };

            let mut cursor = EphemeralCursor {
                source: Ephemeral::Index(table),
                rowid: None,
                current: None,
                null_flag: true,
            };

            let key = OwnedValue::Integer(1);
            let record1 = OwnedRecord {
                values: vec![
                    OwnedValue::Text(LimboText::new(Rc::new("First".to_string()))),
                    OwnedValue::Integer(42),
                ],
            };

            let record2 = OwnedRecord {
                values: vec![
                    OwnedValue::Text(LimboText::new(Rc::new("Second".to_string()))),
                    OwnedValue::Integer(43),
                ],
            };

            cursor.insert(&key, &record1, false).unwrap();
            cursor.insert(&key, &record2, true).unwrap();

            let mut values = record2.values.clone();
            values.push(OwnedValue::Integer(1));
            let result = OwnedRecord { values };

            if let Ephemeral::Index(ref table) = cursor.source {
                assert_eq!(table.rows.len(), 1);
                assert_eq!(cursor.rowid, Some(1));
                assert_eq!(cursor.current, Some(result));
                assert!(!cursor.null_flag);
            }
        }

        #[test]
        fn test_do_seek_index_gt() {
            let mut index = EphemeralIndex {
                rows: BTreeSet::new(),
                columns: vec![],
            };

            let record1 = OwnedRecord {
                values: vec![
                    OwnedValue::Integer(1),
                    OwnedValue::Integer(10),
                    OwnedValue::Integer(100),
                ],
            };
            let record2 = OwnedRecord {
                values: vec![
                    OwnedValue::Integer(2),
                    OwnedValue::Integer(20),
                    OwnedValue::Integer(200),
                ],
            };
            let record3 = OwnedRecord {
                values: vec![
                    OwnedValue::Integer(2),
                    OwnedValue::Integer(25),
                    OwnedValue::Integer(300),
                ],
            };
            let record4 = OwnedRecord {
                values: vec![
                    OwnedValue::Integer(3),
                    OwnedValue::Integer(30),
                    OwnedValue::Integer(400),
                ],
            };

            index.rows.insert(record1.clone());
            index.rows.insert(record2.clone());
            index.rows.insert(record3.clone());
            index.rows.insert(record4.clone());

            let mut cursor = EphemeralCursor {
                source: Ephemeral::Index(index),
                rowid: None,
                current: None,
                null_flag: true,
            };

            let key = OwnedRecord {
                values: vec![OwnedValue::Integer(2), OwnedValue::Integer(20)],
            };

            let search_key = SeekKey::IndexKey(&key);

            let result = cursor.do_seek(search_key, SeekOp::GT).unwrap();

            assert_eq!(result, CursorResult::Ok((Some(300), Some(record3.clone()))));
            assert_eq!(cursor.rowid, Some(300));
            assert_eq!(cursor.current, Some(record3));
            assert!(!cursor.null_flag);
        }
    }
}
