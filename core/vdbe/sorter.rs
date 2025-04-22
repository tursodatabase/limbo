use crate::{translate::collate::CollationSeq, types::ImmutableRecord, RefValue};
use std::cmp::Ordering;

pub struct Sorter {
    records: Vec<ImmutableRecord>,
    current: Option<ImmutableRecord>,
    order: Vec<bool>,
    collation: CollationSeq,
}

impl Sorter {
    pub fn new(order: Vec<bool>, collation: CollationSeq) -> Self {
        Self {
            records: Vec::new(),
            current: None,
            order,
            collation,
        }
    }
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub fn has_more(&self) -> bool {
        self.current.is_some()
    }

    // We do the sorting here since this is what is called by the SorterSort instruction
    pub fn sort(&mut self) {
        self.records.sort_by(|a, b| {
            let cmp_by_idx = |idx: usize, ascending: bool| {
                let mut a = &a.get_value(idx);
                let mut b = &b.get_value(idx);
                if !ascending {
                    let tmp = a;
                    a = b;
                    b = tmp;
                }
                match (a, b) {
                    (RefValue::Text(left), RefValue::Text(right)) => self
                        .collation
                        .compare_strings(left.as_str(), right.as_str()),
                    _ => a.cmp(b),
                }
            };

            let mut cmp_ret = Ordering::Equal;
            for (idx, &is_asc) in self.order.iter().enumerate() {
                cmp_ret = cmp_by_idx(idx, is_asc);
                if cmp_ret != Ordering::Equal {
                    break;
                }
            }
            cmp_ret
        });
        self.records.reverse();
        self.next()
    }
    pub fn next(&mut self) {
        self.current = self.records.pop();
    }
    pub fn record(&self) -> Option<&ImmutableRecord> {
        self.current.as_ref()
    }

    pub fn insert(&mut self, record: &ImmutableRecord) {
        self.records.push(record.clone());
    }
}
