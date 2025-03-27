use tracing::debug;

use crate::storage::pager::Pager;
use crate::storage::sqlite3_ondisk::{
    read_varint, BTreeCell, PageContent, PageType, TableInteriorCell, TableLeafCell,
};
use crate::MvCursor;

use crate::types::{
    compare_immutable, compare_immutable_to_record, CursorResult, ImmutableRecord, OwnedValue,
    RefValue, SeekKey, SeekOp,
};
use crate::{return_corrupt, LimboError, Result};

use std::cell::{Cell, Ref, RefCell};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;

use super::pager::PageRef;
use super::sqlite3_ondisk::{
    write_varint_to_vec, IndexInteriorCell, IndexLeafCell, OverflowCell, DATABASE_HEADER_SIZE,
};

/*
    These are offsets of fields in the header of a b-tree page.
*/

/// type of btree page -> u8
const PAGE_HEADER_OFFSET_PAGE_TYPE: usize = 0;
/// pointer to first freeblock -> u16
/// The second field of the b-tree page header is the offset of the first freeblock, or zero if there are no freeblocks on the page.
/// A freeblock is a structure used to identify unallocated space within a b-tree page.
/// Freeblocks are organized as a chain.
///
/// To be clear, freeblocks do not mean the regular unallocated free space to the left of the cell content area pointer, but instead
/// blocks of at least 4 bytes WITHIN the cell content area that are not in use due to e.g. deletions.
const PAGE_HEADER_OFFSET_FIRST_FREEBLOCK: usize = 1;
/// number of cells in the page -> u16
const PAGE_HEADER_OFFSET_CELL_COUNT: usize = 3;
/// pointer to first byte of cell allocated content from top -> u16
/// SQLite strives to place cells as far toward the end of the b-tree page as it can,
/// in order to leave space for future growth of the cell pointer array.
/// = the cell content area pointer moves leftward as cells are added to the page
const PAGE_HEADER_OFFSET_CELL_CONTENT_AREA: usize = 5;
/// number of fragmented bytes -> u8
/// Fragments are isolated groups of 1, 2, or 3 unused bytes within the cell content area.
const PAGE_HEADER_OFFSET_FRAGMENTED_BYTES_COUNT: usize = 7;
/// if internalnode, pointer right most pointer (saved separately from cells) -> u32
const PAGE_HEADER_OFFSET_RIGHTMOST_PTR: usize = 8;

/// Maximum depth of an SQLite B-Tree structure. Any B-Tree deeper than
/// this will be declared corrupt. This value is calculated based on a
/// maximum database size of 2^31 pages a minimum fanout of 2 for a
/// root-node and 3 for all other internal nodes.
///
/// If a tree that appears to be taller than this is encountered, it is
/// assumed that the database is corrupt.
pub const BTCURSOR_MAX_DEPTH: usize = 20;

/// Evaluate a Result<CursorResult<T>>, if IO return IO.
macro_rules! return_if_io {
    ($expr:expr) => {
        match $expr? {
            CursorResult::Ok(v) => v,
            CursorResult::IO => return Ok(CursorResult::IO),
        }
    };
}

/// Check if the page is unlocked, if not return IO.
macro_rules! return_if_locked {
    ($expr:expr) => {{
        if $expr.is_locked() {
            return Ok(CursorResult::IO);
        }
    }};
}

/// State machine of destroy operations
/// Keep track of traversal so that it can be resumed when IO is encountered
#[derive(Debug, Clone)]
enum DestroyState {
    Start,
    LoadPage,
    ProcessPage,
    ClearOverflowPages { cell: BTreeCell },
    FreePage,
}

struct DestroyInfo {
    state: DestroyState,
}

/// State machine of a write operation.
/// May involve balancing due to overflow.
#[derive(Debug, Clone, Copy)]
enum WriteState {
    Start,
    BalanceStart,
    BalanceNonRoot,
    BalanceNonRootWaitLoadPages,
    Finish,
}

struct BalanceInfo {
    /// Old pages being balanced.
    pages_to_balance: Vec<PageRef>,
    /// Bookkeeping of the rightmost pointer so the PAGE_HEADER_OFFSET_RIGHTMOST_PTR can be updated.
    rightmost_pointer: *mut u8,
    /// Divider cells of old pages
    divider_cells: Vec<Vec<u8>>,
    /// Number of siblings being used to balance
    sibling_count: usize,
    /// First divider cell to remove that marks the first sibling
    first_divider_cell: usize,
}

struct WriteInfo {
    /// State of the write operation state machine.
    state: WriteState,
    balance_info: RefCell<Option<BalanceInfo>>,
}

impl WriteInfo {
    fn new() -> WriteInfo {
        WriteInfo {
            state: WriteState::Start,
            balance_info: RefCell::new(None),
        }
    }
}

/// Holds the state machine for the operation that was in flight when the cursor
/// was suspended due to IO.
enum CursorState {
    None,
    Write(WriteInfo),
    Destroy(DestroyInfo),
}

impl CursorState {
    fn write_info(&self) -> Option<&WriteInfo> {
        match self {
            CursorState::Write(x) => Some(x),
            _ => None,
        }
    }
    fn mut_write_info(&mut self) -> Option<&mut WriteInfo> {
        match self {
            CursorState::Write(x) => Some(x),
            _ => None,
        }
    }

    fn destroy_info(&self) -> Option<&DestroyInfo> {
        match self {
            CursorState::Destroy(x) => Some(x),
            _ => None,
        }
    }
    fn mut_destroy_info(&mut self) -> Option<&mut DestroyInfo> {
        match self {
            CursorState::Destroy(x) => Some(x),
            _ => None,
        }
    }
}

enum OverflowState {
    Start,
    ProcessPage { next_page: u32 },
    Done,
}

pub struct BTreeCursor {
    /// The multi-version cursor that is used to read and write to the database file.
    mv_cursor: Option<Rc<RefCell<MvCursor>>>,
    /// The pager that is used to read and write to the database file.
    pager: Rc<Pager>,
    /// Page id of the root page used to go back up fast.
    root_page: usize,
    /// Rowid and record are stored before being consumed.
    rowid: Cell<Option<u64>>,
    record: RefCell<Option<ImmutableRecord>>,
    null_flag: bool,
    /// Index internal pages are consumed on the way up, so we store going upwards flag in case
    /// we just moved to a parent page and the parent page is an internal index page which requires
    /// to be consumed.
    going_upwards: bool,
    /// Information maintained across execution attempts when an operation yields due to I/O.
    state: CursorState,
    /// Information maintained while freeing overflow pages. Maintained separately from cursor state since
    /// any method could require freeing overflow pages
    overflow_state: Option<OverflowState>,
    /// Page stack used to traverse the btree.
    /// Each cursor has a stack because each cursor traverses the btree independently.
    stack: PageStack,
}

/// Stack of pages representing the tree traversal order.
/// current_page represents the current page being used in the tree and current_page - 1 would be
/// the parent. Using current_page + 1 or higher is undefined behaviour.
struct PageStack {
    /// Pointer to the current page being consumed
    current_page: Cell<i32>,
    /// List of pages in the stack. Root page will be in index 0
    stack: RefCell<[Option<PageRef>; BTCURSOR_MAX_DEPTH + 1]>,
    /// List of cell indices in the stack.
    /// cell_indices[current_page] is the current cell index being consumed. Similarly
    /// cell_indices[current_page-1] is the cell index of the parent of the current page
    /// that we save in case of going back up.
    /// There are two points that need special attention:
    ///  If cell_indices[current_page] = -1, it indicates that the current iteration has reached the start of the current_page
    ///  If cell_indices[current_page] = `cell_count`, it means that the current iteration has reached the end of the current_page
    cell_indices: RefCell<[i32; BTCURSOR_MAX_DEPTH + 1]>,
}

struct CellArray {
    cells: Vec<&'static mut [u8]>, // TODO(pere): make this with references

    number_of_cells_per_page: Vec<u16>, // number of cells in each page
}

impl BTreeCursor {
    pub fn new(
        mv_cursor: Option<Rc<RefCell<MvCursor>>>,
        pager: Rc<Pager>,
        root_page: usize,
    ) -> Self {
        Self {
            mv_cursor,
            pager,
            root_page,
            rowid: Cell::new(None),
            record: RefCell::new(None),
            null_flag: false,
            going_upwards: false,
            state: CursorState::None,
            overflow_state: None,
            stack: PageStack {
                current_page: Cell::new(-1),
                cell_indices: RefCell::new([0; BTCURSOR_MAX_DEPTH + 1]),
                stack: RefCell::new([const { None }; BTCURSOR_MAX_DEPTH + 1]),
            },
        }
    }

    /// Check if the table is empty.
    /// This is done by checking if the root page has no cells.
    fn is_empty_table(&self) -> Result<CursorResult<bool>> {
        if let Some(mv_cursor) = &self.mv_cursor {
            let mv_cursor = mv_cursor.borrow();
            return Ok(CursorResult::Ok(mv_cursor.is_empty()));
        }
        let page = self.pager.read_page(self.root_page)?;
        return_if_locked!(page);

        let cell_count = page.get().contents.as_ref().unwrap().cell_count();
        Ok(CursorResult::Ok(cell_count == 0))
    }

    /// Move the cursor to the previous record and return it.
    /// Used in backwards iteration.
    fn get_prev_record(&mut self) -> Result<CursorResult<(Option<u64>, Option<ImmutableRecord>)>> {
        loop {
            let page = self.stack.top();
            let cell_idx = self.stack.current_cell_index();

            // moved to beginning of current page
            // todo: find a better way to flag moved to end or begin of page
            if self.stack.current_cell_index_less_than_min() {
                loop {
                    if self.stack.current_cell_index() > 0 {
                        self.stack.retreat();
                        break;
                    }
                    if self.stack.has_parent() {
                        self.stack.pop();
                    } else {
                        // moved to begin of btree
                        return Ok(CursorResult::Ok((None, None)));
                    }
                }
                // continue to next loop to get record from the new page
                continue;
            }

            let cell_idx = cell_idx as usize;
            tracing::trace!(
                "get_prev_record current id={} cell={}",
                page.get().id,
                cell_idx
            );
            return_if_locked!(page);
            if !page.is_loaded() {
                self.pager.load_page(page.clone())?;
                return Ok(CursorResult::IO);
            }
            let contents = page.get().contents.as_ref().unwrap();

            let cell_count = contents.cell_count();
            let cell_idx = if cell_idx >= cell_count {
                self.stack.set_cell_index(cell_count as i32 - 1);
                cell_count - 1
            } else {
                cell_idx
            };

            let cell = contents.cell_get(
                cell_idx,
                self.pager.clone(),
                payload_overflow_threshold_max(contents.page_type(), self.usable_space() as u16),
                payload_overflow_threshold_min(contents.page_type(), self.usable_space() as u16),
                self.usable_space(),
            )?;

            match cell {
                BTreeCell::TableInteriorCell(TableInteriorCell {
                    _left_child_page,
                    _rowid,
                }) => {
                    let mem_page = self.pager.read_page(_left_child_page as usize)?;
                    self.stack.push(mem_page);
                    // use cell_index = i32::MAX to tell next loop to go to the end of the current page
                    self.stack.set_cell_index(i32::MAX);
                    continue;
                }
                BTreeCell::TableLeafCell(TableLeafCell {
                    _rowid, _payload, ..
                }) => {
                    self.stack.retreat();
                    let record = crate::storage::sqlite3_ondisk::read_record(&_payload)?;
                    return Ok(CursorResult::Ok((Some(_rowid), Some(record))));
                }
                BTreeCell::IndexInteriorCell(_) => todo!(),
                BTreeCell::IndexLeafCell(_) => todo!(),
            }
        }
    }

    /// Move the cursor to the next record and return it.
    /// Used in forwards iteration, which is the default.
    fn get_next_record(
        &mut self,
        predicate: Option<(SeekKey<'_>, SeekOp)>,
    ) -> Result<CursorResult<(Option<u64>, Option<ImmutableRecord>)>> {
        if let Some(mv_cursor) = &self.mv_cursor {
            let mut mv_cursor = mv_cursor.borrow_mut();
            let rowid = mv_cursor.current_row_id();
            match rowid {
                Some(rowid) => {
                    let record = mv_cursor.current_row().unwrap().unwrap();
                    let record = crate::storage::sqlite3_ondisk::read_record(&record.data)?;
                    mv_cursor.forward();
                    return Ok(CursorResult::Ok((Some(rowid.row_id), Some(record))));
                }
                None => return Ok(CursorResult::Ok((None, None))),
            }
        }
        loop {
            let mem_page_rc = self.stack.top();
            let cell_idx = self.stack.current_cell_index() as usize;

            tracing::trace!("current id={} cell={}", mem_page_rc.get().id, cell_idx);
            return_if_locked!(mem_page_rc);
            if !mem_page_rc.is_loaded() {
                self.pager.load_page(mem_page_rc.clone())?;
                return Ok(CursorResult::IO);
            }
            let mem_page = mem_page_rc.get();

            let contents = mem_page.contents.as_ref().unwrap();

            if cell_idx == contents.cell_count() {
                // do rightmost
                let has_parent = self.stack.has_parent();
                match contents.rightmost_pointer() {
                    Some(right_most_pointer) => {
                        self.stack.advance();
                        let mem_page = self.pager.read_page(right_most_pointer as usize)?;
                        self.stack.push(mem_page);
                        continue;
                    }
                    None => {
                        if has_parent {
                            debug!("moving simple upwards");
                            self.going_upwards = true;
                            self.stack.pop();
                            continue;
                        } else {
                            return Ok(CursorResult::Ok((None, None)));
                        }
                    }
                }
            }

            if cell_idx > contents.cell_count() {
                // end
                let has_parent = self.stack.current() > 0;
                if has_parent {
                    debug!("moving upwards");
                    self.going_upwards = true;
                    self.stack.pop();
                    continue;
                } else {
                    return Ok(CursorResult::Ok((None, None)));
                }
            }
            assert!(cell_idx < contents.cell_count());

            let cell = contents.cell_get(
                cell_idx,
                self.pager.clone(),
                payload_overflow_threshold_max(contents.page_type(), self.usable_space() as u16),
                payload_overflow_threshold_min(contents.page_type(), self.usable_space() as u16),
                self.usable_space(),
            )?;
            match &cell {
                BTreeCell::TableInteriorCell(TableInteriorCell {
                    _left_child_page,
                    _rowid,
                }) => {
                    assert!(predicate.is_none());
                    self.stack.advance();
                    let mem_page = self.pager.read_page(*_left_child_page as usize)?;
                    self.stack.push(mem_page);
                    continue;
                }
                BTreeCell::TableLeafCell(TableLeafCell {
                    _rowid,
                    _payload,
                    first_overflow_page: _,
                }) => {
                    assert!(predicate.is_none());
                    self.stack.advance();
                    let record = crate::storage::sqlite3_ondisk::read_record(_payload)?;
                    return Ok(CursorResult::Ok((Some(*_rowid), Some(record))));
                }
                BTreeCell::IndexInteriorCell(IndexInteriorCell {
                    payload,
                    left_child_page,
                    ..
                }) => {
                    if !self.going_upwards {
                        let mem_page = self.pager.read_page(*left_child_page as usize)?;
                        self.stack.push(mem_page);
                        continue;
                    }

                    self.going_upwards = false;
                    self.stack.advance();

                    let record = crate::storage::sqlite3_ondisk::read_record(payload)?;
                    if predicate.is_none() {
                        let rowid = match record.last_value() {
                            Some(RefValue::Integer(rowid)) => *rowid as u64,
                            _ => unreachable!("index cells should have an integer rowid"),
                        };
                        return Ok(CursorResult::Ok((Some(rowid), Some(record))));
                    }

                    let (key, op) = predicate.as_ref().unwrap();
                    let SeekKey::IndexKey(index_key) = key else {
                        unreachable!("index seek key should be a record");
                    };
                    let order = compare_immutable(&record.get_values(), index_key.get_values());
                    let found = match op {
                        SeekOp::GT => order.is_gt(),
                        SeekOp::GE => order.is_ge(),
                        SeekOp::EQ => order.is_eq(),
                    };
                    if found {
                        let rowid = match record.last_value() {
                            Some(RefValue::Integer(rowid)) => *rowid as u64,
                            _ => unreachable!("index cells should have an integer rowid"),
                        };
                        return Ok(CursorResult::Ok((Some(rowid), Some(record))));
                    } else {
                        continue;
                    }
                }
                BTreeCell::IndexLeafCell(IndexLeafCell { payload, .. }) => {
                    self.stack.advance();
                    let record = crate::storage::sqlite3_ondisk::read_record(payload)?;
                    if predicate.is_none() {
                        let rowid = match record.last_value() {
                            Some(RefValue::Integer(rowid)) => *rowid as u64,
                            _ => unreachable!("index cells should have an integer rowid"),
                        };
                        return Ok(CursorResult::Ok((Some(rowid), Some(record))));
                    }
                    let (key, op) = predicate.as_ref().unwrap();
                    let SeekKey::IndexKey(index_key) = key else {
                        unreachable!("index seek key should be a record");
                    };
                    let order = compare_immutable(&record.get_values(), index_key.get_values());
                    let found = match op {
                        SeekOp::GT => order.is_lt(),
                        SeekOp::GE => order.is_le(),
                        SeekOp::EQ => order.is_le(),
                    };
                    if found {
                        let rowid = match record.last_value() {
                            Some(RefValue::Integer(rowid)) => *rowid as u64,
                            _ => unreachable!("index cells should have an integer rowid"),
                        };
                        return Ok(CursorResult::Ok((Some(rowid), Some(record))));
                    } else {
                        continue;
                    }
                }
            }
        }
    }

    /// Move the cursor to the record that matches the seek key and seek operation.
    /// This may be used to seek to a specific record in a point query (e.g. SELECT * FROM table WHERE col = 10)
    /// or e.g. find the first record greater than the seek key in a range query (e.g. SELECT * FROM table WHERE col > 10).
    /// We don't include the rowid in the comparison and that's why the last value from the record is not included.
    fn do_seek(
        &mut self,
        key: SeekKey<'_>,
        op: SeekOp,
    ) -> Result<CursorResult<(Option<u64>, Option<ImmutableRecord>)>> {
        return_if_io!(self.move_to(key.clone(), op.clone()));

        {
            let page = self.stack.top();
            return_if_locked!(page);

            let contents = page.get().contents.as_ref().unwrap();

            for cell_idx in 0..contents.cell_count() {
                let cell = contents.cell_get(
                    cell_idx,
                    self.pager.clone(),
                    payload_overflow_threshold_max(
                        contents.page_type(),
                        self.usable_space() as u16,
                    ),
                    payload_overflow_threshold_min(
                        contents.page_type(),
                        self.usable_space() as u16,
                    ),
                    self.usable_space(),
                )?;
                match &cell {
                    BTreeCell::TableLeafCell(TableLeafCell {
                        _rowid: cell_rowid,
                        _payload: payload,
                        first_overflow_page: _,
                    }) => {
                        let SeekKey::TableRowId(rowid_key) = key else {
                            unreachable!("table seek key should be a rowid");
                        };
                        let found = match op {
                            SeekOp::GT => *cell_rowid > rowid_key,
                            SeekOp::GE => *cell_rowid >= rowid_key,
                            SeekOp::EQ => *cell_rowid == rowid_key,
                        };
                        self.stack.advance();
                        if found {
                            let record = crate::storage::sqlite3_ondisk::read_record(payload)?;
                            return Ok(CursorResult::Ok((Some(*cell_rowid), Some(record))));
                        }
                    }
                    BTreeCell::IndexLeafCell(IndexLeafCell { payload, .. }) => {
                        let SeekKey::IndexKey(index_key) = key else {
                            unreachable!("index seek key should be a record");
                        };
                        let record = crate::storage::sqlite3_ondisk::read_record(payload)?;
                        let order = compare_immutable(
                            &record.get_values().as_slice()[..record.len() - 1],
                            &index_key.get_values().as_slice()[..],
                        );
                        let found = match op {
                            SeekOp::GT => order.is_gt(),
                            SeekOp::GE => order.is_ge(),
                            SeekOp::EQ => order.is_eq(),
                        };
                        self.stack.advance();
                        if found {
                            let rowid = match record.last_value() {
                                Some(RefValue::Integer(rowid)) => *rowid as u64,
                                _ => unreachable!("index cells should have an integer rowid"),
                            };
                            return Ok(CursorResult::Ok((Some(rowid), Some(record))));
                        }
                    }
                    cell_type => {
                        unreachable!("unexpected cell type: {:?}", cell_type);
                    }
                }
            }
        }

        // We have now iterated over all cells in the leaf page and found no match.
        let is_index = matches!(key, SeekKey::IndexKey(_));
        if is_index {
            // Unlike tables, indexes store payloads in interior cells as well. self.move_to() always moves to a leaf page, so there are cases where we need to
            // move back up to the parent interior cell and get the next record from there to perform a correct seek.
            // an example of how this can occur:
            //
            // we do an index seek for key K with cmp = SeekOp::GT, meaning we want to seek to the first key that is greater than K.
            // in self.move_to(), we encounter an interior cell with key K' = K+2, and move the left child page, which is a leaf page.
            // the reason we move to the left child page is that we know that in an index, all keys in the left child page are less than K' i.e. less than K+2,
            // meaning that the left subtree may contain a key greater than K, e.g. K+1. however, it is possible that it doesn't, in which case the correct
            // next key is K+2, which is in the parent interior cell.
            //
            // In the seek() method, once we have landed in the leaf page and find that there is no cell with a key greater than K,
            // if we were to return Ok(CursorResult::Ok((None, None))), self.record would be None, which is incorrect, because we already know
            // that there is a record with a key greater than K (K' = K+2) in the parent interior cell. Hence, we need to move back up the tree
            // and get the next matching record from there.
            return self.get_next_record(Some((key, op)));
        }

        Ok(CursorResult::Ok((None, None)))
    }

    /// Move the cursor to the root page of the btree.
    fn move_to_root(&mut self) {
        let mem_page = self.pager.read_page(self.root_page).unwrap();
        self.stack.clear();
        self.stack.push(mem_page);
    }

    /// Move the cursor to the rightmost record in the btree.
    fn move_to_rightmost(&mut self) -> Result<CursorResult<()>> {
        self.move_to_root();

        loop {
            let mem_page = self.stack.top();
            let page_idx = mem_page.get().id;
            let page = self.pager.read_page(page_idx)?;
            return_if_locked!(page);
            let contents = page.get().contents.as_ref().unwrap();
            if contents.is_leaf() {
                if contents.cell_count() > 0 {
                    self.stack.set_cell_index(contents.cell_count() as i32 - 1);
                }
                return Ok(CursorResult::Ok(()));
            }

            match contents.rightmost_pointer() {
                Some(right_most_pointer) => {
                    self.stack.set_cell_index(contents.cell_count() as i32 + 1);
                    let mem_page = self.pager.read_page(right_most_pointer as usize)?;
                    self.stack.push(mem_page);
                    continue;
                }

                None => {
                    unreachable!("interior page should have a rightmost pointer");
                }
            }
        }
    }

    pub fn move_to(&mut self, key: SeekKey<'_>, cmp: SeekOp) -> Result<CursorResult<()>> {
        assert!(self.mv_cursor.is_none());
        // For a table with N rows, we can find any row by row id in O(log(N)) time by starting at the root page and following the B-tree pointers.
        // B-trees consist of interior pages and leaf pages. Interior pages contain pointers to other pages, while leaf pages contain the actual row data.
        //
        // Conceptually, each Interior Cell in a interior page has a rowid and a left child node, and the page itself has a right-most child node.
        // Example: consider an interior page that contains cells C1(rowid=10), C2(rowid=20), C3(rowid=30).
        // - All rows with rowids <= 10 are in the left child node of C1.
        // - All rows with rowids > 10 and <= 20 are in the left child node of C2.
        // - All rows with rowids > 20 and <= 30 are in the left child node of C3.
        // - All rows with rowids > 30 are in the right-most child node of the page.
        //
        // There will generally be multiple levels of interior pages before we reach a leaf page,
        // so we need to follow the interior page pointers until we reach the leaf page that contains the row we are looking for (if it exists).
        //
        // Here's a high-level overview of the algorithm:
        // 1. Since we start at the root page, its cells are all interior cells.
        // 2. We scan the interior cells until we find a cell whose rowid is greater than or equal to the rowid we are looking for.
        // 3. Follow the left child pointer of the cell we found in step 2.
        //    a. In case none of the cells in the page have a rowid greater than or equal to the rowid we are looking for,
        //       we follow the right-most child pointer of the page instead (since all rows with rowids greater than the rowid we are looking for are in the right-most child node).
        // 4. We are now at a new page. If it's another interior page, we repeat the process from step 2. If it's a leaf page, we continue to step 5.
        // 5. We scan the leaf cells in the leaf page until we find the cell whose rowid is equal to the rowid we are looking for.
        //    This cell contains the actual data we are looking for.
        // 6. If we find the cell, we return the record. Otherwise, we return an empty result.
        self.move_to_root();

        loop {
            let page = self.stack.top();
            return_if_locked!(page);

            let contents = page.get().contents.as_ref().unwrap();
            if contents.is_leaf() {
                return Ok(CursorResult::Ok(()));
            }

            let mut found_cell = false;
            for cell_idx in 0..contents.cell_count() {
                match &contents.cell_get(
                    cell_idx,
                    self.pager.clone(),
                    payload_overflow_threshold_max(
                        contents.page_type(),
                        self.usable_space() as u16,
                    ),
                    payload_overflow_threshold_min(
                        contents.page_type(),
                        self.usable_space() as u16,
                    ),
                    self.usable_space(),
                )? {
                    BTreeCell::TableInteriorCell(TableInteriorCell {
                        _left_child_page,
                        _rowid,
                    }) => {
                        let SeekKey::TableRowId(rowid_key) = key else {
                            unreachable!("table seek key should be a rowid");
                        };
                        let target_leaf_page_is_in_left_subtree = match cmp {
                            SeekOp::GT => rowid_key < *_rowid,
                            SeekOp::GE => rowid_key <= *_rowid,
                            SeekOp::EQ => rowid_key <= *_rowid,
                        };
                        self.stack.advance();
                        if target_leaf_page_is_in_left_subtree {
                            let mem_page = self.pager.read_page(*_left_child_page as usize)?;
                            self.stack.push(mem_page);
                            found_cell = true;
                            break;
                        }
                    }
                    BTreeCell::TableLeafCell(TableLeafCell {
                        _rowid: _,
                        _payload: _,
                        first_overflow_page: _,
                    }) => {
                        unreachable!(
                            "we don't iterate leaf cells while trying to move to a leaf cell"
                        );
                    }
                    BTreeCell::IndexInteriorCell(IndexInteriorCell {
                        left_child_page,
                        payload,
                        ..
                    }) => {
                        let SeekKey::IndexKey(index_key) = key else {
                            unreachable!("index seek key should be a record");
                        };
                        let record = crate::storage::sqlite3_ondisk::read_record(payload)?;
                        let order = compare_immutable(index_key.get_values(), record.get_values());
                        let target_leaf_page_is_in_the_left_subtree = match cmp {
                            SeekOp::GT => order.is_lt(),
                            SeekOp::GE => order.is_le(),
                            SeekOp::EQ => order.is_le(),
                        };
                        if target_leaf_page_is_in_the_left_subtree {
                            // we don't advance in case of index tree internal nodes because we will visit this node going up
                            let mem_page = self.pager.read_page(*left_child_page as usize)?;
                            self.stack.push(mem_page);
                            found_cell = true;
                            break;
                        } else {
                            self.stack.advance();
                        }
                    }
                    BTreeCell::IndexLeafCell(_) => {
                        unreachable!(
                            "we don't iterate leaf cells while trying to move to a leaf cell"
                        );
                    }
                }
            }

            if !found_cell {
                match contents.rightmost_pointer() {
                    Some(right_most_pointer) => {
                        self.stack.advance();
                        let mem_page = self.pager.read_page(right_most_pointer as usize)?;
                        self.stack.push(mem_page);
                        continue;
                    }
                    None => {
                        unreachable!("we shall not go back up! The only way is down the slope");
                    }
                }
            }
        }
    }

    /// Insert a record into the btree.
    /// If the insert operation overflows the page, it will be split and the btree will be balanced.
    fn insert_into_page(
        &mut self,
        key: &OwnedValue,
        record: &ImmutableRecord,
    ) -> Result<CursorResult<()>> {
        if let CursorState::None = &self.state {
            self.state = CursorState::Write(WriteInfo::new());
        }
        let ret = loop {
            let write_state = {
                let write_info = self
                    .state
                    .mut_write_info()
                    .expect("can't insert while counting");
                write_info.state
            };
            match write_state {
                WriteState::Start => {
                    let page = self.stack.top();
                    let int_key = match key {
                        OwnedValue::Integer(i) => *i as u64,
                        _ => unreachable!("btree tables are indexed by integers!"),
                    };

                    // get page and find cell
                    let (cell_idx, page_type) = {
                        return_if_locked!(page);

                        page.set_dirty();
                        self.pager.add_dirty(page.get().id);

                        let page = page.get().contents.as_mut().unwrap();
                        assert!(matches!(page.page_type(), PageType::TableLeaf));

                        // find cell
                        (self.find_cell(page, int_key), page.page_type())
                    };

                    // if the cell index is less than the total cells, check: if its an existing
                    // rowid, we are going to update / overwrite the cell
                    if cell_idx < page.get_contents().cell_count() {
                        if let BTreeCell::TableLeafCell(tbl_leaf) = page.get_contents().cell_get(
                            cell_idx,
                            self.pager.clone(),
                            payload_overflow_threshold_max(page_type, self.usable_space() as u16),
                            payload_overflow_threshold_min(page_type, self.usable_space() as u16),
                            self.usable_space(),
                        )? {
                            if tbl_leaf._rowid == int_key {
                                tracing::debug!("insert_into_page: found exact match with cell_idx={cell_idx}, overwriting");
                                self.overwrite_cell(page.clone(), cell_idx, record)?;
                                self.state
                                    .mut_write_info()
                                    .expect("expected write info")
                                    .state = WriteState::Finish;
                                continue;
                            }
                        }
                    }
                    // insert cell
                    let mut cell_payload: Vec<u8> = Vec::new();
                    fill_cell_payload(
                        page_type,
                        Some(int_key),
                        &mut cell_payload,
                        record,
                        self.usable_space() as u16,
                        self.pager.clone(),
                    );

                    // insert
                    let overflow = {
                        let contents = page.get().contents.as_mut().unwrap();
                        debug!(
                            "insert_into_page(overflow, cell_count={})",
                            contents.cell_count()
                        );

                        insert_into_cell(
                            contents,
                            cell_payload.as_slice(),
                            cell_idx,
                            self.usable_space() as u16,
                        )?;
                        contents.overflow_cells.len()
                    };
                    let write_info = self
                        .state
                        .mut_write_info()
                        .expect("can't count while inserting");
                    if overflow > 0 {
                        write_info.state = WriteState::BalanceStart;
                    } else {
                        write_info.state = WriteState::Finish;
                    }
                }
                WriteState::BalanceStart
                | WriteState::BalanceNonRoot
                | WriteState::BalanceNonRootWaitLoadPages => {
                    return_if_io!(self.balance());
                }
                WriteState::Finish => {
                    break Ok(CursorResult::Ok(()));
                }
            };
        };
        self.state = CursorState::None;
        ret
    }

    /// Balance a leaf page.
    /// Balancing is done when a page overflows.
    /// see e.g. https://en.wikipedia.org/wiki/B-tree
    ///
    /// This is a naive algorithm that doesn't try to distribute cells evenly by content.
    /// It will try to split the page in half by keys not by content.
    /// Sqlite tries to have a page at least 40% full.
    fn balance(&mut self) -> Result<CursorResult<()>> {
        assert!(
            matches!(self.state, CursorState::Write(_)),
            "Cursor must be in balancing state"
        );
        loop {
            let state = self.state.write_info().expect("must be balancing").state;
            match state {
                WriteState::BalanceStart => {
                    assert!(
                        self.state
                            .write_info()
                            .unwrap()
                            .balance_info
                            .borrow()
                            .is_none(),
                        "BalanceInfo should be empty on start"
                    );
                    let current_page = self.stack.top();
                    {
                        // check if we don't need to balance
                        // don't continue if there are no overflow cells
                        let page = current_page.get().contents.as_mut().unwrap();
                        if page.overflow_cells.is_empty() {
                            let write_info = self.state.mut_write_info().unwrap();
                            write_info.state = WriteState::Finish;
                            return Ok(CursorResult::Ok(()));
                        }
                    }

                    if !self.stack.has_parent() {
                        self.balance_root();
                    }

                    let write_info = self.state.mut_write_info().unwrap();
                    write_info.state = WriteState::BalanceNonRoot;
                    self.stack.pop();
                    self.stack.retreat();
                    return_if_io!(self.balance_non_root());
                }
                WriteState::BalanceNonRoot | WriteState::BalanceNonRootWaitLoadPages => {
                    return_if_io!(self.balance_non_root());
                }
                WriteState::Finish => return Ok(CursorResult::Ok(())),
                _ => panic!("unexpected state on balance {:?}", state),
            }
        }
    }

    /// Balance a non root page by trying to balance cells between a maximum of 3 siblings that should be neighboring the page that overflowed/underflowed.
    fn balance_non_root(&mut self) -> Result<CursorResult<()>> {
        assert!(
            matches!(self.state, CursorState::Write(_)),
            "Cursor must be in balancing state"
        );
        let state = self.state.write_info().expect("must be balancing").state;
        tracing::debug!("balance_non_root(state={:?})", state);
        let (next_write_state, result) = match state {
            WriteState::Start => todo!(),
            WriteState::BalanceStart => todo!(),
            WriteState::BalanceNonRoot => {
                let parent_page = self.stack.top();
                if parent_page.is_locked() {
                    return Ok(CursorResult::IO);
                }
                return_if_locked!(parent_page);
                if !parent_page.is_loaded() {
                    self.pager.load_page(parent_page.clone())?;
                    return Ok(CursorResult::IO);
                }
                parent_page.set_dirty();
                self.pager.add_dirty(parent_page.get().id);
                let parent_contents = parent_page.get().contents.as_ref().unwrap();
                let page_to_balance_idx = self.stack.current_cell_index() as usize;

                debug!(
                    "balance_non_root(parent_id={} page_to_balance_idx={})",
                    parent_page.get().id,
                    page_to_balance_idx
                );
                assert!(matches!(
                    parent_contents.page_type(),
                    PageType::IndexInterior | PageType::TableInterior
                ));
                // Part 1: Find the sibling pages to balance
                let mut pages_to_balance = vec![];
                let number_of_cells_in_parent =
                    parent_contents.cell_count() + parent_contents.overflow_cells.len();

                assert!(
                    parent_contents.overflow_cells.is_empty(),
                    "balancing child page with overflowed parent not yet implemented"
                );
                assert!(page_to_balance_idx <= parent_contents.cell_count());
                // As there will be at maximum 3 pages used to balance:
                // sibling_pointer is the index represeneting one of those 3 pages, and we initialize it to the last possible page.
                // next_divider is the first divider that contains the first page of the 3 pages.
                let (sibling_pointer, first_cell_divider) = match number_of_cells_in_parent {
                    n if n < 2 => (number_of_cells_in_parent, 0),
                    2 => (2, 0),
                    // Here we will have at lest 2 cells and one right pointer, therefore we can get 3 siblings.
                    // In case of 2 we will have all pages to balance.
                    _ => {
                        // In case of > 3 we have to check which ones to get
                        let next_divider = if page_to_balance_idx == 0 {
                            // first cell, take first 3
                            0
                        } else if page_to_balance_idx == number_of_cells_in_parent {
                            // Page corresponds to right pointer, so take last 3
                            number_of_cells_in_parent - 2
                        } else {
                            // Some cell in the middle, so we want to take sibling on left and right.
                            page_to_balance_idx - 1
                        };
                        (2, next_divider)
                    }
                };
                let sibling_count = sibling_pointer + 1;

                let last_sibling_is_right_pointer = sibling_pointer + first_cell_divider
                    - parent_contents.overflow_cells.len()
                    == parent_contents.cell_count();
                // Get the right page pointer that we will need to update later
                let right_pointer = if last_sibling_is_right_pointer {
                    parent_contents.rightmost_pointer_raw().unwrap()
                } else {
                    let (start_of_cell, _) = parent_contents.cell_get_raw_region(
                        first_cell_divider + sibling_pointer,
                        payload_overflow_threshold_max(
                            parent_contents.page_type(),
                            self.usable_space() as u16,
                        ),
                        payload_overflow_threshold_min(
                            parent_contents.page_type(),
                            self.usable_space() as u16,
                        ),
                        self.usable_space(),
                    );
                    let buf = parent_contents.as_ptr().as_mut_ptr();
                    unsafe { buf.add(start_of_cell) }
                };

                // load sibling pages
                // start loading right page first
                let mut pgno: u32 = unsafe { right_pointer.cast::<u32>().read().swap_bytes() };
                let current_sibling = sibling_pointer;
                for i in (0..=current_sibling).rev() {
                    let page = self.pager.read_page(pgno as usize)?;
                    pages_to_balance.push(page);
                    assert_eq!(
                        parent_contents.overflow_cells.len(),
                        0,
                        "overflow in parent is not yet implented while balancing it"
                    );
                    if i == 0 {
                        break;
                    }
                    let next_cell_divider = i + first_cell_divider - 1;
                    pgno = match parent_contents.cell_get(
                        next_cell_divider,
                        self.pager.clone(),
                        payload_overflow_threshold_max(
                            parent_contents.page_type(),
                            self.usable_space() as u16,
                        ),
                        payload_overflow_threshold_min(
                            parent_contents.page_type(),
                            self.usable_space() as u16,
                        ),
                        self.usable_space(),
                    )? {
                        BTreeCell::TableInteriorCell(table_interior_cell) => {
                            table_interior_cell._left_child_page
                        }
                        BTreeCell::IndexInteriorCell(index_interior_cell) => {
                            index_interior_cell.left_child_page
                        }
                        BTreeCell::TableLeafCell(..) | BTreeCell::IndexLeafCell(..) => {
                            unreachable!()
                        }
                    };
                }
                // Reverse in order to keep the right order
                pages_to_balance.reverse();
                self.state
                    .write_info()
                    .unwrap()
                    .balance_info
                    .replace(Some(BalanceInfo {
                        pages_to_balance,
                        rightmost_pointer: right_pointer,
                        divider_cells: Vec::new(),
                        sibling_count,
                        first_divider_cell: first_cell_divider,
                    }));
                (
                    WriteState::BalanceNonRootWaitLoadPages,
                    Ok(CursorResult::IO),
                )
            }
            WriteState::BalanceNonRootWaitLoadPages => {
                let write_info = self.state.write_info().unwrap();
                let mut balance_info = write_info.balance_info.borrow_mut();
                let balance_info = balance_info.as_mut().unwrap();
                let all_loaded = balance_info
                    .pages_to_balance
                    .iter()
                    .all(|page| !page.is_locked());
                if !all_loaded {
                    return Ok(CursorResult::IO);
                }
                // Now do real balancing
                let parent_page = self.stack.top();
                let parent_contents = parent_page.get_contents();
                assert!(
                    parent_contents.overflow_cells.is_empty(),
                    "overflow parent not yet implemented"
                );

                // Get divider cells and max_cells
                let mut max_cells = 0;
                let mut pages_to_balance_new = Vec::new();
                for i in (0..balance_info.sibling_count).rev() {
                    let sibling_page = &balance_info.pages_to_balance[i];
                    let sibling_contents = sibling_page.get_contents();
                    sibling_page.set_dirty();
                    self.pager.add_dirty(sibling_page.get().id);
                    max_cells += sibling_contents.cell_count();
                    if i == 0 {
                        // we don't have left sibling from this one so we break
                        break;
                    }
                    // Since we know we have a left sibling, take the divider that points to left sibling of this page
                    let cell_idx = balance_info.first_divider_cell + i - 1;
                    let (cell_start, cell_len) = parent_contents.cell_get_raw_region(
                        cell_idx,
                        payload_overflow_threshold_max(
                            parent_contents.page_type(),
                            self.usable_space() as u16,
                        ),
                        payload_overflow_threshold_min(
                            parent_contents.page_type(),
                            self.usable_space() as u16,
                        ),
                        self.usable_space(),
                    );
                    let buf = parent_contents.as_ptr();
                    let cell_buf = &buf[cell_start..cell_start + cell_len];

                    // TODO(pere): make this reference and not copy
                    balance_info.divider_cells.push(cell_buf.to_vec());
                    tracing::trace!(
                        "dropping divider cell from parent cell_idx={} count={}",
                        cell_idx,
                        parent_contents.cell_count()
                    );
                    drop_cell(parent_contents, cell_idx, self.usable_space() as u16)?;
                }
                assert_eq!(
                    balance_info.divider_cells.len(),
                    balance_info.sibling_count - 1,
                    "the number of pages balancing must be divided by one less divider"
                );
                // Reverse divider cells to be in order
                balance_info.divider_cells.reverse();

                let mut cell_array = CellArray {
                    cells: Vec::with_capacity(max_cells),
                    number_of_cells_per_page: Vec::new(),
                };

                let mut total_cells_inserted = 0;
                // count_cells_in_old_pages is the prefix sum of cells of each page
                let mut count_cells_in_old_pages = Vec::new();

                let page_type = balance_info.pages_to_balance[0].get_contents().page_type();
                let leaf_data = matches!(page_type, PageType::TableLeaf);
                let leaf = matches!(page_type, PageType::TableLeaf | PageType::IndexLeaf);
                for (i, old_page) in balance_info.pages_to_balance.iter().enumerate() {
                    let old_page_contents = old_page.get_contents();
                    for cell_idx in 0..old_page_contents.cell_count() {
                        let (cell_start, cell_len) = old_page_contents.cell_get_raw_region(
                            cell_idx,
                            payload_overflow_threshold_max(
                                old_page_contents.page_type(),
                                self.usable_space() as u16,
                            ),
                            payload_overflow_threshold_min(
                                old_page_contents.page_type(),
                                self.usable_space() as u16,
                            ),
                            self.usable_space(),
                        );
                        let buf = old_page_contents.as_ptr();
                        let cell_buf = &mut buf[cell_start..cell_start + cell_len];
                        // TODO(pere): make this reference and not copy
                        cell_array.cells.push(to_static_buf(cell_buf));
                    }
                    // Insert overflow cells into correct place
                    let offset = total_cells_inserted;
                    assert!(
                        old_page_contents.overflow_cells.len() <= 1,
                        "todo: check this works for more than one overflow cell"
                    );
                    for overflow_cell in old_page_contents.overflow_cells.iter_mut() {
                        cell_array.cells.insert(
                            offset + overflow_cell.index,
                            to_static_buf(&mut Pin::as_mut(&mut overflow_cell.payload)),
                        );
                    }

                    count_cells_in_old_pages.push(cell_array.cells.len() as u16);

                    let mut cells_inserted =
                        old_page_contents.cell_count() + old_page_contents.overflow_cells.len();

                    if i < balance_info.pages_to_balance.len() - 1 && !leaf_data {
                        // If we are a index page or a interior table page we need to take the divider cell too.
                        // But we don't need the last divider as it will remain the same.
                        let divider_cell = &mut balance_info.divider_cells[i];
                        // TODO(pere): in case of old pages are leaf pages, so index leaf page, we need to strip page pointers
                        // from divider cells in index interior pages (parent) because those should not be included.
                        cells_inserted += 1;
                        cell_array.cells.push(to_static_buf(divider_cell.as_mut()));
                    }
                    total_cells_inserted += cells_inserted;
                }

                // calculate how many pages to allocate
                let mut new_page_sizes = Vec::new();
                let mut k = 0;
                let leaf_correction = if leaf { 4 } else { 0 };
                // number of bytes beyond header, different from global usableSapce which inccludes
                // header
                let usable_space = self.usable_space() - 12 + leaf_correction;
                for i in 0..balance_info.sibling_count {
                    cell_array
                        .number_of_cells_per_page
                        .push(count_cells_in_old_pages[i]);
                    let page = &balance_info.pages_to_balance[i];
                    let page_contents = page.get_contents();
                    let free_space = compute_free_space(page_contents, self.usable_space() as u16);

                    // If we have an empty page of cells, we ignore it
                    if k > 0
                        && cell_array.number_of_cells_per_page[k - 1]
                            == cell_array.number_of_cells_per_page[k]
                    {
                        k -= 1;
                    }
                    if !leaf_data {
                        k += 1;
                    }
                    new_page_sizes.push(usable_space as u16 - free_space);
                    for overflow in &page_contents.overflow_cells {
                        let size = new_page_sizes.last_mut().unwrap();
                        // 2 to account of pointer
                        *size += 2 + overflow.payload.len() as u16;
                    }
                    k += 1;
                }

                // Try to pack as many cells to the left
                let mut sibling_count_new = balance_info.sibling_count;
                let mut i = 0;
                while i < sibling_count_new {
                    // First try to move cells to the right if they do not fit
                    while new_page_sizes[i] > usable_space as u16 {
                        let needs_new_page = i + 1 >= sibling_count_new;
                        if needs_new_page {
                            sibling_count_new += 1;
                            new_page_sizes.push(0);
                            cell_array
                                .number_of_cells_per_page
                                .push(cell_array.cells.len() as u16);
                            assert!(
                                sibling_count_new <= 5,
                                "it is corrupt to require more than 5 pages to balance 3 siblings"
                            );
                        }
                        let size_of_cell_to_remove_from_left =
                            2 + cell_array.cells[cell_array.cell_count(i) - 1].len() as u16;
                        new_page_sizes[i] -= size_of_cell_to_remove_from_left;
                        let size_of_cell_to_move_right = if !leaf_data {
                            if cell_array.number_of_cells_per_page[i]
                                < cell_array.cells.len() as u16
                            {
                                // This means we move to the right page the divider cell and we
                                // promote left cell to divider
                                2 + cell_array.cells[cell_array.cell_count(i)].len() as u16
                            } else {
                                0
                            }
                        } else {
                            size_of_cell_to_remove_from_left
                        };
                        new_page_sizes[i + 1] += size_of_cell_to_move_right;
                        cell_array.number_of_cells_per_page[i] -= 1;
                    }

                    // Now try to take from the right if we didn't have enough
                    while cell_array.number_of_cells_per_page[i] < cell_array.cells.len() as u16 {
                        let size_of_cell_to_remove_from_right =
                            2 + cell_array.cells[cell_array.cell_count(i)].len() as u16;
                        let can_take = new_page_sizes[i] + size_of_cell_to_remove_from_right
                            > usable_space as u16;
                        if can_take {
                            break;
                        }
                        new_page_sizes[i] += size_of_cell_to_remove_from_right;
                        cell_array.number_of_cells_per_page[i] += 1;

                        let size_of_cell_to_remove_from_right = if !leaf_data {
                            if cell_array.number_of_cells_per_page[i]
                                < cell_array.cells.len() as u16
                            {
                                2 + cell_array.cells[cell_array.cell_count(i)].len() as u16
                            } else {
                                0
                            }
                        } else {
                            size_of_cell_to_remove_from_right
                        };

                        new_page_sizes[i + 1] -= size_of_cell_to_remove_from_right;
                    }

                    let we_still_need_another_page =
                        cell_array.number_of_cells_per_page[i] >= cell_array.cells.len() as u16;
                    if we_still_need_another_page {
                        sibling_count_new = i + 1;
                    }
                    i += 1;
                    if i >= sibling_count_new {
                        break;
                    }
                }
                tracing::debug!(
                    "balance_non_root(sibling_count={}, sibling_count_new={}, cells={})",
                    balance_info.sibling_count,
                    sibling_count_new,
                    cell_array.cells.len()
                );

                // Comment borrowed from SQLite src/btree.c
                // The packing computed by the previous block is biased toward the siblings
                // on the left side (siblings with smaller keys). The left siblings are
                // always nearly full, while the right-most sibling might be nearly empty.
                // The next block of code attempts to adjust the packing of siblings to
                // get a better balance.
                //
                // This adjustment is more than an optimization.  The packing above might
                // be so out of balance as to be illegal.  For example, the right-most
                // sibling might be completely empty.  This adjustment is not optional.
                for i in (1..sibling_count_new).rev() {
                    let mut size_right_page = new_page_sizes[i];
                    let mut size_left_page = new_page_sizes[i - 1];
                    let mut cell_left = cell_array.number_of_cells_per_page[i - 1] - 1;
                    // if leaf_data means we don't have divider, so the one we move from left is
                    // the same we add to right (we don't add divider to right).
                    let mut cell_right = cell_left + 1 - leaf_data as u16;
                    loop {
                        let cell_left_size = cell_array.cell_size(cell_left as usize);
                        let cell_right_size = cell_array.cell_size(cell_right as usize);
                        // TODO: add assert nMaxCells

                        let pointer_size = if i == sibling_count_new - 1 { 0 } else { 2 };
                        let would_not_improve_balance = size_right_page + cell_right_size + 2
                            > size_left_page - (cell_left_size + pointer_size);
                        if size_right_page != 0 && would_not_improve_balance {
                            break;
                        }

                        size_left_page -= cell_left_size + 2;
                        size_right_page += cell_right_size + 2;
                        cell_array.number_of_cells_per_page[i - 1] = cell_left;

                        if cell_left == 0 {
                            break;
                        }
                        cell_left -= 1;
                        cell_right -= 1;
                    }

                    new_page_sizes[i] = size_right_page;
                    new_page_sizes[i - 1] = size_left_page;
                    assert!(
                        cell_array.number_of_cells_per_page[i - 1]
                            > if i > 1 {
                                cell_array.number_of_cells_per_page[i - 2]
                            } else {
                                0
                            }
                    );
                }

                // Allocate pages or set dirty if not needed
                for i in 0..sibling_count_new {
                    if i < balance_info.sibling_count {
                        balance_info.pages_to_balance[i].set_dirty();
                        pages_to_balance_new.push(balance_info.pages_to_balance[i].clone());
                    } else {
                        let page = self.pager.do_allocate_page(page_type, 0);
                        pages_to_balance_new.push(page);
                        // Since this page didn't exist before, we can set it to cells length as it
                        // marks them as empty since it is a prefix sum of cells.
                        count_cells_in_old_pages.push(cell_array.cells.len() as u16);
                    }
                }

                // Reassign page numbers in increasing order
                let mut page_numbers = Vec::new();
                for page in pages_to_balance_new.iter() {
                    page_numbers.push(page.get().id);
                }
                page_numbers.sort();
                for (page, new_id) in pages_to_balance_new.iter().zip(page_numbers) {
                    if new_id != page.get().id {
                        page.get().id = new_id;
                        self.pager.put_loaded_page(new_id, page.clone());
                    }
                }

                // Write right pointer in parent page to point to new rightmost page
                let right_page_id = pages_to_balance_new.last().unwrap().get().id as u32;
                let rightmost_pointer = balance_info.rightmost_pointer;
                let rightmost_pointer =
                    unsafe { std::slice::from_raw_parts_mut(rightmost_pointer, 4) };
                rightmost_pointer[0..4].copy_from_slice(&right_page_id.to_be_bytes());

                // Ensure right-child pointer of the right-most new sibling pge points to the page
                // that was originally on that place.
                let is_leaf_page = matches!(page_type, PageType::TableLeaf | PageType::IndexLeaf);
                if !is_leaf_page {
                    let last_page = balance_info.pages_to_balance.last().unwrap();
                    let right_pointer = last_page.get_contents().rightmost_pointer().unwrap();
                    let new_last_page = pages_to_balance_new.last().unwrap();
                    new_last_page
                        .get_contents()
                        .write_u32(PAGE_HEADER_OFFSET_RIGHTMOST_PTR, right_pointer);
                }
                // TODO: pointer map update (vacuum support)
                for (i, page) in pages_to_balance_new
                    .iter()
                    .enumerate()
                    .take(sibling_count_new - 1)
                /* do not take last page */
                {
                    let divider_cell_idx = cell_array.cell_count(i);
                    let mut divider_cell = &mut cell_array.cells[divider_cell_idx];
                    // FIXME: dont use auxiliary space, could be done without allocations
                    let mut new_divider_cell = Vec::new();
                    if !is_leaf_page {
                        // Interior
                        page.get_contents()
                            .write_u32(PAGE_HEADER_OFFSET_RIGHTMOST_PTR, page.get().id as u32);
                        new_divider_cell.extend_from_slice(divider_cell);
                    } else if leaf_data {
                        // Leaf table
                        // FIXME: not needed conversion
                        // FIXME: need to update cell size in order to free correctly?
                        // insert into cell with correct range should be enough
                        divider_cell = &mut cell_array.cells[divider_cell_idx - 1];
                        let (_, n_bytes_payload) = read_varint(divider_cell)?;
                        let (rowid, _) = read_varint(&divider_cell[n_bytes_payload..])?;
                        new_divider_cell.extend_from_slice(&(page.get().id as u32).to_be_bytes());
                        write_varint_to_vec(rowid, &mut new_divider_cell);
                    } else {
                        // Leaf index
                        new_divider_cell.extend_from_slice(divider_cell);
                    }
                    // FIXME: defragment shouldn't be needed
                    defragment_page(parent_contents, self.usable_space() as u16);
                    insert_into_cell(
                        parent_contents,
                        &new_divider_cell,
                        balance_info.first_divider_cell + i,
                        self.usable_space() as u16,
                    )
                    .unwrap();
                }
                // TODO: update pages
                let mut done = vec![false; sibling_count_new];
                for i in (1 - sibling_count_new as i64)..sibling_count_new as i64 {
                    let page_idx = i.unsigned_abs() as usize;
                    if done[page_idx] {
                        continue;
                    }
                    if i >= 0
                        || count_cells_in_old_pages[page_idx - 1]
                            >= cell_array.number_of_cells_per_page[page_idx - 1]
                    {
                        let (start_old_cells, start_new_cells, number_new_cells) = if page_idx == 0
                        {
                            (0, 0, cell_array.cell_count(0))
                        } else {
                            let this_was_old_page = page_idx < balance_info.sibling_count;
                            let start_old_cells = if this_was_old_page {
                                count_cells_in_old_pages[page_idx - 1] as usize
                                    + (!leaf_data) as usize
                            } else {
                                cell_array.cells.len()
                            };
                            let start_new_cells =
                                cell_array.cell_count(page_idx - 1) + (!leaf_data) as usize;
                            (
                                start_old_cells,
                                start_new_cells,
                                cell_array.cell_count(page_idx) - start_new_cells,
                            )
                        };
                        let page = pages_to_balance_new[page_idx].get_contents();
                        edit_page(
                            page,
                            start_old_cells,
                            start_new_cells,
                            number_new_cells,
                            &cell_array,
                            self.usable_space() as u16,
                        )?;
                        tracing::trace!(
                            "edit_page page={} cells={}",
                            pages_to_balance_new[page_idx].get().id,
                            page.cell_count()
                        );
                        page.overflow_cells.clear();

                        done[page_idx] = true;
                    }
                }
                // TODO: balance root
                // TODO: free pages
                (WriteState::BalanceStart, Ok(CursorResult::Ok(())))
            }
            WriteState::Finish => todo!(),
        };
        if matches!(next_write_state, WriteState::BalanceStart) {
            // reset balance state
            let _ = self.state.mut_write_info().unwrap().balance_info.take();
        }
        let write_info = self.state.mut_write_info().unwrap();
        write_info.state = next_write_state;
        result
    }

    /// Balance the root page.
    /// This is done when the root page overflows, and we need to create a new root page.
    /// See e.g. https://en.wikipedia.org/wiki/B-tree
    fn balance_root(&mut self) {
        /* todo: balance deeper, create child and copy contents of root there. Then split root */
        /* if we are in root page then we just need to create a new root and push key there */

        let is_page_1 = {
            let current_root = self.stack.top();
            current_root.get().id == 1
        };

        let offset = if is_page_1 { DATABASE_HEADER_SIZE } else { 0 };

        let root = self.stack.top();
        let root_contents = root.get_contents();
        let child = self.pager.do_allocate_page(root_contents.page_type(), 0);

        tracing::debug!(
            "Balancing root. root={}, rightmost={}",
            root.get().id,
            child.get().id
        );

        self.pager.add_dirty(root.get().id);
        self.pager.add_dirty(child.get().id);

        let root_buf = root_contents.as_ptr();
        let child_contents = child.get_contents();
        let child_buf = child_contents.as_ptr();
        let (root_pointer_start, root_pointer_len) =
            root_contents.cell_pointer_array_offset_and_size();
        let (child_pointer_start, _) = child.get_contents().cell_pointer_array_offset_and_size();

        let top = root_contents.cell_content_area() as usize;

        // 1. Modify child
        // Copy pointers
        child_buf[child_pointer_start..child_pointer_start + root_pointer_len]
            .copy_from_slice(&root_buf[root_pointer_start..root_pointer_start + root_pointer_len]);
        // Copy cell contents
        child_buf[top..].copy_from_slice(&root_buf[top..]);
        // Copy header
        child_buf[0..root_contents.header_size()]
            .copy_from_slice(&root_buf[offset..offset + root_contents.header_size()]);
        // Copy overflow cells
        child_contents.overflow_cells = root_contents.overflow_cells.clone();

        // 2. Modify root
        let new_root_page_type = match root_contents.page_type() {
            PageType::IndexLeaf => PageType::IndexInterior,
            PageType::TableLeaf => PageType::TableInterior,
            _ => unreachable!("invalid root non leaf page type"),
        } as u8;
        // set new page type
        root_contents.write_u8(PAGE_HEADER_OFFSET_PAGE_TYPE, new_root_page_type);
        root_contents.write_u32(PAGE_HEADER_OFFSET_RIGHTMOST_PTR, child.get().id as u32);
        root_contents.write_u16(
            PAGE_HEADER_OFFSET_CELL_CONTENT_AREA,
            self.usable_space() as u16,
        );
        root_contents.write_u16(PAGE_HEADER_OFFSET_CELL_COUNT, 0);
        root_contents.write_u16(PAGE_HEADER_OFFSET_FIRST_FREEBLOCK, 0);

        root_contents.write_u8(PAGE_HEADER_OFFSET_FRAGMENTED_BYTES_COUNT, 0);
        root_contents.overflow_cells.clear();
        self.root_page = root.get().id;
        self.stack.clear();
        self.stack.push(root.clone());
        self.stack.advance();
        self.stack.push(child.clone());
    }

    fn usable_space(&self) -> usize {
        self.pager.usable_space()
    }

    /// Find the index of the cell in the page that contains the given rowid.
    /// BTree tables only.
    fn find_cell(&self, page: &PageContent, int_key: u64) -> usize {
        let mut cell_idx = 0;
        let cell_count = page.cell_count();
        while cell_idx < cell_count {
            match page
                .cell_get(
                    cell_idx,
                    self.pager.clone(),
                    payload_overflow_threshold_max(page.page_type(), self.usable_space() as u16),
                    payload_overflow_threshold_min(page.page_type(), self.usable_space() as u16),
                    self.usable_space(),
                )
                .unwrap()
            {
                BTreeCell::TableLeafCell(cell) => {
                    if int_key <= cell._rowid {
                        break;
                    }
                }
                BTreeCell::TableInteriorCell(cell) => {
                    if int_key <= cell._rowid {
                        break;
                    }
                }
                _ => todo!(),
            }
            cell_idx += 1;
        }
        cell_idx
    }

    pub fn seek_to_last(&mut self) -> Result<CursorResult<()>> {
        return_if_io!(self.move_to_rightmost());
        let (rowid, record) = return_if_io!(self.get_next_record(None));
        if rowid.is_none() {
            let is_empty = return_if_io!(self.is_empty_table());
            assert!(is_empty);
            return Ok(CursorResult::Ok(()));
        }
        self.rowid.replace(rowid);
        self.record.replace(record);
        Ok(CursorResult::Ok(()))
    }

    pub fn is_empty(&self) -> bool {
        self.record.borrow().is_none()
    }

    pub fn root_page(&self) -> usize {
        self.root_page
    }

    pub fn rewind(&mut self) -> Result<CursorResult<()>> {
        if self.mv_cursor.is_some() {
            let (rowid, record) = return_if_io!(self.get_next_record(None));
            self.rowid.replace(rowid);
            self.record.replace(record);
        } else {
            self.move_to_root();

            let (rowid, record) = return_if_io!(self.get_next_record(None));
            self.rowid.replace(rowid);
            self.record.replace(record);
        }
        Ok(CursorResult::Ok(()))
    }

    pub fn last(&mut self) -> Result<CursorResult<()>> {
        assert!(self.mv_cursor.is_none());
        match self.move_to_rightmost()? {
            CursorResult::Ok(_) => self.prev(),
            CursorResult::IO => Ok(CursorResult::IO),
        }
    }

    pub fn next(&mut self) -> Result<CursorResult<()>> {
        let (rowid, record) = return_if_io!(self.get_next_record(None));
        self.rowid.replace(rowid);
        self.record.replace(record);
        Ok(CursorResult::Ok(()))
    }

    pub fn prev(&mut self) -> Result<CursorResult<()>> {
        assert!(self.mv_cursor.is_none());
        match self.get_prev_record()? {
            CursorResult::Ok((rowid, record)) => {
                self.rowid.replace(rowid);
                self.record.replace(record);
                Ok(CursorResult::Ok(()))
            }
            CursorResult::IO => Ok(CursorResult::IO),
        }
    }

    pub fn wait_for_completion(&mut self) -> Result<()> {
        // TODO: Wait for pager I/O to complete
        Ok(())
    }

    pub fn rowid(&self) -> Result<Option<u64>> {
        if let Some(mv_cursor) = &self.mv_cursor {
            let mv_cursor = mv_cursor.borrow();
            return Ok(mv_cursor.current_row_id().map(|rowid| rowid.row_id));
        }
        Ok(self.rowid.get())
    }

    pub fn seek(&mut self, key: SeekKey<'_>, op: SeekOp) -> Result<CursorResult<bool>> {
        assert!(self.mv_cursor.is_none());
        let (rowid, record) = return_if_io!(self.do_seek(key, op));
        self.rowid.replace(rowid);
        self.record.replace(record);
        Ok(CursorResult::Ok(rowid.is_some()))
    }

    pub fn record(&self) -> Ref<Option<ImmutableRecord>> {
        self.record.borrow()
    }

    pub fn insert(
        &mut self,
        key: &OwnedValue,
        record: &ImmutableRecord,
        moved_before: bool, /* Indicate whether it's necessary to traverse to find the leaf page */
    ) -> Result<CursorResult<()>> {
        let int_key = match key {
            OwnedValue::Integer(i) => i,
            _ => unreachable!("btree tables are indexed by integers!"),
        };
        match &self.mv_cursor {
            Some(mv_cursor) => {
                let row_id =
                    crate::mvcc::database::RowID::new(self.table_id() as u64, *int_key as u64);
                let record_buf = record.payload.to_vec();
                let row = crate::mvcc::database::Row::new(row_id, record_buf);
                mv_cursor.borrow_mut().insert(row).unwrap();
            }
            None => {
                if !moved_before {
                    return_if_io!(self.move_to(SeekKey::TableRowId(*int_key as u64), SeekOp::EQ));
                }
                return_if_io!(self.insert_into_page(key, record));
                self.rowid.replace(Some(*int_key as u64));
            }
        };
        Ok(CursorResult::Ok(()))
    }

    pub fn delete(&mut self) -> Result<CursorResult<()>> {
        assert!(self.mv_cursor.is_none());
        let page = self.stack.top();
        return_if_locked!(page);

        if !page.is_loaded() {
            self.pager.load_page(page.clone())?;
            return Ok(CursorResult::IO);
        }

        let target_rowid = match self.rowid.get() {
            Some(rowid) => rowid,
            None => return Ok(CursorResult::Ok(())),
        };

        let contents = page.get().contents.as_ref().unwrap();

        // TODO(Krishna): We are doing this linear search here because seek() is returning the index of previous cell.
        // And the fix is currently not very clear to me.
        // This finds the cell with matching rowid with in a page.
        let mut cell_idx = None;
        for idx in 0..contents.cell_count() {
            let cell = contents.cell_get(
                idx,
                self.pager.clone(),
                payload_overflow_threshold_max(contents.page_type(), self.usable_space() as u16),
                payload_overflow_threshold_min(contents.page_type(), self.usable_space() as u16),
                self.usable_space(),
            )?;

            if let BTreeCell::TableLeafCell(leaf_cell) = cell {
                if leaf_cell._rowid == target_rowid {
                    cell_idx = Some(idx);
                    break;
                }
            }
        }

        let cell_idx = match cell_idx {
            Some(idx) => idx,
            None => return Ok(CursorResult::Ok(())),
        };

        let contents = page.get().contents.as_ref().unwrap();
        let cell = contents.cell_get(
            cell_idx,
            self.pager.clone(),
            payload_overflow_threshold_max(contents.page_type(), self.usable_space() as u16),
            payload_overflow_threshold_min(contents.page_type(), self.usable_space() as u16),
            self.usable_space(),
        )?;

        if cell_idx >= contents.cell_count() {
            return_corrupt!(format!(
                "Corrupted page: cell index {} is out of bounds for page with {} cells",
                cell_idx,
                contents.cell_count()
            ));
        }

        let original_child_pointer = match &cell {
            BTreeCell::TableInteriorCell(interior) => Some(interior._left_child_page),
            _ => None,
        };

        return_if_io!(self.clear_overflow_pages(&cell));

        let page = self.stack.top();
        return_if_locked!(page);
        if !page.is_loaded() {
            self.pager.load_page(page.clone())?;
            return Ok(CursorResult::IO);
        }

        page.set_dirty();
        self.pager.add_dirty(page.get().id);

        let contents = page.get().contents.as_mut().unwrap();

        // If this is an interior node, we need to handle deletion differently
        // For interior nodes:
        // 1. Move cursor to largest entry in left subtree
        // 2. Copy that entry to replace the one being deleted
        // 3. Delete the leaf entry
        if !contents.is_leaf() {
            // 1. Move cursor to largest entry in left subtree
            return_if_io!(self.prev());

            let leaf_page = self.stack.top();

            // 2. Copy that entry to replace the one being deleted
            let leaf_contents = leaf_page.get().contents.as_ref().unwrap();
            let leaf_cell_idx = self.stack.current_cell_index() as usize - 1;
            let predecessor_cell = leaf_contents.cell_get(
                leaf_cell_idx,
                self.pager.clone(),
                payload_overflow_threshold_max(
                    leaf_contents.page_type(),
                    self.usable_space() as u16,
                ),
                payload_overflow_threshold_min(
                    leaf_contents.page_type(),
                    self.usable_space() as u16,
                ),
                self.usable_space(),
            )?;

            // 3. Create an interior cell from the leaf cell
            let mut cell_payload: Vec<u8> = Vec::new();
            match predecessor_cell {
                BTreeCell::TableLeafCell(leaf_cell) => {
                    // Format: [left child page (4 bytes)][rowid varint]
                    if let Some(child_pointer) = original_child_pointer {
                        cell_payload.extend_from_slice(&child_pointer.to_be_bytes());
                        write_varint_to_vec(leaf_cell._rowid, &mut cell_payload);
                    }
                }
                _ => unreachable!("Expected table leaf cell"),
            }
            insert_into_cell(
                contents,
                &cell_payload,
                cell_idx,
                self.usable_space() as u16,
            )
            .unwrap();
            drop_cell(contents, cell_idx, self.usable_space() as u16)?;
        } else {
            // For leaf nodes, simply remove the cell
            drop_cell(contents, cell_idx, self.usable_space() as u16)?;
        }

        // TODO(Krishna): Implement balance after delete. I will implement after balance_nonroot is extended.
        Ok(CursorResult::Ok(()))
    }

    pub fn set_null_flag(&mut self, flag: bool) {
        self.null_flag = flag;
    }

    pub fn get_null_flag(&self) -> bool {
        self.null_flag
    }

    pub fn exists(&mut self, key: &OwnedValue) -> Result<CursorResult<bool>> {
        assert!(self.mv_cursor.is_none());
        let int_key = match key {
            OwnedValue::Integer(i) => i,
            _ => unreachable!("btree tables are indexed by integers!"),
        };
        return_if_io!(self.move_to(SeekKey::TableRowId(*int_key as u64), SeekOp::EQ));
        let page = self.stack.top();
        // TODO(pere): request load
        return_if_locked!(page);

        let contents = page.get().contents.as_ref().unwrap();

        // find cell
        let int_key = match key {
            OwnedValue::Integer(i) => *i as u64,
            _ => unreachable!("btree tables are indexed by integers!"),
        };
        let cell_idx = self.find_cell(contents, int_key);
        if cell_idx >= contents.cell_count() {
            Ok(CursorResult::Ok(false))
        } else {
            let equals = match &contents.cell_get(
                cell_idx,
                self.pager.clone(),
                payload_overflow_threshold_max(contents.page_type(), self.usable_space() as u16),
                payload_overflow_threshold_min(contents.page_type(), self.usable_space() as u16),
                self.usable_space(),
            )? {
                BTreeCell::TableLeafCell(l) => l._rowid == int_key,
                _ => unreachable!(),
            };
            Ok(CursorResult::Ok(equals))
        }
    }

    /// Clear the overflow pages linked to a specific page provided by the leaf cell
    /// Uses a state machine to keep track of it's operations so that traversal can be
    /// resumed from last point after IO interruption
    fn clear_overflow_pages(&mut self, cell: &BTreeCell) -> Result<CursorResult<()>> {
        loop {
            let state = self.overflow_state.take().unwrap_or(OverflowState::Start);

            match state {
                OverflowState::Start => {
                    let first_overflow_page = match cell {
                        BTreeCell::TableLeafCell(leaf_cell) => leaf_cell.first_overflow_page,
                        BTreeCell::IndexLeafCell(leaf_cell) => leaf_cell.first_overflow_page,
                        BTreeCell::IndexInteriorCell(interior_cell) => {
                            interior_cell.first_overflow_page
                        }
                        BTreeCell::TableInteriorCell(_) => return Ok(CursorResult::Ok(())), // No overflow pages
                    };

                    if let Some(page) = first_overflow_page {
                        self.overflow_state = Some(OverflowState::ProcessPage { next_page: page });
                        continue;
                    } else {
                        self.overflow_state = Some(OverflowState::Done);
                    }
                }
                OverflowState::ProcessPage { next_page } => {
                    if next_page < 2
                        || next_page as usize > self.pager.db_header.lock().database_size as usize
                    {
                        self.overflow_state = None;
                        return Err(LimboError::Corrupt("Invalid overflow page number".into()));
                    }
                    let page = self.pager.read_page(next_page as usize)?;
                    return_if_locked!(page);

                    let contents = page.get().contents.as_ref().unwrap();
                    let next = contents.read_u32(0);

                    self.pager.free_page(Some(page), next_page as usize)?;

                    if next != 0 {
                        self.overflow_state = Some(OverflowState::ProcessPage { next_page: next });
                    } else {
                        self.overflow_state = Some(OverflowState::Done);
                    }
                }
                OverflowState::Done => {
                    self.overflow_state = None;
                    return Ok(CursorResult::Ok(()));
                }
            };
        }
    }

    /// Destroys a B-tree by freeing all its pages in an iterative depth-first order.
    /// This ensures child pages are freed before their parents
    /// Uses a state machine to keep track of the operation to ensure IO doesn't cause repeated traversals
    ///
    /// # Example
    /// For a B-tree with this structure (where 4' is an overflow page):
    /// ```text
    ///            1 (root)
    ///           /        \
    ///          2          3
    ///        /   \      /   \
    /// 4' <- 4     5    6     7
    /// ```
    ///
    /// The destruction order would be: [4',4,5,2,6,7,3,1]
    pub fn btree_destroy(&mut self) -> Result<CursorResult<()>> {
        if let CursorState::None = &self.state {
            self.move_to_root();
            self.state = CursorState::Destroy(DestroyInfo {
                state: DestroyState::Start,
            });
        }

        loop {
            let destroy_state = {
                let destroy_info = self
                    .state
                    .destroy_info()
                    .expect("unable to get a mut reference to destroy state in cursor");
                destroy_info.state.clone()
            };

            match destroy_state {
                DestroyState::Start => {
                    let destroy_info = self
                        .state
                        .mut_destroy_info()
                        .expect("unable to get a mut reference to destroy state in cursor");
                    destroy_info.state = DestroyState::LoadPage;
                }
                DestroyState::LoadPage => {
                    let page = self.stack.top();
                    return_if_locked!(page);

                    if !page.is_loaded() {
                        self.pager.load_page(Arc::clone(&page))?;
                        return Ok(CursorResult::IO);
                    }

                    let destroy_info = self
                        .state
                        .mut_destroy_info()
                        .expect("unable to get a mut reference to destroy state in cursor");
                    destroy_info.state = DestroyState::ProcessPage;
                }
                DestroyState::ProcessPage => {
                    let page = self.stack.top();
                    assert!(page.is_loaded()); //  page should be loaded at this time

                    let contents = page.get().contents.as_ref().unwrap();
                    let cell_idx = self.stack.current_cell_index();

                    //  If we've processed all cells in this page, figure out what to do with this page
                    if cell_idx >= contents.cell_count() as i32 {
                        match (contents.is_leaf(), cell_idx) {
                            //  Leaf pages with all cells processed
                            (true, n) if n >= contents.cell_count() as i32 => {
                                let destroy_info = self.state.mut_destroy_info().expect(
                                    "unable to get a mut reference to destroy state in cursor",
                                );
                                destroy_info.state = DestroyState::FreePage;
                                continue;
                            }
                            //  Non-leaf page which has processed all children but not it's potential right child
                            (false, n) if n == contents.cell_count() as i32 => {
                                if let Some(rightmost) = contents.rightmost_pointer() {
                                    let rightmost_page =
                                        self.pager.read_page(rightmost as usize)?;
                                    self.stack.advance();
                                    self.stack.push(rightmost_page);
                                    let destroy_info = self.state.mut_destroy_info().expect(
                                        "unable to get a mut reference to destroy state in cursor",
                                    );
                                    destroy_info.state = DestroyState::LoadPage;
                                } else {
                                    let destroy_info = self.state.mut_destroy_info().expect(
                                        "unable to get a mut reference to destroy state in cursor",
                                    );
                                    destroy_info.state = DestroyState::FreePage;
                                }
                                continue;
                            }
                            //  Non-leaf page which has processed all children and it's right child
                            (false, n) if n > contents.cell_count() as i32 => {
                                let destroy_info = self.state.mut_destroy_info().expect(
                                    "unable to get a mut reference to destroy state in cursor",
                                );
                                destroy_info.state = DestroyState::FreePage;
                                continue;
                            }
                            _ => unreachable!("Invalid cell idx state"),
                        }
                    }

                    //  We have not yet processed all cells in this page
                    //  Get the current cell
                    let cell = contents.cell_get(
                        cell_idx as usize,
                        Rc::clone(&self.pager),
                        payload_overflow_threshold_max(
                            contents.page_type(),
                            self.usable_space() as u16,
                        ),
                        payload_overflow_threshold_min(
                            contents.page_type(),
                            self.usable_space() as u16,
                        ),
                        self.usable_space(),
                    )?;

                    match contents.is_leaf() {
                        //  For a leaf cell, clear the overflow pages associated with this cell
                        true => {
                            let destroy_info = self
                                .state
                                .mut_destroy_info()
                                .expect("unable to get a mut reference to destroy state in cursor");
                            destroy_info.state = DestroyState::ClearOverflowPages { cell };
                            continue;
                        }
                        //  For interior cells, check the type of cell to determine what to do
                        false => match &cell {
                            //  For index interior cells, remove the overflow pages
                            BTreeCell::IndexInteriorCell(_) => {
                                let destroy_info = self.state.mut_destroy_info().expect(
                                    "unable to get a mut reference to destroy state in cursor",
                                );
                                destroy_info.state = DestroyState::ClearOverflowPages { cell };
                                continue;
                            }
                            //  For all other interior cells, load the left child page
                            _ => {
                                let child_page_id = match &cell {
                                    BTreeCell::TableInteriorCell(cell) => cell._left_child_page,
                                    BTreeCell::IndexInteriorCell(cell) => cell.left_child_page,
                                    _ => panic!("expected interior cell"),
                                };
                                let child_page = self.pager.read_page(child_page_id as usize)?;
                                self.stack.advance();
                                self.stack.push(child_page);
                                let destroy_info = self.state.mut_destroy_info().expect(
                                    "unable to get a mut reference to destroy state in cursor",
                                );
                                destroy_info.state = DestroyState::LoadPage;
                                continue;
                            }
                        },
                    }
                }
                DestroyState::ClearOverflowPages { cell } => {
                    match self.clear_overflow_pages(&cell)? {
                        CursorResult::Ok(_) => match cell {
                            //  For an index interior cell, clear the left child page now that overflow pages have been cleared
                            BTreeCell::IndexInteriorCell(index_int_cell) => {
                                let child_page = self
                                    .pager
                                    .read_page(index_int_cell.left_child_page as usize)?;
                                self.stack.advance();
                                self.stack.push(child_page);
                                let destroy_info = self.state.mut_destroy_info().expect(
                                    "unable to get a mut reference to destroy state in cursor",
                                );
                                destroy_info.state = DestroyState::LoadPage;
                                continue;
                            }
                            //  For any leaf cell, advance the index now that overflow pages have been cleared
                            BTreeCell::TableLeafCell(_) | BTreeCell::IndexLeafCell(_) => {
                                self.stack.advance();
                                let destroy_info = self.state.mut_destroy_info().expect(
                                    "unable to get a mut reference to destroy state in cursor",
                                );
                                destroy_info.state = DestroyState::LoadPage;
                            }
                            _ => panic!("unexpected cell type"),
                        },
                        CursorResult::IO => return Ok(CursorResult::IO),
                    }
                }
                DestroyState::FreePage => {
                    let page = self.stack.top();
                    let page_id = page.get().id;

                    self.pager.free_page(Some(page), page_id)?;

                    if self.stack.has_parent() {
                        self.stack.pop();
                        let destroy_info = self
                            .state
                            .mut_destroy_info()
                            .expect("unable to get a mut reference to destroy state in cursor");
                        destroy_info.state = DestroyState::ProcessPage;
                    } else {
                        self.state = CursorState::None;
                        return Ok(CursorResult::Ok(()));
                    }
                }
            }
        }
    }

    pub fn table_id(&self) -> usize {
        self.root_page
    }

    pub fn overwrite_cell(
        &mut self,
        page_ref: PageRef,
        cell_idx: usize,
        record: &ImmutableRecord,
    ) -> Result<CursorResult<()>> {
        // build the new payload
        let page_type = page_ref.get().contents.as_ref().unwrap().page_type();
        let mut new_payload = Vec::new();
        fill_cell_payload(
            page_type,
            self.rowid.get(),
            &mut new_payload,
            record,
            self.usable_space() as u16,
            self.pager.clone(),
        );

        // figure out old cell offset & size
        let (old_offset, old_local_size) = {
            let page = page_ref.get().contents.as_ref().unwrap();
            page.cell_get_raw_region(
                cell_idx,
                payload_overflow_threshold_max(page_type, self.usable_space() as u16),
                payload_overflow_threshold_min(page_type, self.usable_space() as u16),
                self.usable_space(),
            )
        };

        // if it all fits in local space and old_local_size is enough, do an in-place overwrite
        if new_payload.len() <= old_local_size {
            self.overwrite_content(
                page_ref.clone(),
                old_offset,
                &new_payload,
                0,
                new_payload.len(),
            )?;
            // if there's leftover local space (old_local_size > new_payload.len()), zero it or free it
            let remaining = old_local_size - new_payload.len();
            if remaining > 0 {
                let buf = page_ref.get().contents.as_mut().unwrap().as_ptr();
                for i in 0..remaining {
                    buf[old_offset + new_payload.len() + i] = 0;
                }
            }
            Ok(CursorResult::Ok(()))
        } else {
            // doesn't fit, drop it and insert a new one
            drop_cell(
                page_ref.get_contents(),
                cell_idx,
                self.usable_space() as u16,
            )?;
            insert_into_cell(
                page_ref.get_contents(),
                &new_payload,
                cell_idx,
                self.usable_space() as u16,
            )?;
            Ok(CursorResult::Ok(()))
        }
    }

    pub fn overwrite_content(
        &mut self,
        page_ref: PageRef,
        dest_offset: usize,
        new_payload: &[u8],
        src_offset: usize,
        amount: usize,
    ) -> Result<CursorResult<()>> {
        return_if_locked!(page_ref);
        page_ref.set_dirty();
        self.pager.add_dirty(page_ref.get().id);
        let buf = page_ref.get().contents.as_mut().unwrap().as_ptr();

        // if new_payload doesn't have enough data, we fill with zeros
        let n_data = new_payload.len().saturating_sub(src_offset);
        if n_data == 0 {
            // everything is zeros
            for i in 0..amount {
                if buf[dest_offset + i] != 0 {
                    buf[dest_offset + i] = 0;
                }
            }
        } else {
            let copy_len = n_data.min(amount);
            // copy the overlapping portion
            buf[dest_offset..dest_offset + copy_len]
                .copy_from_slice(&new_payload[src_offset..src_offset + copy_len]);

            // if copy_len < amount => fill remainder with 0
            if copy_len < amount {
                for i in copy_len..amount {
                    buf[dest_offset + i] = 0;
                }
            }
        }
        Ok(CursorResult::Ok(()))
    }
}

impl PageStack {
    fn increment_current(&self) {
        self.current_page.set(self.current_page.get() + 1);
    }
    fn decrement_current(&self) {
        self.current_page.set(self.current_page.get() - 1);
    }
    /// Push a new page onto the stack.
    /// This effectively means traversing to a child page.
    fn push(&self, page: PageRef) {
        tracing::trace!(
            "pagestack::push(current={}, new_page_id={})",
            self.current_page.get(),
            page.get().id
        );
        self.increment_current();
        let current = self.current_page.get();
        assert!(
            current < BTCURSOR_MAX_DEPTH as i32,
            "corrupted database, stack is bigger than expected"
        );
        self.stack.borrow_mut()[current as usize] = Some(page);
        self.cell_indices.borrow_mut()[current as usize] = 0;
    }

    /// Pop a page off the stack.
    /// This effectively means traversing back up to a parent page.
    fn pop(&self) {
        let current = self.current_page.get();
        tracing::trace!("pagestack::pop(current={})", current);
        self.cell_indices.borrow_mut()[current as usize] = 0;
        self.stack.borrow_mut()[current as usize] = None;
        self.decrement_current();
    }

    /// Get the top page on the stack.
    /// This is the page that is currently being traversed.
    fn top(&self) -> PageRef {
        let page = self.stack.borrow()[self.current()]
            .as_ref()
            .unwrap()
            .clone();
        tracing::trace!(
            "pagestack::top(current={}, page_id={})",
            self.current(),
            page.get().id
        );
        page
    }

    /// Current page pointer being used
    fn current(&self) -> usize {
        self.current_page.get() as usize
    }

    /// Cell index of the current page
    fn current_cell_index(&self) -> i32 {
        let current = self.current();
        self.cell_indices.borrow()[current]
    }

    /// Check if the current cell index is less than 0.
    /// This means we have been iterating backwards and have reached the start of the page.
    fn current_cell_index_less_than_min(&self) -> bool {
        let cell_idx = self.current_cell_index();
        cell_idx < 0
    }

    /// Advance the current cell index of the current page to the next cell.
    /// We usually advance after going traversing a new page
    fn advance(&self) {
        let current = self.current();
        tracing::trace!("advance {}", self.cell_indices.borrow()[current]);
        self.cell_indices.borrow_mut()[current] += 1;
    }

    fn retreat(&self) {
        let current = self.current();
        tracing::trace!("retreat {}", self.cell_indices.borrow()[current]);
        self.cell_indices.borrow_mut()[current] -= 1;
    }

    fn set_cell_index(&self, idx: i32) {
        let current = self.current();
        self.cell_indices.borrow_mut()[current] = idx
    }

    fn has_parent(&self) -> bool {
        self.current_page.get() > 0
    }

    fn clear(&self) {
        self.current_page.set(-1);
    }
}

impl CellArray {
    pub fn cell_size(&self, cell_idx: usize) -> u16 {
        self.cells[cell_idx].len() as u16
    }

    pub fn cell_count(&self, page_idx: usize) -> usize {
        self.number_of_cells_per_page[page_idx] as usize
    }
}

/// Try to find a free block available and allocate it if found
fn find_free_cell(page_ref: &PageContent, usable_space: u16, amount: usize) -> Result<usize> {
    // NOTE: freelist is in ascending order of keys and pc
    // unuse_space is reserved bytes at the end of page, therefore we must substract from maxpc
    let mut prev_pc = page_ref.offset + PAGE_HEADER_OFFSET_FIRST_FREEBLOCK;
    let mut pc = page_ref.first_freeblock() as usize;
    let maxpc = usable_space as usize - amount;

    while pc <= maxpc {
        if pc + 4 > usable_space as usize {
            return_corrupt!("Free block header extends beyond page");
        }

        let next = page_ref.read_u16_no_offset(pc);
        let size = page_ref.read_u16_no_offset(pc + 2);

        if amount <= size as usize {
            let new_size = size as usize - amount;
            if new_size < 4 {
                // The code is checking if using a free slot that would leave behind a very small fragment (x < 4 bytes)
                // would cause the total fragmentation to exceed the limit of 60 bytes
                // check sqlite docs https://www.sqlite.org/fileformat.html#:~:text=A%20freeblock%20requires,not%20exceed%2060
                if page_ref.num_frag_free_bytes() > 57 {
                    return Ok(0);
                }
                // Delete the slot from freelist and update the page's fragment count.
                page_ref.write_u16(prev_pc, next);
                let frag = page_ref.num_frag_free_bytes() + new_size as u8;
                page_ref.write_u8(PAGE_HEADER_OFFSET_FRAGMENTED_BYTES_COUNT, frag);
                return Ok(pc);
            } else if new_size + pc > maxpc {
                return_corrupt!("Free block extends beyond page end");
            } else {
                // Requested amount fits inside the current free slot so we reduce its size
                // to account for newly allocated space.
                page_ref.write_u16(pc + 2, new_size as u16);
                return Ok(pc + new_size);
            }
        }

        prev_pc = pc;
        pc = next as usize;
        if pc <= prev_pc {
            if pc != 0 {
                return_corrupt!("Free list not in ascending order");
            }
            return Ok(0);
        }
    }
    if pc > maxpc + amount - 4 {
        return_corrupt!("Free block chain extends beyond page end");
    }
    Ok(0)
}

pub fn btree_init_page(page: &PageRef, page_type: PageType, offset: usize, usable_space: u16) {
    // setup btree page
    let contents = page.get();
    debug!("btree_init_page(id={}, offset={})", contents.id, offset);
    let contents = contents.contents.as_mut().unwrap();
    contents.offset = offset;
    let id = page_type as u8;
    contents.write_u8(PAGE_HEADER_OFFSET_PAGE_TYPE, id);
    contents.write_u16(PAGE_HEADER_OFFSET_FIRST_FREEBLOCK, 0);
    contents.write_u16(PAGE_HEADER_OFFSET_CELL_COUNT, 0);

    contents.write_u16(PAGE_HEADER_OFFSET_CELL_CONTENT_AREA, usable_space);

    contents.write_u8(PAGE_HEADER_OFFSET_FRAGMENTED_BYTES_COUNT, 0);
    contents.write_u32(PAGE_HEADER_OFFSET_RIGHTMOST_PTR, 0);
}

fn to_static_buf(buf: &mut [u8]) -> &'static mut [u8] {
    unsafe { std::mem::transmute::<&mut [u8], &'static mut [u8]>(buf) }
}

fn edit_page(
    page: &mut PageContent,
    start_old_cells: usize,
    start_new_cells: usize,
    number_new_cells: usize,
    cell_array: &CellArray,
    usable_space: u16,
) -> Result<()> {
    tracing::trace!(
        "edit_page start_old_cells={} start_new_cells={} number_new_cells={} cell_array={}",
        start_old_cells,
        start_new_cells,
        number_new_cells,
        cell_array.cells.len()
    );
    let end_old_cells = start_old_cells + page.cell_count() + page.overflow_cells.len();
    let end_new_cells = start_new_cells + number_new_cells;
    let mut count_cells = page.cell_count();
    if start_old_cells < start_new_cells {
        let number_to_shift = page_free_array(
            page,
            start_old_cells,
            start_new_cells - start_old_cells,
            cell_array,
            usable_space,
        )?;
        // shift pointers left
        let buf = page.as_ptr();
        let (start, _) = page.cell_pointer_array_offset_and_size();
        buf.copy_within(
            start + (number_to_shift * 2)..start + (count_cells * 2),
            start,
        );
        count_cells -= number_to_shift;
    }
    if end_new_cells < end_old_cells {
        let number_tail_removed = page_free_array(
            page,
            end_new_cells,
            end_old_cells - end_new_cells,
            cell_array,
            usable_space,
        )?;
        assert!(count_cells >= number_tail_removed);
        count_cells -= number_tail_removed;
    }
    // TODO: make page_free_array defragment, for now I'm lazy so this will work for now.
    defragment_page(page, usable_space);
    // TODO: add to start
    if start_new_cells < start_old_cells {
        let count = number_new_cells.min(start_old_cells - start_new_cells);
        page_insert_array(page, start_new_cells, count, cell_array, 0, usable_space)?;
        count_cells += count;
    }
    // TODO: overflow cells
    for i in 0..page.overflow_cells.len() {
        let overflow_cell = &page.overflow_cells[i];
        // cell index in context of new list of cells that should be in the page
        if start_old_cells + overflow_cell.index >= start_new_cells {
            let cell_idx = start_old_cells + overflow_cell.index - start_new_cells;
            if cell_idx < number_new_cells {
                count_cells += 1;
                page_insert_array(
                    page,
                    start_new_cells + cell_idx,
                    1,
                    cell_array,
                    cell_idx,
                    usable_space,
                )?;
            }
        }
    }
    // TODO: append cells to end
    page_insert_array(
        page,
        start_new_cells + count_cells,
        number_new_cells - count_cells,
        cell_array,
        count_cells,
        usable_space,
    )?;
    // TODO: noverflow
    page.write_u16(PAGE_HEADER_OFFSET_CELL_COUNT, number_new_cells as u16);
    Ok(())
}

fn page_free_array(
    page: &mut PageContent,
    first: usize,
    count: usize,
    cell_array: &CellArray,
    usable_space: u16,
) -> Result<usize> {
    let buf = &mut page.as_ptr()[page.offset..usable_space as usize];
    let buf_range = buf.as_ptr_range();
    let mut number_of_cells_removed = 0;
    // TODO: implement fancy smart free block coalescing procedure instead of dumb free to
    // then defragment
    for i in first..first + count {
        let cell = &cell_array.cells[i];
        let cell_pointer = cell.as_ptr_range();
        // check if not overflow cell
        if cell_pointer.start >= buf_range.start && cell_pointer.start < buf_range.end {
            assert!(
                cell_pointer.end >= buf_range.start && cell_pointer.end <= buf_range.end,
                "whole cell should be inside the page"
            );
            // TODO: remove pointer too
            let offset = (cell_pointer.start as usize - buf_range.start as usize) as u16;
            let len = (cell_pointer.end as usize - cell_pointer.start as usize) as u16;
            free_cell_range(page, offset, len, usable_space)?;
            page.write_u16(PAGE_HEADER_OFFSET_CELL_COUNT, page.cell_count() as u16 - 1);
            number_of_cells_removed += 1;
        }
    }
    Ok(number_of_cells_removed)
}
fn page_insert_array(
    page: &mut PageContent,
    first: usize,
    count: usize,
    cell_array: &CellArray,
    mut start_insert: usize,
    usable_space: u16,
) -> Result<()> {
    // TODO: implement faster algorithm, this is doing extra work that's not needed.
    // See pageInsertArray to understand faster way.
    for i in first..first + count {
        insert_into_cell(page, cell_array.cells[i], start_insert, usable_space)?;
        start_insert += 1;
    }
    Ok(())
}

/// Free the range of bytes that a cell occupies.
/// This function also updates the freeblock list in the page.
/// Freeblocks are used to keep track of free space in the page,
/// and are organized as a linked list.
fn free_cell_range(
    page: &mut PageContent,
    mut offset: u16,
    len: u16,
    usable_space: u16,
) -> Result<()> {
    if len < 4 {
        return_corrupt!("Minimum cell size is 4");
    }

    if offset > usable_space.saturating_sub(4) {
        return_corrupt!("Start offset beyond usable space");
    }

    let mut size = len;
    let mut end = offset + len;
    let mut pointer_to_pc = page.offset as u16 + 1;
    // if the freeblock list is empty, we set this block as the first freeblock in the page header.
    let pc = if page.first_freeblock() == 0 {
        0
    } else {
        // if the freeblock list is not empty, and the offset is greater than the first freeblock,
        // then we need to do some more calculation to figure out where to insert the freeblock
        // in the freeblock linked list.
        let first_block = page.first_freeblock();

        let mut pc = first_block;

        while pc < offset {
            if pc <= pointer_to_pc {
                if pc == 0 {
                    break;
                }
                return_corrupt!("free cell range free block not in ascending order");
            }

            let next = page.read_u16_no_offset(pc as usize);
            pointer_to_pc = pc;
            pc = next;
        }

        if pc > usable_space - 4 {
            return_corrupt!("Free block beyond usable space");
        }
        let mut removed_fragmentation = 0;
        if pc > 0 && offset + len + 3 >= pc {
            removed_fragmentation = (pc - end) as u8;

            if end > pc {
                return_corrupt!("Invalid block overlap");
            }
            end = pc + page.read_u16_no_offset(pc as usize + 2);
            if end > usable_space {
                return_corrupt!("Coalesced block extends beyond page");
            }
            size = end - offset;
            pc = page.read_u16_no_offset(pc as usize);
        }

        if pointer_to_pc > page.offset as u16 + 1 {
            let prev_end = pointer_to_pc + page.read_u16_no_offset(pointer_to_pc as usize + 2);
            if prev_end + 3 >= offset {
                if prev_end > offset {
                    return_corrupt!("Invalid previous block overlap");
                }
                removed_fragmentation += (offset - prev_end) as u8;
                size = end - pointer_to_pc;
                offset = pointer_to_pc;
            }
        }
        if removed_fragmentation > page.num_frag_free_bytes() {
            return_corrupt!("Invalid fragmentation count");
        }
        let frag = page.num_frag_free_bytes() - removed_fragmentation;
        page.write_u8(PAGE_HEADER_OFFSET_FRAGMENTED_BYTES_COUNT, frag);
        pc
    };

    if offset <= page.cell_content_area() {
        if offset < page.cell_content_area() {
            return_corrupt!("Free block before content area");
        }
        if pointer_to_pc != page.offset as u16 + PAGE_HEADER_OFFSET_FIRST_FREEBLOCK as u16 {
            return_corrupt!("Invalid content area merge");
        }
        page.write_u16(PAGE_HEADER_OFFSET_FIRST_FREEBLOCK, pc);
        page.write_u16(PAGE_HEADER_OFFSET_CELL_CONTENT_AREA, end);
    } else {
        page.write_u16_no_offset(pointer_to_pc as usize, offset);
        page.write_u16_no_offset(offset as usize, pc);
        page.write_u16_no_offset(offset as usize + 2, size);
    }

    Ok(())
}

/// Defragment a page. This means packing all the cells to the end of the page.
fn defragment_page(page: &PageContent, usable_space: u16) {
    tracing::debug!("defragment_page");
    let cloned_page = page.clone();
    // TODO(pere): usable space should include offset probably
    let mut cbrk = usable_space;

    // TODO: implement fast algorithm

    let last_cell = usable_space - 4;
    let first_cell = cloned_page.unallocated_region_start() as u16;

    if cloned_page.cell_count() > 0 {
        let read_buf = cloned_page.as_ptr();
        let write_buf = page.as_ptr();

        for i in 0..cloned_page.cell_count() {
            let (cell_offset, _) = page.cell_pointer_array_offset_and_size();
            let cell_idx = cell_offset + (i * 2);

            let pc = cloned_page.read_u16_no_offset(cell_idx);
            if pc > last_cell {
                unimplemented!("corrupted page");
            }

            assert!(pc <= last_cell);

            let (_, size) = cloned_page.cell_get_raw_region(
                i,
                payload_overflow_threshold_max(page.page_type(), usable_space),
                payload_overflow_threshold_min(page.page_type(), usable_space),
                usable_space as usize,
            );
            let size = size as u16;
            cbrk -= size;
            if cbrk < first_cell || pc + size > usable_space {
                todo!("corrupt");
            }
            assert!(cbrk + size <= usable_space && cbrk >= first_cell);
            // set new pointer
            page.write_u16_no_offset(cell_idx, cbrk);
            // copy payload
            write_buf[cbrk as usize..cbrk as usize + size as usize]
                .copy_from_slice(&read_buf[pc as usize..pc as usize + size as usize]);
        }
    }

    // assert!( nfree >= 0 );
    // if( data[hdr+7]+cbrk-iCellFirst!=pPage->nFree ){
    //   return SQLITE_CORRUPT_PAGE(pPage);
    // }
    assert!(cbrk >= first_cell);

    // set new first byte of cell content
    page.write_u16(PAGE_HEADER_OFFSET_CELL_CONTENT_AREA, cbrk);
    // set free block to 0, unused spaced can be retrieved from gap between cell pointer end and content start
    page.write_u16(PAGE_HEADER_OFFSET_FIRST_FREEBLOCK, 0);
    page.write_u8(PAGE_HEADER_OFFSET_FRAGMENTED_BYTES_COUNT, 0);
}

/// Insert a record into a cell.
/// If the cell overflows, an overflow cell is created.
/// insert_into_cell() is called from insert_into_page(),
/// and the overflow cell count is used to determine if the page overflows,
/// i.e. whether we need to balance the btree after the insert.
fn insert_into_cell(
    page: &mut PageContent,
    payload: &[u8],
    cell_idx: usize,
    usable_space: u16,
) -> Result<()> {
    assert!(
        cell_idx <= page.cell_count(),
        "attempting to add cell to an incorrect place cell_idx={} cell_count={}",
        cell_idx,
        page.cell_count()
    );
    let free = compute_free_space(page, usable_space);
    const CELL_POINTER_SIZE_BYTES: usize = 2;
    let enough_space = payload.len() + CELL_POINTER_SIZE_BYTES <= free as usize;
    if !enough_space {
        // add to overflow cell
        page.overflow_cells.push(OverflowCell {
            index: cell_idx,
            payload: Pin::new(Vec::from(payload)),
        });
        return Ok(());
    }

    // TODO: insert into cell payload in internal page
    let new_cell_data_pointer = allocate_cell_space(page, payload.len() as u16, usable_space)?;
    let buf = page.as_ptr();

    // copy data
    buf[new_cell_data_pointer as usize..new_cell_data_pointer as usize + payload.len()]
        .copy_from_slice(payload);
    //  memmove(pIns+2, pIns, 2*(pPage->nCell - i));
    let (cell_pointer_array_start, _) = page.cell_pointer_array_offset_and_size();
    let cell_pointer_cur_idx = cell_pointer_array_start + (CELL_POINTER_SIZE_BYTES * cell_idx);

    // move existing pointers forward by CELL_POINTER_SIZE_BYTES...
    let n_cells_forward = page.cell_count() - cell_idx;
    let n_bytes_forward = CELL_POINTER_SIZE_BYTES * n_cells_forward;
    if n_bytes_forward > 0 {
        buf.copy_within(
            cell_pointer_cur_idx..cell_pointer_cur_idx + n_bytes_forward,
            cell_pointer_cur_idx + CELL_POINTER_SIZE_BYTES,
        );
    }
    // ...and insert new cell pointer at the current index
    page.write_u16_no_offset(cell_pointer_cur_idx, new_cell_data_pointer);

    // update cell count
    let new_n_cells = (page.cell_count() + 1) as u16;
    page.write_u16(PAGE_HEADER_OFFSET_CELL_COUNT, new_n_cells);
    Ok(())
}

/// Free blocks can be zero, meaning the "real free space" that can be used to allocate is expected to be between first cell byte
/// and end of cell pointer area.
#[allow(unused_assignments)]
fn compute_free_space(page: &PageContent, usable_space: u16) -> u16 {
    // TODO(pere): maybe free space is not calculated correctly with offset

    // Usable space, not the same as free space, simply means:
    // space that is not reserved for extensions by sqlite. Usually reserved_space is 0.
    let usable_space = usable_space as usize;

    let mut cell_content_area_start = page.cell_content_area();
    // A zero value for the cell content area pointer is interpreted as 65536.
    // See https://www.sqlite.org/fileformat.html
    // The max page size for a sqlite database is 64kiB i.e. 65536 bytes.
    // 65536 is u16::MAX + 1, and since cell content grows from right to left, this means
    // the cell content area pointer is at the end of the page,
    // i.e.
    // 1. the page size is 64kiB
    // 2. there are no cells on the page
    // 3. there is no reserved space at the end of the page
    if cell_content_area_start == 0 {
        cell_content_area_start = u16::MAX;
    }

    // The amount of free space is the sum of:
    // #1. the size of the unallocated region
    // #2. fragments (isolated 1-3 byte chunks of free space within the cell content area)
    // #3. freeblocks (linked list of blocks of at least 4 bytes within the cell content area that are not in use due to e.g. deletions)

    let pointer_size = if matches!(page.page_type(), PageType::TableLeaf | PageType::IndexLeaf) {
        0
    } else {
        4
    };
    let first_cell = page.offset + 8 + pointer_size + (2 * page.cell_count());
    let mut free_space_bytes =
        cell_content_area_start as usize + page.num_frag_free_bytes() as usize;

    // #3 is computed by iterating over the freeblocks linked list
    let mut cur_freeblock_ptr = page.first_freeblock() as usize;
    if cur_freeblock_ptr > 0 {
        if cur_freeblock_ptr < cell_content_area_start as usize {
            // Freeblocks exist in the cell content area e.g. after deletions
            // They should never exist in the unused area of the page.
            todo!("corrupted page");
        }

        let mut next = 0;
        let mut size = 0;
        loop {
            // TODO: check corruption icellast
            next = page.read_u16_no_offset(cur_freeblock_ptr) as usize; // first 2 bytes in freeblock = next freeblock pointer
            size = page.read_u16_no_offset(cur_freeblock_ptr + 2) as usize; // next 2 bytes in freeblock = size of current freeblock
            free_space_bytes += size;
            // Freeblocks are in order from left to right on the page,
            // so next pointer should > current pointer + its size, or 0 if no next block exists.
            if next <= cur_freeblock_ptr + size + 3 {
                break;
            }
            cur_freeblock_ptr = next;
        }

        // Next should always be 0 (NULL) at this point since we have reached the end of the freeblocks linked list
        assert!(
            next == 0,
            "corrupted page: freeblocks list not in ascending order"
        );

        assert!(
            cur_freeblock_ptr + size <= usable_space,
            "corrupted page: last freeblock extends last page end"
        );
    }

    assert!(
        free_space_bytes <= usable_space,
        "corrupted page: free space is greater than usable space"
    );

    // if( nFree>usableSize || nFree<iCellFirst ){
    //   return SQLITE_CORRUPT_PAGE(pPage);
    // }

    free_space_bytes as u16 - first_cell as u16
}

/// Allocate space for a cell on a page.
fn allocate_cell_space(page_ref: &PageContent, amount: u16, usable_space: u16) -> Result<u16> {
    let amount = amount as usize;

    let (cell_offset, _) = page_ref.cell_pointer_array_offset_and_size();
    let gap = cell_offset + 2 * page_ref.cell_count();
    let mut top = page_ref.cell_content_area() as usize;

    // there are free blocks and enough space
    if page_ref.first_freeblock() != 0 && gap + 2 <= top {
        // find slot
        let pc = find_free_cell(page_ref, usable_space, amount)?;
        if pc != 0 {
            return Ok(pc as u16);
        }
        /* fall through, we might need to defragment */
    }

    if gap + 2 + amount > top {
        // defragment
        defragment_page(page_ref, usable_space);
        top = page_ref.read_u16(PAGE_HEADER_OFFSET_CELL_CONTENT_AREA) as usize;
    }

    top -= amount;

    page_ref.write_u16(PAGE_HEADER_OFFSET_CELL_CONTENT_AREA, top as u16);

    assert!(top + amount <= usable_space as usize);
    Ok(top as u16)
}

/// Fill in the cell payload with the record.
/// If the record is too large to fit in the cell, it will spill onto overflow pages.
fn fill_cell_payload(
    page_type: PageType,
    int_key: Option<u64>,
    cell_payload: &mut Vec<u8>,
    record: &ImmutableRecord,
    usable_space: u16,
    pager: Rc<Pager>,
) {
    assert!(matches!(
        page_type,
        PageType::TableLeaf | PageType::IndexLeaf
    ));
    // TODO: make record raw from start, having to serialize is not good
    let record_buf = record.payload.to_vec();

    // fill in header
    if matches!(page_type, PageType::TableLeaf) {
        let int_key = int_key.unwrap();
        write_varint_to_vec(record_buf.len() as u64, cell_payload);
        write_varint_to_vec(int_key, cell_payload);
    } else {
        write_varint_to_vec(record_buf.len() as u64, cell_payload);
    }

    let payload_overflow_threshold_max = payload_overflow_threshold_max(page_type, usable_space);
    debug!(
        "fill_cell_payload(record_size={}, payload_overflow_threshold_max={})",
        record_buf.len(),
        payload_overflow_threshold_max
    );
    if record_buf.len() <= payload_overflow_threshold_max {
        // enough allowed space to fit inside a btree page
        cell_payload.extend_from_slice(record_buf.as_slice());
        return;
    }

    let payload_overflow_threshold_min = payload_overflow_threshold_min(page_type, usable_space);
    // see e.g. https://github.com/sqlite/sqlite/blob/9591d3fe93936533c8c3b0dc4d025ac999539e11/src/dbstat.c#L371
    let mut space_left = payload_overflow_threshold_min
        + (record_buf.len() - payload_overflow_threshold_min) % (usable_space as usize - 4);

    if space_left > payload_overflow_threshold_max {
        space_left = payload_overflow_threshold_min;
    }

    // cell_size must be equal to first value of space_left as this will be the bytes copied to non-overflow page.
    let cell_size = space_left + cell_payload.len() + 4; // 4 is the number of bytes of pointer to first overflow page
    let mut to_copy_buffer = record_buf.as_slice();

    let prev_size = cell_payload.len();
    cell_payload.resize(prev_size + space_left + 4, 0);
    let mut pointer = unsafe { cell_payload.as_mut_ptr().add(prev_size) };
    let mut pointer_to_next = unsafe { cell_payload.as_mut_ptr().add(prev_size + space_left) };
    let mut overflow_pages = Vec::new();

    loop {
        let to_copy = space_left.min(to_copy_buffer.len());
        unsafe { std::ptr::copy(to_copy_buffer.as_ptr(), pointer, to_copy) };

        let left = to_copy_buffer.len() - to_copy;
        if left == 0 {
            break;
        }

        // we still have bytes to add, we will need to allocate new overflow page
        let overflow_page = allocate_overflow_page(pager.clone());
        overflow_pages.push(overflow_page.clone());
        {
            let id = overflow_page.get().id as u32;
            let contents = overflow_page.get().contents.as_mut().unwrap();

            // TODO: take into account offset here?
            let buf = contents.as_ptr();
            let as_bytes = id.to_be_bytes();
            // update pointer to new overflow page
            unsafe { std::ptr::copy(as_bytes.as_ptr(), pointer_to_next, 4) };

            pointer = unsafe { buf.as_mut_ptr().add(4) };
            pointer_to_next = buf.as_mut_ptr();
            space_left = usable_space as usize - 4;
        }

        to_copy_buffer = &to_copy_buffer[to_copy..];
    }

    assert_eq!(cell_size, cell_payload.len());
}

/// Allocate a new overflow page.
/// This is done when a cell overflows and new space is needed.
fn allocate_overflow_page(pager: Rc<Pager>) -> PageRef {
    let page = pager.allocate_page().unwrap();

    // setup overflow page
    let contents = page.get().contents.as_mut().unwrap();
    let buf = contents.as_ptr();
    buf.fill(0);

    page
}

/// Returns the maximum payload size (X) that can be stored directly on a b-tree page without spilling to overflow pages.
///
/// For table leaf pages: X = usable_size - 35
/// For index pages: X = ((usable_size - 12) * 64/255) - 23
///
/// The usable size is the total page size less the reserved space at the end of each page.
/// These thresholds are designed to:
/// - Give a minimum fanout of 4 for index b-trees
/// - Ensure enough payload is on the b-tree page that the record header can usually be accessed
///   without consulting an overflow page
fn payload_overflow_threshold_max(page_type: PageType, usable_space: u16) -> usize {
    match page_type {
        PageType::IndexInterior | PageType::IndexLeaf => {
            ((usable_space as usize - 12) * 64 / 255) - 23 // Index page formula
        }
        PageType::TableInterior | PageType::TableLeaf => {
            usable_space as usize - 35 // Table leaf page formula
        }
    }
}

/// Returns the minimum payload size (M) that must be stored on the b-tree page before spilling to overflow pages is allowed.
///
/// For all page types: M = ((usable_size - 12) * 32/255) - 23
///
/// When payload size P exceeds max_local():
/// - If K = M + ((P-M) % (usable_size-4)) <= max_local(): store K bytes on page
/// - Otherwise: store M bytes on page
///
/// The remaining bytes are stored on overflow pages in both cases.
fn payload_overflow_threshold_min(_page_type: PageType, usable_space: u16) -> usize {
    // Same formula for all page types
    ((usable_space as usize - 12) * 32 / 255) - 23
}

/// Drop a cell from a page.
/// This is done by freeing the range of bytes that the cell occupies.
fn drop_cell(page: &mut PageContent, cell_idx: usize, usable_space: u16) -> Result<()> {
    let (cell_start, cell_len) = page.cell_get_raw_region(
        cell_idx,
        payload_overflow_threshold_max(page.page_type(), usable_space),
        payload_overflow_threshold_min(page.page_type(), usable_space),
        usable_space as usize,
    );
    free_cell_range(page, cell_start as u16, cell_len as u16, usable_space)?;
    if page.cell_count() > 1 {
        shift_pointers_left(page, cell_idx);
    } else {
        page.write_u16(PAGE_HEADER_OFFSET_CELL_CONTENT_AREA, usable_space);
        page.write_u16(PAGE_HEADER_OFFSET_FIRST_FREEBLOCK, 0);
        page.write_u8(PAGE_HEADER_OFFSET_FRAGMENTED_BYTES_COUNT, 0);
    }
    page.write_u16(PAGE_HEADER_OFFSET_CELL_COUNT, page.cell_count() as u16 - 1);
    Ok(())
}

/// Shift pointers to the left once starting from a cell position
/// This is useful when we remove a cell and we want to move left the cells from the right to fill
/// the empty space that's not needed
fn shift_pointers_left(page: &mut PageContent, cell_idx: usize) {
    assert!(page.cell_count() > 0);
    let buf = page.as_ptr();
    let (start, _) = page.cell_pointer_array_offset_and_size();
    let start = start + (cell_idx * 2) + 2;
    let right_cells = page.cell_count() - cell_idx - 1;
    let amount_to_shift = right_cells * 2;
    buf.copy_within(start..start + amount_to_shift, start - 2);
}

#[cfg(test)]
mod tests {
    use rand_chacha::rand_core::RngCore;
    use rand_chacha::rand_core::SeedableRng;
    use rand_chacha::ChaCha8Rng;
    use test_log::test;

    use super::*;
    use crate::fast_lock::SpinLock;
    use crate::io::{Buffer, Completion, MemoryIO, OpenFlags, IO};
    use crate::storage::database::DatabaseFile;
    use crate::storage::page_cache::DumbLruPageCache;
    use crate::storage::sqlite3_ondisk;
    use crate::storage::sqlite3_ondisk::DatabaseHeader;
    use crate::types::Text;
    use crate::vdbe::Register;
    use crate::Connection;
    use crate::{BufferPool, DatabaseStorage, WalFile, WalFileShared, WriteCompletion};
    use std::cell::RefCell;
    use std::ops::Deref;
    use std::panic;
    use std::rc::Rc;
    use std::sync::Arc;

    use tempfile::TempDir;

    use crate::{
        io::BufferData,
        storage::{
            btree::{
                compute_free_space, fill_cell_payload, payload_overflow_threshold_max,
                payload_overflow_threshold_min,
            },
            pager::PageRef,
            sqlite3_ondisk::{BTreeCell, PageContent, PageType},
        },
        types::OwnedValue,
        Database, Page, Pager, PlatformIO,
    };

    use super::{btree_init_page, defragment_page, drop_cell, insert_into_cell};

    #[allow(clippy::arc_with_non_send_sync)]
    fn get_page(id: usize) -> PageRef {
        let page = Arc::new(Page::new(id));

        let drop_fn = Rc::new(|_| {});
        let inner = PageContent {
            offset: 0,
            buffer: Arc::new(RefCell::new(Buffer::new(
                BufferData::new(vec![0; 4096]),
                drop_fn,
            ))),
            overflow_cells: Vec::new(),
        };
        page.get().contents.replace(inner);

        btree_init_page(&page, PageType::TableLeaf, 0, 4096);
        page
    }

    #[allow(clippy::arc_with_non_send_sync)]
    fn get_database() -> Arc<Database> {
        let mut path = TempDir::new().unwrap().into_path();
        path.push("test.db");
        {
            let connection = rusqlite::Connection::open(&path).unwrap();
            connection
                .pragma_update(None, "journal_mode", "wal")
                .unwrap();
        }
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
        let db = Database::open_file(io.clone(), path.to_str().unwrap(), false).unwrap();

        db
    }

    fn ensure_cell(page: &mut PageContent, cell_idx: usize, payload: &Vec<u8>) {
        let cell = page.cell_get_raw_region(
            cell_idx,
            payload_overflow_threshold_max(page.page_type(), 4096),
            payload_overflow_threshold_min(page.page_type(), 4096),
            4096,
        );
        tracing::trace!("cell idx={} start={} len={}", cell_idx, cell.0, cell.1);
        let buf = &page.as_ptr()[cell.0..cell.0 + cell.1];
        assert_eq!(buf.len(), payload.len());
        assert_eq!(buf, payload);
    }

    fn add_record(
        id: usize,
        pos: usize,
        page: &mut PageContent,
        record: ImmutableRecord,
        conn: &Rc<Connection>,
    ) -> Vec<u8> {
        let mut payload: Vec<u8> = Vec::new();
        fill_cell_payload(
            page.page_type(),
            Some(id as u64),
            &mut payload,
            &record,
            4096,
            conn.pager.clone(),
        );
        insert_into_cell(page, &payload, pos, 4096).unwrap();
        payload
    }

    #[test]
    fn test_insert_cell() {
        let db = get_database();
        let conn = db.connect().unwrap();
        let page = get_page(2);
        let page = page.get_contents();
        let header_size = 8;
        let record =
            ImmutableRecord::from_registers(&[Register::OwnedValue(OwnedValue::Integer(1))]);
        let payload = add_record(1, 0, page, record, &conn);
        assert_eq!(page.cell_count(), 1);
        let free = compute_free_space(page, 4096);
        assert_eq!(free, 4096 - payload.len() as u16 - 2 - header_size);

        let cell_idx = 0;
        ensure_cell(page, cell_idx, &payload);
    }

    struct Cell {
        pos: usize,
        payload: Vec<u8>,
    }

    #[test]
    fn test_drop_1() {
        let db = get_database();
        let conn = db.connect().unwrap();

        let page = get_page(2);
        let page = page.get_contents();
        let header_size = 8;

        let mut total_size = 0;
        let mut cells = Vec::new();
        let usable_space = 4096;
        for i in 0..3 {
            let record = ImmutableRecord::from_registers(&[Register::OwnedValue(
                OwnedValue::Integer(i as i64),
            )]);
            let payload = add_record(i, i, page, record, &conn);
            assert_eq!(page.cell_count(), i + 1);
            let free = compute_free_space(page, usable_space);
            total_size += payload.len() as u16 + 2;
            assert_eq!(free, 4096 - total_size - header_size);
            cells.push(Cell { pos: i, payload });
        }

        for (i, cell) in cells.iter().enumerate() {
            ensure_cell(page, i, &cell.payload);
        }
        cells.remove(1);
        drop_cell(page, 1, usable_space).unwrap();

        for (i, cell) in cells.iter().enumerate() {
            ensure_cell(page, i, &cell.payload);
        }
    }

    fn validate_btree(pager: Rc<Pager>, page_idx: usize) -> (usize, bool) {
        let cursor = BTreeCursor::new(None, pager.clone(), page_idx);
        let page = pager.read_page(page_idx).unwrap();
        let page = page.get();
        let contents = page.contents.as_ref().unwrap();
        let page_type = contents.page_type();
        let mut previous_key = None;
        let mut valid = true;
        let mut depth = None;
        for cell_idx in 0..contents.cell_count() {
            let cell = contents
                .cell_get(
                    cell_idx,
                    pager.clone(),
                    payload_overflow_threshold_max(page_type, 4096),
                    payload_overflow_threshold_min(page_type, 4096),
                    cursor.usable_space(),
                )
                .unwrap();
            let current_depth = match cell {
                BTreeCell::TableLeafCell(..) => 1,
                BTreeCell::TableInteriorCell(TableInteriorCell {
                    _left_child_page, ..
                }) => {
                    let (child_depth, child_valid) =
                        validate_btree(pager.clone(), _left_child_page as usize);
                    valid &= child_valid;
                    child_depth
                }
                _ => panic!("unsupported btree cell: {:?}", cell),
            };
            depth = Some(depth.unwrap_or(current_depth + 1));
            if depth != Some(current_depth + 1) {
                tracing::error!("depth is different for child of page {}", page_idx);
                valid = false;
            }
            match cell {
                BTreeCell::TableInteriorCell(TableInteriorCell { _rowid, .. })
                | BTreeCell::TableLeafCell(TableLeafCell { _rowid, .. }) => {
                    if previous_key.is_some() && previous_key.unwrap() >= _rowid {
                        tracing::error!(
                            "keys are in bad order: prev={:?}, current={}",
                            previous_key,
                            _rowid
                        );
                        valid = false;
                    }
                    previous_key = Some(_rowid);
                }
                _ => panic!("unsupported btree cell: {:?}", cell),
            }
        }
        if let Some(right) = contents.rightmost_pointer() {
            let (right_depth, right_valid) = validate_btree(pager.clone(), right as usize);
            valid &= right_valid;
            depth = Some(depth.unwrap_or(right_depth + 1));
            if depth != Some(right_depth + 1) {
                tracing::error!("depth is different for child of page {}", page_idx);
                valid = false;
            }
        }
        (depth.unwrap(), valid)
    }

    fn format_btree(pager: Rc<Pager>, page_idx: usize, depth: usize) -> String {
        let cursor = BTreeCursor::new(None, pager.clone(), page_idx);
        let page = pager.read_page(page_idx).unwrap();
        let page = page.get();
        let contents = page.contents.as_ref().unwrap();
        let page_type = contents.page_type();
        let mut current = Vec::new();
        let mut child = Vec::new();
        for cell_idx in 0..contents.cell_count() {
            let cell = contents
                .cell_get(
                    cell_idx,
                    pager.clone(),
                    payload_overflow_threshold_max(page_type, 4096),
                    payload_overflow_threshold_min(page_type, 4096),
                    cursor.usable_space(),
                )
                .unwrap();
            match cell {
                BTreeCell::TableInteriorCell(cell) => {
                    current.push(format!(
                        "node[rowid:{}, ptr(<=):{}]",
                        cell._rowid, cell._left_child_page
                    ));
                    child.push(format_btree(
                        pager.clone(),
                        cell._left_child_page as usize,
                        depth + 2,
                    ));
                }
                BTreeCell::TableLeafCell(cell) => {
                    current.push(format!(
                        "leaf[rowid:{}, len(payload):{}, overflow:{}]",
                        cell._rowid,
                        cell._payload.len(),
                        cell.first_overflow_page.is_some()
                    ));
                }
                _ => panic!("unsupported btree cell: {:?}", cell),
            }
        }
        if let Some(rightmost) = contents.rightmost_pointer() {
            child.push(format_btree(pager.clone(), rightmost as usize, depth + 2));
        }
        let current = format!(
            "{}-page:{}, ptr(right):{}\n{}+cells:{}",
            " ".repeat(depth),
            page_idx,
            contents.rightmost_pointer().unwrap_or(0),
            " ".repeat(depth),
            current.join(", ")
        );
        if child.is_empty() {
            current
        } else {
            current + "\n" + &child.join("\n")
        }
    }

    fn empty_btree() -> (Rc<Pager>, usize) {
        let db_header = DatabaseHeader::default();
        let page_size = db_header.page_size as usize;

        #[allow(clippy::arc_with_non_send_sync)]
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let io_file = io.open_file("test.db", OpenFlags::Create, false).unwrap();
        let db_file = Arc::new(DatabaseFile::new(io_file));

        let buffer_pool = Rc::new(BufferPool::new(db_header.page_size as usize));
        let wal_shared = WalFileShared::open_shared(&io, "test.wal", db_header.page_size).unwrap();
        let wal_file = WalFile::new(io.clone(), page_size, wal_shared, buffer_pool.clone());
        let wal = Rc::new(RefCell::new(wal_file));

        let page_cache = Arc::new(parking_lot::RwLock::new(DumbLruPageCache::new(10)));
        let pager = {
            let db_header = Arc::new(SpinLock::new(db_header.clone()));
            Pager::finish_open(db_header, db_file, wal, io, page_cache, buffer_pool).unwrap()
        };
        let pager = Rc::new(pager);
        let page1 = pager.allocate_page().unwrap();
        btree_init_page(&page1, PageType::TableLeaf, 0, 4096);
        (pager, page1.get().id)
    }

    #[test]
    #[ignore]
    pub fn btree_insert_fuzz_ex() {
        for sequence in [
            &[
                (777548915, 3364),
                (639157228, 3796),
                (709175417, 1214),
                (390824637, 210),
                (906124785, 1481),
                (197677875, 1305),
                (457946262, 3734),
                (956825466, 592),
                (835875722, 1334),
                (649214013, 1250),
                (531143011, 1788),
                (765057993, 2351),
                (510007766, 1349),
                (884516059, 822),
                (81604840, 2545),
            ]
            .as_slice(),
            &[
                (293471650, 2452),
                (163608869, 627),
                (544576229, 464),
                (705823748, 3441),
            ]
            .as_slice(),
            &[
                (987283511, 2924),
                (261851260, 1766),
                (343847101, 1657),
                (315844794, 572),
            ]
            .as_slice(),
            &[
                (987283511, 2924),
                (261851260, 1766),
                (343847101, 1657),
                (315844794, 572),
                (649272840, 1632),
                (723398505, 3140),
                (334416967, 3874),
            ]
            .as_slice(),
        ] {
            let (pager, root_page) = empty_btree();
            let mut cursor = BTreeCursor::new(None, pager.clone(), root_page);
            for (key, size) in sequence.iter() {
                run_until_done(
                    || {
                        let key = SeekKey::TableRowId(*key as u64);
                        cursor.move_to(key, SeekOp::EQ)
                    },
                    pager.deref(),
                )
                .unwrap();
                let key = OwnedValue::Integer(*key);
                let value = ImmutableRecord::from_registers(&[Register::OwnedValue(
                    OwnedValue::Blob(vec![0; *size]),
                )]);
                tracing::info!("insert key:{}", key);
                run_until_done(|| cursor.insert(&key, &value, true), pager.deref()).unwrap();
                tracing::info!(
                    "=========== btree ===========\n{}\n\n",
                    format_btree(pager.clone(), root_page, 0)
                );
            }
            for (key, _) in sequence.iter() {
                let seek_key = SeekKey::TableRowId(*key as u64);
                assert!(
                    matches!(
                        cursor.seek(seek_key, SeekOp::EQ).unwrap(),
                        CursorResult::Ok(true)
                    ),
                    "key {} is not found",
                    key
                );
            }
        }
    }

    fn rng_from_time() -> (ChaCha8Rng, u64) {
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let rng = ChaCha8Rng::seed_from_u64(seed);
        (rng, seed)
    }

    fn btree_insert_fuzz_run(
        attempts: usize,
        inserts: usize,
        size: impl Fn(&mut ChaCha8Rng) -> usize,
    ) {
        let (mut rng, seed) = rng_from_time();
        tracing::info!("super seed: {}", seed);
        for _ in 0..attempts {
            let (pager, root_page) = empty_btree();
            let mut cursor = BTreeCursor::new(None, pager.clone(), root_page);
            let mut keys = Vec::new();
            let seed = rng.next_u64();
            tracing::info!("seed: {}", seed);
            let mut rng = ChaCha8Rng::seed_from_u64(seed);
            for insert_id in 0..inserts {
                let size = size(&mut rng);
                let key = (rng.next_u64() % (1 << 30)) as i64;
                keys.push(key);
                tracing::info!(
                    "INSERT INTO t VALUES ({}, randomblob({})); -- {}",
                    key,
                    size,
                    insert_id
                );
                run_until_done(
                    || {
                        let key = SeekKey::TableRowId(key as u64);
                        cursor.move_to(key, SeekOp::EQ)
                    },
                    pager.deref(),
                )
                .unwrap();

                let key = OwnedValue::Integer(key);
                let value = ImmutableRecord::from_registers(&[Register::OwnedValue(
                    OwnedValue::Blob(vec![0; size]),
                )]);
                run_until_done(|| cursor.insert(&key, &value, true), pager.deref()).unwrap();
            }
            tracing::info!(
                "=========== btree ===========\n{}\n\n",
                format_btree(pager.clone(), root_page, 0)
            );
            if matches!(validate_btree(pager.clone(), root_page), (_, false)) {
                panic!("invalid btree");
            }
            for key in keys.iter() {
                let seek_key = SeekKey::TableRowId(*key as u64);
                assert!(
                    matches!(
                        cursor.seek(seek_key, SeekOp::EQ).unwrap(),
                        CursorResult::Ok(true)
                    ),
                    "key {} is not found",
                    key
                );
            }
        }
    }
    #[test]
    pub fn test_drop_odd() {
        let db = get_database();
        let conn = db.connect().unwrap();

        let page = get_page(2);
        let page = page.get_contents();
        let header_size = 8;

        let mut total_size = 0;
        let mut cells = Vec::new();
        let usable_space = 4096;
        let total_cells = 10;
        for i in 0..total_cells {
            let record = ImmutableRecord::from_registers(&[Register::OwnedValue(
                OwnedValue::Integer(i as i64),
            )]);
            let payload = add_record(i, i, page, record, &conn);
            assert_eq!(page.cell_count(), i + 1);
            let free = compute_free_space(page, usable_space);
            total_size += payload.len() as u16 + 2;
            assert_eq!(free, 4096 - total_size - header_size);
            cells.push(Cell { pos: i, payload });
        }

        let mut removed = 0;
        let mut new_cells = Vec::new();
        for cell in cells {
            if cell.pos % 2 == 1 {
                drop_cell(page, cell.pos - removed, usable_space).unwrap();
                removed += 1;
            } else {
                new_cells.push(cell);
            }
        }
        let cells = new_cells;
        for (i, cell) in cells.iter().enumerate() {
            ensure_cell(page, i, &cell.payload);
        }

        for (i, cell) in cells.iter().enumerate() {
            ensure_cell(page, i, &cell.payload);
        }
    }

    #[test]
    pub fn btree_insert_fuzz_run_equal_size() {
        for size in 1..8 {
            tracing::info!("======= size:{} =======", size);
            btree_insert_fuzz_run(2, 1024, |_| size);
        }
    }

    #[test]
    #[ignore]
    pub fn btree_insert_fuzz_run_random() {
        btree_insert_fuzz_run(128, 16, |rng| (rng.next_u32() % 4096) as usize);
    }

    #[test]
    #[ignore]
    pub fn btree_insert_fuzz_run_small() {
        btree_insert_fuzz_run(1, 1024, |rng| (rng.next_u32() % 128) as usize);
    }

    #[test]
    #[ignore]
    pub fn btree_insert_fuzz_run_big() {
        btree_insert_fuzz_run(64, 32, |rng| 3 * 1024 + (rng.next_u32() % 1024) as usize);
    }

    #[test]
    #[ignore]
    pub fn btree_insert_fuzz_run_overflow() {
        btree_insert_fuzz_run(64, 32, |rng| (rng.next_u32() % 32 * 1024) as usize);
    }

    #[allow(clippy::arc_with_non_send_sync)]
    fn setup_test_env(database_size: u32) -> (Rc<Pager>, Arc<SpinLock<DatabaseHeader>>) {
        let page_size = 512;
        let mut db_header = DatabaseHeader::default();
        db_header.page_size = page_size;
        db_header.database_size = database_size;
        let db_header = Arc::new(SpinLock::new(db_header));

        let buffer_pool = Rc::new(BufferPool::new(10));

        // Initialize buffer pool with correctly sized buffers
        for _ in 0..10 {
            let vec = vec![0; page_size as usize]; // Initialize with correct length, not just capacity
            buffer_pool.put(Pin::new(vec));
        }

        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db_file = Arc::new(DatabaseFile::new(
            io.open_file("test.db", OpenFlags::Create, false).unwrap(),
        ));

        let drop_fn = Rc::new(|_buf| {});
        let buf = Arc::new(RefCell::new(Buffer::allocate(page_size as usize, drop_fn)));
        {
            let mut buf_mut = buf.borrow_mut();
            let buf_slice = buf_mut.as_mut_slice();
            sqlite3_ondisk::write_header_to_buf(buf_slice, &db_header.lock());
        }

        let write_complete = Box::new(|_| {});
        let c = Completion::Write(WriteCompletion::new(write_complete));
        db_file.write_page(1, buf.clone(), c).unwrap();

        let wal_shared = WalFileShared::open_shared(&io, "test.wal", page_size).unwrap();
        let wal = Rc::new(RefCell::new(WalFile::new(
            io.clone(),
            page_size as usize,
            wal_shared,
            buffer_pool.clone(),
        )));

        let pager = Rc::new(
            Pager::finish_open(
                db_header.clone(),
                db_file,
                wal,
                io,
                Arc::new(parking_lot::RwLock::new(DumbLruPageCache::new(10))),
                buffer_pool,
            )
            .unwrap(),
        );

        pager.io.run_once().unwrap();

        (pager, db_header)
    }

    #[test]
    #[ignore]
    pub fn test_clear_overflow_pages() -> Result<()> {
        let (pager, db_header) = setup_test_env(5);
        let mut cursor = BTreeCursor::new(None, pager.clone(), 1);

        let max_local = payload_overflow_threshold_max(PageType::TableLeaf, 4096);
        let usable_size = cursor.usable_space();

        // Create a large payload that will definitely trigger overflow
        let large_payload = vec![b'A'; max_local + usable_size];

        // Setup overflow pages (2, 3, 4) with linking
        let mut current_page = 2u32;
        while current_page <= 4 {
            let drop_fn = Rc::new(|_buf| {});
            #[allow(clippy::arc_with_non_send_sync)]
            let buf = Arc::new(RefCell::new(Buffer::allocate(
                db_header.lock().page_size as usize,
                drop_fn,
            )));
            let write_complete = Box::new(|_| {});
            let c = Completion::Write(WriteCompletion::new(write_complete));
            pager
                .db_file
                .write_page(current_page as usize, buf.clone(), c)?;
            pager.io.run_once()?;

            let page = cursor.pager.read_page(current_page as usize)?;
            while page.is_locked() {
                cursor.pager.io.run_once()?;
            }

            {
                let contents = page.get().contents.as_mut().unwrap();

                let next_page = if current_page < 4 {
                    current_page + 1
                } else {
                    0
                };
                contents.write_u32(0, next_page); // Write pointer to next overflow page

                let buf = contents.as_ptr();
                buf[4..].fill(b'A');
            }

            current_page += 1;
        }
        pager.io.run_once()?;

        // Create leaf cell pointing to start of overflow chain
        let leaf_cell = BTreeCell::TableLeafCell(TableLeafCell {
            _rowid: 1,
            _payload: large_payload,
            first_overflow_page: Some(2), // Point to first overflow page
        });

        let initial_freelist_pages = db_header.lock().freelist_pages;
        // Clear overflow pages
        let clear_result = cursor.clear_overflow_pages(&leaf_cell)?;
        match clear_result {
            CursorResult::Ok(_) => {
                // Verify proper number of pages were added to freelist
                assert_eq!(
                    db_header.lock().freelist_pages,
                    initial_freelist_pages + 3,
                    "Expected 3 pages to be added to freelist"
                );

                // If this is first trunk page
                let trunk_page_id = db_header.lock().freelist_trunk_page;
                if trunk_page_id > 0 {
                    // Verify trunk page structure
                    let trunk_page = cursor.pager.read_page(trunk_page_id as usize)?;
                    if let Some(contents) = trunk_page.get().contents.as_ref() {
                        // Read number of leaf pages in trunk
                        let n_leaf = contents.read_u32(4);
                        assert!(n_leaf > 0, "Trunk page should have leaf entries");

                        for i in 0..n_leaf {
                            let leaf_page_id = contents.read_u32(8 + (i as usize * 4));
                            assert!(
                                (2..=4).contains(&leaf_page_id),
                                "Leaf page ID {} should be in range 2-4",
                                leaf_page_id
                            );
                        }
                    }
                }
            }
            CursorResult::IO => {
                cursor.pager.io.run_once()?;
            }
        }

        Ok(())
    }

    #[test]
    pub fn test_clear_overflow_pages_no_overflow() -> Result<()> {
        let (pager, db_header) = setup_test_env(5);
        let mut cursor = BTreeCursor::new(None, pager.clone(), 1);

        let small_payload = vec![b'A'; 10];

        // Create leaf cell with no overflow pages
        let leaf_cell = BTreeCell::TableLeafCell(TableLeafCell {
            _rowid: 1,
            _payload: small_payload,
            first_overflow_page: None,
        });

        let initial_freelist_pages = db_header.lock().freelist_pages;

        // Try to clear non-existent overflow pages
        let clear_result = cursor.clear_overflow_pages(&leaf_cell)?;
        match clear_result {
            CursorResult::Ok(_) => {
                // Verify freelist was not modified
                assert_eq!(
                    db_header.lock().freelist_pages,
                    initial_freelist_pages,
                    "Freelist should not change when no overflow pages exist"
                );

                // Verify trunk page wasn't created
                assert_eq!(
                    db_header.lock().freelist_trunk_page,
                    0,
                    "No trunk page should be created when no overflow pages exist"
                );
            }
            CursorResult::IO => {
                cursor.pager.io.run_once()?;
            }
        }

        Ok(())
    }

    #[test]
    fn test_btree_destroy() -> Result<()> {
        let initial_size = 3;
        let (pager, db_header) = setup_test_env(initial_size);
        let mut cursor = BTreeCursor::new(None, pager.clone(), 2);
        assert_eq!(
            db_header.lock().database_size,
            initial_size,
            "Database should initially have 3 pages"
        );

        // Initialize page 2 as a root page (interior)
        let root_page = cursor.pager.read_page(2)?;
        {
            btree_init_page(&root_page, PageType::TableInterior, 0, 512); // Use proper page size
        }

        // Allocate two leaf pages
        let page3 = cursor.pager.allocate_page()?;
        btree_init_page(&page3, PageType::TableLeaf, 0, 512);

        let page4 = cursor.pager.allocate_page()?;
        btree_init_page(&page4, PageType::TableLeaf, 0, 512);

        // Configure the root page to point to the two leaf pages
        {
            let contents = root_page.get().contents.as_mut().unwrap();

            // Set rightmost pointer to page4
            contents.write_u32(PAGE_HEADER_OFFSET_RIGHTMOST_PTR, page4.get().id as u32);

            // Create a cell with pointer to page3
            let cell_content = vec![
                // First 4 bytes: left child pointer (page3)
                (page3.get().id >> 24) as u8,
                (page3.get().id >> 16) as u8,
                (page3.get().id >> 8) as u8,
                page3.get().id as u8,
                // Next byte: rowid as varint (simple value 100)
                100,
            ];

            // Insert the cell
            insert_into_cell(contents, &cell_content, 0, 512)?;
        }

        // Add a simple record to each leaf page
        for page in [&page3, &page4] {
            let contents = page.get().contents.as_mut().unwrap();

            // Simple record with just a rowid and payload
            let record_bytes = vec![
                5,                   // Payload length (varint)
                page.get().id as u8, // Rowid (varint)
                b'h',
                b'e',
                b'l',
                b'l',
                b'o', // Payload
            ];

            insert_into_cell(contents, &record_bytes, 0, 512)?;
        }

        // Verify structure before destruction
        assert_eq!(
            db_header.lock().database_size,
            5, // We should have pages 0-4
            "Database should have 4 pages total"
        );

        // Track freelist state before destruction
        let initial_free_pages = db_header.lock().freelist_pages;
        assert_eq!(initial_free_pages, 0, "should start with no free pages");

        run_until_done(|| cursor.btree_destroy(), pager.deref())?;

        let pages_freed = db_header.lock().freelist_pages - initial_free_pages;
        assert_eq!(pages_freed, 3, "should free 3 pages (root + 2 leaves)");

        Ok(())
    }

    #[test]
    pub fn test_defragment() {
        let db = get_database();
        let conn = db.connect().unwrap();

        let page = get_page(2);
        let page = page.get_contents();
        let header_size = 8;

        let mut total_size = 0;
        let mut cells = Vec::new();
        let usable_space = 4096;
        for i in 0..3 {
            let record = ImmutableRecord::from_registers(&[Register::OwnedValue(
                OwnedValue::Integer(i as i64),
            )]);
            let payload = add_record(i, i, page, record, &conn);
            assert_eq!(page.cell_count(), i + 1);
            let free = compute_free_space(page, usable_space);
            total_size += payload.len() as u16 + 2;
            assert_eq!(free, 4096 - total_size - header_size);
            cells.push(Cell { pos: i, payload });
        }

        for (i, cell) in cells.iter().enumerate() {
            ensure_cell(page, i, &cell.payload);
        }
        cells.remove(1);
        drop_cell(page, 1, usable_space).unwrap();

        for (i, cell) in cells.iter().enumerate() {
            ensure_cell(page, i, &cell.payload);
        }

        defragment_page(page, usable_space);

        for (i, cell) in cells.iter().enumerate() {
            ensure_cell(page, i, &cell.payload);
        }
    }

    #[test]
    pub fn test_drop_odd_with_defragment() {
        let db = get_database();
        let conn = db.connect().unwrap();

        let page = get_page(2);
        let page = page.get_contents();
        let header_size = 8;

        let mut total_size = 0;
        let mut cells = Vec::new();
        let usable_space = 4096;
        let total_cells = 10;
        for i in 0..total_cells {
            let record = ImmutableRecord::from_registers(&[Register::OwnedValue(
                OwnedValue::Integer(i as i64),
            )]);
            let payload = add_record(i, i, page, record, &conn);
            assert_eq!(page.cell_count(), i + 1);
            let free = compute_free_space(page, usable_space);
            total_size += payload.len() as u16 + 2;
            assert_eq!(free, 4096 - total_size - header_size);
            cells.push(Cell { pos: i, payload });
        }

        let mut removed = 0;
        let mut new_cells = Vec::new();
        for cell in cells {
            if cell.pos % 2 == 1 {
                drop_cell(page, cell.pos - removed, usable_space).unwrap();
                removed += 1;
            } else {
                new_cells.push(cell);
            }
        }
        let cells = new_cells;
        for (i, cell) in cells.iter().enumerate() {
            ensure_cell(page, i, &cell.payload);
        }

        defragment_page(page, usable_space);

        for (i, cell) in cells.iter().enumerate() {
            ensure_cell(page, i, &cell.payload);
        }
    }

    #[test]
    pub fn test_fuzz_drop_defragment_insert() {
        let db = get_database();
        let conn = db.connect().unwrap();

        let page = get_page(2);
        let page = page.get_contents();
        let header_size = 8;

        let mut total_size = 0;
        let mut cells = Vec::new();
        let usable_space = 4096;
        let mut i = 1000;
        // let seed = thread_rng().gen();
        // let seed = 15292777653676891381;
        let seed = 9261043168681395159;
        tracing::info!("seed {}", seed);
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        while i > 0 {
            i -= 1;
            match rng.next_u64() % 3 {
                0 => {
                    // allow appends with extra place to insert
                    let cell_idx = rng.next_u64() as usize % (page.cell_count() + 1);
                    let free = compute_free_space(page, usable_space);
                    let record = ImmutableRecord::from_registers(&[Register::OwnedValue(
                        OwnedValue::Integer(i as i64),
                    )]);
                    let mut payload: Vec<u8> = Vec::new();
                    fill_cell_payload(
                        page.page_type(),
                        Some(i as u64),
                        &mut payload,
                        &record,
                        4096,
                        conn.pager.clone(),
                    );
                    if (free as usize) < payload.len() - 2 {
                        // do not try to insert overflow pages because they require balancing
                        continue;
                    }
                    insert_into_cell(page, &payload, cell_idx, 4096).unwrap();
                    assert!(page.overflow_cells.is_empty());
                    total_size += payload.len() as u16 + 2;
                    cells.push(Cell { pos: i, payload });
                }
                1 => {
                    if page.cell_count() == 0 {
                        continue;
                    }
                    let cell_idx = rng.next_u64() as usize % page.cell_count();
                    let (_, len) = page.cell_get_raw_region(
                        cell_idx,
                        payload_overflow_threshold_max(page.page_type(), 4096),
                        payload_overflow_threshold_min(page.page_type(), 4096),
                        usable_space as usize,
                    );
                    drop_cell(page, cell_idx, usable_space).unwrap();
                    total_size -= len as u16 + 2;
                    cells.remove(cell_idx);
                }
                2 => {
                    defragment_page(page, usable_space);
                }
                _ => unreachable!(),
            }
            let free = compute_free_space(page, usable_space);
            assert_eq!(free, 4096 - total_size - header_size);
        }
    }

    #[test]
    pub fn test_fuzz_drop_defragment_insert_issue_1085() {
        // This test is used to demonstrate that issue at https://github.com/tursodatabase/limbo/issues/1085
        // is FIXED.
        let db = get_database();
        let conn = db.connect().unwrap();

        let page = get_page(2);
        let page = page.get_contents();
        let header_size = 8;

        let mut total_size = 0;
        let mut cells = Vec::new();
        let usable_space = 4096;
        let mut i = 1000;
        for seed in [15292777653676891381, 9261043168681395159] {
            tracing::info!("seed {}", seed);
            let mut rng = ChaCha8Rng::seed_from_u64(seed);
            while i > 0 {
                i -= 1;
                match rng.next_u64() % 3 {
                    0 => {
                        // allow appends with extra place to insert
                        let cell_idx = rng.next_u64() as usize % (page.cell_count() + 1);
                        let free = compute_free_space(page, usable_space);
                        let record = ImmutableRecord::from_registers(&[Register::OwnedValue(
                            OwnedValue::Integer(i as i64),
                        )]);
                        let mut payload: Vec<u8> = Vec::new();
                        fill_cell_payload(
                            page.page_type(),
                            Some(i as u64),
                            &mut payload,
                            &record,
                            4096,
                            conn.pager.clone(),
                        );
                        if (free as usize) < payload.len() - 2 {
                            // do not try to insert overflow pages because they require balancing
                            continue;
                        }
                        insert_into_cell(page, &payload, cell_idx, 4096).unwrap();
                        assert!(page.overflow_cells.is_empty());
                        total_size += payload.len() as u16 + 2;
                        cells.push(Cell { pos: i, payload });
                    }
                    1 => {
                        if page.cell_count() == 0 {
                            continue;
                        }
                        let cell_idx = rng.next_u64() as usize % page.cell_count();
                        let (_, len) = page.cell_get_raw_region(
                            cell_idx,
                            payload_overflow_threshold_max(page.page_type(), 4096),
                            payload_overflow_threshold_min(page.page_type(), 4096),
                            usable_space as usize,
                        );
                        drop_cell(page, cell_idx, usable_space).unwrap();
                        total_size -= len as u16 + 2;
                        cells.remove(cell_idx);
                    }
                    2 => {
                        defragment_page(page, usable_space);
                    }
                    _ => unreachable!(),
                }
                let free = compute_free_space(page, usable_space);
                assert_eq!(free, 4096 - total_size - header_size);
            }
        }
    }

    #[test]
    pub fn test_free_space() {
        let db = get_database();
        let conn = db.connect().unwrap();
        let page = get_page(2);
        let page = page.get_contents();
        let header_size = 8;
        let usable_space = 4096;

        let record =
            ImmutableRecord::from_registers(&[Register::OwnedValue(OwnedValue::Integer(0))]);
        let payload = add_record(0, 0, page, record, &conn);
        let free = compute_free_space(page, usable_space);
        assert_eq!(free, 4096 - payload.len() as u16 - 2 - header_size);
    }

    #[test]
    pub fn test_defragment_1() {
        let db = get_database();
        let conn = db.connect().unwrap();

        let page = get_page(2);
        let page = page.get_contents();
        let usable_space = 4096;

        let record =
            ImmutableRecord::from_registers(&[Register::OwnedValue(OwnedValue::Integer(0))]);
        let payload = add_record(0, 0, page, record, &conn);

        assert_eq!(page.cell_count(), 1);
        defragment_page(page, usable_space);
        assert_eq!(page.cell_count(), 1);
        let (start, len) = page.cell_get_raw_region(
            0,
            payload_overflow_threshold_max(page.page_type(), 4096),
            payload_overflow_threshold_min(page.page_type(), 4096),
            usable_space as usize,
        );
        let buf = page.as_ptr();
        assert_eq!(&payload, &buf[start..start + len]);
    }

    #[test]
    pub fn test_insert_drop_insert() {
        let db = get_database();
        let conn = db.connect().unwrap();

        let page = get_page(2);
        let page = page.get_contents();
        let usable_space = 4096;

        let record = ImmutableRecord::from_registers(&[
            Register::OwnedValue(OwnedValue::Integer(0)),
            Register::OwnedValue(OwnedValue::Text(Text::new("aaaaaaaa"))),
        ]);
        let _ = add_record(0, 0, page, record, &conn);

        assert_eq!(page.cell_count(), 1);
        drop_cell(page, 0, usable_space).unwrap();
        assert_eq!(page.cell_count(), 0);

        let record =
            ImmutableRecord::from_registers(&[Register::OwnedValue(OwnedValue::Integer(0))]);
        let payload = add_record(0, 0, page, record, &conn);
        assert_eq!(page.cell_count(), 1);

        let (start, len) = page.cell_get_raw_region(
            0,
            payload_overflow_threshold_max(page.page_type(), 4096),
            payload_overflow_threshold_min(page.page_type(), 4096),
            usable_space as usize,
        );
        let buf = page.as_ptr();
        assert_eq!(&payload, &buf[start..start + len]);
    }

    #[test]
    pub fn test_insert_drop_insert_multiple() {
        let db = get_database();
        let conn = db.connect().unwrap();

        let page = get_page(2);
        let page = page.get_contents();
        let usable_space = 4096;

        let record = ImmutableRecord::from_registers(&[
            Register::OwnedValue(OwnedValue::Integer(0)),
            Register::OwnedValue(OwnedValue::Text(Text::new("aaaaaaaa"))),
        ]);
        let _ = add_record(0, 0, page, record, &conn);

        for _ in 0..100 {
            assert_eq!(page.cell_count(), 1);
            drop_cell(page, 0, usable_space).unwrap();
            assert_eq!(page.cell_count(), 0);

            let record =
                ImmutableRecord::from_registers(&[Register::OwnedValue(OwnedValue::Integer(0))]);
            let payload = add_record(0, 0, page, record, &conn);
            assert_eq!(page.cell_count(), 1);

            let (start, len) = page.cell_get_raw_region(
                0,
                payload_overflow_threshold_max(page.page_type(), 4096),
                payload_overflow_threshold_min(page.page_type(), 4096),
                usable_space as usize,
            );
            let buf = page.as_ptr();
            assert_eq!(&payload, &buf[start..start + len]);
        }
    }

    #[test]
    pub fn test_drop_a_few_insert() {
        let db = get_database();
        let conn = db.connect().unwrap();

        let page = get_page(2);
        let page = page.get_contents();
        let usable_space = 4096;

        let record =
            ImmutableRecord::from_registers(&[Register::OwnedValue(OwnedValue::Integer(0))]);
        let payload = add_record(0, 0, page, record, &conn);
        let record =
            ImmutableRecord::from_registers(&[Register::OwnedValue(OwnedValue::Integer(1))]);
        let _ = add_record(1, 1, page, record, &conn);
        let record =
            ImmutableRecord::from_registers(&[Register::OwnedValue(OwnedValue::Integer(2))]);
        let _ = add_record(2, 2, page, record, &conn);

        drop_cell(page, 1, usable_space).unwrap();
        drop_cell(page, 1, usable_space).unwrap();

        ensure_cell(page, 0, &payload);
    }

    #[test]
    pub fn test_fuzz_victim_1() {
        let db = get_database();
        let conn = db.connect().unwrap();

        let page = get_page(2);
        let page = page.get_contents();
        let usable_space = 4096;

        let record =
            ImmutableRecord::from_registers(&[Register::OwnedValue(OwnedValue::Integer(0))]);
        let _ = add_record(0, 0, page, record, &conn);

        let record =
            ImmutableRecord::from_registers(&[Register::OwnedValue(OwnedValue::Integer(0))]);
        let _ = add_record(0, 0, page, record, &conn);
        drop_cell(page, 0, usable_space).unwrap();

        defragment_page(page, usable_space);

        let record =
            ImmutableRecord::from_registers(&[Register::OwnedValue(OwnedValue::Integer(0))]);
        let _ = add_record(0, 1, page, record, &conn);

        drop_cell(page, 0, usable_space).unwrap();

        let record =
            ImmutableRecord::from_registers(&[Register::OwnedValue(OwnedValue::Integer(0))]);
        let _ = add_record(0, 1, page, record, &conn);
    }

    #[test]
    pub fn test_fuzz_victim_2() {
        let db = get_database();
        let conn = db.connect().unwrap();

        let page = get_page(2);
        let usable_space = 4096;
        let insert = |pos, page| {
            let record =
                ImmutableRecord::from_registers(&[Register::OwnedValue(OwnedValue::Integer(0))]);
            let _ = add_record(0, pos, page, record, &conn);
        };
        let drop = |pos, page| {
            drop_cell(page, pos, usable_space).unwrap();
        };
        let defragment = |page| {
            defragment_page(page, usable_space);
        };
        defragment(page.get_contents());
        defragment(page.get_contents());
        insert(0, page.get_contents());
        drop(0, page.get_contents());
        insert(0, page.get_contents());
        drop(0, page.get_contents());
        insert(0, page.get_contents());
        defragment(page.get_contents());
        defragment(page.get_contents());
        drop(0, page.get_contents());
        defragment(page.get_contents());
        insert(0, page.get_contents());
        drop(0, page.get_contents());
        insert(0, page.get_contents());
        insert(1, page.get_contents());
        insert(1, page.get_contents());
        insert(0, page.get_contents());
        drop(3, page.get_contents());
        drop(2, page.get_contents());
        compute_free_space(page.get_contents(), usable_space);
    }

    #[test]
    pub fn test_fuzz_victim_3() {
        let db = get_database();
        let conn = db.connect().unwrap();

        let page = get_page(2);
        let usable_space = 4096;
        let insert = |pos, page| {
            let record =
                ImmutableRecord::from_registers(&[Register::OwnedValue(OwnedValue::Integer(0))]);
            let _ = add_record(0, pos, page, record, &conn);
        };
        let drop = |pos, page| {
            drop_cell(page, pos, usable_space).unwrap();
        };
        let defragment = |page| {
            defragment_page(page, usable_space);
        };
        let record =
            ImmutableRecord::from_registers(&[Register::OwnedValue(OwnedValue::Integer(0))]);
        let mut payload: Vec<u8> = Vec::new();
        fill_cell_payload(
            page.get_contents().page_type(),
            Some(0),
            &mut payload,
            &record,
            4096,
            conn.pager.clone(),
        );
        insert(0, page.get_contents());
        defragment(page.get_contents());
        insert(0, page.get_contents());
        defragment(page.get_contents());
        insert(0, page.get_contents());
        drop(2, page.get_contents());
        drop(0, page.get_contents());
        let free = compute_free_space(page.get_contents(), usable_space);
        let total_size = payload.len() + 2;
        assert_eq!(
            free,
            usable_space - page.get_contents().header_size() as u16 - total_size as u16
        );
        dbg!(free);
    }

    #[test]
    pub fn btree_insert_sequential() {
        let (pager, root_page) = empty_btree();
        let mut keys = Vec::new();
        for i in 0..10000 {
            let mut cursor = BTreeCursor::new(None, pager.clone(), root_page);
            tracing::info!("INSERT INTO t VALUES ({});", i,);
            let key = OwnedValue::Integer(i);
            let value =
                ImmutableRecord::from_registers(&[Register::OwnedValue(OwnedValue::Integer(i))]);
            tracing::trace!("before insert {}", i);
            run_until_done(
                || {
                    let key = SeekKey::TableRowId(i as u64);
                    cursor.move_to(key, SeekOp::EQ)
                },
                pager.deref(),
            )
            .unwrap();
            run_until_done(|| cursor.insert(&key, &value, true), pager.deref()).unwrap();
            keys.push(i);
        }
        if matches!(validate_btree(pager.clone(), root_page), (_, false)) {
            panic!("invalid btree");
        }
        tracing::trace!(
            "=========== btree ===========\n{}\n\n",
            format_btree(pager.clone(), root_page, 0)
        );
        for key in keys.iter() {
            let mut cursor = BTreeCursor::new(None, pager.clone(), root_page);
            let key = OwnedValue::Integer(*key);
            let exists = run_until_done(|| cursor.exists(&key), pager.deref()).unwrap();
            assert!(exists, "key not found {}", key);
        }
    }

    #[test]
    pub fn test_big_payload_compute_free() {
        let db = get_database();
        let conn = db.connect().unwrap();

        let page = get_page(2);
        let usable_space = 4096;
        let record =
            ImmutableRecord::from_registers(&[Register::OwnedValue(OwnedValue::Blob(vec![
                0;
                3600
            ]))]);
        let mut payload: Vec<u8> = Vec::new();
        fill_cell_payload(
            page.get_contents().page_type(),
            Some(0),
            &mut payload,
            &record,
            4096,
            conn.pager.clone(),
        );
        insert_into_cell(page.get_contents(), &payload, 0, 4096).unwrap();
        let free = compute_free_space(page.get_contents(), usable_space);
        let total_size = payload.len() + 2;
        assert_eq!(
            free,
            usable_space - page.get_contents().header_size() as u16 - total_size as u16
        );
        dbg!(free);
    }

    fn run_until_done<T>(
        mut action: impl FnMut() -> Result<CursorResult<T>>,
        pager: &Pager,
    ) -> Result<T> {
        loop {
            match action()? {
                CursorResult::Ok(res) => {
                    return Ok(res);
                }
                CursorResult::IO => pager.io.run_once().unwrap(),
            }
        }
    }
}
