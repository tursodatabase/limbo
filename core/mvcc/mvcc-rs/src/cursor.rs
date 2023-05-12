use crate::clock::LogicalClock;
use crate::database::{Database, Result, Row, RowID};
use crate::persistent_storage::Storage;

#[derive(Debug)]
pub struct ScanCursor<
    'a,
    Clock: LogicalClock,
    StorageImpl: Storage,
> {
    pub db: &'a Database<Clock, StorageImpl>,
    pub row_ids: Vec<RowID>,
    pub index: usize,
    tx_id: u64,
}

impl<
        'a,
        Clock: LogicalClock,
        StorageImpl: Storage,
    > ScanCursor<'a, Clock, StorageImpl>
{
    pub async fn new(
        db: &'a Database<Clock, StorageImpl>,
        table_id: u64,
    ) -> Result<ScanCursor<'a, Clock, StorageImpl>> {
        let tx_id = db.begin_tx().await;
        let row_ids = db.scan_row_ids_for_table(table_id).await?;
        Ok(Self {
            db,
            tx_id,
            row_ids,
            index: 0,
        })
    }

    pub fn current_row_id(&self) -> Option<RowID> {
        if self.index >= self.row_ids.len() {
            return None;
        }
        Some(self.row_ids[self.index])
    }

    pub async fn current_row(&self) -> Result<Option<Row>> {
        if self.index >= self.row_ids.len() {
            return Ok(None);
        }
        let id = self.row_ids[self.index];
        self.db.read(self.tx_id, id).await
    }

    pub async fn close(self) -> Result<()> {
        self.db.commit_tx(self.tx_id).await
    }

    pub fn forward(&mut self) -> bool {
        self.index += 1;
        self.index < self.row_ids.len()
    }

    pub fn is_empty(&self) -> bool {
        self.index >= self.row_ids.len()
    }
}
