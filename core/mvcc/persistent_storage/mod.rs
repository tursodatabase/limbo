use std::fmt::Debug;

use crate::mvcc::database::{LogRecord, Result, Row, RowVersion, TxTimestampOrID};

#[derive(Debug)]
pub struct Storage {}

impl Storage {
    pub fn new() -> Self {
        Self {}
    }

    pub fn log_tx(&self, record: LogRecord) -> Result<()> {
        println!("{}", log_record_to_json(&record));
        Ok(())
    }

    pub fn read_tx_log(&self) -> Result<Vec<LogRecord>> {
        Ok(vec![])
    }
}

fn log_record_to_json(record: &LogRecord) -> String {
    format!(
        "{{\"t\": {}, \"v\": [{}]}}",
        record.tx_timestamp,
        record
            .row_versions
            .iter()
            .map(|row_version| row_version_to_json(row_version))
            .collect::<Vec<String>>()
            .join(",")
    )
}

fn row_version_to_json(row_version: &RowVersion) -> String {
    let begin = match row_version.begin {
        TxTimestampOrID::TxID(tx_id) => tx_id.to_string(),
        TxTimestampOrID::Timestamp(tx_timestamp) => tx_timestamp.to_string(),
    };
    let end = match row_version.end {
        Some(TxTimestampOrID::TxID(tx_id)) => tx_id.to_string(),
        Some(TxTimestampOrID::Timestamp(tx_timestamp)) => tx_timestamp.to_string(),
        None => "null".to_string(),
    };
    format!(
        "{{\"b\": {}, \"e\": {}, \"r\": {}}}",
        begin,
        end,
        row_to_json(&row_version.row)
    )
}

fn row_to_json(row: &Row) -> String {
    format!(
        "{{\"t\": \"{}\", \"r\": {}, \"d\": \"{}\"}}",
        row.id.table_id,
        row.id.row_id,
        data_to_hex(&row.data)
    )
}

fn data_to_hex(data: &[u8]) -> String {
    let mut hex = String::new();
    for byte in data {
        hex.push_str(&format!("{:02x}", byte));
    }
    hex
}
