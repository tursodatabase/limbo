use limbo_ext::{
    register_extension, ResultCode, VTabCursor, VTabKind, VTabModule, VTabModuleDerive, Value,
};
use std::collections::HashMap;

register_extension! {
    vtabs: { KVStoreVTab },
}

#[derive(VTabModuleDerive, Default)]
pub struct KVStoreVTab {
    store: HashMap<String, String>,
}

pub struct KVStoreCursor {
    keys: Vec<String>,
    values: Vec<String>,
    index: usize,
}

impl VTabModule for KVStoreVTab {
    type VCursor = KVStoreCursor;
    const VTAB_KIND: VTabKind = VTabKind::VirtualTable;
    const NAME: &'static str = "kv_store";
    type Error = String;

    fn create_schema(_args: &[String]) -> String {
        "CREATE TABLE x (key TEXT PRIMARY KEY, value TEXT);".to_string()
    }

    fn open(_args: &[String]) -> Result<Self::VCursor, Self::Error> {
        Ok(KVStoreCursor {
            keys: Vec::new(),
            values: Vec::new(),
            index: 0,
        })
    }

    fn filter(cursor: &mut Self::VCursor, _arg_count: i32, _args: &[Value]) -> ResultCode {
        cursor.index = 0;
        ResultCode::OK
    }

    fn column(cursor: &Self::VCursor, idx: u32) -> Result<Value, Self::Error> {
        match idx {
            0 => Ok(Value::from_text(cursor.keys[cursor.index].clone())),
            1 => Ok(Value::from_text(cursor.values[cursor.index].clone())),
            _ => Err("Invalid column".into()),
        }
    }

    fn next(cursor: &mut Self::VCursor) -> ResultCode {
        cursor.index += 1;
        ResultCode::OK
    }

    fn eof(cursor: &Self::VCursor) -> bool {
        cursor.index >= cursor.keys.len()
    }

    fn update(&mut self, args: &[Value], rowid: Option<i64>) -> Result<Option<i64>, Self::Error> {
        match args.len() {
            1 => {
                let key = args[0].to_text().ok_or("Invalid key")?;
                // Handle DELETE
                self.store.remove(key);
                Ok(None)
            }
            2 => {
                let key = args[0].to_text().ok_or("Invalid key")?;
                let value = args[1].to_text().ok_or("Invalid value")?;
                // Handle INSERT / UPDATE
                self.store.insert(key.to_string(), value.to_string());
                Ok(Some(rowid.unwrap_or(0)))
            }
            _ => {
                println!("args: {:?}", args);
                Err("Invalid arguments for update".into())
            }
        }
    }
}

impl VTabCursor for KVStoreCursor {
    type Error = String;
    fn rowid(&self) -> i64 {
        self.index as i64
    }
    fn column(&self, idx: u32) -> Result<Value, Self::Error> {
        match idx {
            0 => Ok(Value::from_text(self.keys[self.index].clone())),
            1 => Ok(Value::from_text(self.values[self.index].clone())),
            _ => Err("Invalid column".into()),
        }
    }
    fn eof(&self) -> bool {
        self.index >= self.keys.len()
    }
    fn next(&mut self) -> ResultCode {
        self.index += 1;
        ResultCode::OK
    }
}
