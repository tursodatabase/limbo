use lazy_static::lazy_static;
use limbo_ext::{
    register_extension, ResultCode, VTabCursor, VTabKind, VTabModule, VTabModuleDerive, Value,
};
use std::collections::BTreeMap;
use std::sync::Mutex;

lazy_static! {
    static ref GLOBAL_STORE: Mutex<BTreeMap<i64, (String, String)>> = Mutex::new(BTreeMap::new());
}

register_extension! {
    vtabs: { KVStoreVTab },
}

#[derive(VTabModuleDerive, Default)]
pub struct KVStoreVTab;

/// The cursor holds a snapshot of (rowid, key, value) in memory.
pub struct KVStoreCursor {
    rows: Vec<(i64, String, String)>,
    index: usize,
}

/// Implementing the VTabModule trait for KVStoreVTab
impl VTabModule for KVStoreVTab {
    type VCursor = KVStoreCursor;
    const VTAB_KIND: VTabKind = VTabKind::VirtualTable;
    const NAME: &'static str = "kv_store";
    type Error = String;

    fn create_schema(_args: &[Value]) -> String {
        "CREATE TABLE x (key TEXT PRIMARY KEY, value TEXT);".to_string()
    }

    fn open(&self) -> Result<Self::VCursor, Self::Error> {
        Ok(KVStoreCursor {
            rows: Vec::new(),
            index: 0,
        })
    }

    fn filter(cursor: &mut Self::VCursor, _args: &[Value]) -> ResultCode {
        let store = GLOBAL_STORE.lock().unwrap();
        cursor.rows = store
            .iter()
            .map(|(&rowid, (ref key, ref val))| (rowid, key.clone(), val.clone()))
            .collect();
        cursor.rows.sort_by_key(|(rowid, _, _)| *rowid);
        cursor.index = 0;
        ResultCode::OK
    }

    fn insert(&mut self, values: &[Value]) -> Result<i64, Self::Error> {
        let key = values
            .first()
            .and_then(|v| v.to_text())
            .ok_or("Missing key")?
            .to_string();
        let val = values
            .get(1)
            .and_then(|v| v.to_text())
            .ok_or("Missing value")?
            .to_string();
        let rowid = hash_key(&key);
        {
            let mut store = GLOBAL_STORE.lock().unwrap();
            store.insert(rowid, (key, val));
        }
        Ok(rowid)
    }

    fn delete(&mut self, rowid: i64) -> Result<(), Self::Error> {
        let mut store = GLOBAL_STORE.lock().unwrap();
        store.remove(&rowid);
        Ok(())
    }

    fn update(&mut self, rowid: i64, values: &[Value]) -> Result<(), Self::Error> {
        {
            let mut store = GLOBAL_STORE.lock().unwrap();
            store.remove(&rowid);
        }
        let _ = self.insert(values)?;
        Ok(())
    }
    fn eof(cursor: &Self::VCursor) -> bool {
        cursor.index >= cursor.rows.len()
    }

    fn next(cursor: &mut Self::VCursor) -> ResultCode {
        cursor.index += 1;
        if cursor.index >= cursor.rows.len() {
            return ResultCode::EOF;
        }
        ResultCode::OK
    }

    fn column(cursor: &Self::VCursor, idx: u32) -> Result<Value, Self::Error> {
        if cursor.index >= cursor.rows.len() {
            return Err("cursor out of range".into());
        }
        let (_, ref key, ref val) = cursor.rows[cursor.index];
        match idx {
            0 => Ok(Value::from_text(key.clone())), // key
            1 => Ok(Value::from_text(val.clone())), // value
            _ => Err("Invalid column".into()),
        }
    }
}

fn hash_key(key: &str) -> i64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish() as i64
}

impl VTabCursor for KVStoreCursor {
    type Error = String;

    fn rowid(&self) -> i64 {
        if self.index < self.rows.len() {
            self.rows[self.index].0
        } else {
            println!("rowid: -1");
            -1
        }
    }

    fn column(&self, idx: u32) -> Result<Value, Self::Error> {
        <KVStoreVTab as VTabModule>::column(self, idx)
    }

    fn eof(&self) -> bool {
        <KVStoreVTab as VTabModule>::eof(self)
    }

    fn next(&mut self) -> ResultCode {
        <KVStoreVTab as VTabModule>::next(self)
    }
}
