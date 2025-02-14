mod opts;

use antithesis_sdk::*;
use clap::Parser;
use limbo::{Builder, Value};
use opts::Opts;
use std::sync::Arc;

#[tokio::main]
async fn main() {
    antithesis_init();

    let opts = Opts::parse();
    let mut handles = Vec::new();

    for _ in 0..opts.nr_threads {
        // TODO: share the database between threads
        let db = Arc::new(Builder::new_local(":memory:").build().await.unwrap());
        let nr_iterations = opts.nr_iterations;
        let db = db.clone();
        let handle = tokio::spawn(async move {
            let conn = db.connect().unwrap();

            for _ in 0..nr_iterations {
                let mut rows = conn.query("select 1", ()).await.unwrap();
                let row = rows.next().await.unwrap().unwrap();
                let value = row.get_value(0).unwrap();
                assert_always!(matches!(value, Value::Integer(1)), "value is incorrect");
            }
        });
        handles.push(handle);
    }
    for handle in handles {
        handle.await.unwrap();
    }
}
