mod generation;
mod opts;

use antithesis_sdk::*;
use clap::Parser;
use core::panic;
use limbo::Builder;
use opts::Opts;
use std::fs::File;
use std::io::Read;
use std::sync::Arc;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

use crate::generation::generate_plan;

pub struct Plan {
    pub ddl_statements: Vec<String>,
    pub queries_per_thread: Vec<Vec<String>>,
    pub nr_iterations: usize,
    pub nr_threads: usize,
}

fn read_plan_from_log_file(opts: &Opts) -> Result<Plan, Box<dyn std::error::Error + Send + Sync>> {
    let mut file = File::open(&opts.log_file)?;
    let mut buf = String::new();
    let mut plan = Plan {
        ddl_statements: vec![],
        queries_per_thread: vec![],
        nr_iterations: 0,
        nr_threads: 0,
    };
    file.read_to_string(&mut buf).unwrap();
    let mut lines = buf.lines();
    plan.nr_threads = lines.next().expect("missing threads").parse().unwrap();
    plan.nr_iterations = lines
        .next()
        .expect("missing nr_iterations")
        .parse()
        .unwrap();
    let nr_ddl = lines
        .next()
        .expect("number of ddl statements")
        .parse()
        .unwrap();
    for _ in 0..nr_ddl {
        plan.ddl_statements
            .push(lines.next().expect("expected ddl statement").to_string());
    }
    for _ in 0..plan.nr_threads {
        let mut queries = vec![];
        for _ in 0..plan.nr_iterations {
            queries.push(
                lines
                    .next()
                    .expect("missing query for thread {}")
                    .to_string(),
            );
        }
        plan.queries_per_thread.push(queries);
    }
    Ok(plan)
}

pub fn init_tracing() -> Result<WorkerGuard, std::io::Error> {
    let (non_blocking, guard) = tracing_appender::non_blocking(std::io::stderr());
    if let Err(e) = tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(non_blocking)
                .with_ansi(false)
                .with_line_number(true)
                .with_thread_ids(true),
        )
        .with(EnvFilter::from_default_env())
        .try_init()
    {
        println!("Unable to setup tracing appender: {:?}", e);
    }
    Ok(guard)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let _g = init_tracing()?;
    antithesis_init();

    let mut opts = Opts::parse();

    if opts.nr_threads > 1 {
        println!("ERROR: Multi-threaded data access is not yet supported: https://github.com/tursodatabase/limbo/issues/1552");
        return Ok(());
    }

    let plan = if opts.load_log {
        read_plan_from_log_file(&mut opts)?
    } else {
        generate_plan(&opts)?
    };

    let mut handles = Vec::with_capacity(opts.nr_threads);
    let plan = Arc::new(plan);

    let tempfile = tempfile::NamedTempFile::new()?;
    let db_file = if let Some(db_file) = opts.db_file {
        db_file
    } else {
        tempfile.path().to_string_lossy().to_string()
    };

    for thread in 0..opts.nr_threads {
        let db = Arc::new(Builder::new_local(&db_file).build().await?);
        let plan = plan.clone();
        let conn = db.connect()?;

        // Apply each DDL statement individually
        for stmt in &plan.ddl_statements {
            println!("executing ddl {}", stmt);
            if let Err(e) = conn.execute(stmt, ()).await {
                match e {
                    limbo::Error::SqlExecutionFailure(e) => {
                        if e.contains("Corrupt database") {
                            panic!("Error creating table: {}", e);
                        } else {
                            println!("Error creating table: {}", e);
                        }
                    }
                    _ => panic!("Error creating table: {}", e),
                }
            }
        }

        let nr_iterations = opts.nr_iterations;
        let db = db.clone();

        let handle = tokio::spawn(async move {
            let conn = db.connect()?;
            for query_index in 0..nr_iterations {
                let sql = &plan.queries_per_thread[thread][query_index];
                println!("executing: {}", sql);
                if let Err(e) = conn.execute(&sql, ()).await {
                    match e {
                        limbo::Error::SqlExecutionFailure(e) => {
                            if e.contains("Corrupt database") {
                                panic!("Error executing query: {}", e);
                            } else if e.contains("UNIQUE constraint failed") {
                                println!("Skipping UNIQUE constraint violation: {}", e);
                            } else {
                                println!("Error executing query: {}", e);
                            }
                        }
                        _ => panic!("Error executing query: {}", e),
                    }
                }
                let mut res = conn.query("PRAGMA integrity_check", ()).await.unwrap();
                if let Some(row) = res.next().await? {
                    let value = row.get_value(0).unwrap();
                    if value != "ok".into() {
                        panic!("integrity check failed: {:?}", value);
                    }
                } else {
                    panic!("integrity check failed: no rows");
                }
            }
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await??;
    }
    println!("Done. SQL statements written to {}", opts.log_file);
    println!("Database file: {}", db_file);
    Ok(())
}
