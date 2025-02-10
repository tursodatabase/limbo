use criterion::async_executor::FuturesExecutor;
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use limbo_core::mvcc::clock::LocalClock;
use limbo_core::mvcc::database::{MvStore, Row, RowID};
use pprof::criterion::{Output, PProfProfiler};

fn bench_db() -> MvStore<LocalClock, String> {
    let clock = LocalClock::default();
    let storage = limbo_core::mvcc::persistent_storage::Storage::new_noop();
    MvStore::new(clock, storage)
}

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("mvcc-ops-throughput");
    group.throughput(Throughput::Elements(1));

    let db = bench_db();
    group.bench_function("begin_tx + rollback_tx", |b| {
        b.to_async(FuturesExecutor).iter(|| async {
            let tx_id = db.begin_tx();
            db.rollback_tx(tx_id)
        })
    });

    let db = bench_db();
    group.bench_function("begin_tx + commit_tx", |b| {
        b.to_async(FuturesExecutor).iter(|| async {
            let tx_id = db.begin_tx();
            db.commit_tx(tx_id)
        })
    });

    let db = bench_db();
    group.bench_function("begin_tx-read-commit_tx", |b| {
        b.to_async(FuturesExecutor).iter(|| async {
            let tx_id = db.begin_tx();
            db.read(
                tx_id,
                RowID {
                    table_id: 1,
                    row_id: 1,
                },
            )
            .unwrap();
            db.commit_tx(tx_id)
        })
    });

    let db = bench_db();
    group.bench_function("begin_tx-update-commit_tx", |b| {
        b.to_async(FuturesExecutor).iter(|| async {
            let tx_id = db.begin_tx();
            db.update(
                tx_id,
                Row {
                    id: RowID {
                        table_id: 1,
                        row_id: 1,
                    },
                    data: "World".to_string(),
                },
            )
            .unwrap();
            db.commit_tx(tx_id)
        })
    });

    let db = bench_db();
    let tx = db.begin_tx();
    db.insert(
        tx,
        Row {
            id: RowID {
                table_id: 1,
                row_id: 1,
            },
            data: "Hello".to_string(),
        },
    )
    .unwrap();
    group.bench_function("read", |b| {
        b.to_async(FuturesExecutor).iter(|| async {
            db.read(
                tx,
                RowID {
                    table_id: 1,
                    row_id: 1,
                },
            )
            .unwrap();
        })
    });

    let db = bench_db();
    let tx = db.begin_tx();
    db.insert(
        tx,
        Row {
            id: RowID {
                table_id: 1,
                row_id: 1,
            },
            data: "Hello".to_string(),
        },
    )
    .unwrap();
    group.bench_function("update", |b| {
        b.to_async(FuturesExecutor).iter(|| async {
            db.update(
                tx,
                Row {
                    id: RowID {
                        table_id: 1,
                        row_id: 1,
                    },
                    data: "World".to_string(),
                },
            )
            .unwrap();
        })
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench
}
criterion_main!(benches);
