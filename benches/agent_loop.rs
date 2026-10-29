mod support;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::time::{Duration, Instant};
use support::*;

fn configured() -> Criterion {
    let quick = std::env::var_os("AGENT_BENCH_QUICK").is_some();
    Criterion::default()
        .sample_size(if quick { 10 } else { 30 })
        .warm_up_time(Duration::from_millis(if quick { 100 } else { 1_000 }))
        .measurement_time(Duration::from_millis(if quick { 250 } else { 2_000 }))
}

fn memory(c: &mut Criterion) {
    let runtime = runtime();
    let cases = [
        ("minimal", 0, 0, 4, 0, 0),
        ("four_tool_rounds", 4, 1, 4, 0, 0),
        ("tools_8_bound_1", 1, 8, 1, 0, 0),
        ("tools_8_bound_4", 1, 8, 4, 0, 0),
        ("tools_8_bound_8", 1, 8, 8, 0, 0),
        ("stream_chunks_1", 0, 0, 4, 1, 0),
        ("stream_chunks_64", 0, 0, 4, 64, 0),
        ("stream_chunks_1024", 0, 0, 4, 1024, 0),
        ("context_entries_0", 0, 0, 4, 0, 0),
        ("context_entries_1024", 0, 0, 4, 0, 1024),
    ];
    let mut group = c.benchmark_group("engine");
    group.throughput(Throughput::Elements(1));
    for (name, rounds, batch, bound, chunks, history) in cases {
        let fixture = Fixture::new(rounds, batch, bound, chunks, history);
        group.bench_function(name, |b| {
            b.iter_custom(|iterations| {
                runtime.block_on(async {
                    let mut elapsed = Duration::ZERO;
                    for _ in 0..iterations {
                        let prepared = fixture.prepare();
                        let before = fixture.counts.snapshot();
                        let started = Instant::now();
                        let state = fixture.execute(prepared).await;
                        elapsed += started.elapsed();
                        fixture.verify_state(&state);
                        fixture.verify_counts(&before, 1, 1);
                    }
                    elapsed
                })
            })
        });
    }
    group.finish();
    let mut group = c.benchmark_group("conversations");
    group.throughput(Throughput::Elements(CONCURRENT_TOTAL as u64));
    for active in [1, 16, 64] {
        let fixture = Fixture::new(4, 1, 4, 0, 0);
        group.bench_with_input(
            BenchmarkId::new("engine_workers_4", active),
            &active,
            |b, active| {
                b.iter_custom(|iterations| {
                    runtime.block_on(async {
                        let mut elapsed = Duration::ZERO;
                        for _ in 0..iterations {
                            let states = (0..CONCURRENT_TOTAL).map(|_| fixture.prepare()).collect();
                            let before = fixture.counts.snapshot();
                            let started = Instant::now();
                            let completed = concurrent(&fixture, states, *active).await;
                            elapsed += started.elapsed();
                            assert_eq!(completed.len(), CONCURRENT_TOTAL);
                            for (state, _) in completed {
                                fixture.verify_state(&state);
                            }
                            fixture.verify_counts(&before, CONCURRENT_TOTAL, *active);
                        }
                        elapsed
                    })
                });
            },
        );
    }
    group.finish();
}

#[cfg(feature = "sqlite")]
fn durable(c: &mut Criterion) {
    use support::durable::*;
    let runtime = runtime();
    let fixture = Fixture::new(4, 1, 4, 0, 0);
    let mut group = c.benchmark_group("sqlite");
    group.throughput(Throughput::Elements(1));
    group.bench_function("durable_four_tool_rounds", |b| {
        b.iter_custom(|iterations| {
            runtime.block_on(async {
                let mut elapsed = Duration::ZERO;
                for _ in 0..iterations {
                    let mut resident = Resident::new(&fixture, 4).await;
                    let before = fixture.counts.snapshot();
                    let writes = resident.store.metrics();
                    let started = Instant::now();
                    let state = resident.run(&fixture).await;
                    elapsed += started.elapsed();
                    fixture.verify_state(&state);
                    fixture.verify_counts(&before, 1, 1);
                    assert!(resident.store.metrics().transactions > writes.transactions);
                    assert!(resident.store.metrics().writes > writes.writes);
                    resident.close().await;
                }
                elapsed
            })
        })
    });
    group.finish();
    let mut group = c.benchmark_group("sqlite_conversations");
    group.throughput(Throughput::Elements(CONCURRENT_TOTAL as u64));
    for active in [1, 16, 64] {
        let fixture = Fixture::new(4, 1, 4, 0, 0);
        group.bench_with_input(
            BenchmarkId::new("host_workers_4", active),
            &active,
            |b, active| {
                b.iter_custom(|iterations| {
                    runtime.block_on(async {
                        let mut elapsed = Duration::ZERO;
                        for _ in 0..iterations {
                            let mut resident = Resident::new(&fixture, *active).await;
                            let before = fixture.counts.snapshot();
                            let started = Instant::now();
                            let completed = resident.batch(&fixture, CONCURRENT_TOTAL).await;
                            elapsed += started.elapsed();
                            assert_eq!(completed.len(), CONCURRENT_TOTAL);
                            for (state, _) in completed {
                                fixture.verify_state(&state);
                            }
                            fixture.verify_counts(&before, CONCURRENT_TOTAL, *active);
                            resident.close().await;
                        }
                        elapsed
                    })
                });
            },
        );
    }
    group.finish();
    let mut group = c.benchmark_group("recovery");
    group.throughput(Throughput::Elements(1));
    for prior_runs in [0, 512] {
        let fixture = Fixture::new(4, 1, 4, 0, 0);
        let template = runtime.block_on(RecoveryTemplate::new(&fixture, prior_runs));
        group.bench_with_input(
            BenchmarkId::new("restart_to_completion", prior_runs),
            &prior_runs,
            |b, _| {
                b.iter_custom(|iterations| {
                    runtime.block_on(async {
                        let mut elapsed = Duration::ZERO;
                        for _ in 0..iterations {
                            let directory = tempfile::tempdir().unwrap();
                            let path = directory.path().join("sample.sqlite");
                            template.copy_to(&path);
                            let before = fixture.counts.snapshot();
                            let started = Instant::now();
                            let (state, store) = template.resume(&fixture, &path).await;
                            elapsed += started.elapsed();
                            fixture.verify_state(&state);
                            template.verify_counts(&fixture, &before);
                            store.close().await.unwrap();
                        }
                        elapsed
                    })
                });
            },
        );
    }
    group.finish();
}
#[cfg(not(feature = "sqlite"))]
fn durable(_: &mut Criterion) {}

criterion_group! { name = benches; config = configured(); targets = memory, durable }
criterion_main!(benches);
