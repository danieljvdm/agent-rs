//! Diagnostic CPU, per-operation latency, and optional allocation measurements.
//! Criterion's uninstrumented benchmark remains the headline timing source.
#[path = "../benches/support/mod.rs"]
mod support;

use agent_rs::*;
use serde::Serialize;
use std::{future::Future, time::Instant};
use support::*;

#[cfg(feature = "allocation-metrics")]
mod allocations {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    pub struct Counting;
    static ENABLED: AtomicBool = AtomicBool::new(false);
    static COUNT: AtomicU64 = AtomicU64::new(0);
    static BYTES: AtomicU64 = AtomicU64::new(0);
    unsafe impl GlobalAlloc for Counting {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            let pointer = unsafe { System.alloc(layout) };
            if !pointer.is_null() && ENABLED.load(Ordering::Relaxed) {
                COUNT.fetch_add(1, Ordering::Relaxed);
                BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
            }
            pointer
        }
        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            let pointer = unsafe { System.alloc_zeroed(layout) };
            if !pointer.is_null() && ENABLED.load(Ordering::Relaxed) {
                COUNT.fetch_add(1, Ordering::Relaxed);
                BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
            }
            pointer
        }
        unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
            unsafe { System.dealloc(pointer, layout) }
        }
        unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            let pointer = unsafe { System.realloc(pointer, layout, new_size) };
            if !pointer.is_null() && ENABLED.load(Ordering::Relaxed) {
                COUNT.fetch_add(1, Ordering::Relaxed);
                BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
            }
            pointer
        }
    }
    pub fn start() {
        COUNT.store(0, Ordering::SeqCst);
        BYTES.store(0, Ordering::SeqCst);
        ENABLED.store(true, Ordering::SeqCst);
    }
    pub fn stop() -> (Option<u64>, Option<u64>) {
        ENABLED.store(false, Ordering::SeqCst);
        (
            Some(COUNT.load(Ordering::SeqCst)),
            Some(BYTES.load(Ordering::SeqCst)),
        )
    }
}
#[cfg(feature = "allocation-metrics")]
#[global_allocator]
static ALLOCATOR: allocations::Counting = allocations::Counting;
#[cfg(not(feature = "allocation-metrics"))]
mod allocations {
    pub fn start() {}
    pub fn stop() -> (Option<u64>, Option<u64>) {
        (None, None)
    }
}

#[derive(Default, Serialize)]
struct Sample {
    case: String,
    completed_runs: usize,
    elapsed_ns: u64,
    process_cpu_ns: u64,
    allocations: Option<u64>,
    allocated_bytes: Option<u64>,
    model_calls: usize,
    tool_calls: usize,
    first_output_ns: Option<u64>,
    operation_latencies_ns: Vec<u64>,
    transactions: Option<u64>,
    writes: Option<u64>,
}
async fn measure<T>(
    case: &str,
    completed_runs: usize,
    future: impl Future<Output = T>,
) -> (T, Sample) {
    allocations::start();
    let cpu = process_cpu_ns();
    let started = Instant::now();
    let result = future.await;
    let elapsed_ns = started.elapsed().as_nanos() as u64;
    let process_cpu_ns = process_cpu_ns() - cpu;
    let (allocations, allocated_bytes) = allocations::stop();
    (
        result,
        Sample {
            case: case.into(),
            completed_runs,
            elapsed_ns,
            process_cpu_ns,
            allocations,
            allocated_bytes,
            ..Sample::default()
        },
    )
}
fn counts(sample: &mut Sample, fixture: &Fixture, before: &Snapshot) {
    let after = fixture.counts.snapshot();
    sample.model_calls = after.model - before.model;
    sample.tool_calls = after.tools - before.tools;
}

async fn collect(samples: usize, filter: &str) -> Vec<Sample> {
    let mut results = Vec::new();
    for (name, rounds, batch, bound, chunks, history) in [
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
    ] {
        if !name.contains(filter) {
            continue;
        }
        let fixture = Fixture::new(rounds, batch, bound, chunks, history);
        fixture.execute(fixture.prepare()).await;
        for _ in 0..samples {
            let prepared = fixture.prepare();
            let before = fixture.counts.snapshot();
            let (state, mut sample) = measure(name, 1, fixture.execute(prepared)).await;
            fixture.verify_state(&state);
            fixture.verify_counts(&before, 1, 1);
            counts(&mut sample, &fixture, &before);
            results.push(sample);
        }
        // First output is measured from public progress delivery on a separate executor task.
        if chunks > 0 {
            for _ in 0..samples {
                let prepared = fixture.prepare();
                let agent = fixture.agent.clone();
                let (sender, mut receiver) = tokio::sync::mpsc::channel(2048);
                let before = fixture.counts.snapshot();
                let ((state, first, text, observed_chunks), mut sample) =
                    measure(&format!("{name}_observed"), 1, async move {
                        let started = Instant::now();
                        let task = tokio::spawn(async move {
                            let mut state = prepared.admit(&agent);
                            agent
                                .drive(
                                    &mut state,
                                    &MemoryCheckpoint,
                                    RunControl::default().with_events(sender),
                                )
                                .await
                                .unwrap();
                            state
                        });
                        let mut first = None;
                        let mut text = String::new();
                        let mut observed_chunks = 0;
                        while let Some(event) = receiver.recv().await {
                            if let ProgressKind::TextDelta(delta) = event.kind {
                                first.get_or_insert(started.elapsed().as_nanos() as u64);
                                text.push_str(&delta);
                                observed_chunks += 1;
                            }
                        }
                        (task.await.unwrap(), first, text, observed_chunks)
                    })
                    .await;
                fixture.verify_state(&state);
                fixture.verify_counts(&before, 1, 1);
                assert_eq!(text, "x".repeat(1024));
                assert_eq!(observed_chunks, chunks);
                sample.first_output_ns = first;
                counts(&mut sample, &fixture, &before);
                results.push(sample);
            }
        }
    }
    for active in [1, 16, 64] {
        let name = format!("conversations_engine_{active}");
        if !name.contains(filter) {
            continue;
        }
        let fixture = Fixture::new(4, 1, 4, 0, 0);
        for _ in 0..samples {
            let states = (0..CONCURRENT_TOTAL).map(|_| fixture.prepare()).collect();
            let before = fixture.counts.snapshot();
            let (completed, mut sample) = measure(
                &name,
                CONCURRENT_TOTAL,
                concurrent(&fixture, states, active),
            )
            .await;
            assert_eq!(completed.len(), CONCURRENT_TOTAL);
            for (state, latency) in completed {
                fixture.verify_state(&state);
                sample
                    .operation_latencies_ns
                    .push(latency.as_nanos() as u64);
            }
            fixture.verify_counts(&before, CONCURRENT_TOTAL, active);
            counts(&mut sample, &fixture, &before);
            results.push(sample);
        }
    }
    #[cfg(feature = "sqlite")]
    {
        use support::durable::*;
        for active in [1, 16, 64] {
            let name = format!("conversations_sqlite_{active}");
            if !name.contains(filter) {
                continue;
            }
            let fixture = Fixture::new(4, 1, 4, 0, 0);
            for _ in 0..samples {
                let mut resident = Resident::new(&fixture, active).await;
                let before = fixture.counts.snapshot();
                let writes = resident.store.metrics();
                let (completed, mut sample) = measure(
                    &name,
                    CONCURRENT_TOTAL,
                    resident.batch(&fixture, CONCURRENT_TOTAL),
                )
                .await;
                for (state, latency) in completed {
                    fixture.verify_state(&state);
                    sample
                        .operation_latencies_ns
                        .push(latency.as_nanos() as u64);
                }
                fixture.verify_counts(&before, CONCURRENT_TOTAL, active);
                counts(&mut sample, &fixture, &before);
                let after = resident.store.metrics();
                sample.transactions = Some(after.transactions - writes.transactions);
                sample.writes = Some(after.writes - writes.writes);
                results.push(sample);
                resident.close().await;
            }
        }
        if "durable_four_tool_rounds".contains(filter) {
            let fixture = Fixture::new(4, 1, 4, 0, 0);
            for _ in 0..samples {
                let mut resident = Resident::new(&fixture, 4).await;
                let before = fixture.counts.snapshot();
                let writes = resident.store.metrics();
                let (state, mut sample) =
                    measure("durable_four_tool_rounds", 1, resident.run(&fixture)).await;
                fixture.verify_state(&state);
                fixture.verify_counts(&before, 1, 1);
                counts(&mut sample, &fixture, &before);
                let after = resident.store.metrics();
                sample.transactions = Some(after.transactions - writes.transactions);
                sample.writes = Some(after.writes - writes.writes);
                results.push(sample);
                resident.close().await;
            }
        }
        for prior in [0, 512] {
            let name = format!("recovery_prior_runs_{prior}");
            if !name.contains(filter) {
                continue;
            }
            let fixture = Fixture::new(4, 1, 4, 0, 0);
            let template = RecoveryTemplate::new(&fixture, prior).await;
            for _ in 0..samples {
                let directory = tempfile::tempdir().unwrap();
                let path = directory.path().join("sample.sqlite");
                template.copy_to(&path);
                let before = fixture.counts.snapshot();
                let ((state, store), mut sample) =
                    measure(&name, 1, template.resume(&fixture, &path)).await;
                fixture.verify_state(&state);
                template.verify_counts(&fixture, &before);
                counts(&mut sample, &fixture, &before);
                let writes = store.metrics();
                sample.transactions = Some(writes.transactions);
                sample.writes = Some(writes.writes);
                results.push(sample);
                store.close().await.unwrap();
            }
        }
        if "startup_sqlite".contains(filter) {
            for _ in 0..samples {
                let directory = tempfile::tempdir().unwrap();
                let path = directory.path().join("startup.sqlite");
                let (store, sample) = measure(
                    "startup_sqlite",
                    0,
                    agent_rs::sqlite::SqliteStore::open(path),
                )
                .await;
                results.push(sample);
                store.unwrap().close().await.unwrap();
            }
        }
    }
    results
}

fn main() {
    let mut samples = 30;
    let mut filter = String::new();
    let mut output = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--samples" => samples = args.next().expect("sample count").parse::<usize>().unwrap(),
            "--filter" => filter = args.next().expect("filter"),
            "--output" => output = Some(args.next().expect("output path")),
            _ => panic!("unknown option: {arg}"),
        }
    }
    assert!(samples > 0);
    let started = Instant::now();
    let runtime = runtime();
    let startup_ns = started.elapsed().as_nanos() as u64;
    let results = runtime.block_on(collect(samples, &filter));
    assert!(!results.is_empty(), "filter selected no workloads");
    let report = serde_json::json!({"fixture": FIXTURE_VERSION, "workers": WORKERS,
        "allocation_instrumented": cfg!(feature = "allocation-metrics"), "runtime_startup_ns": startup_ns,
        "samples_per_case": samples, "samples": results});
    let report = serde_json::to_string_pretty(&report).unwrap();
    if let Some(path) = output {
        std::fs::write(path, report).unwrap();
    } else {
        println!("{report}");
    }
}
