//! Allocation call stacks for the existing recovery fixture; not a timing benchmark.
#[path = "../benches/support/mod.rs"]
mod support;

use std::path::PathBuf;
use support::durable::RecoveryTemplate;
use support::{FIXTURE_VERSION, Fixture, WORKERS, runtime};

#[global_allocator]
static ALLOCATOR: dhat::Alloc = dhat::Alloc;

fn main() {
    let mut prior_runs = 512;
    let mut output = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--prior-runs" => prior_runs = args.next().expect("prior run count").parse().unwrap(),
            "--out" => output = Some(PathBuf::from(args.next().expect("new output directory"))),
            _ => panic!("unknown option: {arg}"),
        }
    }
    assert!(
        matches!(prior_runs, 0 | 512),
        "use the existing 0 or 512 run fixture"
    );
    let output = output.expect("--out requires a new output directory");
    std::fs::create_dir(&output).expect("output must be a new directory with an existing parent");
    let runtime = runtime();
    let fixture = Fixture::new(4, 1, 4, 0, 0);
    let template = runtime.block_on(RecoveryTemplate::new(&fixture, prior_runs));
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("recovery.sqlite");
    template.copy_to(&database);
    let before = fixture.counts.snapshot();

    // Match benchmark_metrics: opening/claiming/driving are measured; fixture
    // construction, copying the database, final assertions and closing are not.
    let profile_path = output.join("dhat.json");
    let (state, store, stats) = runtime.block_on(async {
        let profiler = dhat::Profiler::builder()
            .file_name(profile_path)
            .trim_backtraces(Some(64))
            .build();
        let (state, store) = template.resume(&fixture, &database).await;
        let stats = dhat::HeapStats::get();
        drop(profiler);
        (state, store, stats)
    });

    fixture.verify_state(&state);
    template.verify_counts(&fixture, &before);
    let after = fixture.counts.snapshot();
    runtime.block_on(store.close()).unwrap();
    let report = serde_json::json!({
        "fixture": FIXTURE_VERSION,
        "prior_runs": prior_runs,
        "prior_context_entries": state.prior_context.len(),
        "workers": WORKERS,
        "model_calls": after.model - before.model,
        "tool_calls": after.tools - before.tools,
        "model_cleanup": after.model_cleanup - before.model_cleanup,
        "tool_cleanup": after.tool_cleanup - before.tool_cleanup,
        "active_operations": after.active,
        "total_blocks": stats.total_blocks,
        "total_bytes": stats.total_bytes,
        "peak_live_bytes": stats.max_bytes,
        "live_bytes_at_end": stats.curr_bytes,
        "timing_valid": false,
        "boundary": "SQLite open, claim, and resume through completed run-owned cleanup"
    });
    std::fs::write(
        output.join("summary.json"),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    println!("{}", output.display());
}
