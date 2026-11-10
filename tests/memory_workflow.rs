#![cfg(all(feature = "memory", feature = "sqlite"))]
#[path = "../examples/support/memory_workflow.rs"]
#[allow(dead_code)]
mod memory_workflow;
use memory_workflow::*;

#[tokio::test]
async fn long_conversations_retain_current_facts_reject_stale_untrusted_sources_and_recover() {
    let directory = tempfile::tempdir().unwrap();
    let report = run_suite(
        &directory.path().join("eval"),
        Mode::Scripted,
        DEFAULT_MODEL,
        1,
        64,
        64,
    )
    .await
    .unwrap();
    assert!(report.passed, "{:#?}", report.samples);
    assert_eq!(report.samples.len(), 15);
    for sample in report.samples {
        assert!(sample.source_history_bytes > CONTEXT_LIMIT * 4);
        assert!(sample.history_preserved);
        if sample.strategy == Strategy::History && sample.case != Case::Deleted {
            assert!(
                !sample.recalled_current_fact,
                "bounded control should lose the old fact"
            );
        } else {
            assert!(sample.recalled_current_fact);
        }
        if sample.case == Case::Recovery {
            assert!(sample.restarted);
            assert_eq!(sample.usage.model_calls, 2);
        }
    }
}
