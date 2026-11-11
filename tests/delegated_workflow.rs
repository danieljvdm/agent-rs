#![cfg(feature = "sqlite")]
#[path = "../examples/support/delegated_workflow.rs"]
mod workflow;

#[tokio::test]
async fn delegated_research_reuses_completed_children_and_grades_results_after_restart() {
    let directory = tempfile::tempdir().unwrap();
    let mut report = workflow::run(&directory.path().join("workflow.sqlite"))
        .await
        .unwrap();
    workflow::grade(&report).unwrap();
    report.lookups += 1;
    assert!(
        workflow::grade(&report).is_err(),
        "duplicate research must fail the eval"
    );
    report.lookups -= 1;
    report.plan.selected = "Atrium Museum".into();
    assert!(
        workflow::grade(&report).is_err(),
        "a venue over budget must fail the eval"
    );
}
