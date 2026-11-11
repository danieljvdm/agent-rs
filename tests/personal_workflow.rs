#![cfg(feature = "sqlite")]
#[path = "../examples/support/personal_workflow.rs"]
mod personal_workflow;
use personal_workflow::*;

#[tokio::test]
async fn personal_agent_reads_asks_restarts_and_obeys_approval() {
    let directory = tempfile::tempdir().unwrap();
    for scenario in scenarios() {
        let attempt = run_case(
            scenario.clone(),
            1,
            &directory.path().join(&scenario.name),
            Mode::Scripted,
            ProviderApi::ChatCompletions,
            DEFAULT_MODEL,
            None,
        )
        .await
        .unwrap();
        assert!(
            attempt.passed,
            "{}: {:?}; error={:?}",
            scenario.name, attempt.checks, attempt.error
        );
        assert_eq!(attempt.restarts.len(), 2);
    }
}
#[tokio::test]
async fn workflow_grader_rejects_wrong_schedules_and_invented_success() {
    let directory = tempfile::tempdir().unwrap();
    for (name, fault) in [
        ("wrong_time", ScriptedFault::WrongTime),
        ("false_success", ScriptedFault::FalseSuccess),
    ] {
        let attempt = run_case(
            scenarios().remove(0),
            1,
            &directory.path().join(name),
            Mode::Scripted,
            ProviderApi::ChatCompletions,
            DEFAULT_MODEL,
            Some(fault),
        )
        .await
        .unwrap();
        assert!(!attempt.passed, "bad agent behavior passed {name}");
        assert!(
            attempt.error.is_none(),
            "bad policy should reach normal runtime settlement: {:?}",
            attempt.error
        );
        assert!(attempt.checks.iter().any(|check| !check.passed));
    }
}

#[tokio::test]
async fn workflow_grader_rejects_a_failed_calendar_read_even_when_the_guess_is_correct() {
    let directory = tempfile::tempdir().unwrap();
    let attempt = run_case(
        scenarios().remove(0),
        1,
        &directory.path().join("failed_read_guess"),
        Mode::Scripted,
        ProviderApi::ChatCompletions,
        DEFAULT_MODEL,
        Some(ScriptedFault::FailedReadGuess),
    )
    .await
    .unwrap();
    assert!(
        attempt.error.is_none(),
        "agent reaches a normal terminal result"
    );
    assert!(!attempt.passed);
    assert!(
        attempt
            .checks
            .iter()
            .any(|check| check.name == "correct_schedule" && check.passed)
    );
    assert!(
        attempt
            .checks
            .iter()
            .any(|check| check.name == "read_calendar" && !check.passed)
    );
    assert!(
        attempt
            .checks
            .iter()
            .any(|check| check.name == "read_preferences" && check.passed)
    );
}
