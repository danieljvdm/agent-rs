#![cfg(all(feature = "google-calendar", feature = "sqlite"))]
#[path = "../examples/support/calendar_fixture.rs"]
mod calendar_fixture;
#[path = "../examples/support/calendar_services.rs"]
mod calendar_services;
#[path = "../examples/support/personal_workflow.rs"]
mod personal_workflow;

use calendar_fixture::{CalendarFixture, TEST_TOKEN, source_events};
use personal_workflow::*;
use std::sync::Arc;

#[tokio::test]
async fn service_workflow_reads_waits_restarts_denies_and_reconciles_actual_http_effects() {
    let directory = tempfile::tempdir().unwrap();
    let mut cases: Vec<_> = scenarios().into_iter().map(|s| (s, false)).collect();
    let mut uncertain = scenarios().remove(0);
    uncertain.name = "lost_write_response".into();
    cases.push((uncertain, true));
    for (scenario, lost) in cases {
        let fixture = CalendarFixture::start(source_events(&scenario));
        fixture.state.lock().unwrap().drop_next_write_response = lost;
        let attempt = run_case_with_services(
            scenario.clone(),
            1,
            &directory.path().join(&scenario.name),
            Mode::Scripted,
            ProviderApi::ChatCompletions,
            DEFAULT_MODEL,
            None,
            ServiceMode::GoogleCalendar {
                client: Arc::new(fixture.client()),
                lose_response: lost,
            },
        )
        .await
        .unwrap();
        assert!(
            attempt.passed,
            "{}: {:?}; {:?}",
            scenario.name, attempt.checks, attempt.error
        );
        assert_eq!(attempt.restarts.len(), if lost { 3 } else { 2 });
        assert_eq!(fixture.events().len(), usize::from(scenario.approve));
        if let Some(steered) = &scenario.steering_title {
            // Steering reaches the real write: the provider holds the steered title.
            assert_eq!(fixture.events()[0]["summary"], steered.as_str());
            let steering_checks = attempt
                .checks
                .iter()
                .filter(|check| check.name.starts_with("steering_"))
                .collect::<Vec<_>>();
            assert_eq!(steering_checks.len(), 4);
            assert!(steering_checks.iter().all(|check| check.passed));
        }
        assert!(
            calendar_services::grade_provider_state(&scenario, &fixture.events(), Some(&attempt))
                .iter()
                .all(|c| c.passed)
        );
        if scenario.approve {
            let mut corrupted = fixture.events();
            corrupted[0]["reminders"] = serde_json::json!({"useDefault":true});
            assert!(
                calendar_services::grade_provider_state(&scenario, &corrupted, Some(&attempt))
                    .iter()
                    .any(|c| !c.passed)
            );
        }
        assert_eq!(
            fixture.state.lock().unwrap().create_requests,
            usize::from(scenario.approve)
        );
        assert!(
            !serde_json::to_string(&attempt)
                .unwrap()
                .contains(TEST_TOKEN)
        );
        for entry in std::fs::read_dir(directory.path().join(&scenario.name)).unwrap() {
            let bytes = std::fs::read(entry.unwrap().path()).unwrap();
            assert!(
                !bytes
                    .windows(TEST_TOKEN.len())
                    .any(|window| window == TEST_TOKEN.as_bytes()),
                "credential leaked into retained evidence or SQLite"
            );
        }
    }
}

#[tokio::test]
async fn service_grader_rejects_wrong_times_and_invented_success() {
    let directory = tempfile::tempdir().unwrap();
    for (name, fault) in [
        ("wrong_time", ScriptedFault::WrongTime),
        ("false_success", ScriptedFault::FalseSuccess),
        ("failed_read", ScriptedFault::FailedReadGuess),
        ("ignored_steering", ScriptedFault::IgnoreSteering),
    ] {
        let scenario = if matches!(fault, ScriptedFault::IgnoreSteering) {
            scenarios()
                .into_iter()
                .find(|scenario| scenario.steering_title.is_some())
                .unwrap()
        } else {
            scenarios().remove(0)
        };
        let fixture = CalendarFixture::start(source_events(&scenario));
        let attempt = run_case_with_services(
            scenario,
            1,
            &directory.path().join(name),
            Mode::Scripted,
            ProviderApi::ChatCompletions,
            DEFAULT_MODEL,
            Some(fault),
            ServiceMode::GoogleCalendar {
                client: Arc::new(fixture.client()),
                lose_response: false,
            },
        )
        .await
        .unwrap();
        assert!(
            attempt.error.is_none(),
            "negative behavior should settle normally: {:?}",
            attempt.error
        );
        assert!(!attempt.passed, "grader accepted {name}");
    }
}

/// The provider oracle grades the steered title, not the original request, so a service
/// reminder that ignores active steering fails even though the workflow settled.
#[test]
fn provider_oracle_requires_the_steered_title() {
    let scenario = scenarios()
        .into_iter()
        .find(|scenario| scenario.steering_title.is_some())
        .unwrap();
    let event = |title: &str| {
        let at = chrono::DateTime::parse_from_rfc3339(&scenario.expected_at()).unwrap();
        serde_json::json!({
            "id": "reminder",
            "summary": title,
            "start": {"dateTime": at.to_rfc3339()},
            "end": {"dateTime": (at + chrono::Duration::minutes(1)).to_rfc3339()},
            "status": "confirmed",
            "visibility": "private",
            "transparency": "transparent",
            "reminders": {"useDefault": false, "overrides": [{"method": "popup", "minutes": 0}]},
        })
    };
    let attempt = AttemptReport {
        scenario: scenario.clone(),
        trial: 1,
        elapsed_ms: 0,
        passed: true,
        error: None,
        state: None,
        reminders: vec![Reminder {
            reminder_id: "reminder".into(),
            title: scenario.expected_title().into(),
            at: scenario.expected_at(),
            idempotency_key: "key".into(),
        }],
        audit: Audit::default(),
        restarts: vec![],
        checks: vec![],
        service: None,
    };
    let contract = |title: &str| {
        calendar_services::grade_provider_state(&scenario, &[event(title)], Some(&attempt))
            .iter()
            .all(|check| check.passed)
    };
    assert!(contract(scenario.expected_title()));
    assert!(!contract(&scenario.title));
}
