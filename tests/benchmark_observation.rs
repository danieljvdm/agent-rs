#![cfg(feature = "sqlite")]

#[path = "../benches/support/mod.rs"]
mod support;

use std::time::{Duration, Instant};
use support::durable::{Resident, join_batch_observation};
use support::{CONCURRENT_TOTAL, Fixture};
use yielded_agent::{Phase, Settlement};

#[tokio::test]
async fn observes_early_completion_before_later_admission_finishes() {
    let fixture = Fixture::new(0, 0, 1, 0, 0);
    let mut first = fixture.prepare().admit(&fixture.agent);
    let mut second = fixture.prepare().admit(&fixture.agent);
    first.phase = Phase::Settled(Settlement::Completed(serde_json::json!({"answer": "done"})));
    second.phase = first.phase.clone();
    let first_id = first.id;
    let second_id = second.id;
    let first_start = Instant::now();
    let second_start = first_start + Duration::from_secs(60);
    let latency = Duration::from_millis(2);
    let (observed, await_observation) = tokio::sync::oneshot::channel();
    let (admitted, await_admission) = tokio::sync::oneshot::channel();

    let submit = async move {
        // A sequential submit-then-observe implementation deadlocks here.
        await_observation.await.unwrap();
        admitted.send(()).unwrap();
        vec![(first_id, first_start), (second_id, second_start)]
    };
    let observe = async move {
        let first_completion = (first, first_start + latency);
        observed.send(()).unwrap();
        await_admission.await.unwrap();
        vec![first_completion, (second, second_start + latency)]
    };
    let completed = tokio::time::timeout(
        Duration::from_secs(1),
        join_batch_observation(2, submit, observe),
    )
    .await
    .expect("completion must be observed while later admission is blocked");
    assert_eq!(completed.len(), 2);
    assert_eq!(completed[0].0.id, first_id);
    assert_eq!(completed[1].0.id, second_id);
    assert_eq!(completed[0].1, latency);
    assert_eq!(completed[1].1, latency);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_public_completion_is_observed_once_while_submitting_a_full_batch() {
    let fixture = Fixture::new(4, 1, 4, 0, 0);
    let mut resident = Resident::new(&fixture, 16).await;
    let before = fixture.counts.snapshot();
    let completed = resident.batch(&fixture, CONCURRENT_TOTAL).await;
    assert_eq!(completed.len(), CONCURRENT_TOTAL);
    let mut ids = std::collections::HashSet::new();
    for (state, latency) in completed {
        assert!(ids.insert(state.id));
        assert!(!latency.is_zero());
        fixture.verify_state(&state);
    }
    fixture.verify_counts(&before, CONCURRENT_TOTAL, 16);
    resident.close().await;
}
