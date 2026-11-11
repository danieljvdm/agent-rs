//! Run with `cargo run --example recurring_personal_agent`.
//! Uses an inbox fixture and an explicit clock; no credentials or services.
#[path = "support/recurring_workflow.rs"]
mod recurring_workflow;

#[tokio::main]
async fn main() -> agent_rs::Result<()> {
    let directory = tempfile::tempdir().map_err(agent_rs::Error::storage)?;
    let states = recurring_workflow::run(directory.path()).await?;
    for state in states {
        println!("Inbox review {}: {:?}", state.id, state.phase);
    }
    println!(
        "Three distinct reviews, including recovery after admission and a coalesced gap; schedule paused."
    );
    Ok(())
}
