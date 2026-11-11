//! A planner that delegates venue research to two attached subagents, survives a restart while
//! one of them waits for input, and plans within budget. Prints the graded report.
#[cfg(feature = "sqlite")]
#[path = "support/delegated_workflow.rs"]
mod workflow;

#[cfg(feature = "sqlite")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let directory = tempfile::tempdir()?;
    let report = workflow::run(&directory.path().join("delegated.sqlite")).await?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

#[cfg(not(feature = "sqlite"))]
fn main() {
    eprintln!("This example requires the sqlite feature.");
}
