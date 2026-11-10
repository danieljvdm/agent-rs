//! An inspectable local workflow: approval → input → time → uncertain mutation → reconciliation,
//! across SQLite restarts. No credentials or network access are needed.
#[cfg(feature = "sqlite")]
#[path = "support/durable_workflow.rs"]
mod workflow;

#[cfg(feature = "sqlite")]
#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> yielded_agent::Result<()> {
    use yielded_agent::{Error, sqlite::SqliteStore};
    let temporary = tempfile::tempdir().map_err(Error::storage)?;
    let path = std::env::args()
        .nth(1)
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| temporary.path().join("workflow.sqlite"));
    println!("Using {}", path.display());
    workflow::run(
        || SqliteStore::open(&path),
        async |store: SqliteStore| store.close().await,
    )
    .await
}
#[cfg(not(feature = "sqlite"))]
fn main() {
    eprintln!("Enable the sqlite feature to run this example.");
}
