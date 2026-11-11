//! The durable workflow example over PostgreSQL, restarting against a fresh session at each
//! boundary. Point `AGENT_RS_TEST_POSTGRES_URL` at an isolated scratch database, or run
//! `scripts/test-postgres.sh`; it creates a dedicated schema and leaves it for inspection.
//! A remote application supplies its own verified TLS connector instead of `NoTls`.
#[path = "support/durable_workflow.rs"]
mod workflow;

use agent_rs::{
    Error, Result,
    postgres::{Config, NoTls, PostgresOptions, PostgresStore},
};

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    let config: Config = std::env::var("AGENT_RS_TEST_POSTGRES_URL")
        .map_err(|_| Error::config("set AGENT_RS_TEST_POSTGRES_URL to an isolated database"))?
        .parse()
        .map_err(Error::config)?;
    let schema = format!("workflow_{}", uuid::Uuid::new_v4().simple());
    println!("Using schema {schema}");
    workflow::run(
        || PostgresStore::connect(&config, &schema, NoTls, PostgresOptions::default()),
        async |store: PostgresStore| store.close().await,
    )
    .await
}
