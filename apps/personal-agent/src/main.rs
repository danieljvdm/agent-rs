use std::{net::SocketAddr, path::PathBuf, sync::Arc};
use yielded_agent::{ModelConfig, openai::OpenAiModel};
use yielded_personal_agent::{AccessToken, Application, notebook::Notebook, router};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.iter().any(|a| a == "--help" || a == "-h") {
        println!(
            "Usage: yielded-personal-agent [--demo]\n\nPERSONAL_AGENT_TOKEN   Required, at least 32 characters; never printed\nPERSONAL_AGENT_DIR     Data directory (default: .personal-agent)\nPERSONAL_AGENT_BIND    Loopback address (default: 127.0.0.1:3000)\nOPENAI_API_KEY        Required unless --demo\nOPENAI_MODEL          Required unless --demo\n\n--demo uses a deterministic local notebook workflow without a model service."
        );
        return Ok(());
    }
    if args.iter().any(|a| a != "--demo") {
        return Err("unknown argument; use --help".into());
    }
    let demo = args.iter().any(|a| a == "--demo");
    let token = AccessToken::new(std::env::var("PERSONAL_AGENT_TOKEN").map_err(|_| "PERSONAL_AGENT_TOKEN is required; use a randomly generated token of at least 32 characters")?)?;
    let bind: SocketAddr = std::env::var("PERSONAL_AGENT_BIND")
        .unwrap_or_else(|_| "127.0.0.1:3000".into())
        .parse()?;
    if !bind.ip().is_loopback() {
        return Err("this single-owner application binds only to loopback addresses".into());
    }
    let directory = PathBuf::from(
        std::env::var_os("PERSONAL_AGENT_DIR").unwrap_or_else(|| ".personal-agent".into()),
    );
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(&directory)?;
    let notebook = Notebook::open(directory.join("notebook.sqlite")).await?;
    let agent = if demo {
        notebook.demo_agent()?
    } else {
        let model = Arc::new(OpenAiModel::new(
            std::env::var("OPENAI_API_KEY")
                .map_err(|_| "OPENAI_API_KEY is required (or use --demo)")?,
        )?);
        let name = std::env::var("OPENAI_MODEL")
            .map_err(|_| "OPENAI_MODEL is required (or use --demo)")?;
        notebook.agent(model, ModelConfig::new(name), "personal-notebook-v1")?
    };
    let listener = tokio::net::TcpListener::bind(bind).await?;
    let address = listener.local_addr()?;
    let app = Application::open(&directory, agent).await?;
    println!(
        "Personal agent: http://{address} ({})",
        if demo { "notebook demo" } else { "live model" }
    );
    let served = axum::serve(listener, router(app.clone(), token))
        .with_graceful_shutdown(shutdown_signal())
        .await;
    let shutdown = app.shutdown().await;
    served?;
    shutdown?;
    Ok(())
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("install SIGTERM handler");
        tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = terminate.recv() => {} }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
