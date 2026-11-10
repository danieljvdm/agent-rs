//! Run with the path to an installed @modelcontextprotocol/server-filesystem entrypoint.
use serde_json::json;
use std::sync::Arc;
use tokio::process::Command;
use tokio_util::sync::CancellationToken;
use yielded_agent::mcp::{McpClient, McpOptions, ServerIdentity, ToolPolicy};
use yielded_agent::{
    Agent, Conversation, Error, HistoryEntry, ModelResponse, Result, RetryPolicy, ScriptedModel,
    ToolCall,
};

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let entrypoint = std::env::args_os()
        .nth(1)
        .ok_or("usage: mcp_filesystem /path/to/server-filesystem/dist/index.js")?;
    let sandbox = tempfile::tempdir()?;
    let path = sandbox.path().join("hello.txt");
    std::fs::write(&path, "Hello through the existing tool boundary.")?;
    let mut command = Command::new("node");
    command.arg(entrypoint).arg(sandbox.path());
    let client = McpClient::connect(
        ServerIdentity::new("example-filesystem", "sandbox-v1"),
        command,
        McpOptions::default(),
        CancellationToken::new(),
    )
    .await?;
    let result: Result<()> = async {
        let read = client.tool(
            "read_text_file",
            "files_read",
            ToolPolicy::default()
                .with_retry(RetryPolicy::ReadOnly)
                .with_requires_approval(false),
        )?;
        let model = Arc::new(ScriptedModel::new(vec![
            ModelResponse::tools(vec![ToolCall {
                id: "read-hello".into(),
                name: "files_read".into(),
                arguments: json!({"path":path}),
            }]),
            ModelResponse::final_output(json!("done")),
        ]));
        let agent = Agent::builder("filesystem-demo", model)
            .instructions("Read the supplied sandbox file.")
            .tool(read)
            .input::<String>()
            .output::<String>()
            .build()?;
        let mut conversation = Conversation::default();
        let output = agent.run(&mut conversation, "Read hello.txt").await?;
        if output != "done" {
            return Err(Error::tool(format!("unexpected output: {output}")));
        }
        let HistoryEntry::Exchange { results, .. } = &conversation.history[1] else {
            return Err(Error::tool("missing filesystem result"));
        };
        if results[0].is_error
            || results[0].value["content"][0]["text"] != "Hello through the existing tool boundary."
        {
            return Err(Error::tool("filesystem returned an unexpected result"));
        }
        println!(
            "Discovered {} tools; validated and read a temporary file through Agent::run.",
            client.tools().len()
        );
        Ok(())
    }
    .await;
    let cleanup = client.close().await;
    result?;
    cleanup?;
    Ok(())
}
