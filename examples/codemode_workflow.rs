//! A planner whose model answers with one script instead of a turn per tool call: it looks up
//! three cities in parallel, reads the forecast of the warmest, and books it, which needs
//! approval. The run waits for that approval, then the script goes on from where it stopped and
//! returns its plan. Prints the script's result and what each call cost.
use agent_rs::codemode::CodeMode;
use agent_rs::{
    Agent, Conversation, Error, HistoryEntry, ModelResponse, Resolution, RetryPolicy, RunControl,
    ScriptedModel, ToolCall, ToolOutput, ToolSpec, typed_tool,
};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::{Value, json};
use std::sync::Arc;

#[derive(Deserialize, JsonSchema)]
struct City {
    /// The city's name.
    city: String,
}

const SCRIPT: &str = r#"
const cities = ["Lisbon", "Oslo", "Cairo"];
const temperatures = await Promise.all(cities.map((city) => temperature({ city })));
const warmest = cities[temperatures.indexOf(Math.max(...temperatures))];
const forecast = await forecast_for({ city: warmest });
console.log(`warmest: ${warmest}, ${forecast}`);
const booking = await book({ city: warmest });
return { warmest, forecast, booking };
"#;

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let temperature = typed_tool::<City, u32, _, _>(
        ToolSpec::new("temperature", "1")
            .with_description("Today's high in degrees Celsius.")
            .with_retry(RetryPolicy::ReadOnly),
        |input, _| async move {
            let high = match input.city.as_str() {
                "Lisbon" => 24,
                "Oslo" => 11,
                "Cairo" => 33,
                _ => 0,
            };
            Ok(ToolOutput::Complete(high))
        },
    );
    let forecast = typed_tool::<City, String, _, _>(
        ToolSpec::new("forecast-for", "1")
            .with_description("Tomorrow's forecast.")
            .with_retry(RetryPolicy::ReadOnly),
        |input, _| async move { Ok(ToolOutput::Complete(format!("sunny in {}", input.city))) },
    );
    let book = typed_tool::<City, Value, _, _>(
        ToolSpec::new("book", "1")
            .with_description("Books a trip. Requires approval.")
            .with_requires_approval(true),
        |input, context| async move {
            Ok(ToolOutput::Complete(
                json!({"city": input.city, "reference": context.idempotency_key}),
            ))
        },
    );
    let code = CodeMode::new("code")
        .tools([temperature, forecast, book])
        .build()?;
    println!("The model is told:\n\n{}", code.spec().description);

    let planner = Agent::builder(
        "planner",
        Arc::new(ScriptedModel::new(vec![
            ModelResponse::tools(vec![ToolCall {
                id: "plan".into(),
                name: "code".into(),
                arguments: json!({ "code": SCRIPT }),
            }]),
            ModelResponse::final_output(json!("Booked the warmest city.")),
        ])),
    )
    .instructions("Plan a trip with one script.")
    .subagents(1, [code])
    .build()?;

    let mut conversation = Conversation::default();
    match planner
        .run(&mut conversation, json!("Plan a warm trip"))
        .await
    {
        Err(Error::Waiting) => {}
        other => return Err(format!("expected to wait for approval, got {other:?}").into()),
    }
    // The script stopped at `book`, a call of its child run awaiting approval.
    let run = conversation.last_run.as_mut().expect("retained");
    let child = &run.children[0].state;
    let waiting = &child.operations[0];
    println!(
        "waiting for approval of {} {}",
        waiting.call.name, waiting.call.arguments
    );
    let child_id = child.id;
    run.resolve_child(
        child_id,
        &waiting.call.id.clone(),
        Resolution::Approve(true),
    )?;
    let answer = planner
        .resume(&mut conversation, RunControl::default())
        .await?;

    let run = conversation.last_run.as_ref().expect("retained");
    let HistoryEntry::Exchange { results, .. } = &run.history[1] else {
        return Err("the delegation's result is missing".into());
    };
    println!(
        "script result: {}",
        serde_json::to_string_pretty(&results[0].value)?
    );
    let child = &run.children[0].state;
    println!(
        "the script made {} calls in {} rounds; the parent made {} model calls of its own",
        child.usage.tool_calls,
        child.usage.model_calls - 1,
        run.usage.model_calls - run.child_usage.model_calls,
    );
    println!("answer: {answer}");
    Ok(())
}
