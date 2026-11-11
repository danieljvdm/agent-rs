//! Personal-agent workflow evaluation with outcome checks. Default mode is deterministic and offline.
#[cfg(feature = "sqlite")]
#[path = "support/personal_workflow.rs"]
mod personal_workflow;

#[cfg(feature = "sqlite")]
#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use personal_workflow::*;
    let mut mode = Mode::Scripted;
    let mut api = ProviderApi::ChatCompletions;
    let mut model = DEFAULT_MODEL.to_owned();
    let mut trials = 1;
    let mut output = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--mode" => {
                mode = match args.next().as_deref() {
                    Some("scripted") => Mode::Scripted,
                    Some("live") => Mode::Live,
                    _ => return Err("mode must be scripted or live".into()),
                }
            }
            "--api" => {
                api = match args.next().as_deref() {
                    Some("chat-completions") => ProviderApi::ChatCompletions,
                    Some("responses") => ProviderApi::Responses,
                    _ => return Err("api must be chat-completions or responses".into()),
                }
            }
            "--model" => model = args.next().ok_or("missing model")?,
            "--trials" => trials = args.next().ok_or("missing trial count")?.parse()?,
            "--out" => {
                output = Some(std::path::PathBuf::from(
                    args.next().ok_or("missing output directory")?,
                ))
            }
            "--help" => {
                println!(
                    "personal_workflow_eval --mode scripted|live --api chat-completions|responses --model {DEFAULT_MODEL} --trials 1 --out NEW_DIRECTORY"
                );
                return Ok(());
            }
            _ => return Err(format!("unknown argument: {arg}").into()),
        }
    }
    let report = run_suite(SuiteOptions {
        mode,
        api,
        model,
        trials,
        output: output.ok_or("--out NEW_DIRECTORY is required")?,
    })
    .await?;
    if !report.passed {
        return Err("one or more workflow evaluations failed; see report.json".into());
    }
    Ok(())
}
#[cfg(not(feature = "sqlite"))]
fn main() {
    eprintln!("Enable sqlite to run the personal workflow evaluation.");
    std::process::exit(2);
}
