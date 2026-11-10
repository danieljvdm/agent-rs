#[cfg(all(feature = "memory", feature = "sqlite"))]
#[path = "support/memory_workflow.rs"]
mod memory_workflow;

#[cfg(all(feature = "memory", feature = "sqlite"))]
#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use memory_workflow::*;
    let mut mode = Mode::Scripted;
    let mut model = DEFAULT_MODEL.to_owned();
    let mut trials = 1;
    let mut history_turns = 256;
    let mut background_records = 0;
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
            "--model" => model = args.next().ok_or("missing model")?,
            "--trials" => trials = args.next().ok_or("missing trial count")?.parse()?,
            "--history-turns" => {
                history_turns = args.next().ok_or("missing history turns")?.parse()?
            }
            "--out" => {
                output = Some(std::path::PathBuf::from(
                    args.next().ok_or("missing output directory")?,
                ))
            }
            "--background-records" => {
                background_records = args
                    .next()
                    .ok_or("missing background record count")?
                    .parse()?
            }
            "--help" => {
                println!(
                    "memory_workflow_eval --mode scripted|live --model {DEFAULT_MODEL} --trials 1 --history-turns 256 --background-records 0 --out NEW_DIRECTORY"
                );
                return Ok(());
            }
            _ => return Err(format!("unknown argument: {arg}").into()),
        }
    }
    let report = run_suite(
        &output.ok_or("--out NEW_DIRECTORY is required")?,
        mode,
        &model,
        trials,
        history_turns,
        background_records,
    )
    .await?;
    println!(
        "{} paired samples; passed={}",
        report.samples.len(),
        report.passed
    );
    if !report.passed {
        return Err("one or more memory evaluations failed; see report.json".into());
    }
    Ok(())
}
#[cfg(not(all(feature = "memory", feature = "sqlite")))]
fn main() {
    eprintln!("Enable memory and sqlite to run this evaluation.");
    std::process::exit(2);
}
