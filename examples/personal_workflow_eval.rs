//! Personal-agent workflow evaluation with outcome checks. Default mode is deterministic and offline.
#[cfg(all(feature = "sqlite", feature = "google-calendar"))]
#[path = "support/calendar_fixture.rs"]
mod calendar_fixture;
#[cfg(all(feature = "sqlite", feature = "google-calendar"))]
#[path = "support/calendar_services.rs"]
mod calendar_services;
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
    let mut services = "files".to_owned();
    let mut allow_service_writes = false;
    let mut cleanup = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--services" => services = args.next().ok_or("missing services backend")?,
            "--allow-service-writes" => allow_service_writes = true,
            "--cleanup-sandbox" => {
                cleanup = Some(std::path::PathBuf::from(
                    args.next().ok_or("missing sandbox manifest")?,
                ))
            }
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
                    "personal_workflow_eval --mode scripted|live --api chat-completions|responses --model {DEFAULT_MODEL} --trials 1 --out NEW_DIRECTORY [--services files|loopback|google-calendar] [--allow-service-writes]\nCleanup: --cleanup-sandbox PATH --allow-service-writes"
                );
                return Ok(());
            }
            _ => return Err(format!("unknown argument: {arg}").into()),
        }
    }
    if services != "files" || cleanup.is_some() {
        #[cfg(feature = "google-calendar")]
        {
            use calendar_services::*;
            if let Some(path) = cleanup {
                cleanup_sandbox(&path, allow_service_writes).await?;
                return Ok(());
            }
            let backend = match services.as_str() {
                "loopback" => ServiceBackend::Loopback,
                "google-calendar" => ServiceBackend::GoogleCalendar,
                _ => return Err("services must be files, loopback or google-calendar".into()),
            };
            let report = run_service_suite(ServiceSuiteOptions {
                workflow: SuiteOptions {
                    mode,
                    api,
                    model,
                    trials,
                    output: output.ok_or("--out NEW_DIRECTORY is required")?,
                },
                backend,
                allow_service_writes,
            })
            .await?;
            if !report.passed {
                return Err("one or more service evaluations failed; see report.json".into());
            }
            return Ok(());
        }
        #[cfg(not(feature = "google-calendar"))]
        {
            let _ = allow_service_writes;
            return Err("service evaluation requires --features google-calendar".into());
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
