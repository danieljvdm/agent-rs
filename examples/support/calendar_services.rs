//! Application composition for opt-in live services and the deterministic HTTP eval.
#![allow(dead_code)]

use super::{
    calendar_fixture::{CalendarFixture, source_events},
    personal_workflow::*,
};
use chrono::{DateTime, Duration as TimeDelta};
use reqwest::{
    Client, Method, StatusCode, Url,
    header::{AUTHORIZATION, HeaderValue},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use yielded_agent::{
    Error, Result,
    google_calendar::{CalendarScope, GoogleCalendar},
};

/// The service suite's own fixture: the provider events, the lost-write case and the oracle. The
/// report records it beside the workflow's [`FIXTURE_VERSION`], whose cases it runs.
pub const SERVICE_FIXTURE_VERSION: &str = "personal-calendar-service-v2";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ServiceBackend {
    Loopback,
    GoogleCalendar,
}
pub struct ServiceSuiteOptions {
    pub workflow: SuiteOptions,
    pub backend: ServiceBackend,
    pub allow_service_writes: bool,
}
#[derive(Serialize)]
pub struct ServiceAttempt {
    pub scenario: String,
    pub workflow: Option<AttemptReport>,
    pub provider_events: Vec<Value>,
    pub provider_checks: Vec<Check>,
    pub http_requests: Option<Vec<super::calendar_fixture::RequestRecord>>,
    pub error: Option<String>,
    pub cleanup_passed: bool,
    pub passed: bool,
}
#[derive(Serialize)]
pub struct ServiceSuiteReport {
    pub fixture: String,
    pub service_fixture: String,
    pub backend: ServiceBackend,
    pub mode: Mode,
    pub api: Option<ProviderApi>,
    pub model: String,
    pub revision: Option<String>,
    pub source_blobs: Vec<(String, Option<String>)>,
    pub started_at_ms: u64,
    pub completed: bool,
    pub passed: bool,
    pub attempts: Vec<ServiceAttempt>,
}

pub fn grade_provider_state(
    scenario: &Scenario,
    events: &[Value],
    workflow: Option<&AttemptReport>,
) -> Vec<Check> {
    let count = events.len() == usize::from(scenario.approve);
    let mut checks = vec![Check {
        name: "provider_effect_count".into(),
        passed: count,
        detail: format!(
            "expected {}, read {} events from the isolated reminder calendar",
            usize::from(scenario.approve),
            events.len()
        ),
    }];
    if scenario.approve {
        let correct = events.first().is_some_and(|event| {
            let expected = DateTime::parse_from_rfc3339(&scenario.expected_at()).unwrap();
            let start = event["start"]["dateTime"]
                .as_str()
                .and_then(|s| DateTime::parse_from_rfc3339(s).ok());
            let end = event["end"]["dateTime"]
                .as_str()
                .and_then(|s| DateTime::parse_from_rfc3339(s).ok());
            event["summary"] == scenario.expected_title()
                && start == Some(expected)
                && end == Some(expected + TimeDelta::minutes(1))
                && event["status"] == "confirmed"
                && event["visibility"] == "private"
                && event["transparency"] == "transparent"
                && event
                    .get("recurrence")
                    .is_none_or(|rules| rules.as_array().is_some_and(|rules| rules.is_empty()))
                && event.get("recurringEventId").is_none()
                && event.get("originalStartTime").is_none()
                && event["reminders"]
                    == json!({"useDefault":false,"overrides":[{"method":"popup","minutes":0}]})
                && event
                    .get("attendees")
                    .is_none_or(|v| v.as_array().is_some_and(|a| a.is_empty()))
                && workflow
                    .and_then(|w| w.reminders.first())
                    .is_some_and(|r| event["id"] == r.reminder_id)
        });
        checks.push(Check { name: "provider_reminder_contract".into(), passed: correct,
            detail: "raw provider event must be one-time and match the independent expected time, title, private popup settings and reported receipt".into() });
    }
    checks
}

pub async fn run_service_suite(options: ServiceSuiteOptions) -> Result<ServiceSuiteReport> {
    if options.workflow.trials == 0 || options.workflow.trials > 20 {
        return Err(invalid("trials must be 1..=20"));
    }
    let token = if options.backend == ServiceBackend::GoogleCalendar {
        if !options.allow_service_writes {
            return Err(invalid(
                "Google service evaluation requires --allow-service-writes to create and delete isolated calendars",
            ));
        }
        Some(
            std::env::var("GOOGLE_CALENDAR_ACCESS_TOKEN")
                .map_err(|_| invalid("GOOGLE_CALENDAR_ACCESS_TOKEN is required"))?,
        )
    } else {
        None
    };
    if options.workflow.mode == Mode::Live
        && std::env::var("OPENAI_API_KEY").map_or(true, |v| v.trim().is_empty())
    {
        return Err(invalid(
            "OPENAI_API_KEY is required for live model evaluation",
        ));
    }
    // The default `eval-results/` parent is Git-ignored and absent in a fresh checkout.
    if let Some(parent) = options.workflow.output.parent() {
        fs::create_dir_all(parent).map_err(storage)?;
    }
    fs::create_dir(&options.workflow.output).map_err(storage)?;
    let mut report = ServiceSuiteReport {
        fixture: FIXTURE_VERSION.into(),
        service_fixture: SERVICE_FIXTURE_VERSION.into(),
        backend: options.backend,
        mode: options.workflow.mode,
        api: (options.workflow.mode == Mode::Live).then_some(options.workflow.api),
        model: if options.workflow.mode == Mode::Live {
            options.workflow.model.clone()
        } else {
            "workflow-script-v1".into()
        },
        revision: git(&["rev-parse", "HEAD"]),
        source_blobs: [
            "src/google_calendar.rs",
            "examples/support/personal_workflow.rs",
            "examples/support/calendar_services.rs",
            "examples/support/calendar_fixture.rs",
        ]
        .into_iter()
        .map(|path| (path.into(), git(&["hash-object", path])))
        .collect(),
        started_at_ms: now_ms(),
        completed: false,
        passed: false,
        attempts: vec![],
    };
    save_json(&options.workflow.output.join("report.json"), &report)?;
    for trial in 1..=options.workflow.trials {
        let mut cases: Vec<_> = scenarios().into_iter().map(|s| (s, false)).collect();
        let mut uncertain = scenarios().remove(0);
        uncertain.name = "uncertain_write".into();
        cases.push((uncertain, true));
        for (scenario, lose_response) in cases {
            let directory = options
                .workflow
                .output
                .join(format!("{trial:02}-{}", scenario.name));
            fs::create_dir(&directory).map_err(storage)?;
            let mut sandbox = token
                .as_ref()
                .map(|token| Sandbox::new(token, directory.join("sandbox.json")))
                .transpose()?;
            let fixture = if options.backend == ServiceBackend::Loopback {
                Some(CalendarFixture::start(source_events(&scenario)))
            } else {
                None
            };
            let setup = if let Some(sandbox) = &mut sandbox {
                sandbox.prepare(&scenario).await
            } else {
                Ok(fixture.as_ref().unwrap().client())
            };
            let mut result = ServiceAttempt {
                scenario: scenario.name.clone(),
                workflow: None,
                provider_events: vec![],
                provider_checks: vec![],
                http_requests: None,
                error: None,
                cleanup_passed: false,
                passed: false,
            };
            match setup {
                Ok(client) => {
                    match run_case_with_services(
                        scenario.clone(),
                        trial,
                        &directory.join("workflow"),
                        options.workflow.mode,
                        options.workflow.api,
                        &options.workflow.model,
                        None,
                        ServiceMode::GoogleCalendar {
                            client: Arc::new(client),
                            lose_response,
                        },
                    )
                    .await
                    {
                        Ok(workflow) => result.workflow = Some(workflow),
                        Err(error) => result.error = Some(error.to_string()),
                    }
                    let observed = if let Some(sandbox) = &sandbox {
                        sandbox.reminder_events().await
                    } else {
                        Ok(fixture.as_ref().unwrap().events())
                    };
                    match observed {
                        Ok(events) => result.provider_events = events,
                        Err(error) => result.error = Some(error.to_string()),
                    }
                    result.provider_checks = grade_provider_state(
                        &scenario,
                        &result.provider_events,
                        result.workflow.as_ref(),
                    );
                }
                Err(error) => result.error = Some(error.to_string()),
            }
            // Retain raw service evidence before cleanup, including failed attempts.
            result.http_requests = fixture
                .as_ref()
                .map(|fixture| fixture.state.lock().unwrap().requests.clone());
            let saved = save_json(&directory.join("service-result.json"), &result);
            let cleaned = if let Some(sandbox) = &mut sandbox {
                sandbox.cleanup().await
            } else {
                Ok(())
            };
            result.cleanup_passed = cleaned.is_ok();
            if let Err(error) = cleaned {
                result.error = Some(error.to_string());
            }
            if let Err(error) = saved {
                result.error = Some(error.to_string());
            }
            result.passed = result.error.is_none()
                && result.cleanup_passed
                && result.workflow.as_ref().is_some_and(|w| w.passed)
                && result.provider_checks.iter().all(|c| c.passed);
            save_json(&directory.join("service-result.json"), &result)?;
            println!(
                "{} trial {trial}: {}",
                scenario.name,
                if result.passed { "PASS" } else { "FAIL" }
            );
            report.attempts.push(result);
            save_json(&options.workflow.output.join("report.json"), &report)?;
        }
    }
    report.completed = true;
    report.passed = report.attempts.iter().all(|a| a.passed);
    save_json(&options.workflow.output.join("report.json"), &report)?;
    Ok(report)
}

#[derive(Clone, Serialize, Deserialize)]
struct CalendarRecord {
    name: String,
    id: Option<String>,
    creation_attempted: bool,
    deleted: bool,
}
#[derive(Serialize, Deserialize)]
struct Manifest {
    version: u32,
    marker: String,
    calendars: Vec<CalendarRecord>,
    cleanup_errors: Vec<String>,
}
struct Sandbox {
    http: LiveHttp,
    manifest: Manifest,
    path: PathBuf,
}
impl Sandbox {
    fn new(token: &str, path: PathBuf) -> Result<Self> {
        let marker = format!("yielded-calendar-eval-{}", uuid::Uuid::new_v4());
        let calendars = ["source", "reminders"]
            .into_iter()
            .map(|purpose| CalendarRecord {
                name: format!("{marker}-{purpose}"),
                id: None,
                creation_attempted: false,
                deleted: false,
            })
            .collect();
        let sandbox = Self {
            http: LiveHttp::new(token)?,
            manifest: Manifest {
                version: 1,
                marker,
                calendars,
                cleanup_errors: vec![],
            },
            path,
        };
        sandbox.save()?;
        Ok(sandbox)
    }
    fn save(&self) -> Result<()> {
        save_json(&self.path, &self.manifest)
    }
    async fn prepare(&mut self, scenario: &Scenario) -> Result<GoogleCalendar> {
        for index in 0..2 {
            self.manifest.calendars[index].creation_attempted = true;
            self.save()?;
            let body = json!({"summary":self.manifest.calendars[index].name, "description":self.manifest.marker, "timeZone":"America/Los_Angeles"});
            let calendar = self
                .http
                .request(Method::POST, &["calendars"], Some(&body), &[])
                .await?;
            self.manifest.calendars[index].id = Some(id(&calendar)?.into());
            self.save()?;
        }
        let source = self.manifest.calendars[0].id.as_deref().unwrap();
        for mut event in source_events(scenario) {
            event["id"] = json!(uuid::Uuid::new_v4().simple().to_string());
            self.http
                .request(
                    Method::POST,
                    &["calendars", source, "events"],
                    Some(&event),
                    &[("sendUpdates", "none")],
                )
                .await?;
        }
        GoogleCalendar::new(
            &self.http.token,
            CalendarScope::new(
                source,
                self.manifest.calendars[1].id.clone().unwrap(),
                format!("{DATE}T00:00:00-08:00"),
                "2026-11-03T00:00:00-08:00",
            ),
        )
    }
    async fn reminder_events(&self) -> Result<Vec<Value>> {
        let target = self.manifest.calendars[1]
            .id
            .as_deref()
            .ok_or_else(|| service("reminder calendar was not created"))?;
        let page = self
            .http
            .request(
                Method::GET,
                &["calendars", target, "events"],
                None,
                &[("showDeleted", "false"), ("maxResults", "2500")],
            )
            .await?;
        if page.get("nextPageToken").is_some() {
            return Err(service(
                "isolated reminder calendar unexpectedly exceeds one page",
            ));
        }
        match page.get("items") {
            Some(Value::Array(items)) => Ok(items.clone()),
            None => Ok(vec![]),
            _ => Err(service("invalid provider event list")),
        }
    }
    async fn cleanup(&mut self) -> Result<()> {
        self.manifest.cleanup_errors.clear();
        for index in 0..self.manifest.calendars.len() {
            let record = self.manifest.calendars[index].clone();
            if record.deleted || !record.creation_attempted {
                continue;
            }
            let result = if let Some(id) = &record.id {
                self.delete_owned_calendar(id, &record.name).await
            } else {
                Err(service(
                    "calendar creation outcome unknown; inspect the exact recorded name before manual cleanup",
                ))
            };
            match result {
                Ok(()) => self.manifest.calendars[index].deleted = true,
                Err(error) => self
                    .manifest
                    .cleanup_errors
                    .push(format!("{}: {error}", record.name)),
            }
            // Continue cleanup of the other owned calendar even after a failure.
            if let Err(error) = self.save() {
                self.manifest.cleanup_errors.push(error.to_string());
            }
        }
        self.save()?;
        if self.manifest.cleanup_errors.is_empty() {
            Ok(())
        } else {
            Err(service(
                "sandbox cleanup incomplete; see sandbox.json and rerun --cleanup-sandbox",
            ))
        }
    }
    async fn delete_owned_calendar(&self, calendar_id: &str, name: &str) -> Result<()> {
        let calendar = self
            .http
            .request_allow_absent(Method::GET, &["calendars", calendar_id], None, &[], true)
            .await?;
        if calendar.is_null() {
            return Ok(());
        }
        if calendar["id"] != calendar_id
            || calendar["summary"] != name
            || calendar["description"] != self.manifest.marker
        {
            return Err(service(
                "refusing cleanup: calendar ownership marker differs",
            ));
        }
        self.http
            .request_allow_absent(Method::DELETE, &["calendars", calendar_id], None, &[], true)
            .await?;
        Ok(())
    }
}

/// Resume cleanup only for concrete calendar IDs with matching provider-side
/// ownership markers. This never searches for, mutates, or deletes personal events.
pub async fn cleanup_sandbox(path: &Path, allow_service_writes: bool) -> Result<()> {
    if !allow_service_writes {
        return Err(invalid("cleanup requires --allow-service-writes"));
    }
    let token = std::env::var("GOOGLE_CALENDAR_ACCESS_TOKEN")
        .map_err(|_| invalid("GOOGLE_CALENDAR_ACCESS_TOKEN is required"))?;
    let manifest: Manifest = serde_json::from_slice(&fs::read(path).map_err(storage)?)
        .map_err(|_| invalid("invalid sandbox manifest"))?;
    let suffix = manifest
        .marker
        .strip_prefix("yielded-calendar-eval-")
        .ok_or_else(|| invalid("invalid sandbox marker"))?;
    uuid::Uuid::parse_str(suffix).map_err(|_| invalid("invalid sandbox marker"))?;
    if manifest.version != 1
        || manifest.calendars.len() != 2
        || manifest
            .calendars
            .iter()
            .zip(["source", "reminders"])
            .any(|(record, purpose)| record.name != format!("{}-{purpose}", manifest.marker))
    {
        return Err(invalid("invalid sandbox ownership manifest"));
    }
    Sandbox {
        http: LiveHttp::new(&token)?,
        manifest,
        path: path.into(),
    }
    .cleanup()
    .await
}

/// Application-only provisioning client. The engine never sees this token or
/// these calendar creation/deletion capabilities.
struct LiveHttp {
    client: Client,
    authorization: HeaderValue,
    token: String,
    base: Url,
}
impl LiveHttp {
    fn new(token: &str) -> Result<Self> {
        if token.trim().is_empty() {
            return Err(config("empty Google access token"));
        }
        let mut authorization = HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|_| config("invalid access token header"))?;
        authorization.set_sensitive(true);
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(10))
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .build()
            .map_err(|_| config("could not build service client"))?;
        Ok(Self {
            client,
            authorization,
            token: token.into(),
            base: Url::parse("https://www.googleapis.com/calendar/v3/").unwrap(),
        })
    }
    async fn request(
        &self,
        method: Method,
        path: &[&str],
        body: Option<&Value>,
        query: &[(&str, &str)],
    ) -> Result<Value> {
        self.request_allow_absent(method, path, body, query, false)
            .await
    }
    async fn request_allow_absent(
        &self,
        method: Method,
        path: &[&str],
        body: Option<&Value>,
        query: &[(&str, &str)],
        allow_absent: bool,
    ) -> Result<Value> {
        let mut url = self.base.clone();
        url.path_segments_mut()
            .unwrap()
            .pop_if_empty()
            .extend(path.iter().copied());
        url.query_pairs_mut().extend_pairs(query.iter().copied());
        let mut request = self
            .client
            .request(method, url)
            .header(AUTHORIZATION, self.authorization.clone());
        if let Some(body) = body {
            request = request.json(body);
        }
        let mut response = request
            .send()
            .await
            .map_err(|_| service("service HTTP request failed; a mutation may have succeeded"))?;
        if allow_absent && matches!(response.status(), StatusCode::NOT_FOUND | StatusCode::GONE) {
            return Ok(Value::Null);
        }
        if !response.status().is_success() {
            return Err(service(&format!(
                "service HTTP status {}",
                response.status().as_u16()
            )));
        }
        if response.status() == StatusCode::NO_CONTENT {
            return Ok(Value::Null);
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| service("service response read failed"))?
        {
            if chunk.len() > 1_048_576_usize.saturating_sub(bytes.len()) {
                return Err(service("service response exceeds byte limit"));
            }
            bytes.extend_from_slice(&chunk);
        }
        let value: Value =
            serde_json::from_slice(&bytes).map_err(|_| service("invalid service JSON"))?;
        if !value.is_object() {
            return Err(service("expected a service JSON object"));
        }
        Ok(value)
    }
}
fn id(value: &Value) -> Result<&str> {
    value["id"]
        .as_str()
        .filter(|id| !id.is_empty() && *id != "primary" && id.len() <= 1024)
        .ok_or_else(|| service("invalid calendar identity"))
}
/// The eval's own arguments or opt-in are wrong.
fn invalid(message: &str) -> Error {
    Error::input(message.to_owned())
}
/// The provisioning client could not be configured.
fn config(message: &str) -> Error {
    Error::config(message.to_owned())
}
/// The live service failed or answered unexpectedly. Messages never quote its responses.
fn service(message: &str) -> Error {
    Error::tool(message.to_owned())
}
fn storage(error: impl std::fmt::Display) -> Error {
    Error::storage(error.to_string())
}
fn now_ms() -> u64 {
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH);
    u64::try_from(now.expect("clock after 1970").as_millis()).expect("milliseconds fit u64")
}
fn save_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let temporary = path.with_extension("tmp");
    let mut file = File::create(&temporary).map_err(storage)?;
    file.write_all(&serde_json::to_vec_pretty(value).map_err(storage)?)
        .map_err(storage)?;
    file.sync_all().map_err(storage)?;
    fs::rename(&temporary, path).map_err(storage)?;
    #[cfg(unix)]
    File::open(path.parent().unwrap())
        .and_then(|f| f.sync_all())
        .map_err(storage)?;
    Ok(())
}
fn git(args: &[&str]) -> Option<String> {
    std::process::Command::new("git")
        .args(args)
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::calendar_fixture::{Reply, TEST_TOKEN};

    #[tokio::test]
    async fn provider_oracle_rejects_recurring_events_for_an_approved_one_time_reminder() {
        let directory = tempfile::tempdir().unwrap();
        let scenario = scenarios().remove(0);
        let fixture = CalendarFixture::start(source_events(&scenario));
        let attempt = run_case_with_services(
            scenario.clone(),
            1,
            &directory.path().join("workflow"),
            Mode::Scripted,
            ProviderApi::ChatCompletions,
            DEFAULT_MODEL,
            None,
            ServiceMode::GoogleCalendar {
                client: Arc::new(fixture.client()),
                lose_response: false,
            },
        )
        .await
        .unwrap();
        assert!(attempt.passed);
        let original = fixture.events();
        assert!(
            grade_provider_state(&scenario, &original, Some(&attempt))
                .iter()
                .all(|c| c.passed)
        );
        for (field, value) in [
            ("recurrence", json!(["RRULE:FREQ=DAILY"])),
            ("recurringEventId", json!("another-recurring-series")),
            (
                "originalStartTime",
                json!({"dateTime":"2026-11-02T09:00:00-08:00"}),
            ),
        ] {
            let mut changed = original.clone();
            changed[0][field] = value;
            assert!(
                grade_provider_state(&scenario, &changed, Some(&attempt))
                    .iter()
                    .any(|c| c.name == "provider_reminder_contract" && !c.passed),
                "oracle accepted recurring provider state: {field}"
            );
        }
    }

    #[tokio::test]
    async fn sandbox_cleanup_checks_ownership_continues_after_failure_and_can_resume() {
        let directory = tempfile::tempdir().unwrap();
        let fixture = CalendarFixture::start(vec![]);
        let mut sandbox = Sandbox::new(TEST_TOKEN, directory.path().join("sandbox.json")).unwrap();
        sandbox.http.base = Url::parse(&fixture.base).unwrap();
        let mut source = json!({"id":"eval-source", "summary":sandbox.manifest.calendars[0].name, "description":sandbox.manifest.marker});
        let target = json!({"id":"eval-target", "summary":sandbox.manifest.calendars[1].name, "description":sandbox.manifest.marker});
        fixture.state.lock().unwrap().replies.extend([
            Reply::Json(200, source.clone()),
            Reply::Json(200, target.clone()),
            Reply::Json(200, json!({})),
            Reply::Json(200, json!({})),
        ]);
        sandbox.prepare(&scenarios()[0]).await.unwrap();
        assert!(
            sandbox
                .manifest
                .calendars
                .iter()
                .all(|c| c.creation_attempted && c.id.is_some())
        );
        // A missing target is not a successful empty-state observation.
        fixture
            .state
            .lock()
            .unwrap()
            .replies
            .push_back(Reply::Json(404, json!({})));
        assert!(sandbox.reminder_events().await.is_err());
        fixture
            .state
            .lock()
            .unwrap()
            .replies
            .push_back(Reply::Json(200, Value::Null));
        assert!(sandbox.reminder_events().await.is_err());
        source["description"] = json!("another owner's calendar");
        fixture.state.lock().unwrap().replies.extend([
            Reply::Json(200, source.clone()),
            Reply::Json(200, target),
            Reply::Raw(204, String::new()),
        ]);
        assert!(sandbox.cleanup().await.is_err());
        assert!(!sandbox.manifest.calendars[0].deleted);
        assert!(sandbox.manifest.calendars[1].deleted);
        let requests = fixture.state.lock().unwrap().requests.clone();
        let deleted: Vec<_> = requests
            .iter()
            .filter(|r| r.method == "DELETE")
            .map(|r| r.path.as_str())
            .collect();
        assert_eq!(deleted, ["/calendar/v3/calendars/eval-target"]);
        assert!(
            !fs::read_to_string(&sandbox.path)
                .unwrap()
                .contains(TEST_TOKEN)
        );
        source["description"] = json!(sandbox.manifest.marker);
        fixture
            .state
            .lock()
            .unwrap()
            .replies
            .extend([Reply::Json(200, source), Reply::Raw(204, String::new())]);
        sandbox.cleanup().await.unwrap();
        assert!(sandbox.manifest.calendars.iter().all(|c| c.deleted));
        let count = fixture.state.lock().unwrap().requests.len();
        sandbox.cleanup().await.unwrap();
        assert_eq!(fixture.state.lock().unwrap().requests.len(), count);
    }

    #[tokio::test]
    async fn uncertain_sandbox_creation_is_recorded_and_never_retried_or_guessed() {
        let directory = tempfile::tempdir().unwrap();
        let fixture = CalendarFixture::start(vec![]);
        let mut sandbox = Sandbox::new(TEST_TOKEN, directory.path().join("sandbox.json")).unwrap();
        sandbox.http.base = Url::parse(&fixture.base).unwrap();
        fixture
            .state
            .lock()
            .unwrap()
            .replies
            .push_back(Reply::Json(500, json!({"error":TEST_TOKEN})));
        assert!(sandbox.prepare(&scenarios()[0]).await.is_err());
        assert!(sandbox.cleanup().await.is_err());
        assert_eq!(fixture.state.lock().unwrap().requests.len(), 1);
        let saved = fs::read_to_string(&sandbox.path).unwrap();
        let manifest: Manifest = serde_json::from_str(&saved).unwrap();
        assert!(manifest.calendars[0].creation_attempted);
        assert!(manifest.calendars[0].id.is_none());
        assert!(!manifest.calendars[1].creation_attempted);
        assert!(!manifest.cleanup_errors.is_empty());
        assert!(!saved.contains(TEST_TOKEN));
    }

    #[tokio::test]
    async fn live_service_evaluation_requires_explicit_write_opt_in() {
        let directory = tempfile::tempdir().unwrap();
        let output = directory.path().join("must-not-exist");
        let result = run_service_suite(ServiceSuiteOptions {
            workflow: SuiteOptions {
                mode: Mode::Scripted,
                api: ProviderApi::ChatCompletions,
                model: DEFAULT_MODEL.into(),
                trials: 1,
                output: output.clone(),
            },
            backend: ServiceBackend::GoogleCalendar,
            allow_service_writes: false,
        })
        .await;
        assert!(
            matches!(result, Err(Error::Input(message)) if message.to_string().contains("--allow-service-writes"))
        );
        assert!(!output.exists());
    }
}
