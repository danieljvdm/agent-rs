//! Deterministic consumer fixture: a planner delegates to two venue researchers, one of which
//! asks for input. The run waits, SQLite is closed and reopened, the child's question is
//! answered, and the planner chooses a venue within budget.
use async_trait::async_trait;
use futures::stream;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio_util::sync::CancellationToken;
use yielded_agent::{sqlite::SqliteStore, *};

pub const FIXTURE: &str = "attached-venue-research-v1";
type Failure = Box<dyn std::error::Error + Send + Sync>;

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ResearchTask {
    venue: String,
    confirm_access: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Venue {
    pub name: String,
    pub cost: u32,
    pub indoors: bool,
    pub source: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Plan {
    pub selected: String,
    pub sources: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct Report {
    pub fixture: &'static str,
    pub plan: Plan,
    pub parent_run_id: RunId,
    pub child_run_ids: Vec<RunId>,
    pub lookups: usize,
    pub completed_child_reused: bool,
    pub usage: Usage,
}

fn spec(name: &str) -> ToolSpec {
    ToolSpec::new(name, "1")
        .with_description(name)
        .with_retry(RetryPolicy::ReadOnly)
}
fn input(request: &ModelRequest) -> &Value {
    request
        .history
        .iter()
        .find_map(|entry| match entry {
            HistoryEntry::Input(value) => Some(value),
            _ => None,
        })
        .expect("fixture input")
}
fn results(request: &ModelRequest) -> &[ToolResult] {
    match request.history.last().expect("fixture history") {
        HistoryEntry::Exchange { results, .. } => results,
        _ => panic!("fixture requires a completed exchange"),
    }
}
fn response(response: ModelResponse) -> Result<ModelStream> {
    Ok(Box::pin(stream::iter([Ok(ModelEvent::Response(response))])))
}

/// Looks the venue up, asks about access when told to, then reports what it found.
struct ResearchModel;
#[async_trait]
impl Model for ResearchModel {
    async fn stream(&self, request: ModelRequest, _: CancellationToken) -> Result<ModelStream> {
        let task: ResearchTask = serde_json::from_value(input(&request).clone()).unwrap();
        if request.turn == 1 {
            let mut tools = vec![ToolCall {
                id: "lookup".into(),
                name: "lookup_venue".into(),
                arguments: json!(task.venue),
            }];
            if task.confirm_access {
                tools.push(ToolCall {
                    id: "access".into(),
                    name: "confirm_access".into(),
                    arguments: json!("Do you have a library card?"),
                });
            }
            return response(ModelResponse::tools(tools));
        }
        let results = results(&request);
        if task.confirm_access {
            let access = results.iter().find(|r| r.call_id == "access").unwrap();
            assert_eq!(access.value, json!(true));
        }
        let lookup = results.iter().find(|r| r.call_id == "lookup").unwrap();
        response(ModelResponse::final_output(lookup.value.clone()))
    }
}
/// Delegates research on two venues, then picks the first indoor one within budget.
struct PlannerModel;
#[async_trait]
impl Model for PlannerModel {
    async fn stream(&self, request: ModelRequest, _: CancellationToken) -> Result<ModelStream> {
        if request.turn == 1 {
            let delegations =
                [("museum", false), ("library", true)].map(|(venue, confirm)| ToolCall {
                    id: venue.into(),
                    name: "research_venue".into(),
                    arguments: json!({"venue": venue, "confirm_access": confirm}),
                });
            return response(ModelResponse::tools(delegations.to_vec()));
        }
        let max_cost: u32 = serde_json::from_value(input(&request).clone()).unwrap();
        let venues: Vec<Venue> = results(&request)
            .iter()
            .map(|result| {
                assert!(!result.is_error);
                let child: ChildResult<Venue> =
                    serde_json::from_value(result.value.clone()).unwrap();
                assert_eq!(child.parent_run_id, request.run_id);
                match child.outcome {
                    ChildOutcome::Completed(venue) => venue,
                    _ => panic!("research failed"),
                }
            })
            .collect();
        let selected = venues
            .iter()
            .find(|venue| venue.indoors && venue.cost <= max_cost)
            .expect("suitable fixture venue")
            .name
            .clone();
        let plan = Plan {
            selected,
            sources: venues.iter().map(|venue| venue.source.clone()).collect(),
        };
        response(ModelResponse::final_output(
            serde_json::to_value(plan).unwrap(),
        ))
    }
}

fn agent(lookups: Arc<AtomicUsize>) -> Result<Agent<u32, Plan>> {
    let lookup = typed_tool::<String, Venue, _, _>(spec("lookup_venue"), move |name, _| {
        lookups.fetch_add(1, Ordering::SeqCst);
        async move {
            let venue = match name.as_str() {
                "museum" => Venue {
                    name: "Atrium Museum".into(),
                    cost: 20,
                    indoors: true,
                    source: "fixture://museum".into(),
                },
                "library" => Venue {
                    name: "Harbor Library".into(),
                    cost: 0,
                    indoors: true,
                    source: "fixture://library".into(),
                },
                _ => return Err(Error::tool("unknown fixture venue")),
            };
            Ok(ToolOutput::Complete(venue))
        }
    });
    let access = typed_tool::<String, bool, _, _>(spec("confirm_access"), |prompt, _| async move {
        Ok(ToolOutput::Wait(WaitRequest::Input { prompt }))
    });
    let researcher = Agent::builder("venue-research-v1", Arc::new(ResearchModel))
        .instructions("Research only the explicit venue; ask for access if requested.")
        .tools([lookup, access])
        .limits(
            Limits::default()
                .with_max_model_calls(3)
                .with_max_tool_calls(3)
                .with_max_tokens(1_000)
                .with_active_budget(Duration::from_secs(10))
                .with_operation_timeout(Duration::from_secs(1))
                .with_cleanup_timeout(Duration::from_millis(100)),
        )
        .input::<ResearchTask>()
        .output::<Venue>()
        .build()?;
    Agent::builder("planner-v1", Arc::new(PlannerModel))
        .instructions("Choose an indoor venue within the supplied budget from the research.")
        .input::<u32>()
        .output::<Plan>()
        .subagents(
            2,
            [Subagent::new(
                "research_venue",
                "Research one venue and return its sourced facts.",
                researcher,
            )],
        )
        .build()
}

pub async fn run(path: &Path) -> std::result::Result<Report, Failure> {
    let lookups = Arc::new(AtomicUsize::new(0));
    let agent = agent(lookups.clone())?;
    let store = SqliteStore::open(path).await?;
    let id = store
        .accept(&agent.erased(), "afternoon", "request-1", json!(10), None)
        .await?;
    let mut waiting = store.claim(id).await?;
    agent
        .drive(&mut waiting, &store, RunControl::default())
        .await?;
    // The museum researcher finished; the library researcher asks a question.
    let completed = waiting
        .children
        .iter()
        .find(|child| child.state.is_terminal())
        .expect("museum completed");
    let completed_id = completed.state.id;
    let completed_before = serde_json::to_value(&completed.state)?;
    let pending = waiting
        .children
        .iter()
        .find(|child| !child.state.is_terminal())
        .expect("library waiting")
        .state
        .id;
    assert_eq!(waiting.wake_at(), None, "a waiting child holds no worker");
    store.close().await?;

    let store = Arc::new(SqliteStore::open(path).await?);
    let options = HostOptions::default()
        .with_max_concurrent_runs(1)
        .with_poll_interval(Duration::from_millis(2));
    let mut host = Host::start(store.clone(), agent, options)?;
    host.resolve_child(id, pending, "access", Resolution::Input(json!(true)))
        .await?;
    let finished = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            host.health()?;
            let state = host.status(id).await?;
            if state.is_terminal() {
                return Ok::<_, Error>(state);
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .map_err(|_| "delegated workflow timed out")??;
    host.shutdown().await?;
    store.close().await?;
    let Phase::Settled(Settlement::Completed(value)) = &finished.phase else {
        return Err(format!("unexpected settlement: {:?}", finished.phase).into());
    };
    let reused = finished.child(completed_id).ok_or("museum child missing")?;
    let report = Report {
        fixture: FIXTURE,
        plan: serde_json::from_value(value.clone())?,
        parent_run_id: id,
        child_run_ids: finished.children.iter().map(|c| c.state.id).collect(),
        lookups: lookups.load(Ordering::SeqCst),
        completed_child_reused: serde_json::to_value(&reused.state)? == completed_before,
        usage: finished.usage,
    };
    grade(&report)?;
    Ok(report)
}

/// The fixture's contract: the right venue from both sources, two distinct children, one lookup
/// each, the completed child reused after the restart, and exactly the work the script needs.
pub fn grade(report: &Report) -> std::result::Result<(), String> {
    if report.plan.selected != "Harbor Library"
        || report.plan.sources != ["fixture://museum", "fixture://library"]
        || report.child_run_ids.len() != 2
        || report.child_run_ids[0] == report.child_run_ids[1]
        || report.lookups != 2
        || !report.completed_child_reused
        || report.usage.model_calls != 6
        || report.usage.tool_calls != 5
    {
        return Err("delegated workflow failed its result, recovery or work-count contract".into());
    }
    Ok(())
}
