//! A bounded resident scheduler over any [`DurableStore`]. Durable discovery is authoritative;
//! notifications only reduce latency. Call `shutdown().await` before releasing resources.
use crate::*;
use async_trait::async_trait;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    sync::{Notify, mpsc},
    task::{JoinHandle, JoinSet},
};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct HostOptions {
    pub max_concurrent_runs: usize,
    pub poll_interval: Duration,
    pub events: Option<mpsc::Sender<ProgressEvent>>,
}
impl Default for HostOptions {
    fn default() -> Self {
        Self {
            max_concurrent_runs: 16,
            poll_interval: Duration::from_millis(100),
            events: None,
        }
    }
}
/// Commits through the host's store. The wrapper lets an unsized store reach the engine's
/// `&dyn Checkpoint`, which a generic parameter cannot upcast to directly.
struct StoreCheckpoint<'a, S: ?Sized>(&'a S);
#[async_trait]
impl<S: DurableStore + ?Sized> Checkpoint for StoreCheckpoint<'_, S> {
    async fn commit(&self, state: &mut RunState) -> Result<()> {
        self.0.commit(state).await
    }
}

/// The resident host for one store. With a concrete `S`, store-specific extensions can add
/// host methods through `impl<S: DurableStore + Extension> Host<S>`, and `host.store()`
/// reaches the store's own API. `Host<dyn DurableStore>` selects storage at run time.
pub struct Host<S: DurableStore + ?Sized> {
    store: Arc<S>,
    agents: Arc<HashMap<String, Agent>>,
    shutdown: CancellationToken,
    notify: Arc<Notify>,
    running: Arc<Mutex<HashMap<RunId, CancellationToken>>>,
    fatal_error: Arc<Mutex<Option<String>>>,
    task: Option<JoinHandle<Result<()>>>,
}
impl<S: DurableStore + ?Sized + 'static> Host<S> {
    pub fn start(store: Arc<S>, agent: Agent, options: HostOptions) -> Result<Self> {
        Self::start_agents(store, [agent], options)
    }
    /// Host several agents over one store. Each claimed run executes with the agent whose ID
    /// it recorded at acceptance; a run whose agent is not registered here keeps a visible
    /// `Incompatible` error until a host with that agent retries it.
    pub fn start_agents(
        store: Arc<S>,
        agents: impl IntoIterator<Item = Agent>,
        options: HostOptions,
    ) -> Result<Self> {
        if options.max_concurrent_runs == 0
            || options.max_concurrent_runs > 1024
            || options.poll_interval.is_zero()
        {
            return Err(Error::Invalid(
                "host requires 1..=1024 run workers and a nonzero discovery interval".into(),
            ));
        }
        let mut registered = HashMap::new();
        for agent in agents {
            if registered.insert(agent.id().to_owned(), agent).is_some() {
                return Err(Error::Invalid("host agent IDs must be unique".into()));
            }
        }
        if registered.is_empty() {
            return Err(Error::Invalid("host requires at least one agent".into()));
        }
        let agents = Arc::new(registered);
        let lease = store.lease()?;
        let shutdown = CancellationToken::new();
        let notify = Arc::new(Notify::new());
        let running = Arc::new(Mutex::new(HashMap::new()));
        let fatal_error = Arc::new(Mutex::new(None));
        let task = {
            let store = store.clone();
            let agents = agents.clone();
            let shutdown = shutdown.clone();
            let notify = notify.clone();
            let running = running.clone();
            let fatal_error = fatal_error.clone();
            tokio::spawn(async move {
                let _lease = lease;
                let result = schedule(store, agents, options, shutdown, notify, running).await;
                if let Err(error) = &result {
                    *fatal_error.lock().expect("host status lock") = Some(error.to_string());
                }
                result
            })
        };
        Ok(Self {
            store,
            agents,
            shutdown,
            notify,
            running,
            fatal_error,
            task: Some(task),
        })
    }
    pub fn store(&self) -> &Arc<S> {
        &self.store
    }
    pub fn health(&self) -> Result<()> {
        if let Some(error) = self.fatal_error.lock().expect("host status lock").as_ref() {
            return Err(Error::Storage(error.clone()));
        }
        if self.shutdown.is_cancelled() {
            return Err(Error::Interrupted);
        }
        Ok(())
    }
    /// Submit to the host's only agent. A host with several agents requires `submit_to`.
    pub async fn submit(
        &self,
        conversation: &str,
        key: &str,
        input: serde_json::Value,
        expires_at_ms: Option<u64>,
    ) -> Result<RunId> {
        let mut agents = self.agents.values();
        let (Some(agent), None) = (agents.next(), agents.next()) else {
            return Err(Error::Invalid(
                "host has several agents; submit with submit_to".into(),
            ));
        };
        self.accept(agent, conversation, key, input, expires_at_ms)
            .await
    }
    /// Submit to the registered agent with ID `agent_id`.
    pub async fn submit_to(
        &self,
        agent_id: &str,
        conversation: &str,
        key: &str,
        input: serde_json::Value,
        expires_at_ms: Option<u64>,
    ) -> Result<RunId> {
        let agent = self
            .agents
            .get(agent_id)
            .ok_or_else(|| Error::Invalid(format!("agent {agent_id} is not registered")))?;
        self.accept(agent, conversation, key, input, expires_at_ms)
            .await
    }
    async fn accept(
        &self,
        agent: &Agent,
        conversation: &str,
        key: &str,
        input: serde_json::Value,
        expires_at_ms: Option<u64>,
    ) -> Result<RunId> {
        self.health()?;
        let id = self
            .store
            .accept(agent, conversation, key, input, expires_at_ms)
            .await?;
        self.notify.notify_one();
        Ok(id)
    }
    pub async fn status(&self, id: RunId) -> Result<RunState> {
        self.store.status(id).await
    }
    pub async fn resolve(&self, id: RunId, call_id: &str, resolution: Resolution) -> Result<()> {
        self.store.resolve(id, call_id, resolution).await?;
        self.notify.notify_one();
        Ok(())
    }
    pub async fn cancel(&self, id: RunId) -> Result<()> {
        // Persist first. A running worker's next commit will be fenced even if its token
        // notification is delayed, or it completes before this method observes the map.
        self.store.request_cancel(id).await?;
        if let Some(cancel) = self.running.lock().expect("running map lock").get(&id) {
            cancel.cancel();
        }
        self.notify.notify_one();
        Ok(())
    }
    pub async fn retry(&self, id: RunId) -> Result<()> {
        self.store.retry(id).await?;
        self.notify.notify_one();
        Ok(())
    }
    /// Stop admission and ask active operations to clean up. Accepted work is retained,
    /// and all owned execution tasks are joined before returning. This is not cancellation.
    pub async fn shutdown(&mut self) -> Result<()> {
        self.shutdown.cancel();
        if let Some(task) = self.task.take() {
            task.await
                .map_err(|e| Error::Storage(format!("host task: {e}")))??;
        }
        Ok(())
    }
}
impl<S: DurableStore + ?Sized> Drop for Host<S> {
    fn drop(&mut self) {
        // Rust has no asynchronous Drop. Explicit shutdown is the cleanup completion barrier.
        self.shutdown.cancel();
    }
}
async fn schedule<S: DurableStore + ?Sized + 'static>(
    store: Arc<S>,
    agents: Arc<HashMap<String, Agent>>,
    options: HostOptions,
    shutdown: CancellationToken,
    notify: Arc<Notify>,
    running: Arc<Mutex<HashMap<RunId, CancellationToken>>>,
) -> Result<()> {
    let mut tasks = JoinSet::new();
    let mut tick = tokio::time::interval(options.poll_interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut scheduler_error = None;
    let mut recovery_after = None;
    let mut recovery_finished = false;
    loop {
        if shutdown.is_cancelled() {
            break;
        }
        let available = options.max_concurrent_runs.saturating_sub(tasks.len());
        let mut due = Vec::new();
        if !recovery_finished && available > 0 {
            match store.recovery_page(recovery_after, available).await {
                Ok(ids) => {
                    recovery_finished = ids.len() < available;
                    if let Some(last) = ids.last() {
                        recovery_after = Some(*last);
                    }
                    due.extend(ids);
                }
                Err(error) => {
                    scheduler_error = Some(error);
                    shutdown.cancel();
                    break;
                }
            }
        }
        match store.due(now_ms(), options.max_concurrent_runs * 2).await {
            Ok(ids) => due.extend(ids),
            Err(error) => {
                scheduler_error = Some(error);
                shutdown.cancel();
                break;
            }
        }
        for id in due {
            if tasks.len() >= options.max_concurrent_runs || shutdown.is_cancelled() {
                break;
            }
            let cancel = CancellationToken::new();
            {
                let mut map = running.lock().expect("running map lock");
                if map.contains_key(&id) {
                    continue;
                }
                map.insert(id, cancel.clone());
            }
            let store = store.clone();
            let agents = agents.clone();
            let control = RunControl {
                cancel,
                shutdown: shutdown.clone(),
                events: options.events.clone(),
            };
            tasks.spawn(async move {
                let mut position = None;
                let result = async {
                    position = Some(store.position(id).await?);
                    let mut state = store.claim(id).await?;
                    position = Some(CheckpointPosition::from(&state));
                    let Some(agent) = agents.get(&state.agent_id) else {
                        return Err(Error::Incompatible(format!(
                            "agent {} is not registered with this host",
                            state.agent_id
                        )));
                    };
                    let result = Engine::new(agent.clone())
                        .drive(&mut state, &StoreCheckpoint(store.as_ref()), control)
                        .await;
                    position = Some(CheckpointPosition::from(&state));
                    result
                }
                .await;
                (id, position, result)
            });
        }
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = notify.notified() => (),
            _ = tick.tick() => (),
            joined = tasks.join_next(), if !tasks.is_empty() => {
                match joined {
                    Some(Ok((id, position, result))) => {
                        running.lock().expect("running map lock").remove(&id);
                        if let Err(error) = result
                            && !matches!(error, Error::Stale | Error::Interrupted)
                            && let Some(position) = position
                            && let Err(error) = store.record_error(id, position, error.to_string()).await
                        {
                            scheduler_error = Some(error); shutdown.cancel(); break;
                        }
                    },
                    Some(Err(error)) => {
                        scheduler_error = Some(Error::Storage(format!("execution task panicked: {error}")));
                        shutdown.cancel(); break;
                    },
                    None => (),
                }
            }
        }
    }
    // The same stop token reaches every operation. Join rather than abort: engine cleanup
    // runs before its future returns. Uncommitted intent is recovered at the next startup.
    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok((id, position, result)) => {
                running.lock().expect("running map lock").remove(&id);
                if let Err(error) = result
                    && !matches!(error, Error::Stale | Error::Interrupted)
                    && let Some(position) = position
                {
                    let _ = store.record_error(id, position, error.to_string()).await;
                }
            }
            Err(error) => {
                scheduler_error.get_or_insert_with(|| {
                    Error::Storage(format!("execution task panicked: {error}"))
                });
            }
        }
    }
    running.lock().expect("running map lock").clear();
    match scheduler_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}
