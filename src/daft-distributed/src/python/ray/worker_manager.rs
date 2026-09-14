use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use common_error::{DaftError, DaftResult};
use common_resource_request::ResourceRequest;
use pyo3::prelude::*;

use super::{task::RayTaskResultHandle, worker::RaySwordfishWorker};
use crate::scheduling::{
    drain::{DrainBarrier, DrainState},
    scheduler::WorkerSnapshot,
    task::{SwordfishTask, TaskContext, TaskResourceRequest},
    worker::{RefreshAction, Worker, WorkerId, WorkerManager, next_refresh_action},
};

const REFRESH_INTERVAL_SECS: Duration = Duration::from_secs(5);
const DEFAULT_AUTOSCALE_INTERVAL_SECS: u64 = 5;
// Environment variable Ray itself reads to configure its autoscaler reconciliation period.
// We read the same variable so our rate-limit matches Ray's actual cycle length.
const RAY_AUTOSCALER_UPDATE_INTERVAL_ENV: &str = "AUTOSCALER_UPDATE_INTERVAL_S";

struct RayWorkerManagerState {
    ray_workers: HashMap<WorkerId, RaySwordfishWorker>,
    node_drains: HashMap<String, DrainBarrier>,
    closed: bool,
    closed_queries: std::collections::HashSet<crate::plan::QueryIdx>,
    /// When the last refresh *completed*. `None` means "refresh at the next opportunity",
    /// which is how `try_autoscale` and `retire_idle_workers` force one.
    last_refresh: Option<Instant>,
    /// Whether any refresh has ever completed. Distinct from `last_refresh`, which gets
    /// reset to `None` to force refreshes long after startup.
    initial_refresh_done: bool,
    max_resources_requested: ResourceRequest,
    pending_release_blacklist: HashMap<WorkerId, Instant>,
    last_autoscale_request_time: Option<Instant>,
    autoscale_interval_secs: Duration,
    worker_startup_timeout: usize,
}

impl RayWorkerManagerState {
    /// Whether enough time has passed since the last completed refresh. `last_refresh` is
    /// also cleared by `try_autoscale` and `retire_idle_workers` to force the next refresh
    /// to happen immediately.
    fn refresh_is_due(&self) -> bool {
        self.last_refresh
            .is_none_or(|last_time| last_time.elapsed() > REFRESH_INTERVAL_SECS)
    }

    /// Node ids `start_ray_workers` should not build an actor for: ones we already have,
    /// plus ones held back by the pending-release grace TTL so a just-retired node is not
    /// immediately respawned.
    fn nodes_to_skip(&mut self) -> Vec<String> {
        let ttl_secs: u64 = std::env::var("DAFT_AUTOSCALING_PENDING_RELEASE_EXCLUDE_SECONDS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(120);
        self.pending_release_blacklist
            .retain(|_, ts| ts.elapsed() < Duration::from_secs(ttl_secs));

        let mut ids = self
            .ray_workers
            .values()
            .filter(|worker| !worker.known_dead)
            .map(|worker| worker.node_id().to_string())
            .collect::<Vec<_>>();
        ids.extend(
            self.pending_release_blacklist
                .keys()
                .map(|id| id.as_ref().to_string()),
        );
        ids.extend(
            self.node_drains
                .iter()
                .filter(|(_, d)| d.state != DrainState::Active)
                .map(|(id, _)| id.clone()),
        );
        ids
    }

    fn install_refresh(&mut self, workers: Vec<RaySwordfishWorker>) {
        for mut worker in workers {
            if let Some(drain) = self.node_drains.get(worker.node_id()) {
                worker.drain.epoch = drain.epoch;
                worker.drain.state = drain.state;
            }
            // A refresh already in progress when an execution finishes still
            // installs its protected actors so the runtime can retire them.
            if self.closed {
                worker.drain.state = DrainState::Draining;
            }
            self.ray_workers.insert(worker.id().clone(), worker);
        }
        self.last_refresh = Some(Instant::now());
        self.initial_refresh_done = true;
    }
}

/// One completed call to `start_ray_workers`.
struct RefreshOutcome {
    workers: Vec<RaySwordfishWorker>,
    elapsed: Duration,
}

/// Tracks the refresh currently running off the scheduler thread, if any.
#[derive(Default)]
struct BackgroundRefresh {
    pending: Option<std::sync::mpsc::Receiver<DaftResult<RefreshOutcome>>>,
}

// Wrapper around the RaySwordfishWorkerManager class in the distributed_swordfish module.
pub(crate) struct RayWorkerManager {
    state: Arc<Mutex<RayWorkerManagerState>>,
    refresh: Mutex<BackgroundRefresh>,
    session: Option<Arc<Py<PyAny>>>,
}

impl RayWorkerManager {
    pub fn stop_execution(&self) {
        let mut state = self.state.lock().unwrap();
        state.closed = true;
        for worker in state.ray_workers.values_mut() {
            if !worker.active_task_details().is_empty() {
                worker.drain.unknown =
                    Some("Execution cancelled; remote termination unconfirmed".into());
            }
        }
    }

    fn check_health(&self) -> DaftResult<()> {
        if let Some(session) = &self.session {
            // A local latch updated by the Python reporting loop, never a network call.
            Python::attach(|py| session.call_method0(py, "check_health"))?;
        }
        Ok(())
    }

    pub fn usage_snapshot(&self) -> String {
        let discovery_pending = self
            .refresh
            .try_lock()
            .map_or(true, |r| r.pending.is_some());
        let state = self.state.lock().unwrap();
        serde_json::json!({
            "discovery_pending": discovery_pending || (!state.initial_refresh_done && !state.closed),
            "workers": state.ray_workers.values().map(RaySwordfishWorker::usage).collect::<Vec<_>>(),
            "closed": state.closed,
            "nodes": state.node_drains,
        }).to_string()
    }

    pub fn prepare_drain(&self, node_id: String, epoch: u64) -> DaftResult<()> {
        let mut state = self.state.lock().unwrap();
        let mut barrier = state.node_drains.get(&node_id).cloned().unwrap_or_default();
        barrier.prepare(epoch)?;
        for worker in state
            .ray_workers
            .values_mut()
            .filter(|w| w.node_id() == node_id)
        {
            worker.drain.prepare(epoch)?;
        }
        state.node_drains.insert(node_id, barrier);
        Ok(())
    }

    pub fn cancel_drain(&self, node_id: String, epoch: u64) -> DaftResult<()> {
        let mut state = self.state.lock().unwrap();
        let mut barrier = state
            .node_drains
            .get(&node_id)
            .cloned()
            .ok_or_else(|| DaftError::ValueError("Unknown drain node".into()))?;
        barrier.cancel(epoch)?;
        for worker in state
            .ray_workers
            .values_mut()
            .filter(|w| w.node_id() == node_id)
        {
            worker.drain.cancel(epoch)?;
        }
        state.node_drains.insert(node_id, barrier);
        Ok(())
    }

    pub fn retire_node(&self, node_id: String, epoch: u64) -> DaftResult<()> {
        // The refresh mutex prevents a protected actor from arriving after the
        // retirement acknowledgement. Never wait for network while holding state.
        let refresh = self.refresh.lock().unwrap();
        if refresh.pending.is_some() {
            return Err(DaftError::ValueError(
                "Worker discovery is still in progress".into(),
            ));
        }
        let workers = {
            let mut state = self.state.lock().unwrap();
            let barrier = state
                .node_drains
                .get(&node_id)
                .ok_or_else(|| DaftError::ValueError("Unknown drain node".into()))?;
            barrier.check_epoch(epoch)?;
            if barrier.state == DrainState::Retired {
                return Ok(());
            }
            let committed = barrier.state == DrainState::Retiring;
            if barrier.state != DrainState::Draining && !committed {
                return Err(DaftError::ValueError("Node is not draining".into()));
            }
            if !committed
                && state
                    .ray_workers
                    .values()
                    .filter(|w| w.node_id() == node_id)
                    .any(|w| !w.known_dead && w.drain_state() != DrainState::ReadyToRetire)
            {
                return Err(DaftError::ValueError(
                    "Node still has dependencies or unknown state".into(),
                ));
            }
            state.node_drains.get_mut(&node_id).unwrap().state = DrainState::Retiring;
            let mut workers = Vec::new();
            for worker in state
                .ray_workers
                .values_mut()
                .filter(|w| w.node_id() == node_id)
            {
                if worker.known_dead || worker.drain.state == DrainState::Retired {
                    continue;
                }
                if !committed {
                    worker
                        .drain
                        .commit(epoch, worker.active_task_details().len())?;
                }
                workers.push(worker.clone());
            }
            workers
        };
        drop(refresh);
        for worker in workers {
            let result = Python::attach(|py| worker.retire(py));
            let mut state = self.state.lock().unwrap();
            let current = state.ray_workers.get_mut(worker.id()).unwrap();
            match result {
                Ok(()) => {
                    current.drain.state = DrainState::Retired;
                    current.drain.unknown = None;
                }
                Err(e) => {
                    current.drain.unknown = Some(format!("Retirement acknowledgement failed: {e}"));
                    return Err(e.into());
                }
            }
        }
        self.state
            .lock()
            .unwrap()
            .node_drains
            .get_mut(&node_id)
            .unwrap()
            .state = DrainState::Retired;
        Ok(())
    }

    fn query_quiescent(&self, query_idx: crate::plan::QueryIdx) -> bool {
        self.state.lock().unwrap().ray_workers.values().all(|w| {
            w.drain.unknown.is_none()
                && !w
                    .active_task_details()
                    .keys()
                    .any(|t| t.query_idx == query_idx)
        })
    }

    pub fn new(worker_startup_timeout: usize, session: Option<Py<PyAny>>) -> Self {
        Self {
            refresh: Mutex::new(BackgroundRefresh::default()),
            session: session.map(Arc::new),
            state: Arc::new(Mutex::new(RayWorkerManagerState {
                ray_workers: HashMap::new(),
                node_drains: HashMap::new(),
                closed: false,
                closed_queries: Default::default(),
                last_refresh: None,
                initial_refresh_done: false,
                max_resources_requested: ResourceRequest::default(),
                pending_release_blacklist: HashMap::new(),
                last_autoscale_request_time: None,
                autoscale_interval_secs: Duration::from_secs(
                    std::env::var(RAY_AUTOSCALER_UPDATE_INTERVAL_ENV)
                        .ok()
                        .and_then(|val| val.parse::<u64>().ok())
                        .unwrap_or(DEFAULT_AUTOSCALE_INTERVAL_SECS),
                ),
                worker_startup_timeout,
            })),
        }
    }

    /// Run one refresh. Deliberately does **not** hold the state lock across the call into
    /// Python: `submit_tasks_to_workers`, `mark_task_finished` and `mark_worker_died` all
    /// take that same lock from the scheduler thread, so holding it here would just move
    /// the stall from the event loop onto the mutex.
    fn run_refresh(
        state: &Arc<Mutex<RayWorkerManagerState>>,
        session: Option<&Py<PyAny>>,
    ) -> DaftResult<RefreshOutcome> {
        let (nodes_to_skip, worker_startup_timeout) = {
            let mut state = state.lock().expect("Failed to lock RayWorkerManagerState");
            (state.nodes_to_skip(), state.worker_startup_timeout)
        };

        let started = Instant::now();
        let workers = Python::attach(|py| {
            if let Some(session) = session {
                return DaftResult::Ok(
                    session
                        .call_method1(py, "start_workers", (nodes_to_skip, worker_startup_timeout))?
                        .extract::<Vec<RaySwordfishWorker>>(py)?,
                );
            }
            let flotilla_module = py.import(pyo3::intern!(py, "daft.runners.flotilla"))?;
            DaftResult::Ok(
                flotilla_module
                    .call_method1(
                        pyo3::intern!(py, "start_ray_workers"),
                        (nodes_to_skip, worker_startup_timeout),
                    )?
                    .extract::<Vec<RaySwordfishWorker>>()?,
            )
        })?;

        DaftResult::Ok(RefreshOutcome {
            workers,
            elapsed: started.elapsed(),
        })
    }

    fn install_refresh(&self, outcome: RefreshOutcome, blocked_loop: bool) {
        let num_started = outcome.workers.len();
        if num_started > 0 {
            // `elapsed_ms` is how long `start_ray_workers` spent inside `ray.wait`, which
            // waits for *every* actor in the batch to report its address. Divided by
            // `num_started` it exposes the head-of-line blocking this refresh is subject
            // to: if the per-worker figure climbs with batch size, one slow actor is
            // holding up the ready ones and the submit/harvest split is worth doing.
            tracing::info!(
                target: "ray_worker_manager",
                num_started,
                elapsed_ms = outcome.elapsed.as_millis(),
                elapsed_ms_per_worker = outcome.elapsed.as_millis() / num_started as u128,
                blocked_loop,
                "Started new Ray workers"
            );
        } else {
            tracing::debug!(
                target: "ray_worker_manager",
                elapsed_ms = outcome.elapsed.as_millis(),
                blocked_loop,
                "Worker refresh found no new nodes"
            );
        }

        self.state
            .lock()
            .expect("Failed to lock RayWorkerManagerState")
            .install_refresh(outcome.workers);
    }

    /// Collect a finished background refresh and start the next one when due.
    ///
    /// Never blocks on `start_ray_workers` after the first call. That matters because the
    /// Python side blocks on `ray.wait` until every actor in a newly-joined batch is ready,
    /// and this runs at the top of the scheduler's event loop -- for the duration of that
    /// wait nothing is dispatched, no task results are collected, and no autoscaling
    /// request is sent, which is exactly the window during a scale-up when the scheduler
    /// has the most work to do.
    fn drive_refresh(&self) -> DaftResult<()> {
        let mut refresh = self
            .refresh
            .lock()
            .expect("Failed to lock BackgroundRefresh");

        if let Some(rx) = refresh.pending.as_ref() {
            match rx.try_recv() {
                Ok(outcome) => {
                    refresh.pending = None;
                    self.install_refresh(outcome?, false);
                }
                // Still running; leave it be.
                Err(std::sync::mpsc::TryRecvError::Empty) => {}
                // The thread died without sending (it panicked). Drop it and retry below.
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    refresh.pending = None;
                    tracing::warn!(
                        target: "ray_worker_manager",
                        "Background worker refresh thread died; retrying on the next tick"
                    );
                }
            }
        }

        let (initial_refresh_done, is_due) = {
            let state = self
                .state
                .lock()
                .expect("Failed to lock RayWorkerManagerState");
            (
                state.initial_refresh_done,
                !state.closed && state.refresh_is_due(),
            )
        };

        match next_refresh_action(initial_refresh_done, is_due, refresh.pending.is_some()) {
            RefreshAction::Wait => {}
            // The very first refresh stays synchronous: the scheduler has no workers to
            // dispatch to until it completes, and `start_ray_workers` raises when no worker
            // at all comes up on initial startup -- an error that has to reach the caller
            // rather than surface a tick later against an empty cluster.
            RefreshAction::RunSynchronously => {
                let outcome = Self::run_refresh(&self.state, self.session.as_deref())?;
                self.install_refresh(outcome, true);
            }
            RefreshAction::Spawn => {
                let (tx, rx) = std::sync::mpsc::channel();
                let state = self.state.clone();
                let session = self.session.clone();
                std::thread::Builder::new()
                    .name("daft-flotilla-worker-refresh".to_string())
                    .spawn(move || {
                        // A send failure just means the manager went away; nothing to do.
                        let _ = tx.send(Self::run_refresh(&state, session.as_deref()));
                    })
                    .map_err(|e| {
                        DaftError::InternalError(format!(
                            "Failed to spawn worker refresh thread: {e}"
                        ))
                    })?;
                refresh.pending = Some(rx);
            }
        }

        Ok(())
    }
}

impl WorkerManager for RayWorkerManager {
    type Worker = RaySwordfishWorker;

    fn submit_tasks_to_workers(
        &self,
        tasks_per_worker: HashMap<WorkerId, Vec<SwordfishTask>>,
    ) -> DaftResult<Vec<RayTaskResultHandle>> {
        self.check_health()?;
        let mut state = self
            .state
            .lock()
            .expect("Failed to lock RayWorkerManagerState");
        let mut task_result_handles =
            Vec::with_capacity(tasks_per_worker.values().map(|v| v.len()).sum());

        Python::attach(|py| {
            for (worker_id, tasks) in tasks_per_worker {
                if state.closed {
                    continue;
                }
                let tasks = tasks
                    .into_iter()
                    .filter(|task| {
                        use crate::scheduling::task::Task;
                        !state
                            .closed_queries
                            .contains(&task.task_context().query_idx)
                    })
                    .collect::<Vec<_>>();
                let Some(worker) = state.ray_workers.get_mut(&worker_id) else {
                    continue;
                };
                // Final admission and recording share the prepare/retire mutex.
                let tasks = tasks
                    .into_iter()
                    .filter(|task| worker.accepts_task(task))
                    .collect::<Vec<_>>();
                let handles = worker.submit_tasks(tasks, py)?;
                task_result_handles.extend(handles);
            }
            DaftResult::Ok(())
        })?;
        DaftResult::Ok(task_result_handles)
    }

    fn worker_snapshots(&self) -> DaftResult<Vec<WorkerSnapshot>> {
        // Kicks off / collects the refresh; only blocks on the very first call.
        self.check_health()?;
        self.drive_refresh()?;

        let state = self
            .state
            .lock()
            .expect("Failed to lock RayWorkerManagerState");
        Ok(state
            .ray_workers
            .values()
            .filter(|worker| !worker.known_dead)
            .map(WorkerSnapshot::from)
            .collect::<Vec<_>>())
    }

    fn mark_task_finished(&self, task_context: TaskContext, worker_id: WorkerId) {
        let mut state = self
            .state
            .lock()
            .expect("Failed to lock RayWorkerManagerState");
        if let Some(worker) = state.ray_workers.get_mut(&worker_id) {
            worker.mark_task_finished(&task_context);
        }
    }

    fn mark_worker_died(&self, worker_id: WorkerId) {
        let mut state = self
            .state
            .lock()
            .expect("Failed to lock RayWorkerManagerState");
        if self.session.is_some() {
            if let Some(worker) = state.ray_workers.get_mut(&worker_id) {
                worker.mark_died();
            }
            state.last_refresh = None;
        } else {
            state.ray_workers.remove(&worker_id);
        }
    }

    fn mark_task_unknown(&self, _task_context: TaskContext, worker_id: WorkerId) {
        if let Some(worker) = self.state.lock().unwrap().ray_workers.get_mut(&worker_id) {
            if !worker.known_dead {
                worker.drain.unknown = Some("remote task termination is unconfirmed".into());
            }
        }
    }

    fn shutdown(&self) -> DaftResult<()> {
        if self.session.is_some() {
            return Err(DaftError::ValueError(
                "Managed shutdown requires acknowledged node retirement".into(),
            ));
        }
        let state = self
            .state
            .lock()
            .expect("Failed to lock RayWorkerManagerState");
        Python::attach(|py| {
            for worker in state.ray_workers.values() {
                worker.shutdown(py);
            }
        });
        Ok(())
    }

    fn cleanup_shuffles(
        &self,
        dirs: Vec<String>,
        shared_dirs: Vec<String>,
        shuffle_ids: Vec<u64>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = DaftResult<()>> + Send + '_>> {
        // Issue the registry drops against the worker set as it stands now, before
        // awaiting anything: the workers that hold these registrations are the ones
        // alive at the end of the query, and a later refresh could retire them.
        // A worker that fails to take the call has lost its registry with itself.
        let unregister_refs = if shuffle_ids.is_empty() {
            Vec::new()
        } else {
            let state = self
                .state
                .lock()
                .expect("Failed to lock RayWorkerManagerState");
            Python::attach(|py| {
                state
                    .ray_workers
                    .values()
                    .filter_map(|worker| worker.unregister_shuffles(py, &shuffle_ids).ok())
                    .collect::<Vec<_>>()
            })
        };

        Box::pin(async move {
            let dirs_result = super::clear_shuffle_dirs_on_all_nodes(dirs, shared_dirs).await;
            // The registrations point at the files just deleted, so drop them even
            // if the delete partly failed — leaving them would only make stale refs
            // look answerable.
            super::await_shuffle_unregistrations(unregister_refs).await?;
            dirs_result
        })
    }

    fn finish_query(
        &self,
        query_idx: crate::plan::QueryIdx,
        dirs: Vec<String>,
        shared_dirs: Vec<String>,
        shuffle_ids: Vec<u64>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = DaftResult<()>> + Send + '_>> {
        Box::pin(async move {
            {
                let mut state = self.state.lock().unwrap();
                state.closed_queries.insert(query_idx);
                if self.session.is_some() {
                    state.closed = true;
                }
            }
            if !self.query_quiescent(query_idx) {
                return Err(DaftError::ValueError(
                    "Query cleanup blocked by unconfirmed remote work".into(),
                ));
            }
            // Strict worker-local cleanup acknowledges both registration and disk
            // removal. Keep every hold if even one RPC fails.
            let workers = self
                .state
                .lock()
                .unwrap()
                .ray_workers
                .values()
                .filter(|w| w.drain.data_queries.contains(&query_idx))
                .cloned()
                .collect::<Vec<_>>();
            let refs = Python::attach(|py| -> PyResult<Vec<Py<PyAny>>> {
                workers
                    .iter()
                    .map(|w| w.cleanup_query(py, &dirs, &shuffle_ids))
                    .collect()
            })?;
            common_runtime::python::execute_python_coroutine::<_, Py<PyAny>>(move |py| {
                py.import("daft.runners.flotilla")?
                    .call_method1("await_flight_shuffle_cleanup", (refs, shared_dirs))
            })
            .await?;
            let mut state = self.state.lock().unwrap();
            for worker in state.ray_workers.values_mut() {
                worker.drain.data_queries.remove(&query_idx);
            }
            Ok(())
        })
    }

    /// Autoscale the Ray cluster by requesting resources from Ray's autoscaler.
    ///
    /// Constraints we operate under:
    /// - There is no reliable programmatic way for Daft to know the cluster's true autoscaling
    ///   ceiling ahead of time (for example, KubeRay `maxReplicas` or other external limits).
    /// - Daft can only observe currently registered Ray workers; it cannot directly account for
    ///   capacity that has already been requested but is still provisioning.
    /// - `ray.autoscaler.sdk.request_resources(bundles=...)` is **asynchronous** and each
    ///   call **replaces** the current demand (it is not additive).
    /// - Ray's autoscaler reconciliation loop processes the request every ~5 seconds by default
    ///   (configurable via `AUTOSCALER_UPDATE_INTERVAL_S`). Calls between cycles overwrite
    ///   each other — only the latest value at reconciliation time is processed.
    /// - If the requested bundles exceed the cluster's maximum capacity (e.g., KubeRay
    ///   `maxReplicas`), the autoscaler refuses to scale **at all** — not even partially.
    /// - We cannot detect whether the Ray autoscaler accepted or rejected the request, and
    ///   observing new workers is not a reliable signal for whether a request succeeded, since
    ///   node provisioning time varies (seconds to minutes depending on the environment).
    ///
    /// Algorithm: since we cannot detect failures and don't know the cluster's max capacity,
    /// we ramp up demand gradually. In each autoscaler cycle, we send one more bundle than the
    /// previous request (tracked via `max_resources_requested` as a high-water mark). The
    /// high-water mark is floored to current cluster resources so the very first cycle
    /// immediately requests scaling beyond current capacity.
    fn try_autoscale(&self, bundles: Vec<TaskResourceRequest>) -> DaftResult<()> {
        if self.session.is_some() {
            // Fixed execution demand was registered before admission. Cluster-wide
            // high-water marks cannot be summed across managed executions.
            return self.check_health();
        }
        let mut state = self
            .state
            .lock()
            .expect("Failed to lock RayWorkerManagerState");

        // 1. Only attempt to grow the request once per Ray autoscaler reconciliation cycle.
        //    Sending more frequently would just overwrite the previous value before Ray processes it.
        if let Some(last_time) = state.last_autoscale_request_time
            && last_time.elapsed() < state.autoscale_interval_secs
        {
            return Ok(());
        }

        // 2. Floor the high-water mark to at least the current cluster's total resources.
        //    On cold start (high-water mark is 0), this lets us skip straight to requesting
        //    beyond current capacity on the very first cycle. When new workers join between
        //    cycles, this jumps the mark up so we don't waste cycles re-requesting resources
        //    the cluster already has.
        let (cluster_num_cpus, cluster_num_gpus, cluster_memory_bytes) = state
            .ray_workers
            .values()
            .fold((0.0, 0.0, 0), |acc, worker| {
                (
                    acc.0 + worker.total_num_cpus(),
                    acc.1 + worker.total_num_gpus(),
                    acc.2 + worker.total_memory_bytes(),
                )
            });
        let high_water_mark_cpus = state
            .max_resources_requested
            .num_cpus()
            .unwrap_or(0.0)
            .max(cluster_num_cpus);
        let high_water_mark_gpus = state
            .max_resources_requested
            .num_gpus()
            .unwrap_or(0.0)
            .max(cluster_num_gpus);
        let high_water_mark_memory = state
            .max_resources_requested
            .memory_bytes()
            .unwrap_or(0)
            .max(cluster_memory_bytes);

        // 3. Accumulate bundles one at a time until the running total surpasses the
        //    high-water mark in any resource dimension (CPU, GPU, or memory). This ensures
        //    each cycle's request is exactly one bundle larger than the previous max —
        //    gradual enough to avoid exceeding an unknown cluster capacity limit.
        let mut cpu_sum = 0.0;
        let mut gpu_sum = 0.0;
        let mut memory_sum = 0;
        let mut surpassed = false;
        let mut selected_bundles = Vec::new();
        for bundle in &bundles {
            cpu_sum += bundle.resource_request.num_cpus().unwrap_or(0.0);
            gpu_sum += bundle.resource_request.num_gpus().unwrap_or(0.0);
            memory_sum += bundle.resource_request.memory_bytes().unwrap_or(0);
            selected_bundles.push(bundle);
            if cpu_sum > high_water_mark_cpus
                || gpu_sum > high_water_mark_gpus
                || memory_sum > high_water_mark_memory
            {
                surpassed = true;
                break;
            }
        }

        // 4. If we went through all pending bundles without surpassing the high-water mark,
        //    the remaining demand is smaller than what we previously requested. Skip this
        //    cycle — Ray still holds our previous (larger) request, so no downscale occurs.
        if !surpassed {
            return Ok(());
        }

        // 5. Send the selected bundles to Ray's autoscaler via request_resources().
        //    Strip zero-valued GPU/memory keys so Ray doesn't interpret them as a demand
        //    for zero-resource bundles on specialized nodes.
        let python_bundles = selected_bundles
            .iter()
            .map(|bundle| {
                let mut dict = HashMap::new();
                dict.insert("CPU", bundle.num_cpus().ceil() as i64);
                let gpu = bundle.num_gpus().ceil() as i64;
                if gpu > 0 {
                    dict.insert("GPU", gpu);
                }
                let memory = bundle.memory_bytes() as i64;
                if memory > 0 {
                    dict.insert("memory", memory);
                }
                dict
            })
            .collect::<Vec<_>>();

        Python::attach(|py| -> DaftResult<()> {
            let flotilla_module = py.import(pyo3::intern!(py, "daft.runners.flotilla"))?;
            flotilla_module.call_method1(pyo3::intern!(py, "try_autoscale"), (python_bundles,))?;
            Ok(())
        })?;

        // Scaling up should immediately allow workers on recently retired nodes to be re-created,
        // and force a refresh so we can observe newly provisioned nodes quickly.
        state.pending_release_blacklist.clear();
        state.last_refresh = None;

        // 6. Record this request as the new high-water mark so the next cycle will
        //    request exactly one bundle more, and so we never send a smaller request.
        state.max_resources_requested =
            ResourceRequest::try_new_internal(Some(cpu_sum), Some(gpu_sum), Some(memory_sum))?;
        state.last_autoscale_request_time = Some(Instant::now());

        Ok(())
    }

    fn retire_idle_workers(
        &self,
        skip_due_to_pending_scale_up: bool,
        force_all_when_cluster_idle: bool,
    ) -> DaftResult<usize> {
        if self.session.is_some() {
            // Only the runtime may initiate managed retirement. In particular,
            // never clear Ray's global demand on query shutdown.
            return Ok(0);
        }
        // 1. Read downscale configuration from the environment. The worker manager owns
        //    every gating decision so the scheduler can stay backend-agnostic.
        //
        //    - `DAFT_AUTOSCALING_DOWNSCALE_ENABLED`: Enables the downscaling feature.
        //      "1" or "true" (case-insensitive) enables it. Defaults to false.
        //    - `DAFT_AUTOSCALING_MIN_SURVIVOR_WORKERS`: Minimum number of workers to keep
        //      running even if they are idle. Prevents brief idle periods from collapsing
        //      the cluster to zero. Defaults to 1.
        let downscale_enabled = std::env::var("DAFT_AUTOSCALING_DOWNSCALE_ENABLED")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        if !downscale_enabled {
            return Ok(0);
        }

        let min_survivor_workers: usize = std::env::var("DAFT_AUTOSCALING_MIN_SURVIVOR_WORKERS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(1);

        // 2. Final-shutdown sweep clears any lingering autoscaling demand even when no
        //    workers end up retired this cycle.
        if force_all_when_cluster_idle {
            Python::attach(|py| -> DaftResult<()> {
                let flotilla_module = py.import(pyo3::intern!(py, "daft.runners.flotilla"))?;
                flotilla_module.call_method0(pyo3::intern!(py, "clear_autoscaling_requests"))?;
                Ok(())
            })?;
        }

        // 3. During an active scale-up, skip downscale so we don't undo demand we just
        //    sent to Ray's autoscaler.
        if skip_due_to_pending_scale_up && !force_all_when_cluster_idle {
            return Ok(0);
        }

        // 4. Determine how many workers we are allowed to retire while honoring the
        //    `min_survivor_workers` floor. The shutdown path bypasses the floor.
        let allowed_to_retire = {
            let state = self
                .state
                .lock()
                .expect("Failed to lock RayWorkerManagerState");
            let num_workers = state.ray_workers.len();
            if force_all_when_cluster_idle {
                num_workers
            } else {
                num_workers.saturating_sub(min_survivor_workers)
            }
        };
        if allowed_to_retire == 0 {
            return Ok(0);
        }

        let idle_secs_threshold: Option<u64> = if force_all_when_cluster_idle {
            None
        } else {
            Some(
                std::env::var("DAFT_AUTOSCALING_DOWNSCALE_IDLE_SECONDS")
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(60),
            )
        };

        let now = Instant::now();

        // Determine the Ray head node id so we can avoid retiring its worker.
        let head_node_id: Option<String> = Python::attach(|py| {
            let flotilla_module = py.import(pyo3::intern!(py, "daft.runners.flotilla"))?;
            let head_id_obj =
                flotilla_module.call_method0(pyo3::intern!(py, "get_head_node_id"))?;
            let head_id = head_id_obj.extract::<Option<String>>()?;
            DaftResult::Ok(head_id)
        })?;

        let (workers_to_release, survivors_after, blacklisted_after) = {
            let mut state = self
                .state
                .lock()
                .expect("Failed to lock RayWorkerManagerState");

            let mut candidates: Vec<(WorkerId, Duration)> = state
                .ray_workers
                .iter()
                .filter_map(|(wid, w)| {
                    // Skip the head node entirely from retirement consideration.
                    if let Some(ref head_id) = head_node_id
                        && w.node_id() == head_id
                    {
                        return None;
                    }

                    if w.can_retire() {
                        let idle_for = w.idle_duration(now);
                        if let Some(threshold) = idle_secs_threshold {
                            if idle_for.as_secs() >= threshold {
                                Some((wid.clone(), idle_for))
                            } else {
                                None
                            }
                        } else {
                            Some((wid.clone(), idle_for))
                        }
                    } else {
                        None
                    }
                })
                .collect();

            candidates.sort_by_key(|(_, d)| std::cmp::Reverse(d.as_secs()));

            let selected: Vec<(WorkerId, Duration)> =
                candidates.into_iter().take(allowed_to_retire).collect();

            let mut workers_to_release = Vec::with_capacity(selected.len());
            for (wid, _idle_for) in selected {
                if let Some(worker) = state.ray_workers.remove(&wid) {
                    state
                        .pending_release_blacklist
                        .insert(wid.clone(), Instant::now());
                    workers_to_release.push(worker);
                }
            }

            let survivors_after = state.ray_workers.len();
            let blacklisted_after = state.pending_release_blacklist.len();

            state.max_resources_requested = ResourceRequest::default();
            state.last_refresh = None;

            (workers_to_release, survivors_after, blacklisted_after)
        };

        if workers_to_release.is_empty() {
            return Ok(0);
        }

        tracing::info!(
            target: "ray_worker_manager",
            "Preparing to release {} workers",
            workers_to_release.len()
        );

        let mut released = 0usize;
        Python::attach(|py| -> DaftResult<()> {
            for mut worker in workers_to_release {
                worker.release(py);
                released += 1;
            }
            Ok(())
        })?;

        Python::attach(|py| -> DaftResult<()> {
            let flotilla_module = py.import(pyo3::intern!(py, "daft.runners.flotilla"))?;
            flotilla_module.call_method0(pyo3::intern!(py, "clear_autoscaling_requests"))?;
            Ok(())
        })?;

        tracing::info!(
            target: "ray_worker_manager",
            released,
            survivors = survivors_after,
            blacklisted = blacklisted_after,
            "Idle cleanup completed"
        );

        Ok(released)
    }
}

#[cfg(test)]
mod drain_tests {
    use super::*;

    #[test]
    fn confirmed_failure_allows_replacement_without_crossing_instance_lifetimes() {
        Python::initialize();
        Python::attach(|py| {
            let manager = RayWorkerManager::new(120, Some(py.None()));
            manager
                .state
                .lock()
                .unwrap()
                .install_refresh(vec![worker(py, "old")]);
            manager.mark_worker_died(Arc::from("old"));
            {
                let mut state = manager.state.lock().unwrap();
                assert!(!state.nodes_to_skip().contains(&"node".to_string()));
                let mut replacement = worker(py, "new");
                replacement.drain.data_queries.insert(1);
                state.install_refresh(vec![replacement]);
            }
            manager.mark_worker_died(Arc::from("old"));
            manager.mark_task_unknown(TaskContext::default(), Arc::from("old"));
            let state = manager.state.lock().unwrap();
            assert_eq!(
                state.ray_workers.get("old").unwrap().drain_state(),
                DrainState::Failed
            );
            assert!(!state.ray_workers.get("new").unwrap().can_retire());
            assert!(
                state
                    .ray_workers
                    .get("new")
                    .unwrap()
                    .drain
                    .data_queries
                    .contains(&1)
            );
        });
    }

    #[test]
    fn retirement_ack_can_be_retried_without_reopening_dispatch() {
        use pyo3::ffi::c_str;
        Python::initialize();
        Python::attach(|py| {
            let module = PyModule::from_code(
                py,
                c_str!(
                    "
class Actor:
    def __init__(self):
        self.calls = 0
    def retire(self):
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError('lost acknowledgement')
"
                ),
                c_str!("drain_test.py"),
                c_str!("drain_test"),
            )
            .unwrap();
            let actor = module.getattr("Actor").unwrap().call0().unwrap().unbind();
            let manager = RayWorkerManager::new(120, None);
            manager
                .state
                .lock()
                .unwrap()
                .install_refresh(vec![RaySwordfishWorker::new(
                    "attempt".into(),
                    actor,
                    1.0,
                    0.0,
                    1024,
                    "localhost".into(),
                    Some("node".into()),
                )]);
            manager.prepare_drain("node".into(), 4).unwrap();
            assert!(manager.retire_node("node".into(), 4).is_err());
            assert!(manager.cancel_drain("node".into(), 4).is_err());
            assert_eq!(
                manager
                    .state
                    .lock()
                    .unwrap()
                    .ray_workers
                    .values()
                    .next()
                    .unwrap()
                    .drain_state(),
                DrainState::Unknown
            );
            manager.retire_node("node".into(), 4).unwrap();
            manager.retire_node("node".into(), 4).unwrap();
            assert_eq!(
                manager
                    .state
                    .lock()
                    .unwrap()
                    .ray_workers
                    .values()
                    .next()
                    .unwrap()
                    .drain_state(),
                DrainState::Retired
            );
        });
    }

    #[test]
    fn prepare_between_scheduling_and_dispatch_prevents_submission() {
        use common_daft_config::DaftExecutionConfig;
        use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan, ShuffleBackend};
        use daft_logical_plan::stats::StatsState;
        use daft_schema::schema::Schema;

        Python::initialize();
        Python::attach(|py| {
            let manager = RayWorkerManager::new(120, None);
            manager
                .state
                .lock()
                .unwrap()
                .install_refresh(vec![worker(py, "attempt")]);
            let schema = Arc::new(Schema::empty());
            let scan = LocalPhysicalPlan::in_memory_scan(
                0,
                schema.clone(),
                0,
                StatsState::NotMaterialized,
                LocalNodeContext::default(),
            );
            let plan = LocalPhysicalPlan::gather_write(
                scan,
                schema,
                ShuffleBackend::Flight {
                    shuffle_id: 1,
                    shuffle_dirs: vec![],
                    compression: None,
                    shared: None,
                },
                StatsState::NotMaterialized,
                LocalNodeContext::default(),
            );
            let task = SwordfishTask::for_recovery_test(
                plan,
                HashMap::new(),
                Arc::new(DaftExecutionConfig::default()),
                1,
            );
            let selected = HashMap::from([(Arc::from("attempt"), vec![task.clone()])]);
            manager.prepare_drain("node".into(), 1).unwrap();
            // A real submit would fail because the test actor handle is None.
            // Returning no handle proves no RPC passed the final drain barrier.
            assert!(
                manager
                    .submit_tasks_to_workers(selected)
                    .unwrap()
                    .is_empty()
            );
            assert!(
                manager
                    .state
                    .lock()
                    .unwrap()
                    .ray_workers
                    .values()
                    .next()
                    .unwrap()
                    .can_retire()
            );

            manager.cancel_drain("node".into(), 1).unwrap();
            assert!(
                manager
                    .submit_tasks_to_workers(HashMap::from([(Arc::from("attempt"), vec![task])]))
                    .is_err()
            );
            let state = manager.state.lock().unwrap();
            let worker = state.ray_workers.values().next().unwrap();
            // Even an ambiguous submit failure records both compute and output
            // protection before entering Python.
            assert_eq!(worker.active_task_details().len(), 1);
            assert!(worker.drain.data_queries.contains(&0));
            assert!(!worker.can_retire());
        });
    }

    fn worker(py: Python<'_>, instance: &str) -> RaySwordfishWorker {
        RaySwordfishWorker::new(
            instance.into(),
            py.None(),
            4.0,
            0.0,
            1024,
            "127.0.0.1".into(),
            Some("node".into()),
        )
    }

    #[test]
    fn refresh_preserves_drain_and_retired_tombstones() {
        Python::initialize();
        Python::attach(|py| {
            let manager = RayWorkerManager::new(120, None);
            manager.prepare_drain("node".into(), 7).unwrap();
            {
                let mut state = manager.state.lock().unwrap();
                state.install_refresh(vec![worker(py, "attempt-1")]);
                let worker = state.ray_workers.values().next().unwrap();
                assert_eq!(worker.drain.epoch, 7);
                assert_eq!(worker.drain_state(), DrainState::ReadyToRetire);
                assert!(state.nodes_to_skip().contains(&"node".to_string()));
            }
            manager.cancel_drain("node".into(), 7).unwrap();
            assert!(manager.prepare_drain("node".into(), 7).is_err());
            manager.prepare_drain("node".into(), 8).unwrap();
            let mut state = manager.state.lock().unwrap();
            // Even a zero TTL and a subsequent scale-up cannot clear a protocol tombstone.
            state.node_drains.get_mut("node").unwrap().state = DrainState::Retired;
            state.ray_workers.clear();
            state.pending_release_blacklist.clear();
            assert!(state.nodes_to_skip().contains(&"node".to_string()));
        });
    }

    #[test]
    fn data_and_unknown_state_prevent_retirement_before_any_rpc() {
        Python::initialize();
        Python::attach(|py| {
            let manager = RayWorkerManager::new(120, None);
            let mut w = worker(py, "attempt-1");
            w.drain.data_queries.insert(1);
            manager.state.lock().unwrap().install_refresh(vec![w]);
            manager.prepare_drain("node".into(), 1).unwrap();
            assert!(manager.retire_node("node".into(), 1).is_err());
            {
                let mut state = manager.state.lock().unwrap();
                let w = state.ray_workers.values_mut().next().unwrap();
                w.drain.data_queries.clear();
                w.drain.unknown = Some("cleanup failed".into());
            }
            assert!(manager.retire_node("node".into(), 1).is_err());
            assert!(!manager.query_quiescent(1));
        });
    }
}
