use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use common_daft_config::DaftExecutionConfig;
use common_display::{DisplayLevel, mermaid::MermaidDisplayOptions};
use common_error::{DaftError, DaftResult};
use common_metrics::{QueryEndState, QueryID};
use common_runtime::RuntimeTask;
use common_tracing::flush_opentelemetry_providers;
use daft_context::{
    DaftContext, Subscriber,
    subscribers::{
        Event, event_header,
        events::{TaskInfo, TaskStartEvent},
    },
};
use daft_local_plan::{ExecutionStats, Input, InputId, LocalPhysicalPlanRef, SourceId, translate};
use daft_logical_plan::LogicalPlanBuilder;
use daft_micropartition::MicroPartition;
use daft_partition_refs::FlightPartitionRef;
use daft_shuffles::server::flight_server::{
    FlightServerConnectionHandle, ShuffleFlightServer, start_server_loop,
};
use futures::{FutureExt, future::BoxFuture};
use tokio::runtime::Handle;
use tokio_util::sync::CancellationToken;
#[cfg(feature = "python")]
use {
    common_daft_config::PyDaftExecutionConfig,
    daft_context::python::PyDaftContext,
    daft_local_plan::python::PyExecutionStats,
    daft_logical_plan::PyLogicalPlanBuilder,
    daft_micropartition::python::PyMicroPartition,
    daft_partition_refs::PyFlightPartitionRef,
    pyo3::{
        Bound, IntoPyObject, PyAny, PyRef, PyResult, Python, pyclass, pymethods, sync::MutexExt,
    },
};

use crate::{
    ExecutionRuntimeContext,
    channel::{Sender, UnboundedSender, create_channel, create_unbounded_channel},
    input_cancel::InputCancelRegistry,
    pipeline::{
        BuilderContext, PipelineMessage, translate_physical_plan_to_pipeline, viz_pipeline_ascii,
        viz_pipeline_mermaid,
    },
    resource_manager::get_or_init_memory_manager,
    runtime_stats::{RuntimeStatsManager, RuntimeStatsManagerHandle},
};

enum ExecutionEngineResultItem {
    Partition(MicroPartition),
    FlightPartitionRef(FlightPartitionRef),
    Error(Arc<DaftError>),
}

/// Global tokio runtime shared by all NativeExecutor instances
static GLOBAL_RUNTIME: OnceLock<Handle> = OnceLock::new();

/// Get or initialize the global tokio runtime
#[cfg(feature = "python")]
fn get_global_runtime() -> &'static Handle {
    GLOBAL_RUNTIME.get_or_init(|| {
        let mut builder = tokio::runtime::Builder::new_current_thread();
        builder.enable_all();
        pyo3_async_runtimes::tokio::init(builder);
        std::thread::spawn(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(futures::future::pending::<()>());
        });
        pyo3_async_runtimes::tokio::get_runtime().handle().clone()
    })
}

#[cfg(not(feature = "python"))]
fn get_global_runtime() -> &'static Handle {
    GLOBAL_RUNTIME.get_or_init(|| {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("build global tokio runtime for NativeExecutor");
        let handle = rt.handle().clone();
        // Keep the runtime alive for the duration of the process.
        std::thread::spawn(move || {
            rt.block_on(futures::future::pending::<()>());
        });
        handle
    })
}

/// Message sent to the execution task to enqueue inputs
pub(crate) struct EnqueueInputMessage {
    /// The input_id for this enqueue operation
    input_id: InputId,
    /// Plan inputs grouped by source_id
    inputs: HashMap<SourceId, Input>,
    /// Sender for results of this input_id
    result_sender: UnboundedSender<ExecutionEngineResultItem>,
}

/// Routes pipeline messages to per-input_id channels.
struct MessageRouter {
    output_senders: HashMap<InputId, UnboundedSender<ExecutionEngineResultItem>>,
    /// Wall-clock start instant when each `input_id` was enqueued to the pipeline.
    input_start_times: HashMap<InputId, Instant>,
}

impl MessageRouter {
    fn new() -> Self {
        Self {
            output_senders: HashMap::new(),
            input_start_times: HashMap::new(),
        }
    }

    /// Route a message to the appropriate channel based on its input_id.
    fn route_message(&mut self, msg: PipelineMessage) {
        match msg {
            PipelineMessage::Flush(input_id) => {
                self.input_start_times.remove(&input_id);
                self.output_senders.remove(&input_id);
            }
            PipelineMessage::Morsel {
                input_id,
                partition,
            } => {
                if let Some(sender) = self.output_senders.get(&input_id) {
                    let _ = sender.send(ExecutionEngineResultItem::Partition(partition));
                }
            }
            PipelineMessage::FlightPartitionRef {
                input_id,
                partition_ref,
            } => {
                if let Some(sender) = self.output_senders.get(&input_id) {
                    let _ =
                        sender.send(ExecutionEngineResultItem::FlightPartitionRef(partition_ref));
                }
            }
        }
    }

    fn insert_output_sender(
        &mut self,
        input_id: InputId,
        sender: UnboundedSender<ExecutionEngineResultItem>,
    ) {
        self.input_start_times.insert(input_id, Instant::now());
        self.output_senders.insert(input_id, sender);
    }
}

impl Drop for MessageRouter {
    fn drop(&mut self) {
        for (input_id, started) in self.input_start_times.drain() {
            log::debug!(
                "NativeExecutor: input_id={input_id} ended without Flush after {:?} (cancel/shutdown?)",
                started.elapsed()
            );
        }
    }
}

/// Per-plan execution state
type PipelineFailure = Arc<OnceLock<Arc<DaftError>>>;

struct PlanState {
    generation: u64,
    failure: PipelineFailure,
    task_handle: RuntimeTask<DaftResult<()>>,
    enqueue_input_sender: Sender<EnqueueInputMessage>,
    stats_handle: RuntimeStatsManagerHandle,
    active_input_ids: HashSet<InputId>,
    skipped_corrupt_files: Arc<std::sync::Mutex<Vec<(String, String, bool)>>>,
}

#[cfg_attr(
    feature = "python",
    pyclass(module = "daft.daft", name = "NativeExecutor", frozen)
)]
pub struct PyNativeExecutor {
    executor: Arc<Mutex<NativeExecutor>>,
    address: Option<String>,
}

#[cfg(feature = "python")]
impl Default for PyNativeExecutor {
    fn default() -> Self {
        Self::new(false, "")
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl PyNativeExecutor {
    #[new]
    pub fn new(is_flotilla_worker: bool, ip: &str) -> Self {
        let executor = NativeExecutor::new(is_flotilla_worker, ip);
        let address = executor.shuffle_address();
        Self {
            executor: Arc::new(Mutex::new(executor)),
            address,
        }
    }

    pub fn shuffle_address(&self) -> Option<String> {
        self.address.clone()
    }

    /// Process-wide IO count for cleanup of an actor exclusive to one managed execution.
    pub fn shuffle_active_operations(&self, py: Python<'_>) -> usize {
        let executor = self.executor.lock_py_attached(py).unwrap();
        executor
            .shuffle_server
            .as_ref()
            .map_or(0, |server| server.active_reads())
            + daft_shuffles::store::writer::background_fsyncs_in_flight()
            + daft_io::shuffle_file::active_shuffle_writes()
    }

    /// Writes owned by these shuffles, including cancelled blocking operations.
    pub fn shuffle_active_writes(&self, shuffle_ids: Vec<u64>) -> usize {
        daft_io::shuffle_file::active_shuffle_writes_for(&shuffle_ids)
    }

    /// Forget registrations for a quiescent query during shuffle cleanup.
    ///
    /// Takes the executor lock only to reach the shuffle server; the registry has
    /// its own lock and nothing under it blocks, so this cannot stall a worker
    /// that is mid-query.
    pub fn unregister_shuffles(&self, py: Python<'_>, shuffle_ids: Vec<u64>) -> PyResult<usize> {
        Ok(self
            .executor
            .lock_py_attached(py)
            .unwrap()
            .unregister_shuffles(&shuffle_ids))
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (local_physical_plan, daft_ctx, input_id, inputs, context=None, maintain_order=true))]
    pub fn run<'py>(
        &self,
        py: Python<'py>,
        local_physical_plan: &daft_local_plan::PyLocalPhysicalPlan,
        daft_ctx: &PyDaftContext,
        input_id: InputId,
        inputs: HashMap<SourceId, Input>,
        context: Option<HashMap<String, String>>,
        maintain_order: bool,
    ) -> PyResult<Bound<'py, pyo3::PyAny>> {
        let daft_ctx: &DaftContext = daft_ctx.into();
        let plan = local_physical_plan.plan.clone();
        let exec_cfg = daft_ctx.execution_config();
        let subscribers = daft_ctx.subscribers();
        let (fingerprint, enqueue_future) = {
            self.executor.lock_py_attached(py).unwrap().run(
                &plan,
                exec_cfg,
                subscribers,
                context,
                inputs,
                input_id,
                maintain_order,
            )?
        };

        let executor = self.executor.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = enqueue_future.await?;
            Ok(PyResultReceiver {
                generation: result.generation,
                result: Arc::new(tokio::sync::Mutex::new(Some(result))),
                fingerprint,
                input_id,
                executor,
            })
        })
    }

    pub fn active_plan_count(&self, py: Python<'_>) -> usize {
        let executor = self.executor.lock_py_attached(py).unwrap();
        executor.plans.len() + executor.retired_plans.len()
    }

    pub fn cancel_plan(&self, py: Python<'_>, fingerprint: u64) -> PyResult<()> {
        self.executor
            .lock_py_attached(py)
            .unwrap()
            .cancel_plan(fingerprint);
        Ok(())
    }

    #[staticmethod]
    pub fn repr_ascii(
        logical_plan_builder: &PyLogicalPlanBuilder,
        cfg: PyDaftExecutionConfig,
        simple: bool,
    ) -> PyResult<String> {
        Ok(NativeExecutor::repr_ascii(
            &logical_plan_builder.builder,
            cfg.config,
            simple,
        ))
    }

    #[staticmethod]
    pub fn repr_mermaid(
        logical_plan_builder: &PyLogicalPlanBuilder,
        cfg: PyDaftExecutionConfig,
        options: MermaidDisplayOptions,
    ) -> PyResult<String> {
        Ok(NativeExecutor::repr_mermaid(
            &logical_plan_builder.builder,
            cfg.config,
            options,
        ))
    }
}

/// Returns a fingerprint that is unique for each call when the caller does not
/// supply one. Using a fixed value (e.g. 0) caused `NativeExecutor::run` to
/// reuse the cached pipeline from a prior execution when the new plan requires
/// a different `InputSender` variant, which reached the `unreachable!` branch
/// in `InputSender::send` (see GitHub issue #7087).
fn next_auto_fingerprint() -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

fn parse_context(ctx: Option<&HashMap<String, String>>) -> (QueryID, u64, Option<u32>) {
    let query_id = ctx
        .as_ref()
        .and_then(|c| c.get("query_id"))
        .map(|s| QueryID::from(s.as_str()))
        .unwrap_or_else(|| QueryID::from(""));
    let fingerprint = ctx
        .as_ref()
        .and_then(|c| c.get("plan_fingerprint"))
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or_else(next_auto_fingerprint);
    let task_id = ctx
        .as_ref()
        .and_then(|c| c.get("task_id"))
        .and_then(|s| s.parse::<u32>().ok());

    (query_id, fingerprint, task_id)
}

// TODO: fix configuration for events
// This is copied from task_lifecycle.rs to avoid the daft-distributed dependency
pub fn task_events_enabled() -> bool {
    if let Ok(val) = std::env::var("DAFT_TASK_EVENTS_ENABLED") {
        matches!(val.trim().to_lowercase().as_str(), "1" | "true")
    } else {
        false // Disabled by default; enable with DAFT_TASK_EVENTS_ENABLED=true
    }
}

/// The core execution loop that drives a pipeline to completion.
/// Receives inputs via `enqueue_input_rx`, routes pipeline outputs to
/// per-input_id channels, and runs until the pipeline finishes, errors,
/// or is cancelled.
async fn run_execution_loop(
    cancel: CancellationToken,
    stats_manager: RuntimeStatsManager,
    mut enqueue_input_rx: crate::channel::Receiver<EnqueueInputMessage>,
    input_senders: Arc<HashMap<SourceId, crate::input_sender::InputSender>>,
    pipeline: Box<dyn crate::pipeline::PipelineNode>,
    maintain_order: bool,
    failure: PipelineFailure,
) -> DaftResult<()> {
    let stats_manager_handle = stats_manager.handle();
    let memory_manager = get_or_init_memory_manager();
    let mut runtime_handle =
        ExecutionRuntimeContext::new(memory_manager.clone(), stats_manager_handle);
    // Root cancellation scope. Nothing cancels in it — only a `StreamingSink`
    // that opts into `cancels_inputs` creates a scope it may write to — but
    // every node needs one to forward, and a sink below a cancelling sink
    // inherits that sink's scope through this same parameter.
    let root_input_cancel = InputCancelRegistry::new();
    let mut output_receiver =
        pipeline.start(maintain_order, &mut runtime_handle, &root_input_cancel)?;

    let mut message_router = MessageRouter::new();
    let mut input_senders = Some(input_senders);
    let mut input_exhausted = false;

    let (result, finish_status) = loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => {
                println!("Execution engine cancelled");
                break (Ok(()), QueryEndState::Canceled);
            }
            _ = tokio::signal::ctrl_c() => {
                println!("Received Ctrl-C, shutting down execution engine");
                break (Ok(()), QueryEndState::Canceled);
            }
            Some(join_result) = runtime_handle.join_next() => {
                if let Err(e) = join_result {
                    if matches!(&e, common_error::DaftError::JoinError(source) if source.is_cancelled()) {
                        break (Ok(()), QueryEndState::Canceled);
                    }
                    break (Err(e), QueryEndState::Failed);
                }
            }
            enqueue_msg = enqueue_input_rx.recv(), if !input_exhausted => {
                if let Some(EnqueueInputMessage { input_id, inputs, result_sender }) = enqueue_msg {
                    message_router.insert_output_sender(input_id, result_sender);
                    let senders = input_senders.as_ref().unwrap();
                    for (key, plan_input) in inputs {
                        if let Some(sender) = senders.get(&key) {
                            let _ = sender.send(input_id, plan_input);
                        }
                    }
                } else {
                    // All senders dropped — drop input channels so
                    // pipeline sources see EOF.
                    input_senders.take();
                    input_exhausted = true;
                }
            }
            msg = output_receiver.recv() => {
                match msg {
                    Some(msg) => {
                        message_router.route_message(msg);
                    }
                    None => {
                        // Drain runtime tasks before closing result channels so an
                        // error can reach every input that did not receive a Flush.
                        let res = runtime_handle.shutdown().await;
                        let status = if res.is_ok() { QueryEndState::Finished } else { QueryEndState::Failed };
                        break (res, status);
                    }
                }
            }
        }
    };

    let result = finish_input_streams(message_router, enqueue_input_rx, result, &failure).await;

    stats_manager.finish(finish_status).await;
    flush_opentelemetry_providers();
    result
}

/// Close a pipeline's inputs and deliver its failure to every unfinished input,
/// including inputs accepted into the queue but not yet routed to the pipeline.
async fn finish_input_streams(
    message_router: MessageRouter,
    mut enqueue_input_rx: crate::channel::Receiver<EnqueueInputMessage>,
    result: DaftResult<()>,
    failure: &PipelineFailure,
) -> DaftResult<()> {
    let result = result.map_err(|error| {
        let error = Arc::new(error);
        // Rejected enqueues must observe the same failure as accepted inputs.
        // Publish it before closing the enqueue channel.
        let _ = failure.set(error.clone());
        for sender in message_router.output_senders.values() {
            let _ = sender.send(ExecutionEngineResultItem::Error(error.clone()));
        }
        error
    });
    // Inputs accepted just before the pipeline failed must receive its error too.
    enqueue_input_rx.close();
    while let Some(message) = enqueue_input_rx.recv().await {
        if let Err(error) = &result {
            let _ = message
                .result_sender
                .send(ExecutionEngineResultItem::Error(error.clone()));
        }
    }
    drop(message_router);

    result.map_err(DaftError::Shared)
}

pub struct NativeExecutor {
    cancel: CancellationToken,
    is_flotilla_worker: bool,
    shuffle_server: Option<Arc<ShuffleFlightServer>>,
    shuffle_server_connection: Option<FlightServerConnectionHandle>,
    plans: HashMap<u64, PlanState>,
    retired_plans: HashMap<(u64, u64), PlanState>,
}

impl NativeExecutor {
    pub fn new(is_flotilla_worker: bool, ip: &str) -> Self {
        // Determine if we are running in a flotilla worker.
        if is_flotilla_worker {
            let shuffle_server = Arc::new(ShuffleFlightServer::new());
            let shuffle_server_connection = Some(start_server_loop(ip, shuffle_server.clone()));

            Self {
                cancel: CancellationToken::new(),
                is_flotilla_worker: true,
                shuffle_server: Some(shuffle_server),
                shuffle_server_connection,
                plans: HashMap::new(),
                retired_plans: HashMap::new(),
            }
        } else {
            Self {
                cancel: CancellationToken::new(),
                is_flotilla_worker: false,
                shuffle_server: None,
                shuffle_server_connection: None,
                plans: HashMap::new(),
                retired_plans: HashMap::new(),
            }
        }
    }

    pub fn shuffle_address(&self) -> Option<String> {
        self.shuffle_server_connection
            .as_ref()
            .map(|conn| conn.shuffle_address())
    }

    /// Drop this worker's Flight registrations for shuffles whose data has been
    /// deleted. See [`ShuffleFlightServer::unregister_shuffles`].
    pub fn unregister_shuffles(&self, shuffle_ids: &[u64]) -> usize {
        self.shuffle_server
            .as_ref()
            .map_or(0, |server| server.unregister_shuffles(shuffle_ids))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn run(
        &mut self,
        local_physical_plan: &LocalPhysicalPlanRef,
        exec_cfg: Arc<DaftExecutionConfig>,
        subscribers: Vec<Arc<dyn Subscriber>>,
        additional_context: Option<HashMap<String, String>>,
        inputs: HashMap<SourceId, Input>,
        input_id: InputId,
        maintain_order: bool,
    ) -> DaftResult<(u64, BoxFuture<'static, DaftResult<ExecutionEngineResult>>)> {
        let (query_id, fingerprint, task_id) = parse_context(additional_context.as_ref());

        if self.is_flotilla_worker {
            debug_assert_eq!(
                task_id,
                Some(input_id),
                "Flotilla invariant violated: task_id must match input_id"
            );
        }

        let task_start_dispatch = if self.is_flotilla_worker
            && task_events_enabled()
            && let Some(task_id) = task_id
        {
            Some((
                Event::TaskStart(TaskStartEvent {
                    header: event_header(query_id.clone()),
                    task: Arc::new(TaskInfo {
                        id: task_id,
                        last_node_id: additional_context
                            .as_ref()
                            .and_then(|ctx| ctx.get("shuffle_reconstruction_node"))
                            .and_then(|id| id.parse().ok())
                            .unwrap_or(0),
                        node_ids: vec![], // TODO: propagate node_ids
                        plan_fingerprint: fingerprint as u32,
                        name: additional_context
                            .as_ref()
                            .and_then(|ctx| ctx.get("shuffle_reconstruction_of"))
                            .map(|id| Arc::from(format!("Shuffle reconstruction of task {id}"))),
                    }),
                    worker_id: None, // TODO: propagate worker id
                }),
                subscribers.clone(),
            ))
        } else {
            None
        };

        if self
            .plans
            .get(&fingerprint)
            .is_some_and(|state| state.enqueue_input_sender.is_closed())
        {
            let state = self.plans.remove(&fingerprint).unwrap();
            self.retired_plans
                .insert((fingerprint, state.generation), state);
        }
        if !self.plans.contains_key(&fingerprint) {
            let cancel = self.cancel.clone();
            let additional_context = additional_context.unwrap_or_default();
            let shuffle_address = self.shuffle_address();
            let ctx = BuilderContext::new_with_context(
                query_id.clone(),
                additional_context,
                self.shuffle_server
                    .as_ref()
                    .map(|server| (server.clone(), shuffle_address.unwrap())),
            );
            let (pipeline, input_senders) =
                translate_physical_plan_to_pipeline(local_physical_plan, &exec_cfg, &ctx)?;

            let handle = get_global_runtime();
            let stats_manager = RuntimeStatsManager::try_new(
                handle,
                &pipeline,
                subscribers,
                query_id,
                self.is_flotilla_worker,
            )?;
            let stats_handle = stats_manager.handle();

            let (enqueue_input_tx, enqueue_input_rx) = create_channel::<EnqueueInputMessage>(1);

            let input_senders = Arc::new(input_senders);
            let failure = PipelineFailure::default();
            let task = run_execution_loop(
                cancel,
                stats_manager,
                enqueue_input_rx,
                input_senders,
                pipeline,
                maintain_order,
                failure.clone(),
            );

            let task_handle = RuntimeTask::new(handle, task);
            self.plans.insert(
                fingerprint,
                PlanState {
                    generation: next_auto_fingerprint(),
                    failure,
                    task_handle,
                    enqueue_input_sender: enqueue_input_tx,
                    stats_handle,
                    active_input_ids: HashSet::new(),
                    skipped_corrupt_files: ctx.skipped_corrupt_files.clone(),
                },
            );
        }

        let plan_state = self.plans.get_mut(&fingerprint).unwrap();
        let enqueue_input_sender = plan_state.enqueue_input_sender.clone();
        let failure = plan_state.failure.clone();
        let generation = plan_state.generation;
        plan_state.active_input_ids.insert(input_id);

        Ok((
            fingerprint,
            async move {
                let mut result =
                    ExecutionEngineResult::enqueue(enqueue_input_sender, inputs, input_id, failure)
                        .await;
                result.generation = generation;
                // Rejected inputs never started execution.
                if result.error.is_none()
                    && let Some((event, subscribers)) = task_start_dispatch
                {
                    dispatch_task_start_event(&subscribers, &event);
                }
                Ok(result)
            }
            .boxed(),
        ))
    }

    /// Finish exactly the generation this input joined. A late finisher can
    /// release a retired execution without touching a replacement pipeline.
    pub fn try_finish(
        &mut self,
        fingerprint: u64,
        input_id: InputId,
        generation: u64,
    ) -> DaftResult<BoxFuture<'static, DaftResult<ExecutionStats>>> {
        let current = self
            .plans
            .get(&fingerprint)
            .is_some_and(|state| state.generation == generation);
        let state = if current {
            self.plans.get_mut(&fingerprint)
        } else {
            self.retired_plans.get_mut(&(fingerprint, generation))
        };
        let Some(plan_state) = state else {
            // Plan already removed (pipeline died and another input_id cleaned it up).
            // Return empty stats; PyResultReceiver retains each unfinished input's
            // error independently of this shared plan's lifetime.
            let query_id = QueryID::from("");
            return Ok(async move { Ok(ExecutionStats::new(query_id, vec![])) }.boxed());
        };

        plan_state.active_input_ids.remove(&input_id);
        let should_remove =
            plan_state.active_input_ids.is_empty() || plan_state.enqueue_input_sender.is_closed();

        if should_remove {
            let plan_state = if current {
                self.plans.remove(&fingerprint).unwrap()
            } else {
                self.retired_plans
                    .remove(&(fingerprint, generation))
                    .unwrap()
            };
            Ok(async move {
                // Try to get stats for this input_id. If the pipeline already died,
                // the stats manager may be finished so this can fail — that's OK.
                let stats = plan_state.stats_handle.take_input_snapshot(input_id).await;
                drop(plan_state.enqueue_input_sender);
                plan_state.task_handle.await??;
                let skipped = plan_state
                    .skipped_corrupt_files
                    .lock()
                    .map(|v| v.clone())
                    .unwrap_or_default();
                // If the snapshot failed (e.g. pipeline died), return empty stats.
                Ok(stats
                    .unwrap_or_else(|_| ExecutionStats::new(QueryID::from(""), vec![]))
                    .with_skipped_corrupt_files(skipped))
            }
            .boxed())
        } else {
            let stats_handle = plan_state.stats_handle.clone();
            let skipped_corrupt_files = plan_state.skipped_corrupt_files.clone();
            Ok(async move {
                let skipped = skipped_corrupt_files
                    .lock()
                    .map(|v| v.clone())
                    .unwrap_or_default();
                Ok(stats_handle
                    .take_input_snapshot(input_id)
                    .await
                    .unwrap_or_else(|_| ExecutionStats::new(QueryID::from(""), vec![]))
                    .with_skipped_corrupt_files(skipped))
            }
            .boxed())
        }
    }

    pub fn cancel_plan(&mut self, fingerprint: u64) {
        // RuntimeTask drop cancels the spawned task
        self.plans.remove(&fingerprint);
        self.retired_plans
            .retain(|(plan, _), _| *plan != fingerprint);
    }

    fn repr_ascii(
        logical_plan_builder: &LogicalPlanBuilder,
        cfg: Arc<DaftExecutionConfig>,
        simple: bool,
    ) -> String {
        let logical_plan = logical_plan_builder.build();
        let (physical_plan, _) = translate(&logical_plan, &HashMap::new()).unwrap();
        let ctx = BuilderContext::new();
        let (pipeline_node, _) =
            translate_physical_plan_to_pipeline(&physical_plan, &cfg, &ctx).unwrap();

        viz_pipeline_ascii(pipeline_node.as_ref(), simple)
    }

    fn repr_mermaid(
        logical_plan_builder: &LogicalPlanBuilder,
        cfg: Arc<DaftExecutionConfig>,
        options: MermaidDisplayOptions,
    ) -> String {
        let logical_plan = logical_plan_builder.build();
        let (physical_plan, _) = translate(&logical_plan, &HashMap::new()).unwrap();
        let ctx = BuilderContext::new();
        let (pipeline_node, _) =
            translate_physical_plan_to_pipeline(&physical_plan, &cfg, &ctx).unwrap();

        let display_type = if options.simple {
            DisplayLevel::Compact
        } else {
            DisplayLevel::Default
        };
        viz_pipeline_mermaid(
            pipeline_node.as_ref(),
            display_type,
            options.bottom_up,
            options.subgraph_options,
        )
    }
}

impl Drop for NativeExecutor {
    fn drop(&mut self) {
        self.cancel.cancel();
        if let Some(conn) = &mut self.shuffle_server_connection {
            let _ = conn.shutdown();
        }
    }
}

pub struct ExecutionEngineResult {
    generation: u64,
    receiver: crate::channel::UnboundedReceiver<ExecutionEngineResultItem>,
    error: Option<Arc<DaftError>>,
}

impl ExecutionEngineResult {
    pub fn generation(&self) -> u64 {
        self.generation
    }
    async fn enqueue(
        sender: Sender<EnqueueInputMessage>,
        inputs: HashMap<SourceId, Input>,
        input_id: InputId,
        failure: PipelineFailure,
    ) -> Self {
        let (result_sender, receiver) = create_unbounded_channel();
        let message = EnqueueInputMessage {
            input_id,
            inputs,
            result_sender,
        };
        let error = if sender.send(message).await.is_err() {
            // Even rejected inputs return a handle so try_finish releases their
            // active tracking. Use the same Arc broadcast to accepted inputs.
            Some(failure.get().cloned().unwrap_or_else(|| {
                Arc::new(DaftError::InternalError(
                    "Plan execution task has died; cannot enqueue new input".into(),
                ))
            }))
        } else {
            None
        };
        Self {
            receiver,
            error,
            generation: 0,
        }
    }

    /// Drain both ordinary and shuffle output for in-process distributed tests.
    pub async fn collect_outputs_for_testing(
        mut self,
    ) -> DaftResult<(Vec<MicroPartition>, Vec<FlightPartitionRef>)> {
        let mut partitions = Vec::new();
        let mut refs = Vec::new();
        while let Some(item) = self.next().await {
            match item {
                ExecutionEngineResultItem::Partition(p) => partitions.push(p),
                ExecutionEngineResultItem::FlightPartitionRef(r) => refs.push(r),
                ExecutionEngineResultItem::Error(_) => unreachable!("next stores errors"),
            }
        }
        if let Some(error) = self.error {
            return Err(DaftError::Shared(error));
        }
        Ok((partitions, refs))
    }

    async fn next(&mut self) -> Option<ExecutionEngineResultItem> {
        match self.receiver.recv().await {
            Some(ExecutionEngineResultItem::Error(error)) => {
                self.error = Some(error);
                None
            }
            item => item,
        }
    }

    /// Consume all pipeline output for this input_id until EOF, returning any
    /// emitted `MicroPartition`s. `FlightPartitionRef` items are skipped (they
    /// are only relevant when shuffles are enabled). Intended for tests that
    /// exercise `NativeExecutor` end-to-end and need the pipeline to finish
    /// producing output before `try_finish` is called — mirroring what the
    /// production Python `__anext__` loop does.
    pub async fn collect_partitions_for_testing(mut self) -> Vec<MicroPartition> {
        let mut out = Vec::new();
        while let Some(item) = self.receiver.recv().await {
            if let ExecutionEngineResultItem::Partition(p) = item {
                out.push(p);
            }
        }
        out
    }
}

#[cfg_attr(
    feature = "python",
    pyclass(module = "daft.daft", name = "PyResultReceiver", frozen)
)]
pub struct PyResultReceiver {
    result: Arc<tokio::sync::Mutex<Option<ExecutionEngineResult>>>,
    fingerprint: u64,
    generation: u64,
    input_id: InputId,
    executor: Arc<Mutex<NativeExecutor>>,
}

#[cfg(feature = "python")]
#[pymethods]
impl PyResultReceiver {
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __anext__<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, pyo3::PyAny>> {
        let result = self.result.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut result = result.lock().await;
            let part = result
                .as_mut()
                .expect("PyResultReceiver.__anext__() should not be called after try_finish().")
                .next()
                .await;
            Python::attach(|py| {
                Ok(match part {
                    None => py.None(),
                    Some(ExecutionEngineResultItem::Error(_)) => unreachable!("next stores errors"),
                    Some(ExecutionEngineResultItem::Partition(partition)) => {
                        PyMicroPartition::from(partition)
                            .into_pyobject(py)?
                            .unbind()
                            .into_any()
                    }
                    Some(ExecutionEngineResultItem::FlightPartitionRef(partition_ref)) => {
                        PyFlightPartitionRef::from(partition_ref)
                            .into_pyobject(py)?
                            .unbind()
                            .into_any()
                    }
                })
            })
        })
    }

    fn try_finish<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let result = self.result.clone();
        let executor = self.executor.clone();
        let fingerprint = self.fingerprint;
        let generation = self.generation;
        let input_id = self.input_id;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            // Take the result to drop the receiver
            let mut result = result.lock().await;
            let error = result
                .take()
                .expect("PyResultReceiver.try_finish() should not be called more than once.")
                .error;
            drop(result);

            // Delegate to NativeExecutor::try_finish
            let finish_future =
                executor
                    .lock()
                    .unwrap()
                    .try_finish(fingerprint, input_id, generation)?;
            let stats = finish_future.await;
            // Always finish tracking this input before returning its pipeline error.
            // Another input may already have removed the shared plan and consumed
            // the execution task's error, so its result alone is not sufficient.
            if let Some(error) = error {
                return Err(DaftError::Shared(error).into());
            }
            let stats = stats?;
            Ok(PyExecutionStats::from(stats))
        })
    }
}

fn dispatch_task_start_event(subscribers: &[Arc<dyn Subscriber>], event: &Event) {
    for subscriber in subscribers {
        if let Err(e) = subscriber.on_event(event.clone()) {
            log::debug!("Failed to dispatch task start event: {}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn late_finisher_cannot_remove_replacement_generation() -> DaftResult<()> {
        use daft_core::prelude::{DataType, Field, Schema};
        use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan};
        use daft_logical_plan::stats::StatsState;
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64)]));
        let plan = LocalPhysicalPlan::in_memory_scan(
            0,
            schema.clone(),
            0,
            StatsState::NotMaterialized,
            LocalNodeContext::default(),
        );
        let mut executor = tokio::task::spawn_blocking(|| NativeExecutor::new(false, ""))
            .await
            .unwrap();
        let context = Some(HashMap::from([("plan_fingerprint".into(), "123".into())]));
        let inputs = HashMap::from([(
            0,
            Input::InMemory(vec![Arc::new(MicroPartition::empty(Some(schema)))]),
        )]);
        let config = Arc::new(DaftExecutionConfig::default());
        let (fingerprint, first) = executor.run(
            &plan,
            config.clone(),
            vec![],
            context.clone(),
            inputs.clone(),
            0,
            true,
        )?;
        let mut first = first.await?;
        while first.next().await.is_some() {}
        // Model the exact dead-pipeline/unfinished-input window: the enqueue
        // channel has closed, but the first receiver has not called try_finish.
        let (closed_sender, receiver) = create_channel(1);
        drop(receiver);
        executor
            .plans
            .get_mut(&fingerprint)
            .unwrap()
            .enqueue_input_sender = closed_sender;
        let (_, second) = executor.run(&plan, config, vec![], context, inputs, 1, true)?;
        let mut second = second.await?;
        assert_ne!(first.generation, second.generation);
        assert_eq!(executor.retired_plans.len(), 1);
        executor
            .try_finish(fingerprint, 0, first.generation)?
            .await?;
        assert_eq!(executor.plans[&fingerprint].generation, second.generation);
        assert!(executor.retired_plans.is_empty());
        while second.next().await.is_some() {}
        assert!(second.error.is_none());
        executor
            .try_finish(fingerprint, 1, second.generation)?
            .await?;
        assert!(executor.plans.is_empty());
        tokio::task::spawn_blocking(move || drop(executor))
            .await
            .unwrap();
        Ok(())
    }

    #[tokio::test]
    async fn pipeline_failure_reaches_unfinished_and_queued_inputs() {
        let (enqueue_tx, enqueue_rx) = create_channel(1);
        let mut router = MessageRouter::new();
        let mut results = Vec::new();
        for input_id in 0..4 {
            let (tx, rx) = create_unbounded_channel();
            results.push(ExecutionEngineResult {
                receiver: rx,
                error: None,
                generation: 0,
            });
            if input_id == 3 {
                assert!(
                    enqueue_tx
                        .send(EnqueueInputMessage {
                            input_id,
                            inputs: HashMap::new(),
                            result_sender: tx,
                        })
                        .await
                        .is_ok()
                );
            } else {
                router.insert_output_sender(input_id, tx);
            }
        }
        // Input 0 finished; input 1 emitted partial output; input 2 emitted
        // nothing; input 3 is accepted but still waiting in the input queue.
        router.route_message(PipelineMessage::Flush(0));
        router.route_message(PipelineMessage::Morsel {
            input_id: 1,
            partition: MicroPartition::empty(None),
        });

        let failure = PipelineFailure::default();
        let error = tokio::time::timeout(
            Duration::from_secs(1),
            finish_input_streams(
                router,
                enqueue_rx,
                Err(DaftError::SocketError("connection reset".into())),
                &failure,
            ),
        )
        .await
        .expect("must finish even while the input sender is alive")
        .unwrap_err();
        let DaftError::Shared(error) = error else {
            panic!("expected the shared pipeline error");
        };
        assert!(Arc::ptr_eq(failure.get().unwrap(), &error));
        assert!(enqueue_tx.is_closed());
        // A new input arriving after queue closure must receive the original
        // error too, rather than an unclassified enqueue failure.
        let mut rejected =
            ExecutionEngineResult::enqueue(enqueue_tx, HashMap::new(), 4, failure).await;
        assert!(rejected.next().await.is_none());
        assert!(Arc::ptr_eq(rejected.error.as_ref().unwrap(), &error));
        assert!(results[0].next().await.is_none());
        assert!(results[0].error.is_none());
        assert!(matches!(
            results[1].next().await,
            Some(ExecutionEngineResultItem::Partition(_))
        ));
        for result in &mut results[1..] {
            assert!(result.next().await.is_none());
            let received = result.error.as_ref().expect("must retain the input error");
            assert!(Arc::ptr_eq(received, &error));
            assert!(received.is_transient());
        }
    }
}
