//! Plan-owned, opt-in reconstruction. Queued tasks bind at dispatch, not submission.
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use common_error::{DaftError, DaftResult, ShuffleFetchFailure};
use common_treenode::DynTreeNode;
use daft_dsl::{AggExpr, Expr, ExprRef};
use daft_local_plan::{FlightMapOutput, Input, LocalPhysicalPlan, ShuffleBackend};
use daft_logical_plan::partitioning::RepartitionSpec;
use daft_partition_refs::FlightPartitionRef;
use daft_schema::{dtype::DataType, schema::Schema};
use futures::{FutureExt, future::BoxFuture};
use tokio::sync::{Mutex as AsyncMutex, OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

use super::{
    scheduler::{SchedulerHandle, SubmittableTask, SubmittedTask},
    task::{SwordfishTask, Task},
};
use crate::pipeline_node::MaterializedOutput;

type OutputKey = (u64, u32, u64);
type MapList = Arc<BTreeMap<String, Vec<FlightMapOutput>>>;
#[derive(Clone, Debug)]
struct Location {
    output: FlightMapOutput,
    server: String,
}
#[derive(Debug)]
enum RunFailure {
    Execution(DaftError),
    Dependency(DaftError),
}
impl RunFailure {
    fn into_error(self) -> DaftError {
        match self {
            Self::Execution(error) | Self::Dependency(error) => error,
        }
    }
}
#[derive(Debug, Default)]
struct RepairState {
    attempts: u32,
    terminal: Option<String>,
}
#[derive(Debug)]
struct Producer {
    template: SwordfishTask,
    selected: Mutex<Location>,
    repair: AsyncMutex<RepairState>,
    num_partitions: usize,
}
#[derive(Debug, Default)]
struct Directory {
    outputs: HashMap<OutputKey, Arc<Producer>>,
    // Keep the source Arc alive: its address must not be reused while cached.
    bindings: HashMap<(u64, usize), (MapList, Option<MapList>)>,
    repairing: HashSet<OutputKey>,
    retained_bytes: usize,
    retained_maps: usize,
}
#[derive(Debug, Default)]
pub(crate) struct ShuffleRecovery {
    directory: Mutex<Directory>,
    version: AtomicU64,
    slots: OnceLock<Arc<Semaphore>>,
}
struct RepairPublicationGuard<'a> {
    recovery: &'a ShuffleRecovery,
    key: OutputKey,
}
impl Drop for RepairPublicationGuard<'_> {
    fn drop(&mut self) {
        let mut directory = self.recovery.directory.lock().unwrap();
        directory.repairing.remove(&self.key);
        directory.bindings.clear();
    }
}
impl ShuffleRecovery {
    pub fn new() -> Self {
        Self::default()
    }

    fn register(&self, task: &SwordfishTask, result: &MaterializedOutput) {
        let Some((shuffle_id, num_partitions)) = producer_spec(task) else {
            return;
        };
        let location = match output_location(result, shuffle_id, num_partitions) {
            Ok(location) => location,
            Err(error) => {
                tracing::warn!(task_id = task.task_id(), %error, "Skipping invalid shuffle recovery recipe");
                return;
            }
        };
        let bytes = task
            .psets()
            .values()
            .flatten()
            .fold(0usize, |sum, p| sum.saturating_add(p.size_bytes()));
        let bytes = task.inputs().values().fold(bytes, |sum, input| {
            sum.saturating_add(match input {
                Input::InMemory(parts) => parts
                    .iter()
                    .fold(0usize, |n, p| n.saturating_add(p.size_bytes())),
                Input::FlightShuffle(reads) => reads.iter().fold(0usize, |n, read| {
                    n.saturating_add(
                        read.inputs_by_server
                            .values()
                            .map(|maps| {
                                maps.len()
                                    .saturating_mul(std::mem::size_of::<FlightMapOutput>())
                            })
                            .sum::<usize>(),
                    )
                }),
                _ => usize::MAX,
            })
        });
        let mut directory = self.directory.lock().unwrap();
        let key = (
            shuffle_id,
            location.output.input_id,
            location.output.attempt,
        );
        if directory.outputs.contains_key(&key) {
            return;
        }
        let cfg = task.config();
        if directory.retained_maps >= cfg.flight_shuffle_recovery_max_retained_maps
            || bytes
                > cfg
                    .flight_shuffle_recovery_max_retained_bytes
                    .saturating_sub(directory.retained_bytes)
        {
            tracing::warn!(
                shuffle_id,
                task_id = task.task_id(),
                bytes,
                "Shuffle recovery retention budget reached; producer will not be replayable"
            );
            return;
        }
        directory.retained_bytes += bytes;
        directory.retained_maps += 1;
        directory.outputs.insert(
            key,
            Arc::new(Producer {
                template: task.clone(),
                selected: Mutex::new(location),
                repair: AsyncMutex::new(RepairState::default()),
                num_partitions,
            }),
        );
    }
    fn lookup(&self, key: OutputKey) -> Option<Arc<Producer>> {
        self.directory.lock().unwrap().outputs.get(&key).cloned()
    }

    pub(crate) fn has_updates(&self) -> bool {
        self.version.load(Ordering::Acquire) != 0
    }

    /// One lock per dispatch, one rewritten list per source Arc per directory
    /// version. All reduce tasks continue sharing the same allocation.
    pub(crate) fn bind(&self, task: &mut SwordfishTask) -> bool {
        if !self.has_updates()
            || !task
                .inputs()
                .values()
                .any(|input| matches!(input, Input::FlightShuffle(_)))
        {
            return true;
        }
        let mut directory = self.directory.lock().unwrap();
        for input in task.inputs_mut().values_mut() {
            let Input::FlightShuffle(reads) = input else {
                continue;
            };
            for read in reads {
                let key = (
                    read.shuffle_id,
                    Arc::as_ptr(&read.inputs_by_server) as usize,
                );
                if let Some((_, bound)) = directory.bindings.get(&key) {
                    let Some(bound) = bound else {
                        return false;
                    };
                    read.inputs_by_server = bound.clone();
                    continue;
                }
                let mut by_server: BTreeMap<String, Vec<FlightMapOutput>> = BTreeMap::new();
                let mut changed = false;
                for (server, maps) in read.inputs_by_server.iter() {
                    for map in maps {
                        let location = directory
                            .outputs
                            .get(&(read.shuffle_id, map.input_id, map.attempt))
                            .map(|p| p.selected.lock().unwrap().clone())
                            .unwrap_or_else(|| Location {
                                output: *map,
                                server: server.clone(),
                            });
                        // Resolve aliases before checking repair ownership. This
                        // also fences consumers still carrying the original Arc
                        // during a second reconstruction of a replacement attempt.
                        if directory.repairing.contains(&(
                            read.shuffle_id,
                            location.output.input_id,
                            location.output.attempt,
                        )) {
                            directory
                                .bindings
                                .insert(key, (read.inputs_by_server.clone(), None));
                            return false;
                        }
                        changed |= location.output != *map || location.server != *server;
                        by_server
                            .entry(location.server)
                            .or_default()
                            .push(location.output);
                    }
                }
                let bound = if changed {
                    Arc::new(by_server)
                } else {
                    read.inputs_by_server.clone()
                };
                directory
                    .bindings
                    .insert(key, (read.inputs_by_server.clone(), Some(bound.clone())));
                read.inputs_by_server = bound;
            }
        }
        true
    }

    #[cfg(test)]
    fn bound(&self, task: &SwordfishTask) -> Option<SwordfishTask> {
        let mut task = task.clone();
        self.bind(&mut task).then_some(task)
    }

    /// Called only after the resource scheduler selected a worker. Waiting tasks
    /// hold no permit; each physical completion releases its execution permit.
    pub(crate) fn try_execution_slot(
        &self,
        task: &SwordfishTask,
    ) -> Result<Option<OwnedSemaphorePermit>, ()> {
        if !task.is_reconstruction() {
            return Ok(None);
        }
        let slots = self.slots.get_or_init(|| {
            Arc::new(Semaphore::new(
                task.config().flight_shuffle_recovery_max_inflight,
            ))
        });
        slots.clone().try_acquire_owned().map(Some).map_err(|_| ())
    }

    fn run<'a>(
        &'a self,
        mut task: SwordfishTask,
        scheduler: &'a SchedulerHandle<SwordfishTask>,
        cancel: CancellationToken,
        ancestors: Vec<OutputKey>,
    ) -> BoxFuture<'a, Result<Option<MaterializedOutput>, RunFailure>> {
        async move {
            for failure_count in 0..=task.config().flight_shuffle_recovery_max_consumer_failures {
                if cancel.is_cancelled() {
                    return Ok(None);
                }
                let result = SubmittableTask::new(task.clone(), cancel.child_token(), vec![])
                    .submit_raw(scheduler)
                    .map_err(RunFailure::Execution)?
                    .await;
                match result {
                    Ok(result) => return Ok(result),
                    Err(error) => {
                        let Some(failure) = error.shuffle_fetch_failure() else {
                            return Err(RunFailure::Execution(error));
                        };
                        if failure_count
                            == task.config().flight_shuffle_recovery_max_consumer_failures
                        {
                            return Err(RunFailure::Dependency(recovery_error(
                                &failure,
                                "consumer recovery budget exhausted",
                            )));
                        }
                        self.repair(&failure, scheduler, cancel.clone(), ancestors.clone())
                            .await
                            .map_err(RunFailure::Dependency)?;
                        task = task.recovery_attempt(scheduler.task_id_counter.next());
                    }
                }
            }
            unreachable!()
        }
        .boxed()
    }

    fn repair<'a>(
        &'a self,
        failure: &'a ShuffleFetchFailure,
        scheduler: &'a SchedulerHandle<SwordfishTask>,
        cancel: CancellationToken,
        mut ancestors: Vec<OutputKey>,
    ) -> BoxFuture<'a, DaftResult<()>> {
        async move {
            let key = (failure.shuffle_id, failure.input_id, failure.attempt);
            let producer = self.lookup(key).ok_or_else(|| recovery_error(
                failure, "producer not retained or not replayable (scan-to-shuffle is unsupported); automatic stage rollback is not supported",
            ))?;
            let cfg = producer.template.config();
            if ancestors.contains(&key) || ancestors.len() >= cfg.flight_shuffle_recovery_max_depth {
                return Err(recovery_error(failure, "cyclic or excessive reconstruction dependencies"));
            }
            ancestors.push(key);
            // Preserve the fair mutex waiter across diagnostics. Only query
            // cancellation, not another owner's execution time, ends this wait.
            let waiting = producer.repair.lock();
            tokio::pin!(waiting);
            let mut repair = loop {
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return Err(recovery_error(failure, "reconstruction cancelled")),
                    owner = &mut waiting => break owner,
                    _ = tokio::time::sleep(Duration::from_millis(cfg.flight_shuffle_recovery_wait_warn_ms)),
                        if cfg.flight_shuffle_recovery_wait_warn_ms > 0 => {
                        tracing::warn!(shuffle_id = failure.shuffle_id, input_id = failure.input_id,
                            "Still waiting for shared shuffle reconstruction owner");
                    }
                }
            };
            let selected = producer.selected.lock().unwrap().clone();
            if (selected.output.input_id, selected.output.attempt) != (failure.input_id, failure.attempt) {
                return Ok(());
            }
            if let Some(reason) = &repair.terminal {
                return Err(recovery_error(failure, reason));
            }
            if repair.attempts >= cfg.flight_shuffle_recovery_max_attempts {
                return Err(recovery_error(failure, "map reconstruction budget exhausted"));
            }
            {
                let mut directory = self.directory.lock().unwrap();
                directory.repairing.insert(key);
                directory.bindings.clear();
                self.version.fetch_add(1, Ordering::Release);
            }
            let _publication = RepairPublicationGuard { recovery: self, key };
            loop {
                if cancel.is_cancelled() {
                    return Err(recovery_error(failure, "reconstruction cancelled"));
                }
                let task = producer.template.reconstruction_attempt(scheduler.task_id_counter.next());
                tracing::warn!(shuffle_id = failure.shuffle_id, input_id = failure.input_id,
                    attempt = failure.attempt, reconstruction = repair.attempts + 1,
                    task_id = task.task_id(), "Reconstructing unavailable shared shuffle output");
                let result = self.run(task, scheduler, cancel.clone(), ancestors.clone()).await;
                if cancel.is_cancelled() || matches!(&result, Ok(None)) {
                    return Err(recovery_error(failure, "reconstruction cancelled"));
                }
                // Dependency/coordination failures belong to that dependency,
                // not this producer's completed-attempt budget or terminal state.
                let completed = match result {
                    Err(RunFailure::Dependency(error)) => return Err(error),
                    Err(RunFailure::Execution(error)) => Err(error),
                    Ok(Some(result)) => output_location(&result, failure.shuffle_id, producer.num_partitions),
                    Ok(None) => unreachable!(),
                };
                repair.attempts += 1;
                let replacement = match completed {
                    Ok(location) => location,
                    Err(error) => {
                        if error.is_transient() && repair.attempts < cfg.flight_shuffle_recovery_max_attempts {
                            continue;
                        }
                        if !error.is_transient() {
                            repair.terminal = Some(error.to_string());
                        }
                        return Err(error);
                    }
                };
                let replacement_key = (failure.shuffle_id, replacement.output.input_id, replacement.output.attempt);
                let mut directory = self.directory.lock().unwrap();
                directory.outputs.insert(replacement_key, producer.clone());
                *producer.selected.lock().unwrap() = replacement;
                directory.bindings.clear();
                self.version.fetch_add(1, Ordering::Release);
                tracing::info!(shuffle_id = failure.shuffle_id, input_id = replacement_key.1,
                    attempt = replacement_key.2, "Published reconstructed shared shuffle output");
                return Ok(());
            }
        }.boxed()
    }
}

pub(crate) fn submit(
    submittable: SubmittableTask<SwordfishTask>,
    scheduler: &SchedulerHandle<SwordfishTask>,
) -> DaftResult<SubmittedTask> {
    let (task, cancel, notifications) = submittable.into_parts();
    let recovery = scheduler.shuffle_recovery.clone();
    if task.config().flight_shuffle_recovery_max_attempts == 0
        || !uses_shared_shuffle(&task)
        || !consumer_replayable(&task.plan())
    {
        return SubmittableTask::new(task, cancel, notifications).submit_raw(scheduler);
    }
    let task_id = task.task_id();
    let scheduler = scheduler.clone();
    let future_cancel = cancel.clone();
    let completion = scheduler
        .recovery_statistics
        .hold_recovery_completion(&task.task_context());
    let future = async move {
        let _completion = completion;
        let executing = async {
            let result = recovery
                .run(task.clone(), &scheduler, future_cancel.clone(), vec![])
                .await
                .map_err(RunFailure::into_error)?;
            if let Some(result) = &result {
                recovery.register(&task, result);
            }
            Ok(result)
        };
        tokio::select! {
            biased;
            _ = future_cancel.cancelled() => Ok(None),
            result = executing => result,
        }
    }
    .boxed();
    Ok(SubmittedTask::from_future(
        task_id,
        future,
        Some(cancel),
        notifications,
    ))
}

fn recovery_error(failure: &ShuffleFetchFailure, reason: &str) -> DaftError {
    DaftError::ComputeError(format!(
        "Cannot recover shared shuffle output: {failure}; {reason}"
    ))
}

fn output_location(
    result: &MaterializedOutput,
    shuffle_id: u64,
    count: usize,
) -> DaftResult<Location> {
    let refs = result.partitions();
    let first = refs
        .first()
        .and_then(|r| r.as_any().downcast_ref::<FlightPartitionRef>())
        .ok_or_else(|| {
            DaftError::InternalError("Shared map task returned no Flight output".into())
        })?;
    let input_id = (first.partition_ref_id >> 32) as u32;
    let mut seen = vec![false; count];
    for reference in refs {
        let valid = reference
            .as_any()
            .downcast_ref::<FlightPartitionRef>()
            .is_some_and(|r| {
                let partition = r.partition_ref_id as u32 as usize;
                if r.shuffle_id != shuffle_id
                    || (r.partition_ref_id >> 32) as u32 != input_id
                    || r.attempt != first.attempt
                    || r.server_address != first.server_address
                    || partition >= count
                    || seen[partition]
                {
                    return false;
                }
                seen[partition] = true;
                true
            });
        if !valid {
            return Err(DaftError::InternalError(
                "Inconsistent reconstructed map output".into(),
            ));
        }
    }
    if refs.len() != count {
        return Err(DaftError::InternalError(
            "Incomplete reconstructed map output".into(),
        ));
    }
    Ok(Location {
        output: FlightMapOutput {
            input_id,
            attempt: first.attempt,
        },
        server: first.server_address.clone(),
    })
}

fn producer_spec(task: &SwordfishTask) -> Option<(u64, usize)> {
    match task.plan().as_ref() {
        LocalPhysicalPlan::RepartitionWrite(write) => match &write.backend {
            ShuffleBackend::Flight {
                shuffle_id,
                shared: Some(_),
                ..
            } if producer_task_replayable(task) => Some((*shuffle_id, write.num_partitions)),
            _ => None,
        },
        _ => None,
    }
}

/// Ordinary tasks and node-local shuffles do not need replay analysis, cloned
/// recipes, or a logical-completion guard on their normal submission path.
fn uses_shared_shuffle(task: &SwordfishTask) -> bool {
    matches!(task.plan().as_ref(), LocalPhysicalPlan::RepartitionWrite(write)
        if matches!(&write.backend, ShuffleBackend::Flight { shared: Some(_), .. }))
        || task.inputs().values().any(|input| {
            matches!(input, Input::FlightShuffle(reads)
                if reads.iter().any(|read| read.shared_root.is_some()))
        })
}

fn scalar_type(dtype: &DataType) -> bool {
    match dtype {
        DataType::List(child) | DataType::FixedSizeList(child, _) => return scalar_type(child),
        DataType::Struct(fields) => return fields.iter().all(|field| scalar_type(&field.dtype)),
        DataType::Map { key, value } => return scalar_type(key) && scalar_type(value),
        _ => {}
    }
    matches!(
        dtype,
        DataType::Null
            | DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(..)
            | DataType::Timestamp(..)
            | DataType::Date
            | DataType::Time(..)
            | DataType::Duration(..)
            | DataType::Interval
            | DataType::Binary
            | DataType::FixedSizeBinary(..)
            | DataType::Uuid
            | DataType::Utf8
    )
}

fn pure_expr(expr: &ExprRef, schema: &Schema) -> bool {
    use daft_dsl::functions::scalar::{BuiltinScalarFnVariant, ScalarFn};
    let pure_builtin = matches!(expr.as_ref(), Expr::ScalarFn(ScalarFn::Builtin(function))
        if matches!(&function.func, BuiltinScalarFnVariant::Sync(_))
        && function.is_deterministic()
        && matches!(function.name(), "abs" | "ceil" | "floor" | "round" | "sign" | "sqrt" | "exp" | "ln" | "log2" | "log10" | "sin" | "cos" | "tan" | "lower" | "upper" | "length" | "reverse" | "capitalize" | "contains" | "startswith" | "endswith" | "lstrip" | "rstrip" | "strip" | "replace"));
    (pure_builtin
        || matches!(
            expr.as_ref(),
            Expr::Column(_)
                | Expr::Literal(_)
                | Expr::Alias(..)
                | Expr::BinaryOp { .. }
                | Expr::Cast(..)
                | Expr::Not(_)
                | Expr::IsNull(_)
                | Expr::NotNull(_)
                | Expr::FillNull(..)
                | Expr::IfElse { .. }
                | Expr::Between(..)
                | Expr::IsIn(..)
                | Expr::Coalesce(_)
                | Expr::List(_)
        ))
        && expr.to_field(schema).is_ok_and(|f| scalar_type(&f.dtype))
        && expr.children().iter().all(|child| pure_expr(child, schema))
}

fn safe_agg(agg: &AggExpr, schema: &Schema) -> bool {
    // These builtins do not call user code. Order-sensitive aggregate results
    // are not admitted as producer replay-equivalence guarantees.
    matches!(
        agg,
        AggExpr::Count(..)
            | AggExpr::CountDistinct(_)
            | AggExpr::Sum(_)
            | AggExpr::Min(_)
            | AggExpr::Max(_)
            | AggExpr::Mean(_)
            | AggExpr::BoolAnd(_)
            | AggExpr::BoolOr(_)
    ) && agg.children().iter().all(|e| pure_expr(e, schema))
}

fn producer_task_replayable(task: &SwordfishTask) -> bool {
    // Retained in-memory inputs and immutable shuffle files are replayable.
    // ScanTasks/GlobPaths do not provide source snapshot guarantees.
    task.inputs()
        .values()
        .all(|input| matches!(input, Input::InMemory(_) | Input::FlightShuffle(_)))
        && producer_plan_replayable(&task.plan())
}

fn producer_plan_replayable(plan: &daft_local_plan::LocalPhysicalPlanRef) -> bool {
    if !plan.schema().fields().iter().all(|f| scalar_type(&f.dtype)) {
        return false;
    }
    let allowed = match plan.as_ref() {
        LocalPhysicalPlan::InMemoryScan(_) | LocalPhysicalPlan::ShuffleRead(_) => true,
        LocalPhysicalPlan::Project(p) => p
            .projection
            .iter()
            .all(|e| pure_expr(e.inner(), p.input.schema())),
        LocalPhysicalPlan::Filter(p) => pure_expr(p.predicate.inner(), p.input.schema()),
        LocalPhysicalPlan::IntoBatches(_) => true,
        LocalPhysicalPlan::RepartitionWrite(p) => {
            matches!(
                &p.backend,
                ShuffleBackend::Flight {
                    shared: Some(_),
                    ..
                }
            ) && match &p.repartition_spec {
                RepartitionSpec::Hash(h) => h.by.iter().all(|e| pure_expr(e, p.input.schema())),
                RepartitionSpec::Range(r) => {
                    r.by.iter().all(|e| pure_expr(e.inner(), p.input.schema()))
                }
                RepartitionSpec::Random(_) => false,
            }
        }
        _ => false,
    };
    allowed && plan.arc_children().iter().all(producer_plan_replayable)
}

// Consumer retry needs absence of external effects, not producer output
// equivalence. Order-sensitive operators are safe when their task has not committed.
fn consumer_replayable(plan: &daft_local_plan::LocalPhysicalPlanRef) -> bool {
    if !plan.schema().fields().iter().all(|f| scalar_type(&f.dtype)) {
        return false;
    }
    let check = |exprs: &[daft_dsl::expr::bound_expr::BoundExpr],
                 input: &daft_local_plan::LocalPhysicalPlanRef| {
        exprs.iter().all(|e| pure_expr(e.inner(), input.schema()))
    };
    let allowed = match plan.as_ref() {
        LocalPhysicalPlan::InMemoryScan(_)
        | LocalPhysicalPlan::ShuffleRead(_)
        | LocalPhysicalPlan::IntoBatches(_)
        | LocalPhysicalPlan::Limit(_)
        | LocalPhysicalPlan::IntoPartitions(_)
        | LocalPhysicalPlan::GatherWrite(_)
        | LocalPhysicalPlan::CrossJoin(_)
        | LocalPhysicalPlan::Concat(_) => true,
        LocalPhysicalPlan::Sort(p) => check(&p.sort_by, &p.input),
        LocalPhysicalPlan::TopN(p) => check(&p.sort_by, &p.input),
        LocalPhysicalPlan::Explode(p) => check(&p.to_explode, &p.input),
        LocalPhysicalPlan::HashJoin(p) => {
            check(&p.left_on, &p.left) && check(&p.right_on, &p.right)
        }
        LocalPhysicalPlan::SortMergeJoin(p) => {
            check(&p.left_on, &p.left) && check(&p.right_on, &p.right)
        }
        LocalPhysicalPlan::AsofJoin(p) => {
            check(&p.left_by, &p.left)
                && check(&p.right_by, &p.right)
                && pure_expr(p.left_on.inner(), p.left.schema())
                && pure_expr(p.right_on.inner(), p.right.schema())
        }
        LocalPhysicalPlan::Project(p) => check(&p.projection, &p.input),
        LocalPhysicalPlan::Filter(p) => pure_expr(p.predicate.inner(), p.input.schema()),
        LocalPhysicalPlan::HashAggregate(p) => {
            check(&p.group_by, &p.input)
                && p.aggregations
                    .iter()
                    .all(|a| safe_agg(a.inner(), p.input.schema()))
        }
        LocalPhysicalPlan::UnGroupedAggregate(p) => p
            .aggregations
            .iter()
            .all(|a| safe_agg(a.inner(), p.input.schema())),
        LocalPhysicalPlan::RepartitionWrite(p) => match &p.repartition_spec {
            RepartitionSpec::Hash(h) => h.by.iter().all(|e| pure_expr(e, p.input.schema())),
            RepartitionSpec::Range(r) => {
                r.by.iter().all(|e| pure_expr(e.inner(), p.input.schema()))
            }
            RepartitionSpec::Random(_) => true,
        },
        _ => false,
    };
    allowed && plan.arc_children().iter().all(consumer_replayable)
}

#[cfg(test)]
mod tests {
    use common_daft_config::DaftExecutionConfig;
    use common_runtime::JoinSet;
    use daft_local_plan::{
        FlightShuffleReadInput, LocalNodeContext, SharedShuffleSpec, ShuffleReadBackend,
    };
    use daft_logical_plan::{
        partitioning::{HashRepartitionConfig, RandomShuffleConfig},
        stats::StatsState,
    };
    use daft_micropartition::MicroPartition;

    use super::*;
    use crate::{
        pipeline_node::test_helpers::{bound_col_x, make_partition, test_schema},
        scheduling::{
            local_worker::{LocalSwordfishWorker, LocalSwordfishWorkerManager},
            scheduler::spawn_scheduler_actor,
        },
        statistics::StatisticsManagerRef,
    };

    struct Harness {
        scheduler: SchedulerHandle<SwordfishTask>,
        tasks: JoinSet<DaftResult<()>>,
        workers: Arc<LocalSwordfishWorkerManager>,
        worker: LocalSwordfishWorker,
        root: tempfile::TempDir,
        shuffle_id: u64,
        config: Arc<DaftExecutionConfig>,
    }

    impl Harness {
        fn new() -> Self {
            let worker_id: Arc<str> = Arc::from("shuffle-recovery-test");
            let worker = LocalSwordfishWorker::with_shuffle(worker_id.clone());
            let workers = Arc::new(LocalSwordfishWorkerManager::new(HashMap::from([(
                worker_id,
                worker.clone(),
            )])));
            let mut tasks = JoinSet::new();
            let scheduler =
                spawn_scheduler_actor(workers.clone(), &mut tasks, StatisticsManagerRef::default());
            Self {
                scheduler,
                tasks,
                workers,
                worker,
                root: tempfile::tempdir().unwrap(),
                shuffle_id: rand::random(),
                config: Arc::new(DaftExecutionConfig {
                    flight_shuffle_recovery_max_attempts: 2,
                    ..DaftExecutionConfig::default()
                }),
            }
        }

        fn producer(&self, start: i64, durability: &str, random: bool) -> SwordfishTask {
            let schema = test_schema();
            let input = make_partition(&(start..start + 90).collect::<Vec<_>>());
            let scan = LocalPhysicalPlan::in_memory_scan(
                0,
                schema.clone(),
                input.size_bytes(),
                StatsState::NotMaterialized,
                LocalNodeContext::default(),
            );
            let spec = if random {
                RepartitionSpec::Random(RandomShuffleConfig::new(Some(3)))
            } else {
                RepartitionSpec::Hash(HashRepartitionConfig::new(
                    Some(3),
                    vec![bound_col_x().into_inner()],
                ))
            };
            let plan = LocalPhysicalPlan::repartition_write(
                scan,
                3,
                schema,
                ShuffleBackend::Flight {
                    shuffle_id: self.shuffle_id,
                    shuffle_dirs: vec![],
                    compression: None,
                    shared: Some(SharedShuffleSpec {
                        root: self.root.path().to_string_lossy().into_owned(),
                        durability: durability.into(),
                    }),
                },
                spec,
                StatsState::NotMaterialized,
                LocalNodeContext::default(),
            );
            SwordfishTask::for_recovery_test(
                plan,
                HashMap::from([(0, Input::InMemory(vec![input]))]),
                self.config.clone(),
                self.scheduler.task_id_counter.next(),
            )
        }

        fn consumer(&self, outputs: &[MaterializedOutput], partition_idx: u32) -> SwordfishTask {
            let mut maps: BTreeMap<String, Vec<FlightMapOutput>> = BTreeMap::new();
            for result in outputs {
                let loc = output_location(result, self.shuffle_id, 3).unwrap();
                maps.entry(loc.server).or_default().push(loc.output);
            }
            let plan = LocalPhysicalPlan::shuffle_read(
                0,
                test_schema(),
                ShuffleReadBackend::Flight,
                StatsState::NotMaterialized,
                LocalNodeContext::default(),
            );
            let reads = vec![FlightShuffleReadInput {
                shuffle_id: self.shuffle_id,
                partition_idx,
                inputs_by_server: Arc::new(maps),
                shared_root: Some(Arc::from(self.root.path().to_str().unwrap())),
            }];
            SwordfishTask::for_recovery_test(
                plan,
                HashMap::from([(0, Input::FlightShuffle(reads))]),
                self.config.clone(),
                self.scheduler.task_id_counter.next(),
            )
        }

        async fn execute(&self, task: SwordfishTask) -> DaftResult<MaterializedOutput> {
            submit(SubmittableTask::task_only(task), &self.scheduler)?
                .await?
                .ok_or_else(|| DaftError::InternalError("test task cancelled".into()))
        }

        fn remove(&self, location: &Location) -> ShuffleFetchFailure {
            let path = daft_shuffles::store::shared_map_file(
                self.root.path().to_str().unwrap(),
                self.shuffle_id,
                location.output.input_id,
                location.output.attempt,
            );
            std::fs::remove_file(&path).unwrap();
            ShuffleFetchFailure {
                shuffle_id: self.shuffle_id,
                input_id: location.output.input_id,
                attempt: location.output.attempt,
                partition_idx: 0,
                path,
                message: "injected deletion".into(),
            }
        }

        async fn shutdown(mut self) {
            drop(self.scheduler);
            while let Some(result) = self.tasks.join_next().await {
                result.unwrap().unwrap();
            }
            tokio::task::spawn_blocking(move || drop((self.workers, self.worker)))
                .await
                .unwrap();
        }
    }

    fn values(result: &MaterializedOutput) -> Vec<i64> {
        let mut out = Vec::new();
        for part in result.partitions() {
            let mp = part.as_any().downcast_ref::<MicroPartition>().unwrap();
            for batch in mp.record_batches().iter() {
                let column = batch.get_column(0).i64().unwrap();
                out.extend((0..batch.len()).map(|i| column.get(i).unwrap()));
            }
        }
        out
    }

    #[tokio::test]
    async fn missing_map_and_unreachable_writer_recover_on_auto_and_rpc() -> DaftResult<()> {
        for route in ["auto", "rpc"] {
            let mut harness = tokio::task::spawn_blocking(Harness::new).await.unwrap();
            Arc::make_mut(&mut harness.config).flight_shuffle_read_source = route.into();
            let original = harness.execute(harness.producer(0, "none", false)).await?;
            let mut consumer = harness.consumer(std::slice::from_ref(&original), 0);
            let expected = values(&harness.execute(consumer.clone()).await?);
            harness.remove(&output_location(&original, harness.shuffle_id, 3)?);
            let Input::FlightShuffle(reads) = consumer.inputs_mut().get_mut(&0).unwrap() else {
                unreachable!()
            };
            let maps = reads[0]
                .inputs_by_server
                .values()
                .flatten()
                .copied()
                .collect();
            reads[0].inputs_by_server =
                Arc::new(BTreeMap::from([("grpc://127.0.0.1:1".into(), maps)]));
            // The old writer address is unreachable and its shared file is gone.
            // A healthy worker can replay the retained recipe.
            assert_eq!(values(&harness.execute(consumer).await?), expected);
            harness.shutdown().await;
        }
        Ok(())
    }

    #[tokio::test]
    async fn transient_reconstruction_exhausts_dispatcher_then_uses_remaining_map_budget()
    -> DaftResult<()> {
        let harness = tokio::task::spawn_blocking(Harness::new).await.unwrap();
        let original = harness.execute(harness.producer(0, "none", false)).await?;
        let consumer = harness.consumer(std::slice::from_ref(&original), 0);
        let expected = values(&harness.execute(consumer.clone()).await?);
        let failure = harness.remove(&output_location(&original, harness.shuffle_id, 3)?);
        // Initial physical attempt plus the dispatcher's default three retries.
        harness.worker.fail_reconstruction_executions(4);
        assert_eq!(values(&harness.execute(consumer).await?), expected);
        let producer = harness
            .scheduler
            .shuffle_recovery
            .lookup((failure.shuffle_id, failure.input_id, failure.attempt))
            .unwrap();
        assert_eq!(producer.repair.lock().await.attempts, 2);
        assert!(producer.repair.lock().await.terminal.is_none());
        drop(producer);
        harness.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn output_validation_failure_is_terminal_but_dependency_rejection_is_not()
    -> DaftResult<()> {
        let harness = tokio::task::spawn_blocking(Harness::new).await.unwrap();
        let original = harness.execute(harness.producer(0, "none", false)).await?;
        let old = output_location(&original, harness.shuffle_id, 3)?;
        let failure = harness.remove(&old);
        let key = (failure.shuffle_id, failure.input_id, failure.attempt);
        let schema = test_schema();
        let plan = LocalPhysicalPlan::in_memory_scan(
            0,
            schema,
            0,
            StatsState::NotMaterialized,
            LocalNodeContext::default(),
        );
        // Corrupt the recipe to model a completed producer with invalid output
        // metadata: execution succeeds but returns data rather than Flight refs.
        let template = SwordfishTask::for_recovery_test(
            plan,
            HashMap::from([(0, Input::InMemory(vec![make_partition(&[1, 2, 3])]))]),
            harness.config.clone(),
            100,
        );
        let producer = Arc::new(Producer {
            template,
            selected: Mutex::new(old),
            repair: AsyncMutex::new(RepairState::default()),
            num_partitions: 3,
        });
        let recovery = &harness.scheduler.shuffle_recovery;
        recovery
            .directory
            .lock()
            .unwrap()
            .outputs
            .insert(key, producer.clone());
        // Coordination rejection must leave this recipe's execution budget alone.
        recovery
            .repair(
                &failure,
                &harness.scheduler,
                CancellationToken::new(),
                vec![key],
            )
            .await
            .unwrap_err();
        assert_eq!(producer.repair.lock().await.attempts, 0);
        assert!(producer.repair.lock().await.terminal.is_none());
        recovery
            .repair(
                &failure,
                &harness.scheduler,
                CancellationToken::new(),
                vec![],
            )
            .await
            .unwrap_err();
        assert_eq!(producer.repair.lock().await.attempts, 1);
        assert!(producer.repair.lock().await.terminal.is_some());
        recovery
            .repair(
                &failure,
                &harness.scheduler,
                CancellationToken::new(),
                vec![],
            )
            .await
            .unwrap_err();
        assert_eq!(producer.repair.lock().await.attempts, 1);
        drop(producer);
        harness.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn wait_warning_does_not_limit_producer_or_consumer_execution() -> DaftResult<()> {
        let mut harness = tokio::task::spawn_blocking(Harness::new).await.unwrap();
        Arc::make_mut(&mut harness.config).flight_shuffle_recovery_wait_warn_ms = 5;
        let original = harness.execute(harness.producer(0, "none", false)).await?;
        let consumer = harness.consumer(std::slice::from_ref(&original), 0);
        let expected = values(&harness.execute(consumer.clone()).await?);
        harness.remove(&output_location(&original, harness.shuffle_id, 3)?);
        // Both the producer reconstruction and retried consumer exceed 5ms.
        harness.worker.set_execution_delay_ms(25);
        let result = harness.execute(consumer).await?;
        assert_eq!(values(&result), expected);
        harness.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn byte_cap_and_invalid_bookkeeping_leave_successful_maps_successful() -> DaftResult<()> {
        let mut harness = tokio::task::spawn_blocking(Harness::new).await.unwrap();
        Arc::make_mut(&mut harness.config).flight_shuffle_recovery_max_retained_bytes = 1;
        let task = harness.producer(0, "none", false);
        let output = harness.execute(task.clone()).await?;
        let recovery = &harness.scheduler.shuffle_recovery;
        assert!(recovery.directory.lock().unwrap().outputs.is_empty());
        let mut config = task.config().as_ref().clone();
        config.flight_shuffle_recovery_max_retained_bytes = usize::MAX;
        let task = SwordfishTask::for_recovery_test(
            task.plan(),
            HashMap::new(),
            Arc::new(config),
            task.task_id(),
        );
        let invalid =
            MaterializedOutput::new(vec![], Arc::from("test"), String::new(), task.task_id());
        recovery.register(&task, &invalid);
        assert!(recovery.directory.lock().unwrap().outputs.is_empty());
        assert!(output_location(&output, harness.shuffle_id, 3).is_ok());
        harness.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn dispatch_rebinds_a_deep_queue_after_prioritized_reconstruction() -> DaftResult<()> {
        let mut harness = tokio::task::spawn_blocking(Harness::new).await.unwrap();
        Arc::make_mut(&mut harness.config).flight_shuffle_recovery_wait_warn_ms = 5;
        let original = harness.execute(harness.producer(0, "none", false)).await?;
        let consumer = harness.consumer(std::slice::from_ref(&original), 0);
        let expected = values(&harness.execute(consumer.clone()).await?);
        let failure = harness.remove(&output_location(&original, harness.shuffle_id, 3)?);
        // Occupy the only worker so every consumer is queued with the old reference.
        let blocker = harness.producer(500, "none", false);
        harness.worker.add_active_task(&blocker);
        let mut queued = Vec::new();
        for _ in 0..128 {
            let task = SwordfishTask::for_recovery_test(
                consumer.plan(),
                consumer.inputs().clone(),
                harness.config.clone(),
                harness.scheduler.task_id_counter.next(),
            );
            // Raw submission deliberately has no consumer retry. A stale dispatch
            // therefore fails this test instead of silently running a second time.
            queued.push(SubmittableTask::task_only(task).submit_raw(&harness.scheduler)?);
        }
        let scheduler = harness.scheduler.clone();
        let repairing = tokio::spawn(async move {
            scheduler
                .shuffle_recovery
                .repair(&failure, &scheduler, CancellationToken::new(), vec![])
                .await
        });
        // The reconstruction is queued longer than the configured warning
        // interval. Queueing and execution must remain unrestricted.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!repairing.is_finished());
        harness.worker.mark_task_finished(blocker.task_context());
        repairing.await.unwrap()?;
        for result in futures::future::try_join_all(queued).await? {
            assert_eq!(values(&result.unwrap()), expected);
        }
        harness.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn rebinding_keeps_one_shared_map_list_for_thousands_of_consumers() -> DaftResult<()> {
        let harness = tokio::task::spawn_blocking(Harness::new).await.unwrap();
        let original = harness.execute(harness.producer(0, "none", false)).await?;
        let task = harness.consumer(std::slice::from_ref(&original), 0);
        let recovery = &harness.scheduler.shuffle_recovery;
        let failure = harness.remove(&output_location(&original, harness.shuffle_id, 3)?);
        recovery
            .repair(
                &failure,
                &harness.scheduler,
                CancellationToken::new(),
                vec![],
            )
            .await?;
        let mut inputs = task.inputs().clone();
        let Input::FlightShuffle(reads) = inputs.get_mut(&0).unwrap() else {
            unreachable!()
        };
        let maps = Arc::make_mut(&mut reads[0].inputs_by_server)
            .values_mut()
            .next()
            .unwrap();
        maps.extend((100_000..109_999).map(|input_id| FlightMapOutput {
            input_id,
            attempt: 1,
        }));
        let task = task.with_recovery_inputs(inputs);
        let bound = recovery.bound(&task).unwrap();
        let Input::FlightShuffle(expected) = &bound.inputs()[&0] else {
            unreachable!()
        };
        for _ in 0..8_000 {
            let bound = recovery.bound(&task).unwrap();
            let Input::FlightShuffle(reads) = &bound.inputs()[&0] else {
                unreachable!()
            };
            assert!(Arc::ptr_eq(
                &expected[0].inputs_by_server,
                &reads[0].inputs_by_server
            ));
        }
        assert_eq!(recovery.directory.lock().unwrap().bindings.len(), 1);
        let producer = recovery
            .lookup((failure.shuffle_id, failure.input_id, failure.attempt))
            .unwrap();
        let selected = producer.selected.lock().unwrap().output;
        {
            let mut directory = recovery.directory.lock().unwrap();
            directory
                .repairing
                .insert((failure.shuffle_id, selected.input_id, selected.attempt));
            directory.bindings.clear();
        }
        // An original reference must wait when its selected replacement is repaired.
        assert!(recovery.bound(&task).is_none());
        drop(producer);
        harness.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn ownership_wait_reports_without_aborting_or_poisoning_producers() -> DaftResult<()> {
        let mut harness = tokio::task::spawn_blocking(Harness::new).await.unwrap();
        Arc::make_mut(&mut harness.config).flight_shuffle_recovery_max_retained_maps = 1;
        Arc::make_mut(&mut harness.config).flight_shuffle_recovery_wait_warn_ms = 5;
        let original = harness.execute(harness.producer(0, "none", false)).await?;
        // Reaching the retention cap must not turn a successful map into a failure.
        harness.execute(harness.producer(90, "none", false)).await?;
        let failure = harness.remove(&output_location(&original, harness.shuffle_id, 3)?);
        let recovery = &harness.scheduler.shuffle_recovery;
        assert_eq!(recovery.directory.lock().unwrap().retained_maps, 1);
        let producer = recovery
            .lookup((failure.shuffle_id, failure.input_id, failure.attempt))
            .unwrap();
        let held = producer.repair.lock().await;
        let scheduler = harness.scheduler.clone();
        let report = failure.clone();
        let waiting = tokio::spawn(async move {
            scheduler
                .shuffle_recovery
                .repair(&report, &scheduler, CancellationToken::new(), vec![])
                .await
        });
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(!waiting.is_finished());
        assert_eq!(held.attempts, 0);
        assert!(held.terminal.is_none());
        drop(held);
        waiting.await.unwrap()?;
        assert_eq!(producer.repair.lock().await.attempts, 1);
        drop(producer);
        harness.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn reconstructs_once_and_restarts_consumers_without_duplicate_rows() -> DaftResult<()> {
        for durability in ["none", "background", "sync"] {
            let harness = tokio::task::spawn_blocking(Harness::new).await.unwrap();
            let first = harness
                .execute(harness.producer(0, durability, false))
                .await?;
            let second = harness
                .execute(harness.producer(90, durability, false))
                .await?;
            let old = output_location(&second, harness.shuffle_id, 3)?;
            let failure = harness.remove(&old);
            let outputs = [first, second];
            let consumers = (0..3).map(|p| harness.execute(harness.consumer(&outputs, p)));
            let results = futures::future::try_join_all(consumers).await?;
            let mut actual: Vec<_> = results.iter().flat_map(values).collect();
            actual.sort_unstable();
            assert_eq!(actual, (0..180).collect::<Vec<_>>());
            let producer = harness
                .scheduler
                .shuffle_recovery
                .lookup((harness.shuffle_id, old.output.input_id, old.output.attempt))
                .unwrap();
            assert_eq!(producer.repair.lock().await.attempts, 1);
            assert_ne!(producer.selected.lock().unwrap().output, old.output);
            // A delayed report from an old execution must not invalidate the new output.
            harness
                .scheduler
                .shuffle_recovery
                .repair(
                    &failure,
                    &harness.scheduler,
                    CancellationToken::new(),
                    vec![],
                )
                .await?;
            assert_eq!(producer.repair.lock().await.attempts, 1);
            drop(producer);
            harness.shutdown().await;
        }
        Ok(())
    }

    #[tokio::test]
    async fn repeated_deletion_exhausts_one_shared_producer_budget() -> DaftResult<()> {
        let harness = tokio::task::spawn_blocking(Harness::new).await.unwrap();
        let original = harness.execute(harness.producer(0, "none", false)).await?;
        let old = output_location(&original, harness.shuffle_id, 3)?;
        let producer = harness
            .scheduler
            .shuffle_recovery
            .lookup((harness.shuffle_id, old.output.input_id, old.output.attempt))
            .unwrap();
        for _ in 0..2 {
            harness.remove(&producer.selected.lock().unwrap().clone());
            harness
                .execute(harness.consumer(std::slice::from_ref(&original), 0))
                .await?;
        }
        harness.remove(&producer.selected.lock().unwrap().clone());
        let error = harness
            .execute(harness.consumer(&[original], 0))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("budget exhausted"), "{error}");
        assert_eq!(producer.repair.lock().await.attempts, 2);
        drop(producer);
        harness.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn disabled_recovery_does_not_retain_or_reconstruct_outputs() -> DaftResult<()> {
        let mut harness = tokio::task::spawn_blocking(Harness::new).await.unwrap();
        Arc::make_mut(&mut harness.config).flight_shuffle_recovery_max_attempts = 0;
        let original = harness.execute(harness.producer(0, "none", false)).await?;
        let failure = harness.remove(&output_location(&original, harness.shuffle_id, 3)?);
        let result = harness.execute(harness.consumer(&[original], 0)).await;
        assert!(
            harness
                .scheduler
                .shuffle_recovery
                .directory
                .lock()
                .unwrap()
                .outputs
                .is_empty()
        );
        harness.shutdown().await;
        let actual = result.unwrap_err().shuffle_fetch_failure().unwrap();
        assert_eq!(
            (actual.shuffle_id, actual.input_id, actual.attempt),
            (failure.shuffle_id, failure.input_id, failure.attempt)
        );
        Ok(())
    }

    #[tokio::test]
    async fn limit_consumer_uses_repaired_refs_on_first_execution() -> DaftResult<()> {
        let harness = tokio::task::spawn_blocking(Harness::new).await.unwrap();
        let original = harness.execute(harness.producer(0, "none", false)).await?;
        let old = output_location(&original, harness.shuffle_id, 3)?;
        harness.remove(&old);
        let read = harness.consumer(std::slice::from_ref(&original), 0);
        let expected = values(&harness.execute(read.clone()).await?);
        // Order-sensitive consumers are safe to restart with replay-equivalent inputs.
        // First execution also observes replacements published by another task.
        let limit = LocalPhysicalPlan::limit(
            read.plan(),
            1_000,
            None,
            StatsState::NotMaterialized,
            LocalNodeContext::default(),
        );
        let task = SwordfishTask::for_recovery_test(
            limit,
            read.inputs().clone(),
            harness.config.clone(),
            harness.scheduler.task_id_counter.next(),
        );
        assert!(consumer_replayable(&task.plan()));
        let result = harness.execute(task).await;
        harness.shutdown().await;
        assert_eq!(values(&result?), expected);
        Ok(())
    }

    #[tokio::test]
    async fn cancellation_releases_repair_and_notifies_once() -> DaftResult<()> {
        use crate::scheduling::task::TaskNotifyToken;
        let harness = tokio::task::spawn_blocking(Harness::new).await.unwrap();
        let original = harness.execute(harness.producer(0, "none", false)).await?;
        let old = output_location(&original, harness.shuffle_id, 3)?;
        harness.remove(&old);
        let recovery = harness.scheduler.shuffle_recovery.clone();
        let producer = recovery
            .lookup((harness.shuffle_id, old.output.input_id, old.output.attempt))
            .unwrap();
        // Hold all reconstruction slots to cancel at a deterministic wait point.
        let slots = recovery
            .slots
            .get_or_init(|| Arc::new(Semaphore::new(4)))
            .acquire_many(4)
            .await
            .unwrap();
        let cancel = CancellationToken::new();
        let (notify, mut notifications) = TaskNotifyToken::new();
        let consumer = harness.consumer(std::slice::from_ref(&original), 0);
        let task_id = consumer.task_id();
        let running = tokio::spawn(submit(
            SubmittableTask::new(consumer, cancel.clone(), vec![notify]),
            &harness.scheduler,
        )?);
        tokio::time::timeout(Duration::from_secs(5), async {
            while producer.repair.try_lock().is_ok() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        cancel.cancel();
        assert!(
            tokio::time::timeout(Duration::from_secs(5), running)
                .await
                .unwrap()
                .unwrap()?
                .is_none()
        );
        assert_eq!(notifications.recv().await, Some(task_id));
        assert_eq!(notifications.recv().await, None);
        assert_eq!(producer.selected.lock().unwrap().output, old.output);
        drop(slots);
        // Cancellation releases ownership; a remaining consumer can still repair.
        harness.execute(harness.consumer(&[original], 0)).await?;
        assert_eq!(producer.repair.lock().await.attempts, 1);
        drop(producer);
        drop(recovery);
        harness.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn reconstructs_missing_dependency_before_descendant() -> DaftResult<()> {
        for max_depth in [1, 16] {
            let mut harness = tokio::task::spawn_blocking(Harness::new).await.unwrap();
            Arc::make_mut(&mut harness.config).flight_shuffle_recovery_max_depth = max_depth;
            let upstream = harness.execute(harness.producer(0, "none", false)).await?;
            let old = output_location(&upstream, harness.shuffle_id, 3)?;
            let upstream_path = daft_shuffles::store::shared_map_file(
                harness.root.path().to_str().unwrap(),
                harness.shuffle_id,
                old.output.input_id,
                old.output.attempt,
            );
            let read = harness.consumer(&[upstream], 0);
            let mut expected = values(&harness.execute(read.clone()).await?);
            expected.sort_unstable();
            harness.shuffle_id = rand::random();
            let plan = LocalPhysicalPlan::repartition_write(
                read.plan(),
                3,
                test_schema(),
                ShuffleBackend::Flight {
                    shuffle_id: harness.shuffle_id,
                    shuffle_dirs: vec![],
                    compression: None,
                    shared: Some(SharedShuffleSpec {
                        root: harness.root.path().to_str().unwrap().into(),
                        durability: "none".into(),
                    }),
                },
                RepartitionSpec::Hash(HashRepartitionConfig::new(
                    Some(3),
                    vec![bound_col_x().into_inner()],
                )),
                StatsState::NotMaterialized,
                LocalNodeContext::default(),
            );
            let task = SwordfishTask::for_recovery_test(
                plan,
                read.inputs().clone(),
                harness.config.clone(),
                harness.scheduler.task_id_counter.next(),
            );
            let downstream = harness.execute(task).await?;
            std::fs::remove_file(upstream_path).unwrap();
            let failure = harness.remove(&output_location(&downstream, harness.shuffle_id, 3)?);
            if max_depth == 1 {
                let recovery = &harness.scheduler.shuffle_recovery;
                let error = recovery
                    .repair(
                        &failure,
                        &harness.scheduler,
                        CancellationToken::new(),
                        vec![],
                    )
                    .await
                    .unwrap_err();
                assert!(
                    error
                        .to_string()
                        .contains("excessive reconstruction dependencies")
                );
                let producer = recovery
                    .lookup((failure.shuffle_id, failure.input_id, failure.attempt))
                    .unwrap();
                assert_eq!(producer.repair.lock().await.attempts, 0);
                assert!(producer.repair.lock().await.terminal.is_none());
                drop(producer);
                harness.shutdown().await;
                continue;
            }
            let outputs = [downstream];
            let results = futures::future::try_join_all(
                (0..3).map(|p| harness.execute(harness.consumer(&outputs, p))),
            )
            .await?;
            let mut actual: Vec<_> = results.iter().flat_map(values).collect();
            actual.sort_unstable();
            assert_eq!(actual, expected);
            // Two producers, each with its original and one replacement alias.
            assert_eq!(
                harness
                    .scheduler
                    .shuffle_recovery
                    .directory
                    .lock()
                    .unwrap()
                    .outputs
                    .len(),
                4
            );
            harness.shutdown().await;
        }
        Ok(())
    }

    #[tokio::test]
    async fn random_partition_output_is_not_reconstructed() -> DaftResult<()> {
        let harness = tokio::task::spawn_blocking(Harness::new).await.unwrap();
        let task = harness.producer(0, "none", true);
        assert!(producer_spec(&task).is_none());
        let result = harness.execute(task).await?;
        harness.remove(&output_location(&result, harness.shuffle_id, 3)?);
        let error = harness
            .execute(harness.consumer(&[result], 0))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("not replayable"), "{error}");
        assert!(
            harness
                .scheduler
                .shuffle_recovery
                .directory
                .lock()
                .unwrap()
                .outputs
                .is_empty()
        );
        harness.shutdown().await;
        Ok(())
    }
}
