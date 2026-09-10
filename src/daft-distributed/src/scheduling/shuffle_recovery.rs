//! Plan-owned reconstruction of replay-equivalent shared shuffle outputs.
//! Resource scheduling remains in SchedulerLoop; no recovery waits inside it.
use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        Arc, Mutex,
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
use tokio::sync::{Mutex as AsyncMutex, Semaphore};
use tokio_util::sync::CancellationToken;

use super::{
    scheduler::{SchedulerHandle, SubmittableTask, SubmittedTask},
    task::{SwordfishTask, Task},
};
use crate::pipeline_node::MaterializedOutput;

// Physical identity; aliases are bounded by the per-producer reconstruction budget.
type OutputKey = (u64, u32, u64);

#[derive(Clone, Debug)]
struct Location {
    output: FlightMapOutput,
    server: String,
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

#[derive(Debug)]
pub(crate) struct ShuffleRecovery {
    outputs: Mutex<HashMap<OutputKey, Arc<Producer>>>,
    version: AtomicU64,
    slots: Semaphore,
    max_attempts: u32,
}

impl ShuffleRecovery {
    pub fn new() -> Self {
        Self {
            outputs: Mutex::new(HashMap::new()),
            version: AtomicU64::new(0),
            slots: Semaphore::new(4),
            max_attempts: std::env::var("DAFT_SHUFFLE_RECOVERY_MAX_ATTEMPTS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(2),
        }
    }

    fn register(&self, task: &SwordfishTask, result: &MaterializedOutput) -> DaftResult<()> {
        let Some((shuffle_id, num_partitions)) = producer_spec(task) else {
            return Ok(());
        };
        let location = output_location(result, shuffle_id, num_partitions)?;
        let key = (
            shuffle_id,
            location.output.input_id,
            location.output.attempt,
        );
        self.outputs.lock().unwrap().entry(key).or_insert_with(|| {
            Arc::new(Producer {
                template: task.clone(),
                selected: Mutex::new(location),
                repair: AsyncMutex::new(RepairState::default()),
                num_partitions,
            })
        });
        Ok(())
    }

    fn lookup(&self, key: OutputKey) -> Option<Arc<Producer>> {
        self.outputs.lock().unwrap().get(&key).cloned()
    }

    /// No directory walk or new map/ref matrix in normal successful execution.
    /// After repair, each task receives a fresh immutable snapshot. Old aliases
    /// continue to resolve to the same logical producer.
    fn bind(&self, task: &SwordfishTask) -> SwordfishTask {
        if self.version.load(Ordering::Acquire) == 0 {
            return task.clone();
        }
        let mut inputs = task.inputs().clone();
        for input in inputs.values_mut() {
            let Input::FlightShuffle(reads) = input else {
                continue;
            };
            for read in reads {
                let mut by_server: BTreeMap<String, Vec<FlightMapOutput>> = BTreeMap::new();
                for (server, maps) in read.inputs_by_server.iter() {
                    for map in maps {
                        let location = self
                            .lookup((read.shuffle_id, map.input_id, map.attempt))
                            .map(|p| p.selected.lock().unwrap().clone())
                            .unwrap_or_else(|| Location {
                                output: *map,
                                server: server.clone(),
                            });
                        by_server
                            .entry(location.server)
                            .or_default()
                            .push(location.output);
                    }
                }
                read.inputs_by_server = Arc::new(by_server);
            }
        }
        task.with_recovery_inputs(inputs)
    }

    fn run<'a>(
        &'a self,
        mut task: SwordfishTask,
        scheduler: &'a SchedulerHandle<SwordfishTask>,
        cancel: CancellationToken,
        ancestors: Vec<OutputKey>,
        reconstruction: bool,
    ) -> BoxFuture<'a, DaftResult<Option<MaterializedOutput>>> {
        async move {
            // Independent from each producer's budget: a consumer can encounter
            // several missing maps, but cannot keep discovering new failures forever.
            for failure_count in 0..=64 {
                if cancel.is_cancelled() {
                    return Ok(None);
                }
                task = self.bind(&task);
                let result = {
                    let _slot = if reconstruction {
                        Some(
                            self.slots
                                .acquire()
                                .await
                                .map_err(|e| DaftError::InternalError(e.to_string()))?,
                        )
                    } else {
                        None
                    };
                    SubmittableTask::new(task.clone(), cancel.child_token(), vec![])
                        .submit(scheduler)?
                        .await
                }; // Release the slot before recursively reconstructing dependencies.
                match result {
                    Ok(result) => return Ok(result),
                    Err(error) => {
                        let Some(failure) = error.shuffle_fetch_failure() else {
                            return Err(error);
                        };
                        if failure_count == 64 {
                            return Err(recovery_error(
                                &failure,
                                "consumer recovery budget exhausted",
                            ));
                        }
                        self.repair(&failure, scheduler, cancel.clone(), ancestors.clone())
                            .await?;
                        task =
                            task.recovery_attempt(scheduler.task_id_counter.next(), reconstruction);
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
            if ancestors.contains(&key) || ancestors.len() >= 16 {
                return Err(recovery_error(failure, "cyclic or excessive reconstruction dependencies"));
            }
            ancestors.push(key);
            let producer = self.lookup(key).ok_or_else(|| recovery_error(failure,
                "producer is not replayable from retained inputs; automatic stage rollback is not supported"))?;
            let mut repair = producer.repair.lock().await;
            // A waiting consumer may report an attempt another consumer already replaced.
            let selected = producer.selected.lock().unwrap().clone();
            if (selected.output.input_id, selected.output.attempt) != (failure.input_id, failure.attempt) {
                return Ok(());
            }
            if let Some(reason) = &repair.terminal { return Err(recovery_error(failure, reason)); }
            if repair.attempts >= self.max_attempts {
                repair.terminal = Some("map reconstruction budget exhausted".into());
                return Err(recovery_error(failure, "map reconstruction budget exhausted"));
            }
            repair.attempts += 1;
            let task = producer.template.recovery_attempt(scheduler.task_id_counter.next(), true);
            tracing::warn!(shuffle_id = failure.shuffle_id, input_id = failure.input_id,
                attempt = failure.attempt, reconstruction = repair.attempts,
                task_id = task.task_id(), "Reconstructing unavailable shared shuffle output");
            let result = self.run(task, scheduler, cancel, ancestors, true).await;
            let result = match result {
                Ok(Some(result)) => result,
                Ok(None) => return Err(recovery_error(failure, "reconstruction cancelled")),
                Err(error) => {
                    repair.terminal = Some(error.to_string());
                    return Err(error);
                }
            };
            let replacement = output_location(&result, failure.shuffle_id, producer.num_partitions)?;
            let replacement_key = (failure.shuffle_id, replacement.output.input_id, replacement.output.attempt);
            // Publish the alias before the selection; consumers can resolve every
            // reference they are handed, including from another reconstruction.
            self.outputs.lock().unwrap().insert(replacement_key, producer.clone());
            *producer.selected.lock().unwrap() = replacement;
            self.version.fetch_add(1, Ordering::Release);
            tracing::info!(shuffle_id = failure.shuffle_id, old_input_id = failure.input_id,
                old_attempt = failure.attempt, input_id = replacement_key.1,
                attempt = replacement_key.2, "Published reconstructed shared shuffle output");
            Ok(())
        }.boxed()
    }
}

pub(crate) fn submit(
    submittable: SubmittableTask<SwordfishTask>,
    scheduler: &SchedulerHandle<SwordfishTask>,
) -> DaftResult<SubmittedTask> {
    let (task, cancel, notifications) = submittable.into_parts();
    let recovery = scheduler.shuffle_recovery.clone();
    if recovery.max_attempts == 0 || !task_replayable(&task, false) {
        return SubmittableTask::new(task, cancel, notifications).submit(scheduler);
    }
    let task_id = task.task_id();
    let scheduler = scheduler.clone();
    let future_cancel = cancel.clone();
    let completion = scheduler
        .recovery_statistics
        .hold_recovery_completion(&task.task_context());
    let future = async move {
        let _completion = completion;
        // Ordinary execution has no recovery timeout. Start a recovery deadline
        // only when a fetch failure is first observed.
        let initial = recovery.bind(&task);
        let result = SubmittableTask::new(initial, future_cancel.child_token(), vec![])
            .submit(&scheduler)?
            .await;
        let result = match result {
            Ok(result) => result,
            Err(error) => {
                let Some(failure) = error.shuffle_fetch_failure() else {
                    return Err(error);
                };
                let recovering = async {
                    recovery
                        .repair(&failure, &scheduler, future_cancel.clone(), vec![])
                        .await?;
                    let retry = task.recovery_attempt(scheduler.task_id_counter.next(), false);
                    recovery
                        .run(retry, &scheduler, future_cancel.clone(), vec![], false)
                        .await
                };
                tokio::select! {
                    biased;
                    _ = future_cancel.cancelled() => return Ok(None),
                    result = tokio::time::timeout(Duration::from_secs(120), recovering) => {
                        result.map_err(|_| recovery_error(&failure, "recovery deadline exceeded"))??
                    }
                }
            }
        };
        if let Some(result) = &result {
            recovery.register(&task, result)?;
        }
        Ok(result)
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
    if !task_replayable(task, true) {
        return None;
    }
    match task.plan().as_ref() {
        LocalPhysicalPlan::RepartitionWrite(write) => match &write.backend {
            ShuffleBackend::Flight {
                shuffle_id,
                shared: Some(_),
                ..
            } => Some((*shuffle_id, write.num_partitions)),
            _ => None,
        },
        _ => None,
    }
}

fn scalar_type(dtype: &DataType) -> bool {
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
    matches!(
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
    ) && expr.to_field(schema).is_ok_and(|f| scalar_type(&f.dtype))
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

fn task_replayable(task: &SwordfishTask, producer: bool) -> bool {
    // Retained in-memory inputs and immutable shuffle files are replayable.
    // ScanTasks/GlobPaths do not provide source snapshot guarantees.
    task.inputs()
        .values()
        .all(|input| matches!(input, Input::InMemory(_) | Input::FlightShuffle(_)))
        && plan_replayable(&task.plan(), producer)
}

fn plan_replayable(plan: &daft_local_plan::LocalPhysicalPlanRef, producer: bool) -> bool {
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
        LocalPhysicalPlan::HashAggregate(p) if !producer => {
            p.group_by
                .iter()
                .all(|e| pure_expr(e.inner(), p.input.schema()))
                && p.aggregations
                    .iter()
                    .all(|a| safe_agg(a.inner(), p.input.schema()))
        }
        LocalPhysicalPlan::UnGroupedAggregate(p) if !producer => p
            .aggregations
            .iter()
            .all(|a| safe_agg(a.inner(), p.input.schema())),
        _ => false,
    };
    allowed
        && plan
            .arc_children()
            .iter()
            .all(|p| plan_replayable(p, producer))
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
        root: tempfile::TempDir,
        shuffle_id: u64,
        config: Arc<DaftExecutionConfig>,
    }

    impl Harness {
        fn new() -> Self {
            let worker_id: Arc<str> = Arc::from("shuffle-recovery-test");
            let workers = Arc::new(LocalSwordfishWorkerManager::new(HashMap::from([(
                worker_id.clone(),
                LocalSwordfishWorker::with_shuffle(worker_id),
            )])));
            let mut tasks = JoinSet::new();
            let scheduler =
                spawn_scheduler_actor(workers.clone(), &mut tasks, StatisticsManagerRef::default());
            Self {
                scheduler,
                tasks,
                workers,
                root: tempfile::tempdir().unwrap(),
                shuffle_id: rand::random(),
                config: Arc::new(DaftExecutionConfig::default()),
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
            tokio::task::spawn_blocking(move || drop(self.workers))
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
        let slots = recovery.slots.acquire_many(4).await.unwrap();
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
        assert_eq!(producer.repair.lock().await.attempts, 2);
        drop(producer);
        drop(recovery);
        harness.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn reconstructs_missing_dependency_before_descendant() -> DaftResult<()> {
        let mut harness = tokio::task::spawn_blocking(Harness::new).await.unwrap();
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
        harness.remove(&output_location(&downstream, harness.shuffle_id, 3)?);
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
                .outputs
                .lock()
                .unwrap()
                .len(),
            4
        );
        harness.shutdown().await;
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
                .outputs
                .lock()
                .unwrap()
                .is_empty()
        );
        harness.shutdown().await;
        Ok(())
    }
}
