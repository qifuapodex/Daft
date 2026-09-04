use std::{collections::HashMap, sync::Arc, time::Duration};

use common_error::{DaftError, DaftResult};
use common_runtime::{JoinSet, JoinSetId};

use super::{
    scheduler::{PendingTask, ScheduledTask},
    task::{Task, TaskResultAwaiter, TaskStatus},
    worker::{Worker, WorkerManager},
};
use crate::{
    scheduling::task::TaskResultHandle,
    statistics::{StatisticsManagerRef, TaskEvent},
};

const DISPATCHER_LOG_TARGET: &str = "DaftFlotillaDispatcher";

/// Retry budget for a task that failed with a transient error (network blips, timeouts,
/// throttling). Deliberately small: the task is re-run as-is, so a dependency that is
/// genuinely down should surface as a query failure quickly rather than after minutes of
/// retrying.
const DEFAULT_MAX_TRANSIENT_RETRIES: u32 = 3;
const MAX_TRANSIENT_RETRIES_ENV: &str = "DAFT_FLOTILLA_TASK_MAX_TRANSIENT_RETRIES";

/// Retry budget for a task whose *worker* went away. Larger than the transient budget
/// because losing a node is not the task's fault and is expected during downscaling --
/// but still bounded: a task that reliably OOMs its actor would otherwise loop forever,
/// and every `WorkerDied` also removes a node from the cluster via `mark_worker_died`.
const DEFAULT_MAX_INFRA_RETRIES: u32 = 10;
const MAX_INFRA_RETRIES_ENV: &str = "DAFT_FLOTILLA_TASK_MAX_INFRA_RETRIES";

/// Ceiling on the exponential backoff between attempts.
const MAX_RETRY_BACKOFF: Duration = Duration::from_secs(30);

fn retry_limit_from_env(var: &str, default: u32) -> u32 {
    std::env::var(var)
        .ok()
        .and_then(|val| val.parse::<u32>().ok())
        .unwrap_or(default)
}

/// Backoff before the next attempt, given how many attempts have already been made.
/// Without this a retry is redispatched on the very next scheduler tick, which for a
/// throttled or briefly unreachable dependency just burns the whole budget in a few
/// seconds. Doubles from 1s and is capped at [`MAX_RETRY_BACKOFF`].
fn retry_backoff(attempts_made: u32) -> Duration {
    Duration::from_secs(1u64 << attempts_made.min(6)).min(MAX_RETRY_BACKOFF)
}

/// What the dispatcher will do with a task that just came back from a worker.
///
/// This has to be decided *before* the statistics event is emitted: the event carries a
/// `retryable` flag that suppresses the terminal `TaskEnd`, so emitting it first would
/// report an attempt that is about to be repeated as a terminal failure and leave the
/// task with two `TaskEnd` events.
#[derive(Clone, Copy, PartialEq, Eq)]
enum TaskDisposition {
    /// Re-enqueue for another attempt.
    Retry,
    /// Give up and report the outcome upstream.
    Terminal,
}

pub(super) struct Dispatcher<W: Worker> {
    // JoinSet of task results futures
    task_result_joinset: JoinSet<TaskStatus>,
    // Mapping of joinset task id to the scheduled task
    // The scheduled task is kept here so that we can reschedule the task if it fails
    joinset_id_to_task: HashMap<JoinSetId, ScheduledTask<W::Task>>,
    statistics_manager: StatisticsManagerRef,
    max_transient_retries: u32,
    max_infra_retries: u32,
}

impl<W: Worker> Dispatcher<W> {
    pub fn new(statistics_manager: StatisticsManagerRef) -> Self {
        Self {
            task_result_joinset: JoinSet::new(),
            joinset_id_to_task: HashMap::new(),
            statistics_manager,
            max_transient_retries: retry_limit_from_env(
                MAX_TRANSIENT_RETRIES_ENV,
                DEFAULT_MAX_TRANSIENT_RETRIES,
            ),
            max_infra_retries: retry_limit_from_env(
                MAX_INFRA_RETRIES_ENV,
                DEFAULT_MAX_INFRA_RETRIES,
            ),
        }
    }

    #[cfg(test)]
    fn with_retry_limits(
        statistics_manager: StatisticsManagerRef,
        max_transient_retries: u32,
        max_infra_retries: u32,
    ) -> Self {
        Self {
            max_transient_retries,
            max_infra_retries,
            ..Self::new(statistics_manager)
        }
    }

    pub fn dispatch_tasks(
        &mut self,
        scheduled_tasks: Vec<ScheduledTask<W::Task>>,
        worker_manager: &Arc<dyn WorkerManager<Worker = W>>,
    ) -> DaftResult<()> {
        let mut worker_to_tasks = HashMap::new();
        let mut task_context_to_task = HashMap::new();

        for scheduled_task in scheduled_tasks {
            let worker_id = scheduled_task.worker_id();
            let task = scheduled_task.task();
            task_context_to_task.insert(task.task_context(), scheduled_task);
            worker_to_tasks
                .entry(worker_id)
                .or_insert_with(Vec::new)
                .push(task);
        }

        let result_handles = worker_manager.submit_tasks_to_workers(worker_to_tasks)?;

        for result_handle in result_handles {
            let scheduled_task = task_context_to_task
                .remove(&result_handle.task_context())
                .expect("Task should be present in task_context_to_task");
            let result_awaiter =
                TaskResultAwaiter::new(result_handle, scheduled_task.cancel_token());
            let id = self
                .task_result_joinset
                .spawn(result_awaiter.await_result());
            self.joinset_id_to_task.insert(id, scheduled_task);
        }

        Ok(())
    }

    /// Await at least one completed task and return any failed tasks that need to be rescheduled.
    /// This method will block until at least one task completes, then poll for any additional
    /// completed tasks that are immediately available.
    pub async fn await_completed_tasks(
        &mut self,
        worker_manager: &Arc<dyn WorkerManager<Worker = W>>,
    ) -> DaftResult<Vec<PendingTask<W::Task>>> {
        let mut failed_tasks = Vec::new();
        let mut task_results = Vec::new();

        // Wait for at least one task to complete
        if let Some((id, task_result)) = self.task_result_joinset.join_next_with_id().await {
            let scheduled_task = self
                .joinset_id_to_task
                .remove(&id)
                .expect("Task should be present in joinset_id_to_task");
            task_results.push(CompletedTask::new(task_result, scheduled_task));

            // Collect any additional completed tasks that are immediately available
            while let Some((id, task_result)) = self.task_result_joinset.try_join_next_with_id() {
                let scheduled_task = self
                    .joinset_id_to_task
                    .remove(&id)
                    .expect("Task should be present in joinset_id_to_task");
                task_results.push(CompletedTask::new(task_result, scheduled_task));
            }

            tracing::info!(target: DISPATCHER_LOG_TARGET, num_tasks = task_results.len(), "Awaited completed tasks");
            tracing::debug!(target: DISPATCHER_LOG_TARGET, completed_tasks = %format!("{:#?}", task_results));

            // Process all completed tasks
            for CompletedTask { task_result, task } in task_results {
                let (worker_id, task, result_tx, canc, attempts) = task.into_inner();

                // Always mark the task as finished regardless of the result
                worker_manager.mark_task_finished(task.task_context(), worker_id.clone());

                // Decide the disposition before emitting the event, so the event's
                // `retryable` flag matches what actually happens to the task.
                // `attempts` counts failures *before* this one, so the run that just
                // finished is attempt number `attempts + 1`.
                let disposition = match &task_result {
                    Ok(TaskStatus::Failed { error })
                        if error.is_transient() && attempts < self.max_transient_retries =>
                    {
                        TaskDisposition::Retry
                    }
                    Ok(TaskStatus::WorkerDied | TaskStatus::WorkerUnavailable)
                        if attempts < self.max_infra_retries =>
                    {
                        TaskDisposition::Retry
                    }
                    _ => TaskDisposition::Terminal,
                };

                // Send the event to the statistics manager
                let event = TaskEvent::new(
                    task.task_context(),
                    &task_result,
                    worker_id.clone(),
                    disposition == TaskDisposition::Retry,
                );
                self.statistics_manager.handle_event(event)?;

                match task_result {
                    Ok(task_status) => match task_status {
                        // Task completed successfully, send the result to the result_tx
                        TaskStatus::Success { result, .. } => {
                            if result_tx.send(Ok(Some(result))).is_err() {
                                tracing::error!(target: DISPATCHER_LOG_TARGET, error = "Failed to send result of task to result_tx", task_context = ?task.task_context());
                            }
                        }
                        // Task failed. Transient errors get another attempt; everything
                        // else goes straight upstream and fails the query.
                        TaskStatus::Failed { error } => match disposition {
                            TaskDisposition::Retry => {
                                let backoff = retry_backoff(attempts);
                                tracing::warn!(
                                    target: DISPATCHER_LOG_TARGET,
                                    attempt = attempts + 1,
                                    max_attempts = self.max_transient_retries + 1,
                                    backoff_secs = backoff.as_secs(),
                                    error = %error,
                                    task_context = ?task.task_context(),
                                    "Task failed with a transient error, retrying"
                                );
                                failed_tasks.push(PendingTask::retry(
                                    task,
                                    result_tx,
                                    canc,
                                    attempts + 1,
                                    backoff,
                                ));
                            }
                            TaskDisposition::Terminal => {
                                if attempts > 0 {
                                    // Forward the original error rather than wrapping it:
                                    // the underlying cause is what the user needs, and the
                                    // attempt count is only useful in the logs.
                                    tracing::error!(
                                        target: DISPATCHER_LOG_TARGET,
                                        attempts = attempts + 1,
                                        error = %error,
                                        task_context = ?task.task_context(),
                                        "Task still failing after exhausting its transient retry budget"
                                    );
                                }
                                if result_tx.send(Err(error)).is_err() {
                                    tracing::error!(target: DISPATCHER_LOG_TARGET, error = "Failed to send error of task to result_tx", task_context = ?task.task_context());
                                }
                            }
                        },
                        // Task cancelled, do nothing
                        TaskStatus::Cancelled => {}
                        // The task's worker went away. Retry it on another worker, but
                        // only up to `max_infra_retries`: a task that keeps killing its
                        // actor would otherwise loop forever, and each `WorkerDied` also
                        // drops a node from the cluster.
                        status @ (TaskStatus::WorkerDied | TaskStatus::WorkerUnavailable) => {
                            let worker_died = matches!(status, TaskStatus::WorkerDied);
                            if worker_died {
                                worker_manager.mark_worker_died(worker_id);
                            }
                            match disposition {
                                TaskDisposition::Retry => {
                                    let backoff = retry_backoff(attempts);
                                    tracing::warn!(
                                        target: DISPATCHER_LOG_TARGET,
                                        attempt = attempts + 1,
                                        max_attempts = self.max_infra_retries + 1,
                                        backoff_secs = backoff.as_secs(),
                                        worker_died,
                                        task_context = ?task.task_context(),
                                        "Task lost its worker, retrying"
                                    );
                                    failed_tasks.push(PendingTask::retry(
                                        task,
                                        result_tx,
                                        canc,
                                        attempts + 1,
                                        backoff,
                                    ));
                                }
                                TaskDisposition::Terminal => {
                                    let error = DaftError::InternalError(format!(
                                        "Task {:?} lost its worker on all {} attempts; giving up. \
                                         This usually means the task itself is killing the worker \
                                         (e.g. running it out of memory). Set {} to allow more attempts.",
                                        task.task_context(),
                                        attempts + 1,
                                        MAX_INFRA_RETRIES_ENV,
                                    ));
                                    tracing::error!(
                                        target: DISPATCHER_LOG_TARGET,
                                        attempts = attempts + 1,
                                        worker_died,
                                        task_context = ?task.task_context(),
                                        "Task exhausted its worker-loss retry budget"
                                    );
                                    if result_tx.send(Err(error)).is_err() {
                                        tracing::error!(target: DISPATCHER_LOG_TARGET, error = "Failed to send error of task to result_tx", task_context = ?task.task_context());
                                    }
                                }
                            }
                        }
                    },
                    // Task failed because of panic in joinset, send the error to the result_tx
                    Err(e) => {
                        if result_tx.send(Err(e)).is_err() {
                            tracing::error!(target: DISPATCHER_LOG_TARGET, error = "Failed to send error of task to result_tx", task_context = ?task.task_context());
                        }
                    }
                }
            }
        }

        Ok(failed_tasks)
    }

    pub fn has_running_tasks(&self) -> bool {
        !self.task_result_joinset.is_empty()
    }
}

struct CompletedTask<T: Task> {
    task_result: DaftResult<TaskStatus>,
    task: ScheduledTask<T>,
}

impl<T: Task> CompletedTask<T> {
    fn new(task_result: DaftResult<TaskStatus>, task: ScheduledTask<T>) -> Self {
        Self { task_result, task }
    }
}

impl<T: Task> std::fmt::Debug for CompletedTask<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CompletedTask({:?}, {:?})",
            self.task.task().task_context(),
            self.task_result
        )
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use rand::{Rng, SeedableRng, rngs::StdRng};

    use super::*;
    use crate::{
        scheduling::{
            scheduler::{
                SchedulerHandle, SubmittableTask, SubmittedTask, test_utils::setup_workers,
            },
            task::{SchedulingStrategy, tests::MockTaskFailure},
            tests::{MockTask, MockTaskBuilder, create_mock_partition_ref},
            worker::{
                WorkerId,
                tests::{MockWorker, MockWorkerManager},
            },
        },
        utils::channel::create_oneshot_channel,
    };

    fn setup_dispatcher_test_context(
        worker_configs: &[(WorkerId, usize)],
    ) -> (
        Dispatcher<MockWorker>,
        Arc<dyn WorkerManager<Worker = MockWorker>>,
    ) {
        setup_dispatcher_test_context_with_retry_limits(
            worker_configs,
            DEFAULT_MAX_TRANSIENT_RETRIES,
            DEFAULT_MAX_INFRA_RETRIES,
        )
    }

    /// Retry limits are injected rather than read from the environment: they are
    /// process-global there, and these tests run in parallel in one process.
    fn setup_dispatcher_test_context_with_retry_limits(
        worker_configs: &[(WorkerId, usize)],
        max_transient_retries: u32,
        max_infra_retries: u32,
    ) -> (
        Dispatcher<MockWorker>,
        Arc<dyn WorkerManager<Worker = MockWorker>>,
    ) {
        let workers = setup_workers(worker_configs);
        let worker_manager: Arc<dyn WorkerManager<Worker = MockWorker>> =
            Arc::new(MockWorkerManager::new(workers));
        (
            Dispatcher::with_retry_limits(
                StatisticsManagerRef::default(),
                max_transient_retries,
                max_infra_retries,
            ),
            worker_manager,
        )
    }

    /// Build a `ScheduledTask` that has already failed `attempts` times, so a single
    /// dispatch can exercise behaviour partway through (or at the end of) a retry budget.
    fn scheduled_task_with_attempts(
        task: MockTask,
        worker_id: WorkerId,
        attempts: u32,
    ) -> (ScheduledTask<MockTask>, SubmittedTask) {
        let (pending, submitted) =
            SchedulerHandle::prepare_task_for_submission(SubmittableTask::task_only(task));
        let (task, result_tx, cancel_token) = pending.into_inner();
        let pending = PendingTask::retry(task, result_tx, cancel_token, attempts, Duration::ZERO);
        (ScheduledTask::new(pending, worker_id), submitted)
    }

    #[tokio::test]
    async fn test_dispatcher_basic_task() -> DaftResult<()> {
        let worker_id: WorkerId = Arc::from("worker1");
        let (mut dispatcher, worker_manager) =
            setup_dispatcher_test_context(&[(worker_id.clone(), 1)]);

        let partition_ref = create_mock_partition_ref(100, 100);
        let task = MockTaskBuilder::new(partition_ref.clone()).build();
        let submittable_task = SubmittableTask::task_only(task);
        let (schedulable_task, submitted_task) =
            SchedulerHandle::prepare_task_for_submission(submittable_task);

        let scheduled_tasks = vec![ScheduledTask::new(schedulable_task, worker_id)];
        dispatcher.dispatch_tasks(scheduled_tasks, &worker_manager)?;

        // Wait for task completion
        let failed_tasks = dispatcher.await_completed_tasks(&worker_manager).await?;
        assert!(failed_tasks.is_empty());

        let result = submitted_task.await?;
        let partition = result.unwrap().partitions()[0].clone();
        assert!(Arc::ptr_eq(&partition, &partition_ref));

        Ok(())
    }

    #[tokio::test]
    async fn test_dispatcher_multiple_tasks() -> DaftResult<()> {
        let worker_id: WorkerId = Arc::from("worker1");
        let (mut dispatcher, worker_manager) =
            setup_dispatcher_test_context(&[(worker_id.clone(), 4)]);

        let num_tasks = 100;
        let mut rng = StdRng::from_os_rng();
        let (scheduled_tasks, submitted_tasks) = (0..num_tasks)
            .map(|i| {
                let task = MockTaskBuilder::new(create_mock_partition_ref(100 + i, 1024 * (i + 1)))
                    .with_task_id(i as u32)
                    .with_sleep_duration(std::time::Duration::from_millis(
                        rng.random_range(50..100),
                    ))
                    .build();
                let submittable_task = SubmittableTask::task_only(task);
                let (schedulable_task, submitted_task) =
                    SchedulerHandle::prepare_task_for_submission(submittable_task);
                (
                    ScheduledTask::new(schedulable_task, worker_id.clone()),
                    submitted_task,
                )
            })
            .unzip::<_, _, Vec<ScheduledTask<MockTask>>, Vec<SubmittedTask>>();

        dispatcher.dispatch_tasks(scheduled_tasks, &worker_manager)?;

        // Wait for all tasks to complete
        while dispatcher.has_running_tasks() {
            let failed_tasks = dispatcher.await_completed_tasks(&worker_manager).await?;
            assert!(failed_tasks.is_empty());
        }

        // Verify results
        for (i, submitted_task) in submitted_tasks.into_iter().enumerate() {
            let result = submitted_task.await?;
            let partition = result.unwrap().partitions()[0].clone();
            assert_eq!(partition.num_rows(), 100 + i);
            assert_eq!(partition.size_bytes(), 1024 * (i + 1));
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_dispatcher_cancelled_task() -> DaftResult<()> {
        let worker_id: WorkerId = Arc::from("worker1");
        let (mut dispatcher, worker_manager) =
            setup_dispatcher_test_context(&[(worker_id.clone(), 1)]);

        let partition_ref = create_mock_partition_ref(100, 100);
        let (cancel_notifier, cancel_receiver) = create_oneshot_channel();
        let task = MockTaskBuilder::new(partition_ref.clone())
            .with_cancel_notifier(cancel_notifier)
            .build();
        let submittable_task = SubmittableTask::task_only(task);
        let (schedulable_task, submitted_task) =
            SchedulerHandle::prepare_task_for_submission(submittable_task);

        let scheduled_tasks = vec![ScheduledTask::new(schedulable_task, worker_id)];
        dispatcher.dispatch_tasks(scheduled_tasks, &worker_manager)?;

        drop(submitted_task);
        cancel_receiver.await.unwrap();

        Ok(())
    }

    #[tokio::test]
    async fn test_task_error_basic() -> DaftResult<()> {
        let worker_id: WorkerId = Arc::from("worker1");
        let (mut dispatcher, worker_manager) =
            setup_dispatcher_test_context(&[(worker_id.clone(), 1)]);

        let task = MockTaskBuilder::new(create_mock_partition_ref(100, 1024))
            .with_failure(MockTaskFailure::Error("test error".to_string()))
            .build();
        let submittable_task = SubmittableTask::task_only(task);
        let (schedulable_task, submitted_task) =
            SchedulerHandle::prepare_task_for_submission(submittable_task);

        let scheduled_tasks = vec![ScheduledTask::new(schedulable_task, worker_id)];
        dispatcher.dispatch_tasks(scheduled_tasks, &worker_manager)?;

        let failed_tasks = dispatcher.await_completed_tasks(&worker_manager).await?;
        assert!(failed_tasks.is_empty());

        let result = submitted_task.await;
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "DaftError::InternalError test error"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_task_panic_basic() -> DaftResult<()> {
        let worker_id: WorkerId = Arc::from("worker1");
        let (mut dispatcher, worker_manager) =
            setup_dispatcher_test_context(&[(worker_id.clone(), 1)]);

        let task = MockTaskBuilder::new(create_mock_partition_ref(100, 1024))
            .with_failure(MockTaskFailure::Panic("test panic".to_string()))
            .build();

        let submittable_task = SubmittableTask::task_only(task);
        let (schedulable_task, submitted_task) =
            SchedulerHandle::prepare_task_for_submission(submittable_task);
        let scheduled_tasks = vec![ScheduledTask::new(schedulable_task, worker_id)];
        dispatcher.dispatch_tasks(scheduled_tasks, &worker_manager)?;

        let failed_tasks = dispatcher.await_completed_tasks(&worker_manager).await?;
        assert!(failed_tasks.is_empty());

        let result = submitted_task.await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("test panic"));

        Ok(())
    }

    #[tokio::test]
    async fn test_task_worker_died() -> DaftResult<()> {
        let worker_id: WorkerId = Arc::from("worker1");
        let (mut dispatcher, worker_manager) =
            setup_dispatcher_test_context(&[(worker_id.clone(), 1)]);

        // Verify worker is initially present
        let initial_snapshots = worker_manager.worker_snapshots()?;
        assert_eq!(initial_snapshots.len(), 1);
        assert_eq!(initial_snapshots[0].worker_id(), &worker_id);

        let task = MockTaskBuilder::new(create_mock_partition_ref(100, 1024))
            .with_failure(MockTaskFailure::WorkerDied)
            .build();
        let submittable_task = SubmittableTask::task_only(task);
        let (schedulable_task, _submitted_task) =
            SchedulerHandle::prepare_task_for_submission(submittable_task);

        let scheduled_tasks = vec![ScheduledTask::new(schedulable_task, worker_id.clone())];
        dispatcher.dispatch_tasks(scheduled_tasks, &worker_manager)?;

        let failed_tasks = dispatcher.await_completed_tasks(&worker_manager).await?;

        // Task should be returned as a failed task that needs rescheduling
        assert_eq!(failed_tasks.len(), 1);
        let failed_task = &failed_tasks[0];
        assert_eq!(failed_task.task_context().task_id, 0);

        // Verify that the worker that died is no longer in the worker snapshots
        let worker_snapshots = worker_manager.worker_snapshots()?;
        assert!(
            worker_snapshots.is_empty(),
            "Dead worker should not appear in worker snapshots"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_task_worker_unavailable() -> DaftResult<()> {
        let worker_id: WorkerId = Arc::from("worker1");
        let (mut dispatcher, worker_manager) =
            setup_dispatcher_test_context(&[(worker_id.clone(), 1)]);

        // Verify worker is initially present
        let initial_snapshots = worker_manager.worker_snapshots()?;
        assert_eq!(initial_snapshots.len(), 1);
        assert_eq!(initial_snapshots[0].worker_id(), &worker_id);

        let task = MockTaskBuilder::new(create_mock_partition_ref(100, 1024))
            .with_failure(MockTaskFailure::WorkerUnavailable)
            .build();
        let submittable_task = SubmittableTask::task_only(task);
        let (schedulable_task, _submitted_task) =
            SchedulerHandle::prepare_task_for_submission(submittable_task);

        let scheduled_tasks = vec![ScheduledTask::new(schedulable_task, worker_id.clone())];
        dispatcher.dispatch_tasks(scheduled_tasks, &worker_manager)?;

        let failed_tasks = dispatcher.await_completed_tasks(&worker_manager).await?;

        // Task should be returned as a failed task that needs rescheduling
        assert_eq!(failed_tasks.len(), 1);
        let failed_task = &failed_tasks[0];
        assert_eq!(failed_task.task_context().task_id, 0);

        // Verify that the worker is still present in snapshots (unavailable != dead)
        let worker_snapshots = worker_manager.worker_snapshots()?;
        assert_eq!(
            worker_snapshots.len(),
            1,
            "Worker should still be present when unavailable"
        );
        assert_eq!(worker_snapshots[0].worker_id(), &worker_id);

        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_failed_tasks() -> DaftResult<()> {
        // Use multiple workers to avoid interference between tasks
        let worker1_id: WorkerId = Arc::from("worker1");
        let worker2_id: WorkerId = Arc::from("worker2");
        let worker3_id: WorkerId = Arc::from("worker3");
        let (mut dispatcher, worker_manager) = setup_dispatcher_test_context(&[
            (worker1_id.clone(), 1),
            (worker2_id.clone(), 1),
            (worker3_id.clone(), 1),
        ]);

        let tasks = vec![
            MockTaskBuilder::new(create_mock_partition_ref(100, 1024))
                .with_task_id(1)
                .with_scheduling_strategy(SchedulingStrategy::WorkerAffinity {
                    worker_id: worker1_id.clone(),
                    soft: false,
                })
                .with_failure(MockTaskFailure::WorkerDied)
                .build(),
            MockTaskBuilder::new(create_mock_partition_ref(200, 2048))
                .with_task_id(2)
                .with_scheduling_strategy(SchedulingStrategy::WorkerAffinity {
                    worker_id: worker2_id.clone(),
                    soft: false,
                })
                .with_failure(MockTaskFailure::WorkerUnavailable)
                .build(),
            MockTaskBuilder::new(create_mock_partition_ref(300, 3072))
                .with_task_id(3)
                .with_scheduling_strategy(SchedulingStrategy::WorkerAffinity {
                    worker_id: worker3_id.clone(),
                    soft: false,
                })
                .with_failure(MockTaskFailure::WorkerDied)
                .build(),
        ];

        let (scheduled_tasks, _submitted_tasks) = tasks
            .into_iter()
            .zip(vec![
                worker1_id.clone(),
                worker2_id.clone(),
                worker3_id.clone(),
            ])
            .map(|(task, worker_id)| {
                let submittable_task = SubmittableTask::task_only(task);
                let (schedulable_task, submitted_task) =
                    SchedulerHandle::prepare_task_for_submission(submittable_task);
                (
                    ScheduledTask::new(schedulable_task, worker_id),
                    submitted_task,
                )
            })
            .unzip::<_, _, Vec<ScheduledTask<MockTask>>, Vec<SubmittedTask>>();

        // Dispatch all tasks at once
        dispatcher.dispatch_tasks(scheduled_tasks, &worker_manager)?;

        // Wait for all tasks to complete and collect failed tasks
        let mut all_failed_tasks = Vec::new();
        while dispatcher.has_running_tasks() {
            let failed_tasks = dispatcher.await_completed_tasks(&worker_manager).await?;
            all_failed_tasks.extend(failed_tasks);
        }

        // All tasks should be returned as failed tasks that need rescheduling
        assert_eq!(all_failed_tasks.len(), 3);

        // Check that we have all the expected task IDs
        let mut failed_task_ids: Vec<_> = all_failed_tasks
            .iter()
            .map(|task| task.task_context().task_id)
            .collect();
        failed_task_ids.sort_unstable();
        assert_eq!(failed_task_ids, vec![1, 2, 3]);

        // Verify worker state: Workers 1 and 3 should be dead, worker 2 should be alive
        let worker_snapshots = worker_manager.worker_snapshots()?;
        assert_eq!(
            worker_snapshots.len(),
            1,
            "Only worker2 should remain alive"
        );
        assert_eq!(worker_snapshots[0].worker_id(), &worker2_id);

        Ok(())
    }

    #[tokio::test]
    async fn test_transient_error_is_retried() -> DaftResult<()> {
        let worker_id: WorkerId = Arc::from("worker1");
        let (mut dispatcher, worker_manager) =
            setup_dispatcher_test_context(&[(worker_id.clone(), 1)]);

        let task = MockTaskBuilder::new(create_mock_partition_ref(100, 1024))
            .with_failure(MockTaskFailure::TransientError(
                "connection reset".to_string(),
            ))
            .build();
        let (scheduled_task, _submitted_task) = scheduled_task_with_attempts(task, worker_id, 0);
        dispatcher.dispatch_tasks(vec![scheduled_task], &worker_manager)?;

        let failed_tasks = dispatcher.await_completed_tasks(&worker_manager).await?;
        assert_eq!(failed_tasks.len(), 1);
        assert_eq!(failed_tasks[0].attempts(), 1);
        // The retry is backed off, so it is queued but not immediately dispatchable.
        assert!(!failed_tasks[0].is_ready(Instant::now()));

        Ok(())
    }

    #[tokio::test]
    async fn test_transient_error_gives_up_at_retry_limit() -> DaftResult<()> {
        let worker_id: WorkerId = Arc::from("worker1");
        let (mut dispatcher, worker_manager) =
            setup_dispatcher_test_context_with_retry_limits(&[(worker_id.clone(), 1)], 2, 10);

        let task = MockTaskBuilder::new(create_mock_partition_ref(100, 1024))
            .with_failure(MockTaskFailure::TransientError("timeout".to_string()))
            .build();
        // Two attempts already made: the budget is spent.
        let (scheduled_task, submitted_task) = scheduled_task_with_attempts(task, worker_id, 2);
        dispatcher.dispatch_tasks(vec![scheduled_task], &worker_manager)?;

        let failed_tasks = dispatcher.await_completed_tasks(&worker_manager).await?;
        assert!(failed_tasks.is_empty());

        // The original error is forwarded, not wrapped, so the cause stays visible.
        let result = submitted_task.await;
        assert!(result.unwrap_err().to_string().contains("timeout"));

        Ok(())
    }

    #[tokio::test]
    async fn test_non_transient_error_is_not_retried() -> DaftResult<()> {
        let worker_id: WorkerId = Arc::from("worker1");
        let (mut dispatcher, worker_manager) =
            setup_dispatcher_test_context(&[(worker_id.clone(), 1)]);

        let task = MockTaskBuilder::new(create_mock_partition_ref(100, 1024))
            .with_failure(MockTaskFailure::Error("data error".to_string()))
            .build();
        let (scheduled_task, submitted_task) = scheduled_task_with_attempts(task, worker_id, 0);
        dispatcher.dispatch_tasks(vec![scheduled_task], &worker_manager)?;

        let failed_tasks = dispatcher.await_completed_tasks(&worker_manager).await?;
        assert!(failed_tasks.is_empty());
        assert!(submitted_task.await.is_err());

        Ok(())
    }

    /// A lost worker must not reset the attempt counter. Before this was fixed, a task
    /// alternating between transient failures and worker deaths could retry forever,
    /// dropping a node from the cluster on every `WorkerDied`.
    #[tokio::test]
    async fn test_worker_loss_preserves_attempt_count() -> DaftResult<()> {
        let worker_id: WorkerId = Arc::from("worker1");
        let (mut dispatcher, worker_manager) =
            setup_dispatcher_test_context(&[(worker_id.clone(), 1)]);

        let task = MockTaskBuilder::new(create_mock_partition_ref(100, 1024))
            .with_failure(MockTaskFailure::WorkerDied)
            .build();
        let (scheduled_task, _submitted_task) = scheduled_task_with_attempts(task, worker_id, 2);
        dispatcher.dispatch_tasks(vec![scheduled_task], &worker_manager)?;

        let failed_tasks = dispatcher.await_completed_tasks(&worker_manager).await?;
        assert_eq!(failed_tasks.len(), 1);
        assert_eq!(failed_tasks[0].attempts(), 3);

        Ok(())
    }

    #[tokio::test]
    async fn test_worker_loss_gives_up_at_infra_retry_limit() -> DaftResult<()> {
        let worker_id: WorkerId = Arc::from("worker1");
        let (mut dispatcher, worker_manager) =
            setup_dispatcher_test_context_with_retry_limits(&[(worker_id.clone(), 1)], 3, 2);

        let task = MockTaskBuilder::new(create_mock_partition_ref(100, 1024))
            .with_failure(MockTaskFailure::WorkerDied)
            .build();
        let (scheduled_task, submitted_task) = scheduled_task_with_attempts(task, worker_id, 2);
        dispatcher.dispatch_tasks(vec![scheduled_task], &worker_manager)?;

        let failed_tasks = dispatcher.await_completed_tasks(&worker_manager).await?;
        assert!(failed_tasks.is_empty());

        let error = submitted_task.await.unwrap_err().to_string();
        assert!(
            error.contains("lost its worker on all 3 attempts"),
            "{error}"
        );
        assert!(error.contains(MAX_INFRA_RETRIES_ENV), "{error}");

        Ok(())
    }
}
