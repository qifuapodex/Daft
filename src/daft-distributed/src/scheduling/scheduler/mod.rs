use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap},
    time::{Duration, Instant},
};

use super::{
    task::{SchedulingStrategy, Task, TaskDetails},
    worker::{Worker, WorkerId},
};
use crate::{
    pipeline_node::MaterializedOutput,
    scheduling::task::{TaskContext, TaskMetadata, TaskResourceRequest},
    utils::channel::OneshotSender,
};

mod default;
mod linear;
mod scheduler_actor;

use common_error::DaftResult;
pub(crate) use scheduler_actor::{
    SchedulerHandle, SubmittableTask, SubmittedTask, spawn_scheduler_actor,
};
use tokio_util::sync::CancellationToken;

pub(super) trait Scheduler<T: Task>: Send + Sync {
    fn update_worker_state(&mut self, worker_snapshots: &[WorkerSnapshot]);
    fn enqueue_tasks(&mut self, tasks: Vec<PendingTask<T>>);
    /// Returns `(scheduled, cancelled)`. Cancelled tasks are popped from the
    /// pending queue without being dispatched; caller emits terminal events.
    fn schedule_tasks(&mut self) -> (Vec<ScheduledTask<T>>, Vec<PendingTask<T>>);
    fn get_autoscaling_request(&mut self) -> Option<Vec<TaskResourceRequest>>;
    fn num_pending_tasks(&self) -> usize;
}

/// Why a `WorkerAffinity` target could not take the task. The two cases get different
/// treatment: a busy target is worth waiting for, a missing one never comes back.
enum AffinityTarget {
    Busy,
    Missing,
}

fn pending_tasks_in_priority_order<T: Task>(
    pending_tasks: &BinaryHeap<PendingTask<T>>,
) -> Vec<&PendingTask<T>> {
    let mut ordered_tasks = pending_tasks
        .iter()
        .filter(|t| !t.is_cancelled())
        .collect::<Vec<_>>();
    // Match the order that repeated BinaryHeap::pop() calls would produce.
    ordered_tasks.sort_unstable_by(|a, b| b.cmp(a));
    ordered_tasks
}

pub(crate) struct PendingTask<T: Task> {
    task: T,
    result_tx: OneshotSender<DaftResult<Option<MaterializedOutput>>>,
    cancel_token: CancellationToken,
    /// Number of times this task has already been dispatched and come back failed.
    /// Zero for a task that has never run.
    attempts: u32,
    /// Set on a retry: the task stays in the pending queue but is not eligible for
    /// dispatch until this instant, so a retry storm does not hammer a failing
    /// dependency at the scheduler's tick rate.
    not_before: Option<Instant>,
    /// Set on a retry: the worker the previous attempt failed on. Spread scheduling
    /// prefers any other worker, so a node with a local problem (a bad NIC, a stale DNS
    /// cache) does not get handed the same task again and again.
    avoid_worker: Option<WorkerId>,
}

impl<T: Task> PendingTask<T> {
    pub fn new(
        task: T,
        result_tx: OneshotSender<DaftResult<Option<MaterializedOutput>>>,
        cancel_token: CancellationToken,
    ) -> Self {
        Self {
            task,
            result_tx,
            cancel_token,
            attempts: 0,
            not_before: None,
            avoid_worker: None,
        }
    }

    /// Rebuild a task that failed and is being given another attempt. `attempts` is the
    /// number of attempts already made (i.e. including the one that just failed), the task
    /// will not be dispatched again until `backoff` has elapsed, and `failed_on` is the
    /// worker to steer away from when the task is next placed.
    pub fn retry(
        task: T,
        result_tx: OneshotSender<DaftResult<Option<MaterializedOutput>>>,
        cancel_token: CancellationToken,
        attempts: u32,
        backoff: Duration,
        failed_on: WorkerId,
    ) -> Self {
        Self {
            task,
            result_tx,
            cancel_token,
            attempts,
            not_before: Some(Instant::now() + backoff),
            avoid_worker: Some(failed_on),
        }
    }

    pub fn attempts(&self) -> u32 {
        self.attempts
    }

    /// Whether this task's retry backoff (if any) has elapsed.
    pub fn is_ready(&self, now: Instant) -> bool {
        self.not_before.is_none_or(|not_before| now >= not_before)
    }

    /// The worker the previous attempt failed on, if this is a retry.
    pub fn avoid_worker(&self) -> Option<&WorkerId> {
        self.avoid_worker.as_ref()
    }

    pub fn strategy(&self) -> &SchedulingStrategy {
        self.task.strategy()
    }

    pub fn task_context(&self) -> TaskContext {
        self.task.task_context()
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancel_token.is_cancelled()
    }

    pub fn into_inner(
        self,
    ) -> (
        T,
        OneshotSender<DaftResult<Option<MaterializedOutput>>>,
        CancellationToken,
    ) {
        (self.task, self.result_tx, self.cancel_token)
    }

    fn task_metadata(&self) -> TaskMetadata {
        self.task.task_metadata()
    }
}

impl<T: Task> std::fmt::Debug for PendingTask<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SchedulableTask({:?}, {:?})",
            self.task_context(),
            TaskDetails::from(&self.task)
        )
    }
}

impl<T: Task> PartialEq for PendingTask<T> {
    fn eq(&self, other: &Self) -> bool {
        self.task.task_id() == other.task.task_id()
    }
}

impl<T: Task> Eq for PendingTask<T> {}

impl<T: Task> PartialOrd for PendingTask<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: Task> Ord for PendingTask<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.task.priority().cmp(&other.task.priority())
    }
}

pub(super) struct ScheduledTask<T: Task> {
    task: T,
    result_tx: OneshotSender<DaftResult<Option<MaterializedOutput>>>,
    cancel_token: CancellationToken,
    worker_id: WorkerId,
    attempts: u32,
    not_before: Option<Instant>,
    avoid_worker: Option<WorkerId>,
}

impl<T: Task> ScheduledTask<T> {
    pub fn new(pending_task: PendingTask<T>, worker_id: WorkerId) -> Self {
        let attempts = pending_task.attempts();
        let not_before = pending_task.not_before;
        let avoid_worker = pending_task.avoid_worker.clone();
        let (mut task, result_tx, cancel_token) = pending_task.into_inner();
        task.set_attempt(attempts);
        Self {
            task,
            result_tx,
            cancel_token,
            worker_id,
            attempts,
            not_before,
            avoid_worker,
        }
    }

    pub fn prepare_dispatch(
        &mut self,
        recovery: &super::shuffle_recovery::ShuffleRecovery,
    ) -> bool {
        self.task.prepare_dispatch(recovery)
    }
    pub fn defer(self) -> PendingTask<T> {
        PendingTask {
            task: self.task,
            result_tx: self.result_tx,
            cancel_token: self.cancel_token,
            attempts: self.attempts,
            not_before: self.not_before,
            avoid_worker: self.avoid_worker,
        }
    }

    pub fn worker_id(&self) -> WorkerId {
        self.worker_id.clone()
    }

    pub fn task_ref(&self) -> &T {
        &self.task
    }

    pub fn task(&self) -> T {
        self.task.clone()
    }

    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel_token.clone()
    }

    pub fn into_inner(
        self,
    ) -> (
        WorkerId,
        T,
        OneshotSender<DaftResult<Option<MaterializedOutput>>>,
        CancellationToken,
        u32,
    ) {
        (
            self.worker_id,
            self.task,
            self.result_tx,
            self.cancel_token,
            self.attempts,
        )
    }
}

impl<T: Task> std::fmt::Debug for ScheduledTask<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ScheduledTask(worker_id = {}, {:?}, {:?}, {:?})",
            self.worker_id,
            self.task.task_context(),
            TaskDetails::from(&self.task),
            self.task.strategy()
        )
    }
}

#[derive(Clone)]
pub(crate) struct WorkerSnapshot {
    worker_id: WorkerId,
    total_num_cpus: f64,
    total_num_gpus: f64,
    active_task_details: HashMap<TaskContext, TaskDetails>,
    drain_state: super::drain::DrainState,
}

impl WorkerSnapshot {
    pub fn new(
        worker_id: WorkerId,
        total_num_cpus: f64,
        total_num_gpus: f64,
        active_task_details: HashMap<TaskContext, TaskDetails>,
    ) -> Self {
        Self {
            worker_id,
            total_num_cpus,
            total_num_gpus,
            active_task_details,
            drain_state: super::drain::DrainState::Active,
        }
    }

    pub fn active_num_cpus(&self) -> f64 {
        self.active_task_details
            .values()
            .map(|details| details.num_cpus())
            .sum()
    }

    pub fn active_num_gpus(&self) -> f64 {
        self.active_task_details
            .values()
            .map(|details| details.num_gpus())
            .sum::<f64>()
    }

    pub fn available_num_cpus(&self) -> f64 {
        self.total_num_cpus - self.active_num_cpus()
    }

    pub fn available_num_gpus(&self) -> f64 {
        self.total_num_gpus - self.active_num_gpus()
    }

    pub fn total_num_cpus(&self) -> f64 {
        self.total_num_cpus
    }

    #[allow(dead_code)]
    pub fn total_num_gpus(&self) -> f64 {
        self.total_num_gpus
    }

    #[cfg(test)]
    pub fn worker_id(&self) -> &WorkerId {
        &self.worker_id
    }

    // TODO: Potentially include memory as well, and also be able to overschedule tasks.
    pub fn can_schedule_task(&self, task: &impl Task) -> bool {
        use super::drain::DrainState;
        match self.drain_state {
            DrainState::Active => {}
            DrainState::Draining if matches!(task.strategy(), SchedulingStrategy::WorkerAffinity { worker_id, soft: false } if worker_id == &self.worker_id) =>
                {}
            _ => return false,
        }
        if task.resource_request().num_gpus() > 0.0 && self.available_num_gpus() == 0.0 {
            return false;
        }
        self.available_num_cpus() >= task.resource_request().num_cpus()
    }
}

impl std::fmt::Debug for WorkerSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "WorkerSnapshot(worker_id = {}, total_num_cpus = {}, total_num_gpus = {}, active_task_details = {:#?})",
            self.worker_id, self.total_num_cpus, self.total_num_gpus, self.active_task_details
        )
    }
}

impl<W: Worker> From<&W> for WorkerSnapshot {
    fn from(worker: &W) -> Self {
        let mut snapshot = Self::new(
            worker.id().clone(),
            worker.total_num_cpus(),
            worker.total_num_gpus(),
            worker.active_task_details(),
        );
        snapshot.drain_state = worker.drain_state();
        snapshot
    }
}

#[cfg(test)]
pub(super) mod test_utils {

    use std::{collections::HashMap, sync::Arc};

    use super::*;
    use crate::scheduling::{
        task::TaskID,
        tests::{MockTask, MockTaskBuilder},
        worker::tests::MockWorker,
    };

    fn check_drain_scheduling<S: Scheduler<MockTask> + Default>() {
        use crate::scheduling::drain::DrainState;
        let target: WorkerId = Arc::from("draining");
        let other: WorkerId = Arc::from("active");
        let mut draining = WorkerSnapshot::new(target.clone(), 4.0, 0.0, HashMap::new());
        draining.drain_state = DrainState::Draining;
        let active = WorkerSnapshot::new(other.clone(), 4.0, 0.0, HashMap::new());
        let mut scheduler = S::default();
        scheduler.update_worker_state(&[draining.clone(), active.clone()]);
        scheduler.enqueue_tasks(vec![
            create_spread_task(Some(1)),
            create_worker_affinity_task(&target, false, Some(2)),
        ]);
        let (mut tasks, _) = scheduler.schedule_tasks();
        if tasks.len() == 1 {
            // LinearScheduler intentionally runs one task at a time.
            scheduler.update_worker_state(&[draining.clone(), active.clone()]);
            tasks.extend(scheduler.schedule_tasks().0);
        }
        assert_eq!(tasks.len(), 2);
        for task in tasks {
            let expected = if task.task_ref().task_context().task_id == 1 {
                &other
            } else {
                &target
            };
            assert_eq!(&task.worker_id(), expected);
        }
        // With no local dependency left, a future Ray-backed affinity task can
        // move to another worker rather than deadlocking a ready target.
        for state in [DrainState::ReadyToRetire, DrainState::Unknown] {
            draining.drain_state = state;
            scheduler.update_worker_state(&[draining.clone(), active.clone()]);
            scheduler.enqueue_tasks(vec![create_worker_affinity_task(&target, false, Some(3))]);
            let (tasks, _) = scheduler.schedule_tasks();
            assert_eq!(tasks.len(), 1);
            assert_eq!(tasks[0].worker_id(), other);
        }
    }

    #[test]
    fn default_scheduler_drains_without_starving_affinity() {
        check_drain_scheduling::<super::default::DefaultScheduler<MockTask>>();
    }

    #[test]
    fn linear_scheduler_drains_without_starving_affinity() {
        check_drain_scheduling::<super::linear::LinearScheduler<MockTask>>();
    }

    #[test]
    fn scheduled_task_context_preserves_attempt_through_deferral() {
        use common_daft_config::DaftExecutionConfig;
        use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan};
        use daft_logical_plan::stats::StatsState;

        use crate::scheduling::task::SwordfishTask;

        let plan = LocalPhysicalPlan::in_memory_scan(
            0,
            Arc::new(daft_schema::schema::Schema::empty()),
            0,
            StatsState::NotMaterialized,
            LocalNodeContext::default(),
        );
        let task = SwordfishTask::for_recovery_test(
            plan,
            HashMap::new(),
            Arc::new(DaftExecutionConfig::default()),
            1,
        );
        let (pending, _submitted) =
            SchedulerHandle::prepare_task_for_submission(SubmittableTask::task_only(task));
        let scheduled = ScheduledTask::new(pending, Arc::from("worker"));
        assert_eq!(scheduled.task_ref().context()["task_attempt"], "0");
        let (_, task, result_tx, cancel_token, _) = scheduled.into_inner();
        let pending = PendingTask::retry(
            task,
            result_tx,
            cancel_token,
            2,
            Duration::ZERO,
            Arc::from("worker"),
        );
        let scheduled = ScheduledTask::new(pending, Arc::from("retry-worker"));
        assert_eq!(scheduled.task_ref().context()["task_attempt"], "2");
        let deferred = scheduled.defer();
        assert_eq!(deferred.attempts(), 2);
        let rescheduled = ScheduledTask::new(deferred, Arc::from("another-worker"));
        assert_eq!(rescheduled.task_ref().context()["task_attempt"], "2");
    }

    #[test]
    fn deferral_preserves_retry_backoff_and_worker_exclusion() {
        let bad_worker: WorkerId = Arc::from("bad-worker");
        let task = MockTaskBuilder::default().build();
        let mut pending = PendingTask::retry(
            task,
            tokio::sync::oneshot::channel().0,
            CancellationToken::new(),
            3,
            Duration::from_secs(30),
            bad_worker.clone(),
        );
        let deadline = pending.not_before;
        for _ in 0..3 {
            pending = ScheduledTask::new(pending, Arc::from("healthy-worker")).defer();
            assert_eq!(pending.not_before, deadline);
            assert_eq!(pending.avoid_worker(), Some(&bad_worker));
            assert!(!pending.is_ready(Instant::now()));
            assert_eq!(pending.attempts(), 3);
        }
    }

    // Helper function to create workers with given configurations
    pub fn setup_workers(configs: &[(WorkerId, usize)]) -> HashMap<WorkerId, MockWorker> {
        configs
            .iter()
            .map(|(id, num_slots)| {
                let worker = MockWorker::new(id.clone(), *num_slots as f64, 0.0);
                (id.clone(), worker)
            })
            .collect::<HashMap<_, _>>()
    }

    // Helper function to setup scheduler with workers
    pub fn setup_scheduler<S: Scheduler<MockTask> + Default>(
        workers: &HashMap<WorkerId, MockWorker>,
    ) -> S {
        let mut scheduler = S::default();
        scheduler.update_worker_state(
            workers
                .values()
                .map(WorkerSnapshot::from)
                .collect::<Vec<_>>()
                .as_slice(),
        );
        scheduler
    }

    pub fn create_schedulable_task(mock_task: MockTask) -> PendingTask<MockTask> {
        PendingTask::new(
            mock_task,
            tokio::sync::oneshot::channel().0,
            tokio_util::sync::CancellationToken::new(),
        )
    }

    /// A spread task shaped like a retry: it already failed once on `failed_on`, so the
    /// scheduler should steer it elsewhere.
    pub fn create_retry_spread_task(
        id: Option<TaskID>,
        failed_on: &WorkerId,
    ) -> PendingTask<MockTask> {
        let task = MockTaskBuilder::default()
            .with_scheduling_strategy(SchedulingStrategy::Spread)
            .with_task_id(id.unwrap_or_default())
            .build();
        PendingTask::retry(
            task,
            tokio::sync::oneshot::channel().0,
            tokio_util::sync::CancellationToken::new(),
            1,
            std::time::Duration::ZERO,
            failed_on.clone(),
        )
    }

    pub fn create_spread_task(id: Option<TaskID>) -> PendingTask<MockTask> {
        let task = MockTaskBuilder::default()
            .with_scheduling_strategy(SchedulingStrategy::Spread)
            .with_task_id(id.unwrap_or_default())
            .build();
        create_schedulable_task(task)
    }

    pub fn create_worker_affinity_task(
        worker_id: &WorkerId,
        soft: bool,
        id: Option<TaskID>,
    ) -> PendingTask<MockTask> {
        let task = MockTaskBuilder::default()
            .with_scheduling_strategy(SchedulingStrategy::WorkerAffinity {
                worker_id: worker_id.clone(),
                soft,
            })
            .with_task_id(id.unwrap_or_default())
            .build();
        create_schedulable_task(task)
    }
}
