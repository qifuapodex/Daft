use std::{
    collections::{BinaryHeap, HashMap, HashSet},
    time::Instant,
};

use super::{
    AffinityTarget, PendingTask, ScheduledTask, Scheduler, WorkerSnapshot,
    scheduler_actor::SCHEDULER_LOG_TARGET,
};
use crate::scheduling::{
    task::{SchedulingStrategy, Task, TaskDetails, TaskResourceRequest},
    worker::WorkerId,
};

pub(super) struct DefaultScheduler<T: Task> {
    pending_tasks: BinaryHeap<PendingTask<T>>,
    worker_snapshots: HashMap<WorkerId, WorkerSnapshot>,
    autoscaling_threshold: f64,
    /// Affinity targets already warned about, so the missing-target warning is emitted
    /// once per worker rather than once per pending task per tick.
    warned_missing_affinity_targets: HashSet<WorkerId>,
}

impl<T: Task> Default for DefaultScheduler<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Task> DefaultScheduler<T> {
    pub fn new() -> Self {
        let threshold = Self::get_threshold_from_env().unwrap_or(1.25);
        Self::with_autoscaling_threshold(threshold)
    }

    pub fn with_autoscaling_threshold(autoscaling_threshold: f64) -> Self {
        assert!(
            autoscaling_threshold >= 1.0,
            "Autoscaling threshold must be >= 1.0, got: {}",
            autoscaling_threshold
        );
        Self {
            pending_tasks: BinaryHeap::new(),
            worker_snapshots: HashMap::new(),
            autoscaling_threshold,
            warned_missing_affinity_targets: HashSet::new(),
        }
    }

    fn get_threshold_from_env() -> Option<f64> {
        std::env::var("DAFT_AUTOSCALING_THRESHOLD")
            .ok()
            .and_then(|val| val.parse::<f64>().ok())
    }

    // Spread scheduling: Schedule tasks to the worker with the most available slots, to
    // TODO: Change the approach to instead spread based on tasks of the same 'type', i.e. from the same pipeline node.
    fn try_schedule_spread_task(&self, task: &T, avoid: Option<&WorkerId>) -> Option<WorkerId> {
        // `avoid` is a preference, not a constraint: if the worker a retry just failed on
        // is the only one with room, placing the task there beats leaving it pending.
        self.best_spread_candidate(task, avoid)
            .or_else(|| self.best_spread_candidate(task, None))
    }

    fn best_spread_candidate(&self, task: &T, avoid: Option<&WorkerId>) -> Option<WorkerId> {
        self.worker_snapshots
            .iter()
            .filter(|(_, worker)| {
                worker.can_schedule_task(task) && avoid != Some(&worker.worker_id)
            })
            .max_by_key(|(_, worker)| {
                (worker.available_num_cpus() + worker.available_num_gpus()) as usize
            })
            .map(|(id, _)| id.clone())
    }

    // Soft worker affinity scheduling: Schedule task to the worker if it has capacity
    // Otherwise, fallback to spread scheduling
    fn try_schedule_worker_affinity_task(
        &mut self,
        task: &PendingTask<T>,
        worker_id: &WorkerId,
        soft: bool,
    ) -> Option<WorkerId> {
        // Resolve the target before touching `self` mutably below.
        let target = match self.worker_snapshots.get(worker_id) {
            // Target worker exists and has capacity
            Some(worker) if worker.can_schedule_task(&task.task) => {
                return Some(worker.worker_id.clone());
            }
            // Target worker exists but is busy: soft affinity falls back, hard affinity waits
            Some(_) => AffinityTarget::Busy,
            // Target worker is missing from the snapshots
            None => AffinityTarget::Missing,
        };

        match target {
            AffinityTarget::Busy if !soft => None,
            AffinityTarget::Busy => self.try_schedule_spread_task(&task.task, task.avoid_worker()),
            AffinityTarget::Missing => {
                // Fall back to spread regardless of the soft flag: the worker most likely
                // died, and holding out for it would deadlock the task. Warn once per
                // target -- this runs for every pending task on every tick, so an
                // unconditional log here is thousands of lines a second under backlog.
                if self
                    .warned_missing_affinity_targets
                    .insert(worker_id.clone())
                {
                    tracing::warn!(
                        target: SCHEDULER_LOG_TARGET,
                        worker_id = %worker_id,
                        "Affinity target missing from worker snapshots; falling back to spread scheduling"
                    );
                }
                self.try_schedule_spread_task(&task.task, task.avoid_worker())
            }
        }
    }

    fn try_schedule_task(&mut self, task: &PendingTask<T>) -> Option<WorkerId> {
        match task.strategy() {
            SchedulingStrategy::Spread => {
                self.try_schedule_spread_task(&task.task, task.avoid_worker())
            }
            SchedulingStrategy::WorkerAffinity { worker_id, soft } => {
                let (worker_id, soft) = (worker_id.clone(), *soft);
                self.try_schedule_worker_affinity_task(task, &worker_id, soft)
            }
        }
    }

    fn needs_autoscaling(&self) -> bool {
        // If there are no pending tasks, we don't need to autoscale
        if self.pending_tasks.is_empty() {
            return false;
        }

        // If there are no workers, we need to autoscale
        if self.worker_snapshots.is_empty() {
            return true;
        }

        // If the ratio of pending tasks to total capacity is greater than the autoscaling threshold, we need to autoscale
        let total_capacity: usize = self
            .worker_snapshots
            .values()
            .map(|worker| worker.total_num_cpus() as usize)
            .sum();

        let ratio = self.pending_tasks.len() as f64 / total_capacity as f64;

        ratio > self.autoscaling_threshold
    }
}

impl<T: Task> Scheduler<T> for DefaultScheduler<T> {
    fn enqueue_tasks(&mut self, tasks: Vec<PendingTask<T>>) {
        self.pending_tasks.extend(tasks);
    }

    // TODO: Currently, workers are never given more tasks than they can handle (based on resources)
    // However, this can cause the scheduler to have too many pending tasks, creating a bottleneck in scheduling.
    // Potentially, we should allow workers to maintain a backlog queue of tasks, and automatically run them when they have capacity.
    // Key thing is that this should be profiled and tested.
    fn schedule_tasks(&mut self) -> (Vec<ScheduledTask<T>>, Vec<PendingTask<T>>) {
        let mut scheduled = Vec::new();
        let mut unscheduled = Vec::new();
        let mut cancelled = Vec::new();
        let now = Instant::now();
        while let Some(task) = self.pending_tasks.pop() {
            if task.is_cancelled() {
                cancelled.push(task);
                continue;
            }
            // A retry that is still backing off stays queued but is not dispatchable yet.
            if !task.is_ready(now) {
                unscheduled.push(task);
                continue;
            }
            if let Some(worker_id) = self.try_schedule_task(&task) {
                self.worker_snapshots
                    .get_mut(&worker_id)
                    .expect("Worker should be present in DefaultScheduler")
                    .active_task_details
                    .insert(task.task_context(), TaskDetails::from(&task.task));
                scheduled.push(ScheduledTask::new(task, worker_id));
            } else {
                unscheduled.push(task);
            }
        }
        self.pending_tasks.extend(unscheduled);
        (scheduled, cancelled)
    }

    fn update_worker_state(&mut self, worker_snapshots: &[WorkerSnapshot]) {
        self.worker_snapshots = worker_snapshots
            .iter()
            .map(|snapshot| (snapshot.worker_id.clone(), snapshot.clone()))
            .collect();
        // A worker whose actor was rebuilt on the same node reappears under the same id;
        // drop it from the warned set so a genuine later loss is reported again.
        let present = &self.worker_snapshots;
        self.warned_missing_affinity_targets
            .retain(|worker_id| !present.contains_key(worker_id));
    }

    fn num_pending_tasks(&self) -> usize {
        self.pending_tasks.len()
    }

    fn get_autoscaling_request(&mut self) -> Option<Vec<TaskResourceRequest>> {
        // If we need to autoscale, return the resource requests of the pending tasks
        let needs_autoscaling = self.needs_autoscaling();
        needs_autoscaling.then(|| {
            super::pending_tasks_in_priority_order(&self.pending_tasks)
                .into_iter()
                .map(|task| task.task.resource_request().clone())
                .collect()
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_resource_request::ResourceRequest;

    use super::*;
    use crate::scheduling::{
        scheduler::test_utils::{
            create_retry_spread_task, create_schedulable_task, create_spread_task,
            create_worker_affinity_task, setup_scheduler, setup_workers,
        },
        tests::{MockTask, MockTaskBuilder},
        worker::tests::MockWorker,
    };

    #[test]
    fn test_default_scheduler_spread_scheduling_homogeneous_workers() {
        let worker_1: WorkerId = Arc::from("worker1");
        let worker_2: WorkerId = Arc::from("worker2");
        let worker_3: WorkerId = Arc::from("worker3");

        let workers = setup_workers(&[
            (worker_1.clone(), 3), // 3 slots available
            (worker_2.clone(), 3), // 3 slots available
            (worker_3.clone(), 3), // 3 slots available
        ]);

        let mut scheduler: DefaultScheduler<MockTask> = setup_scheduler(&workers);

        // Create tasks with Spread strategy
        let initial_tasks = vec![
            create_spread_task(Some(1)),
            create_spread_task(Some(2)),
            create_spread_task(Some(3)),
        ];

        // Enqueue and schedule tasks
        scheduler.enqueue_tasks(initial_tasks);
        let (result, _) = scheduler.schedule_tasks();

        // All tasks should be scheduled because there is enough capacity
        assert_eq!(result.len(), 3);
        assert_eq!(scheduler.num_pending_tasks(), 0);

        // Count tasks per worker
        let mut worker_task_counts: HashMap<&WorkerId, usize> = HashMap::new();
        for scheduled_task in &result {
            *worker_task_counts
                .entry(&scheduled_task.worker_id)
                .or_insert(0) += 1;
        }

        // Verify distribution - worker3 should have 1 task (most slots), worker2 should have 1 task, worker1 should have 1 task
        assert_eq!(*worker_task_counts.get(&worker_3).unwrap(), 1);
        assert_eq!(*worker_task_counts.get(&worker_2).unwrap(), 1);
        assert_eq!(*worker_task_counts.get(&worker_1).unwrap(), 1);
    }

    #[test]
    fn test_default_scheduler_spread_scheduling_heterogeneous_workers() {
        let worker_1: WorkerId = Arc::from("worker1");
        let worker_2: WorkerId = Arc::from("worker2");
        let worker_3: WorkerId = Arc::from("worker3");

        let workers = setup_workers(&[
            (worker_1.clone(), 1), // 1 slot available
            (worker_2.clone(), 2), // 2 slots available
            (worker_3.clone(), 3), // 3 slots available
        ]);

        let mut scheduler: DefaultScheduler<MockTask> = setup_scheduler(&workers);

        // Create tasks with Spread strategy
        let initial_tasks = vec![
            create_spread_task(Some(1)),
            create_spread_task(Some(2)),
            create_spread_task(Some(3)),
        ];

        // Enqueue and schedule tasks
        scheduler.enqueue_tasks(initial_tasks);
        let (result, _) = scheduler.schedule_tasks();

        // All tasks should be scheduled because there is enough capacity
        assert_eq!(result.len(), 3);
        assert_eq!(scheduler.num_pending_tasks(), 0);

        // Count tasks per worker
        let mut worker_task_counts: HashMap<&WorkerId, usize> = HashMap::new();
        for scheduled_task in &result {
            *worker_task_counts
                .entry(&scheduled_task.worker_id)
                .or_insert(0) += 1;
        }

        // Verify distribution - worker3 should have 2 tasks (most slots), worker2 should have 1 task, worker1 should have 0 tasks
        assert_eq!(*worker_task_counts.get(&worker_3).unwrap(), 2);
        assert_eq!(*worker_task_counts.get(&worker_2).unwrap(), 1);
        assert!(!worker_task_counts.contains_key(&worker_1));
    }

    #[test]
    fn test_default_scheduler_soft_node_affinity_scheduling() {
        let worker_1: WorkerId = Arc::from("worker1");
        let worker_2: WorkerId = Arc::from("worker2");
        let worker_3: WorkerId = Arc::from("worker3");

        let workers = setup_workers(&[
            (worker_1.clone(), 1), // 1 slot available
            (worker_2.clone(), 1), // 1 slot available
            (worker_3.clone(), 2), // 2 slots available
        ]);

        let mut scheduler: DefaultScheduler<MockTask> = setup_scheduler(&workers);

        // Create tasks with Node Affinity strategies
        let tasks = vec![
            create_worker_affinity_task(&worker_1, true, Some(1)), // should go to worker 1
            create_worker_affinity_task(&worker_2, true, Some(2)), // should go to worker 2
        ];

        scheduler.enqueue_tasks(tasks);
        let (result, _) = scheduler.schedule_tasks();

        // 2 tasks should be scheduled
        assert_eq!(result.len(), 2);
        assert_eq!(scheduler.num_pending_tasks(), 0);
        for scheduled_task in &result {
            if let SchedulingStrategy::WorkerAffinity { worker_id, .. } =
                &scheduled_task.task().strategy()
            {
                assert_eq!(scheduled_task.worker_id, *worker_id);
            }
        }

        // Create tasks again, now the worker snapshots are:
        // worker1: 0 slots available
        // worker2: 0 slots available
        // worker3: 2 slots available
        // Regardless of which worker the task is affinity to, it should go to worker 3
        let tasks = vec![
            create_worker_affinity_task(&worker_1, true, Some(3)),
            create_worker_affinity_task(&worker_2, true, Some(4)),
            create_worker_affinity_task(&worker_3, true, Some(5)),
        ];

        scheduler.enqueue_tasks(tasks);
        let (result, _) = scheduler.schedule_tasks();

        // Only 2 tasks should be scheduled, because worker 3 has 2 slots available
        assert_eq!(result.len(), 2);
        assert_eq!(scheduler.num_pending_tasks(), 1);
        for scheduled_task in &result {
            assert_eq!(scheduled_task.worker_id, worker_3);
        }
    }

    #[test]
    fn test_default_scheduler_hard_node_affinity_scheduling() {
        let worker_1: WorkerId = Arc::from("worker1");
        let worker_2: WorkerId = Arc::from("worker2");
        let worker_3: WorkerId = Arc::from("worker3");

        let workers = setup_workers(&[
            (worker_1.clone(), 1), // 1 slot available
            (worker_2.clone(), 2), // 2 slots available
            (worker_3.clone(), 3), // 3 slots available
        ]);

        let mut scheduler: DefaultScheduler<MockTask> = setup_scheduler(&workers);

        // Create tasks with Node Affinity strategies
        let tasks = vec![
            create_worker_affinity_task(&worker_1, false, Some(1)),
            create_worker_affinity_task(&worker_2, false, Some(2)),
            create_worker_affinity_task(&worker_3, false, Some(3)),
        ];

        scheduler.enqueue_tasks(tasks);
        let (scheduled_tasks, _) = scheduler.schedule_tasks();

        // 3 tasks should be scheduled, 1 for each worker
        assert_eq!(scheduled_tasks.len(), 3);
        assert_eq!(scheduler.num_pending_tasks(), 0);
        for scheduled_task in &scheduled_tasks {
            if let SchedulingStrategy::WorkerAffinity { worker_id, .. } =
                &scheduled_task.task().strategy()
            {
                assert_eq!(scheduled_task.worker_id, *worker_id);
            } else {
                panic!("Task should have worker affinity strategy");
            }
        }

        // Create tasks again
        let tasks = vec![
            create_worker_affinity_task(&worker_1, false, Some(1)), // should not be scheduled (worker busy)
            create_worker_affinity_task(&worker_2, false, Some(2)),
            create_worker_affinity_task(&worker_3, false, Some(3)),
        ];

        scheduler.enqueue_tasks(tasks);
        let (scheduled_tasks, _) = scheduler.schedule_tasks();

        // worker 1 should not be available, worker 2 should have 1 slot available, worker 3 should have 2 slots available
        assert_eq!(scheduled_tasks.len(), 2);
        assert_eq!(scheduler.num_pending_tasks(), 1);
        for scheduled_task in &scheduled_tasks {
            if let SchedulingStrategy::WorkerAffinity { worker_id, .. } =
                &scheduled_task.task().strategy()
            {
                assert_eq!(scheduled_task.worker_id, *worker_id);
            } else {
                panic!("Task should have worker affinity strategy");
            }
        }
    }

    #[test]
    fn test_default_scheduler_with_priority_scheduling() {
        let worker_1: WorkerId = Arc::from("worker1");
        let worker_2: WorkerId = Arc::from("worker2");

        let workers = setup_workers(&[
            (worker_1, 1), // 1 slot available
            (worker_2, 1), // 1 slot available
        ]);

        let mut scheduler: DefaultScheduler<MockTask> = setup_scheduler(&workers);

        // Add a lot of low priority tasks
        let tasks = (0..100)
            .map(|_| create_schedulable_task(MockTaskBuilder::default().with_priority(1).build()))
            .collect();

        scheduler.enqueue_tasks(tasks);
        let (result, _) = scheduler.schedule_tasks();

        // Only 2 tasks should be scheduled (one per worker)
        assert_eq!(result.len(), 2);
        assert_eq!(scheduler.num_pending_tasks(), 98);

        // Add a high-priority task
        let high_priority_task =
            create_schedulable_task(MockTaskBuilder::default().with_priority(100).build());
        scheduler.enqueue_tasks(vec![high_priority_task]);

        // The high-priority task should not be scheduled because worker1 is full
        let (result, _) = scheduler.schedule_tasks();
        assert_eq!(result.len(), 0);
        assert_eq!(scheduler.num_pending_tasks(), 99);

        // Update scheduler state to add a new worker with 1 slot available
        let worker_3: WorkerId = Arc::from("worker3");
        let new_worker = MockWorker::new(worker_3.clone(), 1.0, 0.0);
        let new_worker_snapshot = WorkerSnapshot::from(&new_worker);
        scheduler.update_worker_state(&[new_worker_snapshot]);

        // The high-priority task should now be scheduled to the new worker
        let (result, _) = scheduler.schedule_tasks();
        assert_eq!(result.len(), 1);
        assert_eq!(scheduler.num_pending_tasks(), 98);
        assert_eq!(result[0].worker_id, worker_3);
    }

    #[test]
    fn test_default_scheduler_with_resource_request_scheduling_big_tasks_first() {
        let worker_1: WorkerId = Arc::from("worker1");
        let worker_2: WorkerId = Arc::from("worker2");
        let worker_3: WorkerId = Arc::from("worker3");

        let workers = setup_workers(&[
            (worker_1.clone(), 1), // 1 slot available
            (worker_2.clone(), 2), // 2 slots available
            (worker_3.clone(), 3), // 3 slots available
        ]);

        let mut scheduler: DefaultScheduler<MockTask> = setup_scheduler(&workers);

        let tasks = vec![
            create_schedulable_task(
                MockTaskBuilder::default()
                    .with_task_id(3)
                    .with_resource_request(
                        ResourceRequest::try_new_internal(Some(3.0), None, None).unwrap(), // 3 CPUs
                    )
                    .build(),
            ),
            create_schedulable_task(
                MockTaskBuilder::default()
                    .with_task_id(2)
                    .with_resource_request(
                        ResourceRequest::try_new_internal(Some(2.0), None, None).unwrap(), // 2 CPUs
                    )
                    .build(),
            ),
            create_schedulable_task(
                MockTaskBuilder::default()
                    .with_task_id(1)
                    .with_resource_request(
                        ResourceRequest::try_new_internal(Some(1.0), None, None).unwrap(), // 1 CPU
                    )
                    .build(),
            ),
        ];

        scheduler.enqueue_tasks(tasks);
        let (result, _) = scheduler.schedule_tasks();

        assert_eq!(result.len(), 3);
        assert_eq!(scheduler.num_pending_tasks(), 0);
        for scheduled_task in &result {
            if scheduled_task.worker_id == worker_1 {
                assert_eq!(scheduled_task.task().task_id(), 1);
            } else if scheduled_task.worker_id == worker_2 {
                assert_eq!(scheduled_task.task().task_id(), 2);
            } else if scheduled_task.worker_id == worker_3 {
                assert_eq!(scheduled_task.task().task_id(), 3);
            }
        }
    }

    // TODO: This test currently fails because the scheduler is currently not optimal, we should fix this by using a bin packing algorithm.
    // In this test case, we have 3 workers with 1, 2, and 3 slots available, and 3 tasks requesting 1, 2, and 3 CPUs.
    // In the ideal case, the scheduler should schedule the tasks in the following order:
    // 1. Task 1 (1 CPU) to worker 1 (1 slot available)
    // 2. Task 2 (2 CPUs) to worker 2 (2 slots available)
    // 3. Task 3 (3 CPUs) to worker 3 (3 slots available)
    // However, the scheduler currently schedules the tasks simply by picking the worker with the most available slots.
    // This results in the following schedule:
    // 1. Task 1 (1 CPU) to worker 3 (3 slots available)
    // 2. Task 2 (2 CPUs) to worker 2 or 3 (2 slots available)
    // 3. Task 3 (3 CPUs) is unscheduled (no worker has 3 slots available)
    #[test]
    #[ignore = "This test is currently failing because the scheduler is currently not optimal, we should fix this by using a bin packing algorithm."]
    fn test_default_scheduler_with_resource_request_scheduling_small_tasks_first() {
        let worker_1: WorkerId = Arc::from("worker1");
        let worker_2: WorkerId = Arc::from("worker2");
        let worker_3: WorkerId = Arc::from("worker3");

        let workers = setup_workers(&[
            (worker_1.clone(), 1), // 1 slot available
            (worker_2.clone(), 2), // 2 slots available
            (worker_3.clone(), 3), // 3 slots available
        ]);

        let mut scheduler: DefaultScheduler<MockTask> = setup_scheduler(&workers);

        let tasks = vec![
            create_schedulable_task(
                MockTaskBuilder::default()
                    .with_task_id(1)
                    .with_resource_request(
                        ResourceRequest::try_new_internal(Some(1.0), None, None).unwrap(), // 1 CPU
                    )
                    .build(),
            ),
            create_schedulable_task(
                MockTaskBuilder::default()
                    .with_task_id(2)
                    .with_resource_request(
                        ResourceRequest::try_new_internal(Some(2.0), None, None).unwrap(), // 2 CPUs
                    )
                    .build(),
            ),
            create_schedulable_task(
                MockTaskBuilder::default()
                    .with_task_id(3)
                    .with_resource_request(
                        ResourceRequest::try_new_internal(Some(3.0), None, None).unwrap(), // 3 CPUs
                    )
                    .build(),
            ),
        ];

        scheduler.enqueue_tasks(tasks);
        let (result, _) = scheduler.schedule_tasks();

        assert_eq!(result.len(), 3);
        assert_eq!(scheduler.num_pending_tasks(), 0);
        for scheduled_task in &result {
            if scheduled_task.worker_id == worker_1 {
                assert_eq!(scheduled_task.task().task_id(), 1);
            } else if scheduled_task.worker_id == worker_2 {
                assert_eq!(scheduled_task.task().task_id(), 2);
            } else if scheduled_task.worker_id == worker_3 {
                assert_eq!(scheduled_task.task().task_id(), 3);
            }
        }
    }

    #[test]
    fn test_scheduling_with_empty_workers() {
        let mut scheduler: DefaultScheduler<MockTask> = setup_scheduler(&HashMap::new());

        let tasks = vec![
            create_spread_task(Some(1)),
            create_worker_affinity_task(&Arc::from("worker1"), true, Some(1)),
        ];

        scheduler.enqueue_tasks(tasks);
        let (result, _) = scheduler.schedule_tasks();

        assert_eq!(result.len(), 0);
        assert_eq!(scheduler.num_pending_tasks(), 2);
    }

    #[test]
    fn test_scheduling_with_more_tasks_than_workers() {
        let worker_1: WorkerId = Arc::from("worker1");
        let worker_2: WorkerId = Arc::from("worker2");

        let workers = setup_workers(&[
            (worker_1.clone(), 1), // 1 slot available
            (worker_2.clone(), 1), // 1 slot available
        ]);

        let mut scheduler: DefaultScheduler<MockTask> = setup_scheduler(&workers);

        // Create 5 tasks with Spread strategy - more than available workers
        let tasks = vec![
            create_spread_task(Some(1)),
            create_spread_task(Some(2)),
            create_spread_task(Some(3)),
            create_spread_task(Some(4)),
            create_spread_task(Some(5)),
        ];

        scheduler.enqueue_tasks(tasks);
        let (result, _) = scheduler.schedule_tasks();

        // Only 2 tasks should be scheduled (1 per worker)
        assert_eq!(result.len(), 2);
        assert_eq!(scheduler.num_pending_tasks(), 3);

        // Count tasks per worker - each should have exactly 1
        let mut worker_task_counts: HashMap<&WorkerId, usize> = HashMap::new();
        for scheduled_task in &result {
            *worker_task_counts
                .entry(&scheduled_task.worker_id)
                .or_insert(0) += 1;
        }

        assert_eq!(*worker_task_counts.get(&worker_1).unwrap(), 1);
        assert_eq!(*worker_task_counts.get(&worker_2).unwrap(), 1);
    }

    #[test]
    fn test_default_scheduler_with_no_workers_can_autoscale() {
        let mut scheduler: DefaultScheduler<MockTask> = setup_scheduler(&HashMap::new());

        let tasks = vec![create_spread_task(Some(1))];

        scheduler.enqueue_tasks(tasks);
        let (result, _) = scheduler.schedule_tasks();

        assert_eq!(result.len(), 0);
        assert_eq!(scheduler.num_pending_tasks(), 1);
        assert_eq!(scheduler.get_autoscaling_request().unwrap().len(), 1);
    }

    #[test]
    fn test_default_scheduler_with_insufficient_worker_capacity_can_autoscale() {
        let worker_1: WorkerId = Arc::from("worker1");

        // Create a worker with only 1 slot available
        let workers = setup_workers(&[
            (worker_1, 1), // 1 slot available
        ]);

        let mut scheduler: DefaultScheduler<MockTask> = setup_scheduler(&workers);

        // Create 5 tasks - more than the single worker can handle
        let tasks = vec![
            create_spread_task(Some(1)),
            create_spread_task(Some(2)),
            create_spread_task(Some(3)),
            create_spread_task(Some(4)),
            create_spread_task(Some(5)),
        ];

        scheduler.enqueue_tasks(tasks);
        let (result, _) = scheduler.schedule_tasks();

        // Only 1 task should be scheduled (worker capacity is 1)
        assert_eq!(result.len(), 1);
        assert_eq!(scheduler.num_pending_tasks(), 4);

        // Should request 4 workers (ratio 5 total demand / 1 capacity = 5.0 > default threshold 1.25)
        assert_eq!(scheduler.get_autoscaling_request().unwrap().len(), 4);
    }

    #[test]
    fn test_default_scheduler_autoscaling_request_follows_priority_order() {
        let mut scheduler: DefaultScheduler<MockTask> = setup_scheduler(&HashMap::new());

        let tasks = vec![
            PendingTask::new(
                MockTaskBuilder::default()
                    .with_priority(1)
                    .with_resource_request(
                        ResourceRequest::try_new_internal(Some(1.0), None, None).unwrap(),
                    )
                    .build(),
                crate::utils::channel::create_oneshot_channel().0,
                tokio_util::sync::CancellationToken::new(),
            ),
            PendingTask::new(
                MockTaskBuilder::default()
                    .with_priority(3)
                    .with_resource_request(
                        ResourceRequest::try_new_internal(Some(3.0), None, None).unwrap(),
                    )
                    .build(),
                crate::utils::channel::create_oneshot_channel().0,
                tokio_util::sync::CancellationToken::new(),
            ),
            PendingTask::new(
                MockTaskBuilder::default()
                    .with_priority(2)
                    .with_resource_request(
                        ResourceRequest::try_new_internal(Some(2.0), None, None).unwrap(),
                    )
                    .build(),
                crate::utils::channel::create_oneshot_channel().0,
                tokio_util::sync::CancellationToken::new(),
            ),
        ];

        scheduler.enqueue_tasks(tasks);

        let autoscaling_request = scheduler.get_autoscaling_request().unwrap();
        let requested_cpus = autoscaling_request
            .iter()
            .map(TaskResourceRequest::num_cpus)
            .collect::<Vec<_>>();

        assert_eq!(requested_cpus, vec![3.0, 2.0, 1.0]);
    }

    #[test]
    fn test_default_scheduler_with_sufficient_worker_capacity_no_autoscale() {
        let worker_1: WorkerId = Arc::from("worker1");
        let worker_2: WorkerId = Arc::from("worker2");

        // Create workers with sufficient capacity
        let workers = setup_workers(&[
            (worker_1, 2), // 2 slots available
            (worker_2, 3), // 3 slots available
        ]);

        let mut scheduler: DefaultScheduler<MockTask> = setup_scheduler(&workers);

        // Create 3 tasks - less than total worker capacity (5)
        let tasks = vec![
            create_spread_task(Some(1)),
            create_spread_task(Some(2)),
            create_spread_task(Some(3)),
        ];

        scheduler.enqueue_tasks(tasks);
        let (result, _) = scheduler.schedule_tasks();

        // All tasks should be scheduled
        assert_eq!(result.len(), 3);
        assert_eq!(scheduler.num_pending_tasks(), 0);

        // Should not request autoscaling
        assert!(scheduler.get_autoscaling_request().is_none());
    }

    #[test]
    fn test_hard_affinity_fallback_on_missing_worker() {
        // Hard affinity to a worker that's not in snapshots should fall back to spread
        let worker_1: WorkerId = Arc::from("worker1");
        let worker_2: WorkerId = Arc::from("worker2");
        let missing_worker: WorkerId = Arc::from("worker999");

        let workers = setup_workers(&[(worker_1, 4), (worker_2.clone(), 8)]);
        let mut scheduler: DefaultScheduler<MockTask> = setup_scheduler(&workers);

        // Hard affinity to missing worker
        let task = create_worker_affinity_task(&missing_worker, false, Some(1));
        scheduler.enqueue_tasks(vec![task]);

        let (scheduled, _) = scheduler.schedule_tasks();
        assert_eq!(scheduled.len(), 1);
        // Should have fallen back to spread (worker2 has more capacity)
        assert_eq!(scheduled[0].worker_id, worker_2);
    }

    #[test]
    fn test_hard_affinity_waits_when_worker_busy() {
        // Hard affinity to a busy worker should wait (return None), not fall back
        let worker_1: WorkerId = Arc::from("worker1");
        let worker_2: WorkerId = Arc::from("worker2");

        let workers = setup_workers(&[(worker_1.clone(), 1), (worker_2.clone(), 4)]);
        let mut scheduler: DefaultScheduler<MockTask> = setup_scheduler(&workers);

        // Occupy worker1's only slot with a hard-affinity task
        let occupy_task = create_worker_affinity_task(&worker_1, false, Some(1));
        scheduler.enqueue_tasks(vec![occupy_task]);
        let (scheduled, _) = scheduler.schedule_tasks();
        assert_eq!(scheduled.len(), 1);
        assert_eq!(scheduled[0].worker_id, worker_1);

        // Hard affinity to now-busy worker1: must wait, not spill to worker2
        let task = create_worker_affinity_task(&worker_1, false, Some(2));
        scheduler.enqueue_tasks(vec![task]);

        let (scheduled, _) = scheduler.schedule_tasks();
        assert_eq!(scheduled.len(), 0);
        assert_eq!(scheduler.num_pending_tasks(), 1);

        // worker1 disappears from the cluster (died / retired): the pinned task must not wedge
        let worker_2_snapshot = WorkerSnapshot::from(&MockWorker::new(worker_2.clone(), 4.0, 0.0));
        scheduler.update_worker_state(&[worker_2_snapshot]);
        let (scheduled, _) = scheduler.schedule_tasks();
        assert_eq!(scheduled.len(), 1);
        assert_eq!(scheduled[0].worker_id, worker_2);
        assert_eq!(scheduler.num_pending_tasks(), 0);
    }

    #[test]
    fn test_soft_affinity_falls_back_on_missing_worker() {
        // The missing-worker branch is shared by both affinity modes; make sure soft
        // affinity keeps falling back through it.
        let worker_1: WorkerId = Arc::from("worker1");
        let missing_worker: WorkerId = Arc::from("worker999");

        let workers = setup_workers(&[(worker_1.clone(), 4)]);
        let mut scheduler: DefaultScheduler<MockTask> = setup_scheduler(&workers);

        scheduler.enqueue_tasks(vec![create_worker_affinity_task(
            &missing_worker,
            true,
            Some(1),
        )]);

        let (scheduled, _) = scheduler.schedule_tasks();
        assert_eq!(scheduled.len(), 1);
        assert_eq!(scheduled[0].worker_id, worker_1);
    }

    #[test]
    fn test_retry_avoids_the_worker_it_failed_on() {
        // worker1 has the most free capacity, so spread would normally pick it. A retry
        // that already failed there should go elsewhere instead.
        let worker_1: WorkerId = Arc::from("worker1");
        let worker_2: WorkerId = Arc::from("worker2");

        let workers = setup_workers(&[(worker_1.clone(), 8), (worker_2.clone(), 4)]);
        let mut scheduler: DefaultScheduler<MockTask> = setup_scheduler(&workers);

        scheduler.enqueue_tasks(vec![create_spread_task(Some(1))]);
        let (scheduled, _) = scheduler.schedule_tasks();
        assert_eq!(scheduled[0].worker_id, worker_1);

        scheduler.update_worker_state(
            &setup_workers(&[(worker_1.clone(), 8), (worker_2.clone(), 4)])
                .values()
                .map(WorkerSnapshot::from)
                .collect::<Vec<_>>(),
        );
        scheduler.enqueue_tasks(vec![create_retry_spread_task(Some(2), &worker_1)]);
        let (scheduled, _) = scheduler.schedule_tasks();
        assert_eq!(scheduled.len(), 1);
        assert_eq!(scheduled[0].worker_id, worker_2);
    }

    #[test]
    fn test_retry_uses_failed_worker_when_it_is_the_only_option() {
        // Avoiding the previous worker is a preference: with nowhere else to go, the task
        // must still be scheduled rather than left pending forever.
        let worker_1: WorkerId = Arc::from("worker1");

        let workers = setup_workers(&[(worker_1.clone(), 4)]);
        let mut scheduler: DefaultScheduler<MockTask> = setup_scheduler(&workers);

        scheduler.enqueue_tasks(vec![create_retry_spread_task(Some(1), &worker_1)]);
        let (scheduled, _) = scheduler.schedule_tasks();
        assert_eq!(scheduled.len(), 1);
        assert_eq!(scheduled[0].worker_id, worker_1);
    }

    #[test]
    fn test_missing_affinity_target_warns_once_until_the_worker_returns() {
        let worker_1: WorkerId = Arc::from("worker1");
        let missing_worker: WorkerId = Arc::from("worker999");

        let workers = setup_workers(&[(worker_1.clone(), 4)]);
        let mut scheduler: DefaultScheduler<MockTask> = setup_scheduler(&workers);

        for id in 1..=3 {
            scheduler.enqueue_tasks(vec![create_worker_affinity_task(
                &missing_worker,
                false,
                Some(id),
            )]);
            scheduler.schedule_tasks();
        }
        assert_eq!(scheduler.warned_missing_affinity_targets.len(), 1);

        // The worker comes back (an actor rebuilt on the same node keeps its id), so a
        // later disappearance is worth reporting again.
        scheduler.update_worker_state(
            &setup_workers(&[(worker_1, 4), (missing_worker, 4)])
                .values()
                .map(WorkerSnapshot::from)
                .collect::<Vec<_>>(),
        );
        assert!(scheduler.warned_missing_affinity_targets.is_empty());
    }
}
