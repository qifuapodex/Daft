use std::{
    collections::{BinaryHeap, HashMap},
    time::Instant,
};

use super::{
    PendingTask, ScheduledTask, Scheduler, WorkerSnapshot, scheduler_actor::SCHEDULER_LOG_TARGET,
};
use crate::scheduling::{
    task::{SchedulingStrategy, Task, TaskDetails, TaskResourceRequest},
    worker::WorkerId,
};

pub(super) struct LinearScheduler<T: Task> {
    worker_snapshots: HashMap<WorkerId, WorkerSnapshot>,
    pending_tasks: BinaryHeap<PendingTask<T>>,
}

impl<T: Task> Default for LinearScheduler<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Task> LinearScheduler<T> {
    pub fn new() -> Self {
        Self {
            worker_snapshots: HashMap::new(),
            pending_tasks: BinaryHeap::new(),
        }
    }

    fn try_schedule_spread_task(&self, task: &T) -> Option<WorkerId> {
        self.worker_snapshots
            .iter()
            .filter(|(_, worker)| worker.can_schedule_task(task))
            .max_by_key(|(_, worker)| {
                (worker.available_num_cpus() + worker.available_num_gpus()) as usize
            })
            .map(|(id, _)| id.clone())
    }

    fn try_schedule_worker_affinity_task(
        &self,
        task: &T,
        worker_id: &WorkerId,
        soft: bool,
    ) -> Option<WorkerId> {
        match self.worker_snapshots.get(worker_id) {
            Some(worker) if worker.can_schedule_task(task) => {
                // Target worker exists and has capacity
                Some(worker.worker_id.clone())
            }
            Some(_) => {
                // Target worker exists but is busy: soft affinity falls back, hard affinity waits
                if soft {
                    self.try_schedule_spread_task(task)
                } else {
                    None
                }
            }
            None => {
                // Target worker missing from snapshots: fall back to spread regardless of soft flag
                // (worker likely died; keeping hard affinity would deadlock the task)
                tracing::warn!(
                    target: SCHEDULER_LOG_TARGET,
                    worker_id = %worker_id,
                    "Affinity target missing from worker snapshots; falling back to spread scheduling"
                );
                self.try_schedule_spread_task(task)
            }
        }
    }

    fn try_schedule_task(&self, task: &PendingTask<T>) -> Option<WorkerId> {
        match task.strategy() {
            SchedulingStrategy::Spread => self.try_schedule_spread_task(&task.task),
            SchedulingStrategy::WorkerAffinity { worker_id, soft } => {
                self.try_schedule_worker_affinity_task(&task.task, worker_id, *soft)
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

        false
    }
}

impl<T: Task> Scheduler<T> for LinearScheduler<T> {
    fn update_worker_state(&mut self, worker_snapshots: &[WorkerSnapshot]) {
        self.worker_snapshots = worker_snapshots
            .iter()
            .map(|snapshot| (snapshot.worker_id.clone(), snapshot.clone()))
            .collect();
    }

    fn enqueue_tasks(&mut self, tasks: Vec<PendingTask<T>>) {
        self.pending_tasks.extend(tasks);
    }

    fn schedule_tasks(&mut self) -> (Vec<ScheduledTask<T>>, Vec<PendingTask<T>>) {
        // Check if any worker has active tasks
        let has_active_tasks = self
            .worker_snapshots
            .values()
            .any(|worker| !worker.active_task_details.is_empty());

        // If there are active tasks, don't schedule any new ones
        if has_active_tasks {
            return (Vec::new(), Vec::new());
        }

        let mut scheduled = Vec::new();
        let mut unscheduled = Vec::new();
        let mut cancelled = Vec::new();

        // Process all tasks in the queue
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
                    .unwrap()
                    .active_task_details
                    .insert(task.task_context(), TaskDetails::from(&task.task));
                scheduled.push(ScheduledTask::new(task, worker_id));
                // Only schedule one task
                break;
            } else {
                unscheduled.push(task);
            }
        }

        // Put unscheduled tasks back in the queue
        self.pending_tasks.extend(unscheduled);
        (scheduled, cancelled)
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
                .next()
                .map(|task| vec![task.task.resource_request().clone()])
                .unwrap_or_default()
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
            create_schedulable_task, create_spread_task, create_worker_affinity_task,
            setup_scheduler, setup_workers,
        },
        task::tests::{MockTask, MockTaskBuilder},
    };

    #[test]
    fn test_linear_scheduler_spread_scheduling() {
        let worker_1: WorkerId = Arc::from("worker1");
        let worker_2: WorkerId = Arc::from("worker2");
        let worker_3: WorkerId = Arc::from("worker3");

        let workers = setup_workers(&[(worker_1, 1), (worker_2, 2), (worker_3, 3)]);

        let mut scheduler: LinearScheduler<MockTask> = setup_scheduler(&workers);

        // Create tasks with Spread strategy
        let tasks = vec![
            create_spread_task(Some(1)),
            create_spread_task(Some(2)),
            create_spread_task(Some(3)),
        ];

        // Enqueue and schedule tasks
        scheduler.enqueue_tasks(tasks);
        let (result, _) = scheduler.schedule_tasks();

        // Only one task should be scheduled because of linear scheduling
        assert_eq!(result.len(), 1);
        assert_eq!(scheduler.num_pending_tasks(), 2);

        // Try to schedule more tasks - should fail because one task is already running
        let (result, _) = scheduler.schedule_tasks();
        assert_eq!(result.len(), 0);
        assert_eq!(scheduler.num_pending_tasks(), 2);

        // Update worker state to reflect that the workers are all idle
        let worker_snapshots = workers
            .values()
            .map(WorkerSnapshot::from)
            .collect::<Vec<_>>();
        scheduler.update_worker_state(&worker_snapshots);

        // Now we should be able to schedule another task
        let (result, _) = scheduler.schedule_tasks();
        assert_eq!(result.len(), 1);
        assert_eq!(scheduler.num_pending_tasks(), 1);
    }

    #[test]
    fn test_linear_scheduler_soft_node_affinity_scheduling() {
        let worker_1: WorkerId = Arc::from("worker1");
        let worker_2: WorkerId = Arc::from("worker2");
        let worker_3: WorkerId = Arc::from("worker3");

        let workers = setup_workers(&[(worker_1.clone(), 1), (worker_2.clone(), 1), (worker_3, 2)]);

        let mut scheduler: LinearScheduler<MockTask> = setup_scheduler(&workers);

        // Create tasks with soft Node Affinity strategies
        let tasks = vec![
            create_worker_affinity_task(&worker_1, true, Some(1)),
            create_worker_affinity_task(&worker_2, true, Some(2)),
        ];

        scheduler.enqueue_tasks(tasks);
        let (result, _) = scheduler.schedule_tasks();

        // Only one task should be scheduled
        assert_eq!(result.len(), 1);
        assert_eq!(scheduler.num_pending_tasks(), 1);

        // Verify the scheduled task went to its preferred worker
        if let SchedulingStrategy::WorkerAffinity { worker_id, .. } = &result[0].task.strategy() {
            assert_eq!(&result[0].worker_id, worker_id);
        } else {
            panic!("Task should have worker affinity strategy");
        }

        let (result, _) = scheduler.schedule_tasks();
        assert_eq!(result.len(), 0);
        assert_eq!(scheduler.num_pending_tasks(), 1);

        // Update worker state to reflect that the workers are all idle
        let worker_snapshots = workers
            .values()
            .map(WorkerSnapshot::from)
            .collect::<Vec<_>>();
        scheduler.update_worker_state(&worker_snapshots);

        let (result, _) = scheduler.schedule_tasks();
        assert_eq!(result.len(), 1);
        assert_eq!(scheduler.num_pending_tasks(), 0);

        if let SchedulingStrategy::WorkerAffinity { worker_id, .. } = &result[0].task.strategy() {
            assert_eq!(&result[0].worker_id, worker_id);
        } else {
            panic!("Task should have worker affinity strategy");
        }
    }

    #[test]
    fn test_linear_scheduler_autoscaling_request_follows_priority_order() {
        let mut scheduler: LinearScheduler<MockTask> = setup_scheduler(&HashMap::new());

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

        assert_eq!(autoscaling_request.len(), 1);
        assert_eq!(autoscaling_request[0].num_cpus(), 3.0);
    }

    #[test]
    fn test_linear_scheduler_hard_node_affinity_scheduling() {
        let worker_1: WorkerId = Arc::from("worker1");
        let worker_2: WorkerId = Arc::from("worker2");
        let worker_3: WorkerId = Arc::from("worker3");

        let workers = setup_workers(&[
            (worker_1.clone(), 1),
            (worker_2.clone(), 2),
            (worker_3.clone(), 3),
        ]);

        let mut scheduler: LinearScheduler<MockTask> = setup_scheduler(&workers);

        // Create tasks with hard Node Affinity strategies
        let tasks = vec![
            create_worker_affinity_task(&worker_1, false, Some(1)),
            create_worker_affinity_task(&worker_2, false, Some(2)),
            create_worker_affinity_task(&worker_3, false, Some(3)),
        ];

        scheduler.enqueue_tasks(tasks);
        let (result, _) = scheduler.schedule_tasks();

        // Only one task should be scheduled
        assert_eq!(result.len(), 1);
        assert_eq!(scheduler.num_pending_tasks(), 2);

        // Verify the scheduled task went to its preferred worker
        if let SchedulingStrategy::WorkerAffinity { worker_id, .. } = &result[0].task.strategy() {
            assert_eq!(&result[0].worker_id, worker_id);
        } else {
            panic!("Task should have worker affinity strategy");
        }

        let (result, _) = scheduler.schedule_tasks();
        assert_eq!(result.len(), 0);
        assert_eq!(scheduler.num_pending_tasks(), 2);

        // Update worker state to reflect that the workers are all idle
        let worker_snapshots = workers
            .values()
            .map(WorkerSnapshot::from)
            .collect::<Vec<_>>();
        scheduler.update_worker_state(&worker_snapshots);

        let (result, _) = scheduler.schedule_tasks();
        assert_eq!(result.len(), 1);
        assert_eq!(scheduler.num_pending_tasks(), 1);

        if let SchedulingStrategy::WorkerAffinity { worker_id, .. } = &result[0].task.strategy() {
            assert_eq!(&result[0].worker_id, worker_id);
        } else {
            panic!("Task should have worker affinity strategy");
        }
    }

    #[test]
    fn test_linear_scheduler_with_priority_scheduling() {
        let worker_1: WorkerId = Arc::from("worker1");
        let worker_2: WorkerId = Arc::from("worker2");

        let workers = setup_workers(&[(worker_1, 1), (worker_2, 1)]);

        let mut scheduler: LinearScheduler<MockTask> = setup_scheduler(&workers);

        // Add a low priority task
        let low_priority_task = create_schedulable_task(
            MockTaskBuilder::default()
                .with_task_id(1)
                .with_priority(1)
                .with_scheduling_strategy(SchedulingStrategy::Spread)
                .build(),
        );
        scheduler.enqueue_tasks(vec![low_priority_task]);

        // Add a high priority task
        let high_priority_task = create_schedulable_task(
            MockTaskBuilder::default()
                .with_priority(100)
                .with_task_id(2)
                .with_scheduling_strategy(SchedulingStrategy::Spread)
                .build(),
        );
        scheduler.enqueue_tasks(vec![high_priority_task]);

        // The high priority task should be scheduled first
        let (result, _) = scheduler.schedule_tasks();
        assert_eq!(result.len(), 1);
        assert_eq!(scheduler.num_pending_tasks(), 1);
        assert_eq!(result[0].task.task_id(), 2);
    }

    #[test]
    fn test_linear_scheduler_with_no_workers() {
        let mut scheduler: LinearScheduler<MockTask> = setup_scheduler(&HashMap::new());

        let tasks = vec![
            create_spread_task(Some(1)),
            create_worker_affinity_task(&Arc::from("worker1"), true, Some(2)),
        ];

        scheduler.enqueue_tasks(tasks);
        let (result, _) = scheduler.schedule_tasks();

        assert_eq!(result.len(), 0);
        assert_eq!(scheduler.num_pending_tasks(), 2);
    }

    #[test]
    fn test_linear_hard_affinity_fallback_on_missing_worker() {
        // Hard affinity to a worker that's not in snapshots should fall back to spread
        let worker_1: WorkerId = Arc::from("worker1");
        let worker_2: WorkerId = Arc::from("worker2");
        let missing_worker: WorkerId = Arc::from("worker999");

        let workers = setup_workers(&[(worker_1, 4), (worker_2.clone(), 8)]);
        let mut scheduler: LinearScheduler<MockTask> = setup_scheduler(&workers);

        // Hard affinity to missing worker
        let task = create_worker_affinity_task(&missing_worker, false, Some(1));
        scheduler.enqueue_tasks(vec![task]);

        let (scheduled, _) = scheduler.schedule_tasks();
        assert_eq!(scheduled.len(), 1);
        // Should have fallen back to spread (worker2 has more capacity)
        assert_eq!(scheduled[0].worker_id, worker_2);
    }

    #[test]
    fn test_linear_hard_affinity_waits_when_worker_busy() {
        // Hard affinity should wait when worker is busy, but fall back when worker disappears
        let worker_1: WorkerId = Arc::from("worker1");
        let worker_2: WorkerId = Arc::from("worker2");

        let workers = setup_workers(&[(worker_1.clone(), 1), (worker_2.clone(), 4)]);
        let mut scheduler: LinearScheduler<MockTask> = setup_scheduler(&workers);

        // Occupy worker1's only slot with a hard-affinity task
        let occupy_task = create_worker_affinity_task(&worker_1, false, Some(1));
        scheduler.enqueue_tasks(vec![occupy_task]);
        let (scheduled, _) = scheduler.schedule_tasks();
        assert_eq!(scheduled.len(), 1);
        assert_eq!(scheduled[0].worker_id, worker_1);

        // Enqueue another hard-affinity task to worker1 (which is now busy)
        let task = create_worker_affinity_task(&worker_1, false, Some(2));
        scheduler.enqueue_tasks(vec![task]);

        // Linear scheduler won't schedule anything while a task is active
        let (scheduled, _) = scheduler.schedule_tasks();
        assert_eq!(scheduled.len(), 0);
        assert_eq!(scheduler.num_pending_tasks(), 1);

        // worker1 disappears from the cluster (died / retired): update to only worker2
        let idle_worker_2 = setup_workers(&[(worker_2.clone(), 4)]);
        scheduler.update_worker_state(
            &idle_worker_2
                .values()
                .map(WorkerSnapshot::from)
                .collect::<Vec<_>>(),
        );

        // Now the pinned task should fall back to worker2 (worker1 missing = fallback)
        let (scheduled, _) = scheduler.schedule_tasks();
        assert_eq!(scheduled.len(), 1);
        assert_eq!(scheduled[0].worker_id, worker_2);
        assert_eq!(scheduler.num_pending_tasks(), 0);
    }

    /// `update_worker_state` replaces the snapshot map wholesale rather than merging into
    /// it. That matters here: the linear scheduler refuses to schedule while *any* worker
    /// shows an active task, so a merge would leave a dead worker's stale
    /// `active_task_details` in the map forever and wedge the scheduler permanently.
    #[test]
    fn test_linear_vanished_worker_is_dropped_from_snapshots() {
        let worker_1: WorkerId = Arc::from("worker1");
        let worker_2: WorkerId = Arc::from("worker2");

        let workers = setup_workers(&[(worker_1.clone(), 1), (worker_2.clone(), 4)]);
        let mut scheduler: LinearScheduler<MockTask> = setup_scheduler(&workers);

        // Put an active task on worker1, then have worker1 vanish while it is still running.
        scheduler.enqueue_tasks(vec![create_worker_affinity_task(&worker_1, true, Some(1))]);
        let (scheduled, _) = scheduler.schedule_tasks();
        assert_eq!(scheduled.len(), 1);
        assert_eq!(scheduled[0].worker_id, worker_1);

        let surviving = setup_workers(&[(worker_2.clone(), 4)]);
        scheduler.update_worker_state(
            &surviving
                .values()
                .map(WorkerSnapshot::from)
                .collect::<Vec<_>>(),
        );

        // With worker1 gone there are no active tasks left, so scheduling resumes.
        scheduler.enqueue_tasks(vec![create_spread_task(Some(2))]);
        let (scheduled, _) = scheduler.schedule_tasks();
        assert_eq!(scheduled.len(), 1);
        assert_eq!(scheduled[0].worker_id, worker_2);
    }
}
