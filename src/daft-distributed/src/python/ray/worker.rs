use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use common_error::DaftResult;
use pyo3::prelude::*;

use super::{RaySwordfishTask, task::RayTaskResultHandle};
use crate::scheduling::{
    drain::{DrainBarrier, DrainState},
    scheduler::WorkerSnapshot,
    task::{SwordfishTask, Task, TaskContext, TaskDetails},
    worker::{Worker, WorkerId},
};

type ActiveTaskDetails = HashMap<TaskContext, TaskDetails>;

#[derive(Debug, Clone, Copy)]
pub(crate) enum ActorState {
    Ready,
    Busy,
    Idle,
    Releasing,
    Released,
}

#[pyclass(module = "daft.daft", name = "RaySwordfishWorker", from_py_object)]
#[derive(Debug, Clone)]
pub(crate) struct RaySwordfishWorker {
    worker_id: WorkerId,
    node_id: String,
    pub(crate) drain: DrainBarrier,
    pub(crate) known_dead: bool,
    ray_worker_handle: Arc<Py<PyAny>>,
    num_cpus: f64,
    total_memory_bytes: usize,
    num_gpus: f64,
    active_task_details: ActiveTaskDetails,
    ip_address: String,
    last_task_finished_at: Instant,
    state: ActorState,
}

#[pymethods]
impl RaySwordfishWorker {
    #[new]
    #[pyo3(signature = (worker_id, ray_worker_handle, num_cpus, num_gpus, total_memory_bytes, ip_address, node_id = None))]
    pub fn new(
        worker_id: String,
        ray_worker_handle: pyo3::Py<pyo3::PyAny>,
        num_cpus: f64,
        num_gpus: f64,
        total_memory_bytes: usize,
        ip_address: String,
        node_id: Option<String>,
    ) -> Self {
        Self {
            node_id: node_id.unwrap_or_else(|| worker_id.clone()),
            drain: DrainBarrier::default(),
            known_dead: false,
            worker_id: Arc::from(worker_id),
            ray_worker_handle: Arc::new(ray_worker_handle),
            num_cpus,
            num_gpus,
            total_memory_bytes,
            active_task_details: Default::default(),
            ip_address,
            last_task_finished_at: Instant::now(),
            state: ActorState::Ready,
        }
    }
}

impl RaySwordfishWorker {
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub fn mark_died(&mut self) {
        self.known_dead = true;
        self.active_task_details.clear();
        self.drain.data_queries.clear();
        self.drain.unknown = None;
    }

    pub fn can_retire(&self) -> bool {
        self.drain.can_retire(self.active_task_details.len())
    }

    pub fn accepts_task(&self, task: &SwordfishTask) -> bool {
        WorkerSnapshot::from(self).can_schedule_task(task)
    }

    pub fn usage(&self) -> serde_json::Value {
        serde_json::json!({
            "worker_instance_id": self.worker_id,
            "node_id": self.node_id,
            "state": self.drain_state(),
            "drain_epoch": self.drain.epoch,
            "active_tasks": self.active_task_details.len(),
            "logical_cpus": self.active_num_cpus(),
            "local_data_queries": self.drain.data_queries,
            "unknown": self.drain.unknown,
            "failure_confirmed": self.known_dead,
            "can_retire": self.can_retire(),
        })
    }

    /// Called outside the manager lock. The Python bridge waits for actor death.
    pub fn retire(&self, py: Python<'_>) -> PyResult<()> {
        self.ray_worker_handle.call_method0(py, "retire")?;
        Ok(())
    }
    pub fn total_memory_bytes(&self) -> usize {
        self.total_memory_bytes
    }

    pub fn set_state(&mut self, state: ActorState) {
        self.state = state;
    }

    pub fn mark_task_finished(&mut self, task_context: &TaskContext) {
        self.active_task_details.remove(task_context);
        self.last_task_finished_at = Instant::now();
        if self.active_task_details.is_empty() {
            self.set_state(ActorState::Idle);
        }
    }

    pub fn forget_query_tasks(&mut self, query_idx: crate::plan::QueryIdx) {
        self.active_task_details
            .retain(|task, _| task.query_idx != query_idx);
        if self.active_task_details.is_empty() {
            self.set_state(ActorState::Idle);
        }
    }

    /// Ask this worker's actor to forget `shuffle_ids`, returning the pending
    /// call so the caller can await the whole fan-out at once.
    pub fn unregister_shuffles(&self, py: Python<'_>, shuffle_ids: &[u64]) -> PyResult<Py<PyAny>> {
        self.ray_worker_handle.call_method1(
            py,
            pyo3::intern!(py, "unregister_shuffles"),
            (shuffle_ids.to_vec(),),
        )
    }

    pub fn cleanup_query(
        &self,
        py: Python<'_>,
        dirs: &[String],
        shuffle_ids: &[u64],
    ) -> PyResult<Py<PyAny>> {
        self.ray_worker_handle.call_method1(
            py,
            "cleanup_query",
            (dirs.to_vec(), shuffle_ids.to_vec()),
        )
    }

    pub fn submit_tasks(
        &mut self,
        tasks: Vec<SwordfishTask>,
        py: Python<'_>,
    ) -> DaftResult<Vec<RayTaskResultHandle>> {
        let mut task_handles = Vec::with_capacity(tasks.len());
        for task in tasks {
            let task_context = task.task_context();
            let task_details = TaskDetails::from(&task);

            if task.retains_local_data() {
                self.drain.data_queries.insert(task_context.query_idx);
            }
            // Submission can fail after Ray accepted the work. Record it first;
            // an ambiguous failure must continue to block retirement.
            self.active_task_details
                .insert(task_context.clone(), task_details);

            let ray_swordfish_task = RaySwordfishTask::new(task);
            let py_task_handle = self.ray_worker_handle.call_method1(
                py,
                pyo3::intern!(py, "submit_task"),
                (ray_swordfish_task,),
            )?;
            let coroutine = py_task_handle.call_method0(py, pyo3::intern!(py, "get_result"))?;

            if self.active_task_details.len() == 1 {
                self.set_state(ActorState::Busy);
            }

            let ray_task_result_handle = RayTaskResultHandle::new(
                task_context,
                py_task_handle,
                coroutine,
                self.worker_id.clone(),
                self.ip_address.clone(),
            );
            task_handles.push(ray_task_result_handle);
        }

        Ok(task_handles)
    }

    pub fn is_idle(&self) -> bool {
        self.active_task_details.is_empty()
    }

    pub fn idle_duration(&self, now: Instant) -> Duration {
        if self.is_idle() {
            now.saturating_duration_since(self.last_task_finished_at)
        } else {
            Duration::from_secs(0)
        }
    }

    #[allow(dead_code)]
    pub fn shutdown(&self, py: Python<'_>) {
        self.ray_worker_handle
            .call_method0(py, pyo3::intern!(py, "shutdown"))
            .expect("Failed to shutdown RaySwordfishWorker");
    }

    pub fn release(&mut self, py: Python<'_>) {
        let inflight = self.active_task_details.len();
        if !self.can_retire() {
            tracing::warn!(
                target: "ray_swordfish_worker",
                worker_id = %self.worker_id,
                inflight_tasks = inflight,
                "Cannot release worker while task, data, or unknown dependencies remain."
            );
            return;
        }

        self.set_state(ActorState::Releasing);
        self.shutdown(py);
        self.set_state(ActorState::Released);
    }
}

impl Worker for RaySwordfishWorker {
    type Task = SwordfishTask;
    type TaskResultHandle = RayTaskResultHandle;

    fn id(&self) -> &WorkerId {
        &self.worker_id
    }

    fn drain_state(&self) -> DrainState {
        if self.known_dead {
            DrainState::Failed
        } else {
            self.drain.status(self.active_task_details.len())
        }
    }

    fn total_num_cpus(&self) -> f64 {
        self.num_cpus
    }

    fn total_num_gpus(&self) -> f64 {
        self.num_gpus
    }

    fn active_num_cpus(&self) -> f64 {
        self.active_task_details
            .values()
            .map(|details| details.num_cpus())
            .sum()
    }

    fn active_num_gpus(&self) -> f64 {
        self.active_task_details
            .values()
            .map(|details| details.num_gpus())
            .sum()
    }

    fn active_task_details(&self) -> HashMap<TaskContext, TaskDetails> {
        self.active_task_details.clone()
    }
}
