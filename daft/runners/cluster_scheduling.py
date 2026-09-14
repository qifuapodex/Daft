"""Experimental, versioned interface for an externally managed shared Ray cluster.

The client runs on the head, must be thread safe, and must acknowledge worker
protection before returning from register_worker. It owns neither Daft tasks nor
their data. A runtime must retain protection until retire_node_workers succeeds;
missing reports are UNKNOWN, never permission to reclaim a node.
"""

from __future__ import annotations

import math
import threading
import time
import uuid
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from collections.abc import Callable

PROTOCOL_VERSION = 1


@dataclass(frozen=True)
class ExecutionIdentity:
    cluster_instance_id: str
    job_attempt_id: str
    execution_id: str


class ClusterSchedulingClient(Protocol):
    """All methods acknowledge success by returning; exceptions fail closed.

    register_worker must atomically join the node's participant set and establish
    Ray-visible protection, or reject admission if the node is draining.
    Revisions are monotonically increasing within one execution. No method may
    replace/clear a cluster-global request on behalf of just this execution.
    """

    def register_execution(self, identity: dict[str, str], capabilities: dict[str, Any]) -> int:
        """Register identity and return the negotiated protocol version."""
        ...

    def update_execution_demand(self, execution_id: str, revision: int, requested_total_cpus: float) -> None: ...

    def register_worker(self, execution_id: str, worker_instance_id: str, node_id: str) -> bool:
        """Return exactly True once this worker's node protection is established."""
        ...

    def report_node_usage(self, execution_id: str, node_id: str, revision: int, usage: dict[str, Any]) -> None: ...

    def finish_execution(self, execution_id: str, revision: int, cleanup_status: str) -> None:
        """Withdraw only this execution's demand; retain unresolved node protection."""
        ...


@dataclass(frozen=True)
class ClusterSchedulingConfig:
    """Serializable configuration, supplied before the first Ray query.

    client_factory is called on the head once per execution. requested_cpus is
    that execution's fixed total demand, never the observed cluster size. A
    runtime also accounting for a job declaration must deduplicate by job ID.
    """

    client_factory: Callable[[ExecutionIdentity], ClusterSchedulingClient]
    cluster_instance_id: str
    job_attempt_id: str
    requested_cpus: float = 0
    report_interval_seconds: float = 2
    report_timeout_seconds: float = 30

    def __post_init__(self) -> None:
        if not self.cluster_instance_id or not self.job_attempt_id:
            raise ValueError("cluster_instance_id and job_attempt_id must be nonempty")
        if not math.isfinite(self.requested_cpus) or self.requested_cpus < 0:
            raise ValueError("requested_cpus must be a finite nonnegative total")
        if not math.isfinite(self.report_interval_seconds) or self.report_interval_seconds <= 0:
            raise ValueError("report_interval_seconds must be positive and finite")
        if (
            not math.isfinite(self.report_timeout_seconds)
            or self.report_timeout_seconds <= self.report_interval_seconds
        ):
            raise ValueError("report_timeout_seconds must exceed the report interval")


class ManagedExecution:
    """Head-local bridge; scheduler health checks perform no network I/O."""

    def __init__(self, config: ClusterSchedulingConfig, control_actor: Any = None) -> None:
        self.config = config
        self.control_actor = control_actor
        self.identity = ExecutionIdentity(config.cluster_instance_id, config.job_attempt_id, uuid.uuid4().hex)
        self.client = config.client_factory(self.identity)
        self._lock = threading.Lock()
        self._failure: str | None = None
        self._revision = 0
        self._finished = False
        self._opened = False
        self._has_workers = False
        self._last_ack = time.monotonic()

    @property
    def failure(self) -> str | None:
        if (
            self._opened
            and self._has_workers
            and time.monotonic() - self._last_ack > self.config.report_timeout_seconds
        ):
            self._failure = "control acknowledgements expired"
        return self._failure

    @property
    def finished(self) -> bool:
        return self._finished

    def complete_retirement(self) -> None:
        # Called after the final report acknowledged every actor's termination.
        self._has_workers = False

    def invalidate(self, reason: str) -> None:
        self._failure = reason

    def check_health(self) -> None:
        failure = self.failure
        if failure is not None:
            raise RuntimeError(f"Managed cluster scheduling unavailable: {failure}")

    def _call(self, method: str, *args: Any) -> Any:
        self.check_health()
        try:
            result = getattr(self.client, method)(*args)
            self._last_ack = time.monotonic()
            return result
        except Exception as error:
            self._failure = str(error)
            raise

    def _next_revision(self) -> int:
        self._revision += 1
        return self._revision

    def open(self) -> None:
        with self._lock:
            version = self._call(
                "register_execution",
                asdict(self.identity),
                {
                    "protocol_version": PROTOCOL_VERSION,
                    "control_actor": self.control_actor,
                    "capacity_mode": "fixed",
                    "data_retention": "query_cleanup",
                    "worker_retirement": True,
                },
            )
            if type(version) is not int or version != PROTOCOL_VERSION:
                self._failure = f"incompatible protocol version: {version!r}"
                self.check_health()
            self._opened = True
            self._call(
                "update_execution_demand",
                self.identity.execution_id,
                self._next_revision(),
                self.config.requested_cpus,
            )

    def register_worker(self, worker_instance_id: str, node_id: str) -> bool:
        with self._lock:
            if self._finished:
                return False
            accepted = self._call("register_worker", self.identity.execution_id, worker_instance_id, node_id)
            if type(accepted) is not bool:
                self._failure = "register_worker did not acknowledge protection with a boolean"
                self.check_health()
            if accepted:
                self._has_workers = True
            return accepted

    def start_workers(self, existing_worker_ids: list[str], worker_startup_timeout: int) -> list[Any]:
        from daft.runners.flotilla import start_ray_workers

        self.check_health()
        return start_ray_workers(existing_worker_ids, worker_startup_timeout, session=self)

    def report(self, snapshot: dict[str, Any]) -> None:
        with self._lock:
            by_node: dict[str, list[dict[str, Any]]] = {}
            for worker in snapshot["workers"]:
                by_node.setdefault(worker["node_id"], []).append(worker)
            for node_id, workers in by_node.items():
                self._call(
                    "report_node_usage",
                    self.identity.execution_id,
                    node_id,
                    self._next_revision(),
                    {
                        "workers": workers,
                        "active_tasks": sum(w["active_tasks"] for w in workers),
                        "logical_cpus": sum(w["logical_cpus"] for w in workers),
                        "local_data_dependencies": sum(len(w["local_data_queries"]) for w in workers),
                        # Includes publication/reconstruction and reads: task holds
                        # precede submission, data holds precede output publication.
                        "inflight_operations": sum(w["active_tasks"] for w in workers),
                        "unknown": [w["unknown"] for w in workers if w["unknown"]],
                    },
                )

    def finish(self, cleanup_status: str) -> None:
        with self._lock:
            if self._finished:
                return
            self._call("finish_execution", self.identity.execution_id, self._next_revision(), cleanup_status)
            self._finished = True
