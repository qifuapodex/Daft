from __future__ import annotations

import asyncio
from dataclasses import replace

import pytest

from daft.runners.cluster_scheduling import ClusterSchedulingConfig, ManagedExecution


class FakeRuntime:
    """Small ledger with independent capacity and node-participant records."""

    def __init__(self):
        self.executions = {}
        self.demand = {}
        self.workers = {}
        self.revisions = {}
        self.finished = {}
        self.draining = set()
        self.fail = False

    def __call__(self, identity):
        return self

    def register_execution(self, identity, capabilities):
        assert capabilities["capacity_mode"] == "fixed"
        self.executions[identity["execution_id"]] = identity
        return 1

    def _revision(self, execution_id, revision):
        if self.fail:
            raise ConnectionError("coordinator unavailable")
        assert revision > self.revisions.get(execution_id, 0)
        self.revisions[execution_id] = revision

    def update_execution_demand(self, execution_id, revision, requested_total_cpus):
        self._revision(execution_id, revision)
        self.demand[execution_id] = requested_total_cpus

    def register_worker(self, execution_id, worker_instance_id, node_id):
        if self.fail:
            raise ConnectionError("node protection unavailable")
        if node_id in self.draining:
            return False
        self.workers[execution_id, worker_instance_id] = node_id
        return True

    def report_node_usage(self, execution_id, node_id, revision, usage):
        self._revision(execution_id, revision)

    def finish_execution(self, execution_id, revision, cleanup_status):
        self._revision(execution_id, revision)
        self.demand.pop(execution_id)
        self.finished[execution_id] = cleanup_status
        # Demand withdrawal does not imply worker retirement.


def config(runtime, cpus=1000):
    return ClusterSchedulingConfig(runtime, "cluster-instance", "job-attempt", requested_cpus=cpus)


def test_capacity_and_protection_are_independent():
    runtime = FakeRuntime()
    sessions = [
        ManagedExecution(replace(config(runtime, cpus), job_attempt_id=str(cpus))) for cpus in (1000, 2000, 3000)
    ]
    for session, total in zip(sessions, (1000, 3000, 6000)):
        session.open()
        assert sum(runtime.demand.values()) == total
        assert session.register_worker("worker-" + session.identity.execution_id, "shared-node")
    sessions[0].finish("UNKNOWN")
    sessions[0].finish("UNKNOWN")
    assert sum(runtime.demand.values()) == 5000
    assert len(runtime.workers) == 3
    sessions[2].finish("CLEAN")
    assert sum(runtime.demand.values()) == 2000
    sessions[1].finish("CLEAN")
    assert sum(runtime.demand.values()) == 0
    assert len(runtime.workers) == 3


def test_concurrent_queries_have_distinct_execution_and_revision_scopes():
    runtime = FakeRuntime()
    a, b = ManagedExecution(config(runtime)), ManagedExecution(config(runtime))
    a.open()
    b.open()
    assert a.identity.execution_id != b.identity.execution_id
    assert a.register_worker("a", "node")
    assert b.register_worker("b", "node")
    a.finish("CLEAN")
    assert b.identity.execution_id in runtime.demand
    assert runtime.workers[b.identity.execution_id, "b"] == "node"


def test_client_failure_is_latched_and_never_clears_another_demand():
    runtime = FakeRuntime()
    session = ManagedExecution(config(runtime))
    session.open()
    runtime.fail = True
    with pytest.raises(ConnectionError):
        session.finish("CLEAN")
    runtime.fail = False
    with pytest.raises(RuntimeError, match="unavailable"):
        session.register_worker("worker", "node")
    assert sum(runtime.demand.values()) == 1000


def test_admission_rejects_draining_nodes_and_requires_explicit_ack():
    runtime = FakeRuntime()
    session = ManagedExecution(config(runtime))
    session.open()
    runtime.draining.add("node")
    assert not session.register_worker("worker", "node")
    assert not runtime.workers
    runtime.register_worker = lambda *args: None
    with pytest.raises(RuntimeError, match="acknowledge"):
        session.register_worker("worker", "node")


def test_protocol_mismatch_prevents_capacity_and_worker_admission():
    runtime = FakeRuntime()
    runtime.register_execution = lambda *args: 2
    session = ManagedExecution(config(runtime))
    with pytest.raises(RuntimeError, match="protocol"):
        session.open()
    assert not runtime.demand
    with pytest.raises(RuntimeError, match="protocol"):
        session.register_worker("worker", "node")


def test_stale_control_acknowledgements_block_dispatch(monkeypatch):
    runtime = FakeRuntime()
    session = ManagedExecution(config(runtime))
    session.open()
    assert session.register_worker("worker", "node")
    monkeypatch.setattr("daft.runners.cluster_scheduling.time.monotonic", lambda: session._last_ack + 31)
    with pytest.raises(RuntimeError, match="expired"):
        session.check_health()
    assert runtime.workers


@pytest.mark.parametrize("cpus", [-1, float("inf"), float("nan")])
def test_invalid_total_demand(cpus):
    with pytest.raises(ValueError):
        config(FakeRuntime(), cpus)


def test_strict_cleanup_preserves_shared_data_after_unacknowledged_worker(tmp_path):
    from daft.runners.flotilla import await_flight_shuffle_cleanup

    async def run():
        shared = tmp_path / "shared"
        shared.mkdir()
        data = shared / "map"
        data.write_bytes(b"still needed")
        failed = asyncio.get_running_loop().create_future()
        failed.set_exception(ConnectionError("worker cleanup unacknowledged"))
        with pytest.raises(RuntimeError, match="not acknowledged"):
            await await_flight_shuffle_cleanup([failed], [str(shared)])
        assert data.read_bytes() == b"still needed"
        await await_flight_shuffle_cleanup([], [str(shared)])
        assert not shared.exists()

    asyncio.run(run())
