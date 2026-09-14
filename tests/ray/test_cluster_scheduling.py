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


def test_strict_cleanup_preserves_shared_data_after_unacknowledged_worker(tmp_path, monkeypatch):
    import shutil

    from daft.runners import flotilla
    from daft.runners.flotilla import await_flight_shuffle_cleanup

    scheduled = []

    class Delete:
        def options(self, **kwargs):
            scheduled.append(kwargs["scheduling_strategy"].node_id)
            return self

        async def remote(self, dirs):
            for directory in dirs:
                if __import__("os").path.exists(directory):
                    shutil.rmtree(directory)

    node_id = "1" * 56
    monkeypatch.setattr(
        flotilla.ray,
        "nodes",
        lambda: [
            {"NodeID": "2" * 56, "Alive": True, "Resources": {"CPU": 0}},
            {"NodeID": node_id, "Alive": True, "Resources": {"CPU": 2}},
        ],
    )
    monkeypatch.setattr(flotilla, "_remove_shuffle_dirs_strict", Delete())

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
        assert scheduled == [node_id]
        # A producer actor may be dead while its node and local files survive.
        orphan = tmp_path / "orphaned-local-shuffle"
        orphan.mkdir()
        (orphan / "map").write_bytes(b"old output")
        await await_flight_shuffle_cleanup([], [], [str(orphan)], [node_id])
        assert not orphan.exists()
        assert scheduled == [node_id, node_id]

    asyncio.run(run())


def _control(config_value):
    from daft.runners.flotilla import RemoteFlotillaRunner

    cls = RemoteFlotillaRunner.__ray_metadata__.modified_class
    control = cls.__new__(cls)
    control.cluster_scheduling = config_value
    control.worker_startup_timeout = 120
    control.managed_executions = {}
    control.reporting_tasks = {}
    control.node_drains = {}
    control.cancelled_drain_epochs = {}
    control.retiring_nodes = set()
    control.retired_nodes = set()
    control.retirement_lock = asyncio.Lock()
    control.curr_plans = {}
    control.curr_result_gens = {}
    control.plan_runner = None
    return control


class _ControlRunner:
    def __init__(self, closed=True, workers=None):
        self.closed = closed
        self.workers = workers or []
        self.retire_calls = 0

    def get_node_usage_snapshot(self):
        import json

        return json.dumps({"closed": self.closed, "discovery_pending": not self.closed, "workers": self.workers})

    def retire_node_workers(self, node, epoch):
        import time

        time.sleep(0.02)
        self.retire_calls += 1

    def retire_completed_workers(self):
        for worker in self.workers:
            worker["state"] = "RETIRED"

    def stop_execution(self):
        self.closed = True

    def run_plan(self, *args):
        raise ValueError("injected translation error")


def test_unrelated_failed_session_does_not_block_node_retirement():
    runtime = FakeRuntime()
    session = ManagedExecution(config(runtime))
    session.open()
    session.register_worker("worker-a", "node-a")
    session.invalidate("lost control acknowledgement")
    control = _control(config(runtime))
    control.managed_executions["a"] = (session, _ControlRunner())
    control.node_drains = {"node-a": 1, "node-b": 1}
    assert control.get_node_drain_status("node-a", 1)["state"] == "UNKNOWN"
    assert control.get_node_drain_status("node-b", 1)["state"] == "READY_TO_RETIRE"
    # Unfinished discovery still blocks even a node not in the installed set.
    control.managed_executions["a"][1].closed = False
    assert control.get_node_drain_status("node-b", 1)["state"] == "DRAINING"


def test_duplicate_retirement_is_serialized():
    async def run():
        runtime = FakeRuntime()
        session = ManagedExecution(config(runtime))
        runner = _ControlRunner()
        control = _control(config(runtime))
        control.managed_executions["a"] = (session, runner)
        control.node_drains["node"] = 1
        results = await asyncio.gather(control.retire_node_workers("node", 1), control.retire_node_workers("node", 1))
        assert [r["state"] for r in results] == ["RETIRED", "RETIRED"]
        assert runner.retire_calls == 1

    asyncio.run(run())


def test_completed_execution_is_removed_only_after_terminal_report():
    class Runtime(FakeRuntime):
        final_ack = False
        final_allowed = True

        def report_node_usage(self, execution, node, revision, usage):
            super().report_node_usage(execution, node, revision, usage)
            if all(w["state"] == "RETIRED" for w in usage["workers"]):
                if not self.final_allowed:
                    raise ConnectionError("terminal report lost")
                self.final_ack = True

    async def run(allow_ack):
        runtime = Runtime()
        runtime.final_allowed = allow_ack
        cfg = replace(config(runtime), report_interval_seconds=0.01, report_timeout_seconds=1)
        session = ManagedExecution(cfg)
        session.open()
        session.register_worker("worker", "node")
        session.finish("CLEAN")
        worker = {
            "worker_instance_id": "worker",
            "node_id": "node",
            "state": "ACTIVE",
            "active_tasks": 0,
            "logical_cpus": 0,
            "local_data_queries": [],
            "unknown": None,
            "can_retire": True,
        }
        control = _control(cfg)
        control.managed_executions["a"] = (session, _ControlRunner(workers=[worker]))
        await asyncio.wait_for(control._report_usage("a"), 2)
        assert runtime.final_ack == allow_ack
        assert ("a" not in control.managed_executions) == allow_ack

    asyncio.run(run(True))
    asyncio.run(run(False))


def test_startup_failure_closes_execution_and_factory_runs_off_event_loop(monkeypatch):
    import threading
    from types import SimpleNamespace

    from daft.runners import flotilla

    runtime = FakeRuntime()

    def factory(identity):
        assert threading.current_thread() is not threading.main_thread()
        return runtime

    cfg = replace(config(runtime), client_factory=factory, report_interval_seconds=0.01, report_timeout_seconds=1)
    runner = _ControlRunner(closed=False)
    monkeypatch.setattr(flotilla, "DistributedPhysicalPlanRunner", lambda *args, **kwargs: runner)
    monkeypatch.setattr(flotilla.ray, "get_runtime_context", lambda: SimpleNamespace(current_actor=None))

    async def run():
        control = _control(cfg)
        with pytest.raises(ValueError, match="translation"):
            await control.run_plan(SimpleNamespace(idx=lambda: "query"), {})
        assert runner.closed
        assert not runtime.demand
        await asyncio.gather(*list(control.reporting_tasks.values()))
        assert not control.managed_executions
        control.node_drains["node"] = 1
        assert control.get_node_drain_status("node", 1)["state"] == "READY_TO_RETIRE"

    asyncio.run(run())


def test_negotiated_open_failure_withdraws_only_its_own_demand():
    class Runtime(FakeRuntime):
        def update_execution_demand(self, execution, revision, total):
            super().update_execution_demand(execution, revision, total)
            raise ConnectionError("demand acknowledgement lost")

    runtime = Runtime()
    runtime.demand["other"] = 2000
    session = ManagedExecution(config(runtime))
    with pytest.raises(ConnectionError):
        session.open()
    session.abort_open()
    assert runtime.demand == {"other": 2000}
    assert session.failure is not None
    assert session.finished


def test_managed_configuration_rejects_native_noop(monkeypatch):
    from types import SimpleNamespace

    import daft.runners

    monkeypatch.setattr(daft.runners, "_get_runner", lambda: SimpleNamespace(name="native"))
    with pytest.raises(RuntimeError, match="requires a Ray runner"):
        daft.runners.set_runner_ray(noop_if_initialized=True, cluster_scheduling=config(FakeRuntime()))


def test_rejected_node_is_skipped_until_backoff_expires(monkeypatch):
    from daft.runners import cluster_scheduling, flotilla

    runtime = FakeRuntime()
    session = ManagedExecution(config(runtime))
    session.open()
    runtime.draining.add("node")
    assert not session.register_worker("worker", "node")
    skipped = []
    monkeypatch.setattr(flotilla, "start_ray_workers", lambda nodes, timeout, **kwargs: skipped.append(nodes) or [])
    session.start_workers([], 120)
    assert "node" in skipped[-1]
    deadline = session._rejected_nodes["node"]
    monkeypatch.setattr(cluster_scheduling.time, "monotonic", lambda: deadline + 1)
    session.start_workers([], 120)
    assert "node" not in skipped[-1]
