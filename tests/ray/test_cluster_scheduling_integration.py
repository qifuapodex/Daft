"""Local Ray protocol integration; no physical nodes are removed."""

from __future__ import annotations

import os
import subprocess
import sys

import pytest


@pytest.mark.integration
def test_managed_flight_execution_and_retirement():
    result = subprocess.run(
        [sys.executable, __file__],
        env={**os.environ, "DAFT_RUNNER": "ray", "DAFT_PROGRESS_BAR": "0", "RAY_DISABLE_DASHBOARD": "1"},
        capture_output=True,
        check=False,
        text=True,
        timeout=180,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def _run():
    import tempfile
    import time
    from concurrent.futures import ThreadPoolExecutor

    import ray

    import daft
    from daft.runners import get_or_create_runner
    from daft.runners.cluster_scheduling import ClusterSchedulingConfig

    @ray.remote(num_cpus=0)
    class Ledger:
        def __init__(self):
            self.demand = {}
            self.finished = {}
            self.workers = set()
            self.revisions = {}

        def call(self, method, args):
            if method == "register_execution":
                return 1
            if method == "register_worker":
                execution, worker, node = args
                self.workers.add((execution, worker, node))
                return True
            execution, revision, value = args[0], args[-2], args[-1]
            assert revision > self.revisions.get(execution, 0)
            self.revisions[execution] = revision
            if method == "update_execution_demand":
                self.demand[execution] = value
            elif method == "finish_execution":
                self.demand.pop(execution)
                self.finished[execution] = value

        def snapshot(self):
            return self.demand, self.finished, self.workers

    class Client:
        def __init__(self, ledger):
            self.ledger = ledger

        def __getattr__(self, method):
            if method.startswith("_"):
                raise AttributeError(method)
            return lambda *args: ray.get(self.ledger.call.remote(method, args))

    ray.init(num_cpus=2, include_dashboard=False, object_store_memory=100 * 1024**2)
    try:
        ledger = Ledger.remote()

        def factory(identity):
            from daft.runners import flotilla

            def forbid_global(*args, **kwargs):
                raise AssertionError("managed execution wrote global autoscaling demand")

            flotilla.try_autoscale = forbid_global
            flotilla.clear_autoscaling_requests = forbid_global
            os.environ["DAFT_AUTOSCALING_DOWNSCALE_ENABLED"] = "true"
            return Client(ledger)

        daft.set_runner_ray(cluster_scheduling=ClusterSchedulingConfig(factory, "local-test", "job", 2))
        with tempfile.TemporaryDirectory() as shuffle_dir:
            daft.context.set_execution_config(
                shuffle_algorithm="flight_shuffle",
                flight_shuffle_dirs=[shuffle_dir],
                shuffle_aggregation_default_partitions=2,
            )
            # Warm up the driver's lazy runner before concurrent execution.
            cached = daft.from_pydict({"k": [0, 1], "v": [1, 1]}).select((daft.col("v") + 1).alias("v")).collect()

            def query(offset):
                return (
                    daft.from_pydict({"k": [0, 1] * 100, "v": list(range(offset, offset + 200))})
                    .repartition(2, "k")
                    .groupby("k")
                    .agg(daft.col("v").sum())
                    .sort("k")
                    .to_pydict()
                )

            with ThreadPoolExecutor(2) as pool:
                a, b = list(pool.map(query, [0, 100]))
            assert a == {"k": [0, 1], "v": [9900, 10000]}
            assert b == {"k": [0, 1], "v": [19900, 20000]}
            demand, finished, workers = ray.get(ledger.snapshot.remote())
            assert not demand
            assert len(finished) >= 3 and set(finished.values()) == {"CLEAN"}, finished
            assert len({worker for _, worker, _ in workers}) == len(workers)

            control = get_or_create_runner().flotilla_plan_runner.runner
            snapshots = ray.get(control.get_node_usage_snapshot.remote())
            assert len(snapshots) >= 3
            assert all(w["can_retire"] for s in snapshots.values() for w in s["workers"])
            nodes = {node for _, _, node in workers}
            for node in nodes:
                status = ray.get(control.prepare_node_drain.remote(node, 1))
                deadline = time.monotonic() + 15
                while status["state"] != "READY_TO_RETIRE" and time.monotonic() < deadline:
                    time.sleep(0.05)
                    status = ray.get(control.get_node_drain_status.remote(node, 1))
                assert status["state"] == "READY_TO_RETIRE", status
                ray.get(control.cancel_node_drain.remote(node, 1))
                ray.get(control.prepare_node_drain.remote(node, 2))
                status = ray.get(control.retire_node_workers.remote(node, 2))
                assert status["state"] == "RETIRED", status
                with pytest.raises(ray.exceptions.RayTaskError):
                    ray.get(control.cancel_node_drain.remote(node, 2))
            # Native Ray result ownership survives Daft actor retirement.
            assert cached.to_pydict() == {"v": [2, 2]}
    finally:
        ray.shutdown()


if __name__ == "__main__":
    _run()
