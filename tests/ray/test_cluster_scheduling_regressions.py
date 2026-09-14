"""Exercise the real Rust scheduler with deterministic, process-local worker fakes."""

from __future__ import annotations

import os
import subprocess
import sys

import pytest


@pytest.mark.parametrize("case", ["unavailable", "cancel", "wrap", "refresh", "submit", "standalone_cleanup"])
def test_scheduler_lifecycle_regressions(case):
    result = subprocess.run(
        [sys.executable, __file__, case],
        env={**os.environ, "DAFT_RUNNER": "native", "DAFT_PROGRESS_BAR": "0", "RAY_DISABLE_DASHBOARD": "1"},
        check=False,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def _run(case):
    import asyncio
    import json
    import tempfile
    import threading
    import time
    import uuid
    from pathlib import Path

    import daft
    from daft.daft import DistributedPhysicalPlan, DistributedPhysicalPlanRunner, RaySwordfishWorker, RayTaskResult
    from daft.event_loop import set_event_loop
    from daft.runners import flotilla

    cleanup_calls = []
    discovery_done = threading.Event()

    async def cleanup(*args):
        cleanup_calls.append(args)

    flotilla.clear_flight_shuffle_dirs_on_all_nodes = cleanup
    flotilla.await_flight_shuffle_cleanup = cleanup
    flotilla.try_autoscale = lambda *args: None

    class Handle:
        calls = 0
        cancelled = 0
        retired = 0
        cancelling = case == "cancel"

        def submit_task(self, task):
            self.calls += 1
            if case == "submit" and self.calls == 1:
                raise ValueError("injected submission failure")
            return self

        async def get_result(self):
            if case == "unavailable" and self.calls == 1:
                return RayTaskResult.worker_unavailable()
            if self.cancelling:
                await asyncio.Future()
            if case == "refresh":
                await asyncio.sleep(7)
            raise ValueError("injected terminal failure")

        def cancel(self):
            self.cancelled += 1

        def retire(self):
            self.retired += 1

        def cleanup_query(self, *args):
            raise AssertionError("standalone cleanup must not wait for process-wide IO counters")

        def unregister_shuffles(self, *args):
            raise ValueError("dead actor; its node's disk must still be cleaned")

    handle = Handle()

    class Session:
        def check_health(self):
            pass

        def start_workers(self, skip, timeout):
            if skip:
                if case == "refresh":
                    time.sleep(4)
                    discovery_done.set()
                    return [RaySwordfishWorker("late", handle, 2.0, 0.0, 1024**3, "localhost", "late-node")]
                return []
            return [RaySwordfishWorker("worker", handle, 2.0, 0.0, 1024**3, "localhost", "node")]

    session = Session()
    flotilla.start_ray_workers = session.start_workers

    async def run(path):
        set_event_loop(asyncio.get_running_loop())
        runner = DistributedPhysicalPlanRunner(cluster_session=session if case == "refresh" else None)
        cfg = daft.context.get_context().daft_execution_config
        df = daft.read_csv(path).select(daft.col("a") + 1)
        if case == "standalone_cleanup":
            daft.context.set_execution_config(
                shuffle_algorithm="flight_shuffle", flight_shuffle_dirs=[str(Path(path).parent)]
            )
            cfg = daft.context.get_context().daft_execution_config
            df = df.repartition(2, "a")
        builder = df._builder.optimize(cfg)._builder

        def plan():
            return DistributedPhysicalPlan.from_logical_plan_builder(builder, str(uuid.uuid4()), cfg)

        async def expect_failure(stream):
            with pytest.raises(ValueError, match="injected"):
                await asyncio.wait_for(stream.__anext__(), 20)
            await stream.close()

        first_plan = plan()
        stream = runner.run_plan(first_plan, {})
        if case == "cancel":
            pending = asyncio.ensure_future(stream.__anext__())
            while handle.calls == 0:
                await asyncio.sleep(0.01)
            await asyncio.wait_for(stream.close(), 10)
            await asyncio.gather(pending, return_exceptions=True)
            assert handle.cancelled == 1
            handle.cancelling = False
        else:
            await expect_failure(stream)
        if case == "refresh":
            assert await asyncio.to_thread(discovery_done.wait, 10)
            snapshot = json.loads(runner.get_node_usage_snapshot())
            assert snapshot["closed"] and not snapshot["discovery_pending"]
            assert {w["node_id"] for w in snapshot["workers"]} == {"node", "late-node"}
            runner.prepare_node_drain("node", 1)
            runner.retire_node_workers("node", 1)
            runner.retire_completed_workers()
            assert all(w["state"] == "RETIRED" for w in json.loads(runner.get_node_usage_snapshot())["workers"])
            return
        snapshot = json.loads(runner.get_node_usage_snapshot())
        assert all(
            w["active_tasks"] == 0 and w["unknown"] is None and not w["local_data_queries"] for w in snapshot["workers"]
        )
        if case == "standalone_cleanup":
            assert any(args[0] for args in cleanup_calls)
            return
        if case == "wrap":
            for _ in range(65536):
                next_plan = plan()
            assert next_plan.idx() != first_plan.idx()
        else:
            next_plan = plan()
        previous = handle.calls
        await expect_failure(runner.run_plan(next_plan, {}))
        assert handle.calls == previous + 1
        assert cleanup_calls

    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "input.csv"
        path.write_text("a\n1\n2\n")
        asyncio.run(run(str(path)))


if __name__ == "__main__":
    _run(sys.argv[1])
