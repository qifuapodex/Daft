from __future__ import annotations

import os
import subprocess
import sys

import pytest

from tests.conftest import get_tests_daft_runner_name

pytestmark = pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="Requires the Ray task retry loop")


DRIVER = r"""
import json
import os
from functools import partial

import ray
from ray.cluster_utils import Cluster

import daft
from daft.exceptions import SocketError
from daft.io._generator import read_generator
from daft.recordbatch.recordbatch import RecordBatch


@ray.remote(num_cpus=0)
class Attempts:
    def __init__(self):
        self.calls = {}
        self.nodes = {}

    def record(self, partition, node):
        self.calls[partition] = self.calls.get(partition, 0) + 1
        self.nodes[node] = self.nodes.get(node, 0) + 1
        return self.calls[partition]

    def get(self):
        return self.calls, self.nodes


cluster = Cluster()
try:
    for _ in range(2):
        cluster.add_node(num_cpus=int(os.environ["DAFT_TEST_GENERATOR_CPUS"]),
                         include_dashboard=False, object_store_memory=128 * 1024 * 1024)
    ray.init(address=cluster.address)
    daft.set_runner_ray()
    schema = RecordBatch.from_pydict({"id": [0]}).schema()
    for trial in range(10):
        actor = Attempts.remote()
        def generate(partition):
            attempt = ray.get(actor.record.remote(partition, ray.get_runtime_context().get_node_id()))
            start = partition * 128
            yield RecordBatch.from_pydict({"id": list(range(start, start + 64))})
            if partition % 16 == 0 and attempt == 1:
                raise SocketError(f"generator={partition} attempt={attempt}")
            yield RecordBatch.from_pydict({"id": list(range(start + 64, start + 128))})

        try:
            with daft.execution_config_ctx(
                enable_scan_task_split_and_merge=False,
                scantask_max_parallel=int(os.environ["DAFT_TEST_GENERATOR_SCAN_PARALLELISM"]),
            ):
                ids = read_generator((partial(generate, p) for p in range(64)), schema).to_pydict()["id"]
            calls, nodes = ray.get(actor.get.remote())
            assert sorted(ids) == list(range(8192)), calls
            assert all(calls[p] == 2 for p in range(0, 64, 16)), calls
            assert all(count <= 2 for count in calls.values()), calls
            print(json.dumps({"trial": trial, "calls": calls, "nodes": nodes}), flush=True)
        except Exception:
            print("FAILED", trial, ray.get(actor.get.remote()), flush=True)
            assert daft.from_pydict({"v": [1, 2, 3]}).sum("v").to_pydict() == {"v": [6]}
            raise
        finally:
            ray.kill(actor)

    # Isolation must not replenish the budget or swallow the original error.
    actor = Attempts.remote()
    try:
        def persistent_failure():
            ray.get(actor.record.remote(0, ray.get_runtime_context().get_node_id()))
            yield RecordBatch.from_pydict({"id": [0]})
            raise SocketError("persistent failure after partial output")

        try:
            read_generator(iter([persistent_failure]), schema).to_pydict()
        except SocketError as error:
            assert "persistent failure after partial output" in str(error)
        else:
            raise AssertionError("persistent failure was swallowed")
        calls, nodes = ray.get(actor.get.remote())
        assert calls == {0: 3}, calls
        print("BUDGET_EXHAUSTED", calls, nodes, flush=True)
    finally:
        ray.kill(actor)
finally:
    ray.shutdown()
    cluster.shutdown()
"""


@pytest.mark.parametrize("num_cpus,scan_parallelism", [(8, 0), (32, 1)], ids=["concurrent", "queued"])
def test_generator_failures_do_not_cascade_across_retries(tmp_path, num_cpus, scan_parallelism):
    # Use a fresh driver and workers so every dispatcher reads the same budget.
    # A serial scan with more task slots also exercises failures of queued inputs.
    env = dict(os.environ)
    env.update(
        DAFT_FLOTILLA_TASK_MAX_TRANSIENT_RETRIES="2",
        DAFT_RUNNER="ray",
        DAFT_PROGRESS_BAR="0",
        RAY_ADDRESS="local",
        DAFT_TEST_GENERATOR_CPUS=str(num_cpus),
        DAFT_TEST_GENERATOR_SCAN_PARALLELISM=str(scan_parallelism),
        DAFT_TRACE="daft_local_execution::run=debug,DaftFlotillaDispatcher=debug",
        RAY_DEDUP_LOGS="0",
    )
    with (tmp_path / "driver.log").open("w+") as log:
        result = subprocess.run(
            [sys.executable, "-c", DRIVER], env=env, stdout=log, stderr=log, timeout=600, check=False
        )
        log.seek(0)
        assert result.returncode == 0, log.read()
