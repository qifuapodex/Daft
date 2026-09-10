from __future__ import annotations

import os
from functools import partial

import pytest
import ray

import daft
from daft import exceptions
from daft.io._generator import read_generator
from daft.recordbatch.recordbatch import RecordBatch
from daft.runners import get_or_create_runner
from tests.conftest import get_tests_daft_runner_name

pytestmark = pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="Requires the Ray task retry loop")


@pytest.fixture
def generator_calls():
    get_or_create_runner()

    @ray.remote(num_cpus=0)
    class Counter:
        def __init__(self):
            self.calls = {}

        def record(self, partition):
            self.calls[partition] = self.calls.get(partition, 0) + 1
            return self.calls[partition]

        def get(self):
            return self.calls

    counter = Counter.remote()
    try:
        yield counter
    finally:
        ray.kill(counter)


@pytest.mark.parametrize(
    "exc_type",
    [
        exceptions.ConnectTimeoutError,
        exceptions.ReadTimeoutError,
        exceptions.ByteStreamError,
        exceptions.SocketError,
        exceptions.ThrottleError,
        exceptions.MiscTransientError,
    ],
)
def test_read_generator_retries_transient_errors(generator_calls, exc_type):
    batch = RecordBatch.from_pydict({"x": [1, 2]})

    def generate():
        attempt = ray.get(generator_calls.record.remote(0))
        if attempt == 1:
            raise exc_type("retry this generator")
        yield batch

    assert read_generator(iter([generate]), batch.schema()).to_pydict() == {"x": [1, 2]}
    assert ray.get(generator_calls.get.remote()) == {0: 2}


@pytest.mark.parametrize("repeat", range(5))
def test_read_generator_retries_after_batch_without_duplicates(generator_calls, repeat):
    batch = RecordBatch.from_pydict({"x": [0]})

    def generate(partition):
        attempt = ray.get(generator_calls.record.remote(partition))
        yield RecordBatch.from_pydict({"x": [partition * 2]})
        if partition == 0 and attempt == 1:
            raise exceptions.SocketError("failure after a batch")
        yield RecordBatch.from_pydict({"x": [partition * 2 + 1]})

    # Keep each generator in its own task. Concurrent tasks may share a worker
    # pipeline, so its failure can also interrupt the other partition.
    with daft.execution_config_ctx(enable_scan_task_split_and_merge=False):
        result = read_generator((partial(generate, i) for i in range(2)), batch.schema()).to_pydict()

    calls = ray.get(generator_calls.get.remote())
    assert calls[0] == 2
    assert calls[1] in (1, 2)
    assert sorted(result["x"]) == [0, 1, 2, 3], calls


def test_read_generator_does_not_retry_permanent_errors(generator_calls):
    batch = RecordBatch.from_pydict({"x": [1]})

    def generate():
        ray.get(generator_calls.record.remote(0))
        raise ValueError("permanent generator failure")
        yield batch

    with pytest.raises(ValueError, match="permanent generator failure"):
        read_generator(iter([generate]), batch.schema()).to_pydict()

    assert ray.get(generator_calls.get.remote()) == {0: 1}


def test_read_generator_exhausts_transient_retry_budget(generator_calls):
    batch = RecordBatch.from_pydict({"x": [1]})

    def generate():
        ray.get(generator_calls.record.remote(0))
        raise exceptions.SocketError("persistent generator failure")
        yield batch

    with pytest.raises(exceptions.SocketError, match="persistent generator failure"):
        read_generator(iter([generate]), batch.schema()).to_pydict()

    retries = int(os.environ.get("DAFT_FLOTILLA_TASK_MAX_TRANSIENT_RETRIES", "3"))
    assert ray.get(generator_calls.get.remote()) == {0: retries + 1}
