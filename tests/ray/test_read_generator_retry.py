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


def test_read_generator_retries_after_batch_without_duplicates(generator_calls):
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


@pytest.mark.parametrize("shuffle_algorithm", ["map_reduce", "flight_shuffle"])
@pytest.mark.parametrize("source,target", [(2, 3), (8, 9), (8, 12), (8, 15)])
@pytest.mark.parametrize("fail_extra_split", [False, True])
def test_into_partitions_split_count_after_retry(
    generator_calls, tmp_path, shuffle_algorithm, source, target, fail_extra_split
):
    # Exercise retries for both split factors: the first input gets an extra
    # output partition, while the last input uses the base split factor.
    failing_partition = 0 if fail_extra_split else source - 1
    schema = RecordBatch.from_pydict({"x": [0]}).schema()

    def generate(partition):
        attempt = ray.get(generator_calls.record.remote(partition))
        start = partition * 100
        yield RecordBatch.from_pydict({"x": list(range(start, start + 50))})
        if partition == failing_partition and attempt == 1:
            raise exceptions.SocketError("retry an input with a different split factor")
        yield RecordBatch.from_pydict({"x": list(range(start + 50, start + 100))})

    with daft.execution_config_ctx(
        shuffle_algorithm=shuffle_algorithm,
        flight_shuffle_dirs=[str(tmp_path)],
        enable_scan_task_split_and_merge=False,
    ):
        df = read_generator((partial(generate, i) for i in range(source)), schema).into_partitions(target)
        parts = list(df.iter_partitions())
        values = [v for p in ray.get(parts) for v in p.to_pydict()["x"]]

    calls = ray.get(generator_calls.get.remote())
    assert calls[failing_partition] >= 2, calls
    assert sorted(values) == list(range(source * 100)), calls
    assert len(parts) == target, calls


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
