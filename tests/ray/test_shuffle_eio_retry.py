from __future__ import annotations

import os
import pickle
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

import daft
from daft.context import get_context
from daft.exceptions import DaftShuffleIoError
from tests.conftest import get_tests_daft_runner_name


def test_shuffle_eio_config_validation_and_pickle():
    config = get_context().daft_execution_config
    assert config.flight_shuffle_eio_max_retries == 3
    assert config.flight_shuffle_eio_local_max_retries == 6
    assert config.flight_shuffle_eio_local_initial_backoff_ms == 1000
    assert config.flight_shuffle_eio_local_max_backoff_ms == 32000
    with daft.execution_config_ctx(
        flight_shuffle_eio_local_max_retries=2,
        flight_shuffle_eio_local_initial_backoff_ms=7,
        flight_shuffle_eio_local_max_backoff_ms=50,
        flight_shuffle_eio_max_retries=7,
        flight_shuffle_eio_initial_backoff_ms=5,
        flight_shuffle_eio_max_backoff_ms=40,
    ):
        restored = pickle.loads(pickle.dumps(get_context().daft_execution_config))
        assert restored.flight_shuffle_eio_local_max_retries == 2
        assert restored.flight_shuffle_eio_local_initial_backoff_ms == 7
        assert restored.flight_shuffle_eio_local_max_backoff_ms == 50
        assert restored.flight_shuffle_eio_max_retries == 7
        assert restored.flight_shuffle_eio_initial_backoff_ms == 5
        assert restored.flight_shuffle_eio_max_backoff_ms == 40
    with (
        pytest.raises(ValueError, match="must be >="),
        daft.execution_config_ctx(flight_shuffle_eio_initial_backoff_ms=100, flight_shuffle_eio_max_backoff_ms=10),
    ):
        pass
    with pytest.raises((ValueError, OverflowError)), daft.execution_config_ctx(flight_shuffle_eio_max_retries=-1):
        pass

    with (
        pytest.raises(ValueError, match="must be >="),
        daft.execution_config_ctx(
            flight_shuffle_eio_local_initial_backoff_ms=100, flight_shuffle_eio_local_max_backoff_ms=10
        ),
    ):
        pass
    with pytest.raises((ValueError, OverflowError)), daft.execution_config_ctx(flight_shuffle_eio_local_max_retries=-1):
        pass


def test_shuffle_eio_plan_pickle_roundtrip():
    from daft.daft import DistributedPhysicalPlan

    with daft.execution_config_ctx(flight_shuffle_eio_max_retries=7):
        config = get_context().daft_execution_config
        df = daft.from_pydict({"k": [1, 2]}).repartition(2, "k")
        plan = DistributedPhysicalPlan.from_logical_plan_builder(
            df._builder.optimize(config)._builder, "eio-pickle-test", config
        )
        factory, (payload,) = plan.__reduce__()
        assert factory.__name__ == "_from_serialized_shuffle_eio_v2"
        restored = pickle.loads(pickle.dumps(plan))
        assert restored.idx() == plan.idx()
        assert restored.__reduce__()[1] == (payload,)
        with pytest.raises(ValueError, match="Trailing bytes"):
            factory(payload + b"extra")


@pytest.mark.parametrize("errno", [5, 13, 28, 30])
def test_shuffle_io_error_survives_pickle_and_ray(errno):
    ray = pytest.importorskip("ray")
    error = DaftShuffleIoError("read", "/shuffle/map.arrow", errno, "injected")
    wrapped = ray.exceptions.RayTaskError("run_plan", "traceback", error)
    for restored in [pickle.loads(pickle.dumps(error)), ray.exceptions.RayError.from_bytes(wrapped.to_bytes()).cause]:
        assert isinstance(restored, DaftShuffleIoError)
        assert vars(restored) == vars(error)


@pytest.fixture(scope="module")
def shuffle_io_injector(tmp_path_factory):
    if sys.platform != "linux" or shutil.which("cc") is None:
        pytest.skip("Real syscall injection requires Linux and a C compiler")
    library = tmp_path_factory.mktemp("shuffle-io-injector") / "inject.so"
    subprocess.run(
        [
            "cc",
            "-shared",
            "-fPIC",
            "-O2",
            "-Wall",
            "-Werror",
            str(Path(__file__).with_name("shuffle_eio_injection.c")),
            "-o",
            str(library),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return library


DRIVER = r"""
import os
from pathlib import Path
import ray
import daft
from daft.exceptions import DaftShuffleIoError

root = Path(os.environ["DAFT_TEST_SHUFFLE_IO_ROOT"])
placement, read_source, retries, expected_errno = os.environ["DAFT_TEST_CASE"].split(",")
cluster = None
if os.environ["DAFT_TEST_SHUFFLE_IO_OPERATION"] == "read":
    from ray.cluster_utils import Cluster
    cluster = Cluster()
    cluster.add_node(num_cpus=1, include_dashboard=False, object_store_memory=128 * 1024 * 1024)
    cluster.add_node(num_cpus=1, object_store_memory=128 * 1024 * 1024)
    ray.init(address=cluster.address)
else:
    ray.init(num_cpus=2, include_dashboard=False, object_store_memory=256 * 1024 * 1024)
daft.set_runner_ray()
try:
    cfg = dict(shuffle_algorithm="flight_shuffle", flight_shuffle_dirs=[str(root)],
        flight_shuffle_compression="none", flight_shuffle_eio_max_retries=int(retries),
        flight_shuffle_eio_local_max_retries=int(os.environ.get("DAFT_TEST_LOCAL_EIO_RETRIES", "0")),
        flight_shuffle_eio_local_initial_backoff_ms=10, flight_shuffle_eio_local_max_backoff_ms=20,
        flight_shuffle_eio_initial_backoff_ms=10, flight_shuffle_eio_max_backoff_ms=20)
    if placement == "shared_only":
        cfg.update(flight_shuffle_placement=placement, flight_shuffle_shared_dir=str(root),
            flight_shuffle_shared_durability=os.environ.get("DAFT_TEST_DURABILITY", "sync"), flight_shuffle_read_source=read_source)
    # Large uncompressed IPC messages make the read failure happen after earlier
    # batches have entered the consumer. Distinct rows detect duplication and loss.
    n = 8192
    values = [str(i) + ":" + "x" * 2048 for i in range(n)]
    data = {"k": list(range(n)), "v": values}
    if os.environ.get("DAFT_TEST_SOURCE") == "parquet":
        import pyarrow as pa
        import pyarrow.parquet as pq
        path = root / "input.parquet"
        pq.write_table(pa.table(data), path)
        source = daft.read_parquet(str(path))
    elif os.environ.get("DAFT_TEST_SOURCE") == "generator":
        from daft.recordbatch.recordbatch import RecordBatch
        from daft.io._generator import read_generator
        batch = RecordBatch.from_pydict(data)
        def generate():
            with (root / "generator-calls").open("a") as f:
                f.write("called\n")
            yield batch
        source = read_generator(iter([generate]), batch.schema())
    else:
        source = daft.from_pydict(data)
    with daft.execution_config_ctx(**cfg):
        df = (source.into_partitions(2) if os.environ.get("DAFT_TEST_SOURCE") == "streaming"
              else source.repartition(2, "k")).sort("k")
        try:
            result = df.to_pydict()
        except DaftShuffleIoError as error:
            assert expected_errno, repr(error)
            assert error.errno == int(expected_errno), repr(error)
            assert str(root) in error.path, repr(error)
            print("EXPECTED_ERROR", repr(error), flush=True)
        else:
            assert not expected_errno, "failure was incorrectly swallowed"
            assert sorted(zip(result["k"], result["v"])) == list(enumerate(values))
            print("EXACT_RESULT", n, flush=True)
    faults = list(root.glob("fault-*"))
    assert faults, "fault injector did not intercept a shuffle syscall"
    print("INJECTED", len(faults), [p.read_text() for p in faults], flush=True)
finally:
    ray.shutdown()
    if cluster is not None:
        cluster.shutdown()
"""


CANCEL_DRIVER = r"""
import os
from pathlib import Path
import threading
import time

import daft
import ray
from daft.runners import get_or_create_runner

root = Path(os.environ["DAFT_TEST_SHUFFLE_IO_ROOT"])
ray.init(num_cpus=2, include_dashboard=False, object_store_memory=256 * 1024 * 1024)
daft.set_runner_ray()
try:
    cfg = dict(shuffle_algorithm="flight_shuffle", flight_shuffle_dirs=[str(root)],
        flight_shuffle_compression="none", flight_shuffle_eio_local_max_retries=6,
        flight_shuffle_eio_local_initial_backoff_ms=32000,
        flight_shuffle_eio_local_max_backoff_ms=32000,
        enable_scan_task_split_and_merge=False)
    if os.environ["DAFT_TEST_PLACEMENT"] == "shared_only":
        cfg.update(flight_shuffle_placement="shared_only", flight_shuffle_shared_dir=str(root),
            flight_shuffle_read_source="shared", flight_shuffle_shared_durability="sync")
    data = {"k": list(range(8192)), "v": [str(i) + ":" + "x" * 2048 for i in range(8192)]}
    source = daft.from_pydict(data)
    outcome = {}
    def consume():
        try:
            with daft.execution_config_ctx(**cfg):
                outcome["result"] = source.repartition(2, "k").sort("k").to_pydict()
        except Exception as error:
            outcome["error"] = repr(error)
    thread = threading.Thread(target=consume, daemon=True)
    thread.start()
    deadline = time.monotonic() + 60
    while not list(root.glob("fault-*")) and thread.is_alive() and time.monotonic() < deadline:
        time.sleep(0.05)
    assert list(root.glob("fault-*")), outcome
    control = get_or_create_runner().flotilla_plan_runner.runner
    plan_ids = ray.get(control.__ray_call__.remote(lambda actor: list(actor.curr_plans)))
    assert len(plan_ids) == 1, plan_ids
    started = time.monotonic()
    ray.get(control.cancel_plan.remote(plan_ids[0]), timeout=15)
    thread.join(timeout=max(0, 16 - (time.monotonic() - started)))
    cancelled_seconds = time.monotonic() - started
    assert not thread.is_alive(), "query did not stop after cancellation"
    assert cancelled_seconds < 16
    assert not outcome.get("result", {}).get("k"), outcome
    # Cleanup only removes these directories after every worker acknowledges
    # zero active writers. A zero-row result alone does not prove write drain.
    assert not list(root.glob("daft_shuffle/*")), "shuffle writes did not drain and clean up"
    faults = {p.name: p.read_text() for p in root.glob("fault-*")}
    time.sleep(2)
    assert {p.name: p.read_text() for p in root.glob("fault-*")} == faults, "writes continued after cancel"
    assert not list(root.glob("daft_shuffle/*")), "cancelled writer recreated its files"
    with daft.execution_config_ctx(shuffle_algorithm="map_reduce"):
        assert daft.from_pydict({"v": [1, 2, 3]}).agg(daft.col("v").sum()).to_pydict() == {"v": [6]}
    print("CANCEL_DRAINED", cancelled_seconds, "faults", len(faults), flush=True)
finally:
    ray.shutdown()
"""


@pytest.mark.parametrize("placement", ["shared_only", "local_only"])
@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="Requires local Ray task execution")
def test_cancel_during_shuffle_eio_backoff(tmp_path, shuffle_io_injector, placement):
    env = dict(os.environ)
    env.update(
        LD_PRELOAD=str(shuffle_io_injector),
        DAFT_TEST_SHUFFLE_IO_ROOT=str(tmp_path),
        DAFT_TEST_SHUFFLE_IO_OPERATION="write",
        DAFT_TEST_SHUFFLE_IO_FAILURES="100",
        DAFT_TEST_SHUFFLE_IO_ERRNO="5",
        DAFT_TEST_PLACEMENT=placement,
        DAFT_RUNNER="ray",
        DAFT_PROGRESS_BAR="0",
        RAY_ADDRESS="local",
    )
    result = subprocess.run(
        [sys.executable, "-c", CANCEL_DRIVER], env=env, capture_output=True, text=True, timeout=120, check=False
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "CANCEL_DRAINED" in result.stdout


@pytest.mark.parametrize(
    "operation,placement,read_source,retries,failures,errno,expected_errno,min_offset,source",
    [
        ("open", "local_only", "auto", 2, 1, 5, "", 0, "memory"),
        ("write", "local_only", "auto", 2, 1, 5, "", 0, "memory"),
        ("write", "shared_only", "shared", 2, 1, 5, "", 0, "memory"),
        ("fsync", "shared_only", "shared", 2, 1, 5, "", 0, "memory"),
        ("read", "local_only", "auto", 2, 1, 5, "", 6 * 1024 * 1024, "memory"),
        ("read", "shared_only", "shared", 2, 1, 5, "", 6 * 1024 * 1024, "memory"),
        ("write", "local_only", "auto", 0, 1, 5, "5", 0, "memory"),
        ("write", "local_only", "auto", 2, 100, 5, "5", 0, "memory"),
        ("write", "local_only", "auto", 2, 1, 13, "13", 0, "memory"),
        ("write", "local_only", "auto", 2, 1, 5, "", 0, "parquet"),
        ("write", "local_only", "auto", 2, 1, 5, "", 0, "streaming"),
        ("open_read", "local_only", "auto", 2, 1, 5, "", 0, "memory"),
    ],
)
@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="Requires local Ray task execution")
def test_real_shuffle_io_failure(
    tmp_path,
    shuffle_io_injector,
    operation,
    placement,
    read_source,
    retries,
    failures,
    errno,
    expected_errno,
    min_offset,
    source,
    local_retries=0,
    short_write=False,
    extra_env=None,
    expected_faults=None,
):
    env = dict(os.environ)
    env.update(
        LD_PRELOAD=str(shuffle_io_injector),
        DAFT_TEST_SHUFFLE_IO_ROOT=str(tmp_path),
        DAFT_TEST_SHUFFLE_IO_OPERATION=operation,
        DAFT_TEST_SHUFFLE_IO_FAILURES=str(failures),
        DAFT_TEST_SHUFFLE_IO_ERRNO=str(errno),
        DAFT_TEST_SHUFFLE_IO_MIN_OFFSET=str(min_offset),
        DAFT_TEST_SOURCE=source,
        DAFT_TEST_LOCAL_EIO_RETRIES=str(local_retries),
        DAFT_TEST_CASE=f"{placement},{read_source},{retries},{expected_errno}",
        DAFT_RUNNER="ray",
        DAFT_PROGRESS_BAR="0",
        RAY_ADDRESS="local",
    )
    if extra_env:
        env.update(extra_env)
    if short_write:
        env["DAFT_TEST_SHUFFLE_IO_SHORT_WRITE"] = "1"
    result = subprocess.run(
        [sys.executable, "-c", DRIVER], env=env, capture_output=True, text=True, timeout=180, check=False
    )
    assert result.returncode == 0, result.stdout + result.stderr
    faults = list(tmp_path.glob("fault-*"))
    if source == "streaming":
        assert all("partition_ref_" in p.read_text() for p in faults)
    if expected_faults is not None:
        assert len(faults) == expected_faults
    elif failures == 1:
        assert len(faults) == 1
    else:
        # One map task fails on every attempt, and no consumer can start.
        assert len({p.read_text().split(maxsplit=3)[-1].strip() for p in faults}) == retries + 1


@pytest.mark.parametrize(
    "operation,placement,read_source,source,short_write",
    [
        ("open", "local_only", "auto", "memory", False),
        ("write", "local_only", "auto", "memory", True),
        ("write", "shared_only", "shared", "memory", True),
        ("write", "local_only", "auto", "streaming", True),
        ("read", "local_only", "auto", "memory", False),
        ("read", "shared_only", "shared", "memory", False),
        ("open_read", "local_only", "auto", "memory", False),
    ],
)
@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="Requires local Ray task execution")
def test_real_local_shuffle_io_failure(
    tmp_path, shuffle_io_injector, operation, placement, read_source, source, short_write
):
    # With task retries disabled, success proves recovery did not re-execute the task.
    test_real_shuffle_io_failure(
        tmp_path,
        shuffle_io_injector,
        operation,
        placement,
        read_source,
        0,
        1,
        5,
        "",
        6 * 1024 * 1024 if operation == "read" else 0,
        source,
        local_retries=2,
        short_write=short_write,
    )


@pytest.mark.parametrize(
    "operation,errno,failures,expected", [("write", 5, 100, "5"), ("write", 13, 1, "13"), ("fsync", 5, 1, "5")]
)
@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="Requires local Ray task execution")
def test_local_retry_exhaustion_and_unsafe_commit_errors(
    tmp_path, shuffle_io_injector, operation, errno, failures, expected
):
    test_real_shuffle_io_failure(
        tmp_path,
        shuffle_io_injector,
        operation,
        "shared_only",
        "shared",
        0,
        failures,
        errno,
        expected,
        0,
        "memory",
        local_retries=2,
    )


@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="Requires local Ray task execution")
def test_local_retry_exhaustion_falls_back_to_task_retry(tmp_path, shuffle_io_injector):
    test_real_shuffle_io_failure(
        tmp_path,
        shuffle_io_injector,
        "write",
        "local_only",
        "auto",
        1,
        4,
        5,
        "",
        0,
        "memory",
        local_retries=2,
    )


@pytest.mark.parametrize("source", ["memory", "streaming"])
@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="Requires local Ray task execution")
def test_interrupted_write_does_not_spin_with_eio_retries_disabled(tmp_path, shuffle_io_injector, source):
    test_real_shuffle_io_failure(tmp_path, shuffle_io_injector, "write", "local_only", "auto", 0, 1, 4, "", 0, source)


@pytest.mark.parametrize(
    "placement,source,durability",
    [
        ("local_only", "memory", "none"),
        ("local_only", "streaming", "none"),
        ("shared_only", "memory", "none"),
        ("shared_only", "memory", "background"),
        ("shared_only", "memory", "sync"),
    ],
)
@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="Requires local Ray task execution")
def test_recovered_write_syncs_original_fd_before_returning(
    tmp_path, shuffle_io_injector, placement, source, durability
):
    test_real_shuffle_io_failure(
        tmp_path,
        shuffle_io_injector,
        "write",
        placement,
        "auto",
        0,
        1,
        5,
        "",
        0,
        source,
        local_retries=3,
        extra_env={
            "DAFT_TEST_SHUFFLE_IO_TRACE_SYNC": "1",
            "DAFT_TEST_SHUFFLE_IO_PARTIAL_EFFECT": "1",
            "DAFT_TEST_DURABILITY": durability,
        },
    )
    writes = list(tmp_path.glob("write-error-fd-*"))
    assert len(writes) == 1
    sync = tmp_path / writes[0].name.replace("write-error-fd-", "recovery-sync-fd-")
    assert sync.read_text() == writes[0].read_text()


@pytest.mark.parametrize(
    "sequence,retries,expected_errno",
    [
        ("write,read,write", 0, ""),
        ("write,fdatasync", 0, "5"),
        ("write,fdatasync", 1, ""),
    ],
)
@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="Requires local Ray task execution")
def test_recovery_pass_retries_io_but_never_swallows_sync_failure(
    tmp_path, shuffle_io_injector, sequence, retries, expected_errno
):
    test_real_shuffle_io_failure(
        tmp_path,
        shuffle_io_injector,
        "write",
        "local_only",
        "auto",
        retries,
        1,
        5,
        expected_errno,
        0,
        "memory",
        local_retries=3,
        extra_env={"DAFT_TEST_SHUFFLE_IO_SEQUENCE": sequence},
        expected_faults=len(sequence.split(",")),
    )


@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="Requires local Ray task execution")
def test_eio_task_retry_does_not_reexecute_python_scan(tmp_path, shuffle_io_injector):
    test_real_shuffle_io_failure(
        tmp_path, shuffle_io_injector, "write", "local_only", "auto", 3, 1, 5, "5", 0, "generator"
    )
    assert (tmp_path / "generator-calls").read_text().splitlines() == ["called"]
