"""True FlightGather collect timings, with a full-data global window oracle.

Run matching release wheels in separate processes in a predeclared ABBA order.
The default creates a private local Ray instance; --address connects to an
existing cluster. Local runs do not replace the two-worker shared-mount study.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import io
import json
import os
import platform
import select
import tempfile
import time
from pathlib import Path

import ray

import daft
from daft import Window
from daft.context import get_context
from daft.functions import row_number


def emit(**record):
    print(json.dumps(record, sort_keys=True), flush=True)


def control_perf(fds, command):
    """Gate an inherited perf session around collect, outside elapsed timing."""
    if fds is None:
        return
    control, acknowledgement = fds
    os.write(control, (command + "\n").encode())
    ready, _, _ = select.select([acknowledgement], [], [], 30)
    response = os.read(acknowledgement, 4096) if ready else b""
    # perf versions may include the C string's terminating NUL in the reply.
    if response.rstrip(b"\0\r\n") != b"ack":
        raise RuntimeError(f"perf did not acknowledge {command}: {response!r}")


@ray.remote(num_cpus=0)
def worker_environment():
    import socket

    extension = Path(daft.daft.__file__)
    quota = {}
    for path in [
        "/sys/fs/cgroup/cpu.max",
        "/sys/fs/cgroup/cpu/cpu.cfs_quota_us",
        "/sys/fs/cgroup/cpu/cpu.cfs_period_us",
    ]:
        if Path(path).exists():
            quota[path] = Path(path).read_text().strip()
    return {
        "host": socket.gethostname(),
        "extension_sha256": hashlib.sha256(extension.read_bytes()).hexdigest(),
        "cpu_affinity": sorted(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else None,
        "cpu_quota": quota,
        "dependencies": {name: importlib.metadata.version(name) for name in ["ray", "pyarrow", "numpy", "pandas"]},
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--label", required=True)
    parser.add_argument("--case", choices=["flight-gather"], required=True)
    parser.add_argument("--samples", type=int, default=3)
    parser.add_argument("--address")
    parser.add_argument("--cpus", type=int, default=8)
    parser.add_argument("--compression", choices=["lz4", "none"], default="lz4")
    parser.add_argument("--root", type=Path, default=Path(tempfile.gettempdir()))
    parser.add_argument("--inspect-input", action="store_true", help="Diagnostic input inspection before warmup")
    parser.add_argument("--perf-control", type=Path, help="Diagnostic perf control FIFO; requires --perf-ack")
    parser.add_argument("--perf-ack", type=Path, help="Diagnostic perf acknowledgement FIFO")
    args = parser.parse_args()
    if args.samples < 1:
        parser.error("sample count must be positive")
    if (args.perf_control is None) != (args.perf_ack is None):
        parser.error("--perf-control and --perf-ack must be supplied together")

    # Pin each job's package and diagnostic settings even when several wheel
    # versions share one persistent test cluster. Ray uses separate worker pools
    # for distinct runtime environments; the extension hashes below verify this.
    job_env = {
        name: os.environ[name]
        for name in ["PYTHONPATH", "RAYON_NUM_THREADS", "OMP_NUM_THREADS", "DAFT_TRACE", "DAFT_TRACE_FORMAT"]
        if name in os.environ
    }
    runtime_env = {"env_vars": job_env} if job_env else None
    if args.address:
        ray_context = ray.init(address=args.address, runtime_env=runtime_env)
    else:
        store_bytes = 3 * 1024**3
        ray_context = ray.init(
            num_cpus=args.cpus, include_dashboard=False, object_store_memory=store_bytes, runtime_env=runtime_env
        )
    daft.set_runner_ray()
    extension = Path(daft.daft.__file__)
    extension_hash = hashlib.sha256(extension.read_bytes()).hexdigest()
    nodes = [{"id": node["NodeID"], "resources": node["Resources"]} for node in ray.nodes() if node["Alive"]]
    from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

    workers = ray.get(
        [
            worker_environment.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(node["id"], soft=False)
            ).remote()
            for node in nodes
        ]
    )
    assert all(worker["extension_sha256"] == extension_hash for worker in workers), "worker extension mismatch"
    emit(
        event="environment",
        label=args.label,
        extension=str(extension),
        extension_sha256=extension_hash,
        python=platform.python_version(),
        dependencies={name: importlib.metadata.version(name) for name in ["ray", "pyarrow", "numpy", "pandas"]},
        nodes=nodes,
        workers=workers,
        local_cluster=args.address is None,
        cache="warm; not cleared",
        thread_environment={
            name: os.getenv(name) for name in ["DAFT_NUM_THREADS", "RAYON_NUM_THREADS", "OMP_NUM_THREADS"]
        },
    )
    perf_fds = (
        (os.open(args.perf_control, os.O_RDWR), os.open(args.perf_ack, os.O_RDWR))
        if args.perf_control is not None
        else None
    )
    try:
        # The temporary directory is unique to this process. Query cleanup and
        # Ray shutdown happen before the directory is removed.
        with tempfile.TemporaryDirectory(prefix="daft-p3-collect-", dir=args.root) as directory:
            settings = {
                "shuffle_algorithm": "flight_shuffle",
                "flight_shuffle_dirs": [str(Path(directory) / "local")],
                "flight_shuffle_placement": "shared_only",
                "flight_shuffle_shared_dir": str(Path(directory) / "shared"),
                "flight_shuffle_read_source": "shared",
                "flight_shuffle_compression": args.compression,
                "flight_shuffle_shared_durability": "sync",
                "flight_shuffle_recovery_max_attempts": 0,
                "experimental_shuffle_aqe": False,
                "enable_scan_task_split_and_merge": False,
                "shuffle_aggregation_default_partitions": 128,
            }
            retry_supported = hasattr(get_context().daft_execution_config, "flight_shuffle_eio_local_max_retries")
            if retry_supported:
                settings["flight_shuffle_eio_local_max_retries"] = 6
            emit(event="configuration", label=args.label, settings=settings, local_retry_supported=retry_supported)
            with daft.execution_config_ctx(**settings):
                rows = 64_000
                source = (
                    daft.from_pydict({"k": [i % 128 for i in range(rows)], "v": list(range(rows))})
                    .into_partitions(32)
                    .collect()
                )

                emit(event="input", label=args.label, partitions=source._result_cache.num_partitions(), rows=rows)
                if args.inspect_input:
                    for partition_id, materialized in source._result_cache.value.items():
                        partition = materialized.micropartition()
                        emit(
                            event="input_partition",
                            label=args.label,
                            partition_id=partition_id,
                            rows=len(partition),
                            size_bytes=partition.size_bytes(),
                            arrow_batches=[len(batch) for batch in partition.to_arrow().to_batches()],
                        )
                for trial in range(args.samples + 1):
                    query = source.with_column("ordinal", row_number().over(Window().order_by("v")))
                    if trial == 0:
                        plan = io.StringIO()
                        query.explain(show_all=True, file=plan)
                        physical = plan.getvalue().split("== Physical Plan ==", 1)[1]
                        assert "FlightGather" in physical, physical
                        emit(event="physical_plan", label=args.label, plan=physical, expected="FlightGather")
                    if trial > 0:
                        control_perf(perf_fds, "enable")
                    wall_start = time.time()
                    started = time.perf_counter()
                    try:
                        result = query.collect()
                        seconds = time.perf_counter() - started
                        wall_end = time.time()
                    finally:
                        if trial > 0:
                            control_perf(perf_fds, "disable")
                    partitions = result._result_cache.num_partitions()
                    data = result.to_pydict()
                    assert sorted(zip(data["v"], data["k"])) == [(i, i % 128) for i in range(rows)]
                    assert partitions == 1
                    assert data["ordinal"] == [v + 1 for v in data["v"]]
                    emit(
                        event="sample",
                        label=args.label,
                        case=args.case,
                        trial=trial,
                        warmup=trial == 0,
                        profiled=perf_fds is not None and trial > 0,
                        seconds=seconds,
                        wall_start=wall_start,
                        wall_end=wall_end,
                        rows=rows,
                        partitions=partitions,
                        oracle="PASS",
                    )
                    del result, query, data
                # Query once after all samples so telemetry does not change the
                # collect timing. These internal APIs are recorded against the
                # pinned Ray version, alongside the rest of the environment.
                from ray._private.internal_api import get_memory_info_reply, get_state_from_address

                state = get_state_from_address(ray_context.address_info["gcs_address"])
                stats = get_memory_info_reply(state, timeout_seconds=30).store_stats
                emit(event="object_store", label=args.label, spilled_bytes=stats.spilled_bytes_total)
                if args.address is None:
                    assert stats.spilled_bytes_total == 0, "object-store spilling invalidates this local comparison"
            ray.shutdown()
    finally:
        ray.shutdown()
        if perf_fds is not None:
            for fd in perf_fds:
                os.close(fd)


if __name__ == "__main__":
    main()
