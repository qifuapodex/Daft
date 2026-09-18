"""Interleave version and storage comparisons with full ABBA cycles.

Builds must be prepared before this script starts. Writer jobs and a private
two-worker Ray cluster run in separate phases, with no overlapping jobs. All
formal samples and warmups are retained, including slow observations.

Example (Python environment needs Ray and the wheel dependencies):
  python storage_abba.py --baseline-binary /tmp/old-writer \
    --candidate-binary /tmp/new-writer --baseline-wheel /tmp/old.whl \
    --candidate-wheel /tmp/new.whl --shared-root /mnt/juicefs \
    --output /tmp/shuffle-run --cycles 8 --samples 3

The writer test (src/daft-shuffles/src/write_bench.rs) and
flight_gather_collect.py generate all input data and verify the output.
This script generates builds.json, manifests, raw samples, per-view statistics
and combined summary.csv/min_median.json under --output. No checked-in dataset
is needed. Reuse a generated builds.json with --builds to verify pinned hashes.
For A/A controls, pass the same artifacts for both versions. GNU time, taskset,
strace, findmnt and lscpu must be installed; do not overlap builds or profiling
with measurement. Storage roots must already exist.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import zipfile
from pathlib import Path

ORDER = [
    "baseline-local",
    "candidate-local",
    "baseline-shared",
    "candidate-shared",
    "candidate-shared",
    "baseline-shared",
    "candidate-local",
    "baseline-local",
]
VIEWS = {
    "versions-local": ([0, 1, 6, 7], "baseline-local", "candidate-local"),
    "versions-shared": ([2, 3, 4, 5], "baseline-shared", "candidate-shared"),
    "disks-baseline": ([0, 2, 5, 7], "baseline-local", "baseline-shared"),
    "disks-candidate": ([1, 3, 4, 6], "candidate-local", "candidate-shared"),
}
WRITERS = [("stream-single", "none"), ("stream-wide", "none"), ("oneshot-small", "none"), ("stream-wide", "lz4")]


def sha(path):
    with Path(path).open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def save(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def build_manifest(args, parser):
    versions = ["baseline", "candidate"]
    artifacts = ["binary", "wheel"]
    paths = [getattr(args, f"{version}_{artifact}") for version in versions for artifact in artifacts]
    if args.builds:
        if any(paths):
            parser.error("use either --builds or the four binary/wheel arguments")
        builds = json.loads(args.builds.read_text())
    else:
        if not all(paths):
            parser.error("provide --builds or all four --baseline/--candidate binary/wheel arguments")
        builds = {
            version: {
                artifact: {"path": str(getattr(args, f"{version}_{artifact}").resolve())} for artifact in artifacts
            }
            for version in versions
        }
    for version in versions:
        for artifact in artifacts:
            entry = builds[version][artifact]
            path = Path(entry["path"]).resolve()
            digest = sha(path)
            if args.builds:
                assert entry["sha256"] == digest, (version, artifact, "hash mismatch")
            entry.update(path=str(path), sha256=digest)
        wheel = builds[version]["wheel"]
        with zipfile.ZipFile(wheel["path"]) as archive:
            extensions = [name for name in archive.namelist() if Path(name).match("daft/daft*.so")]
            assert len(extensions) == 1, "expected one Linux Daft extension per wheel"
            with archive.open(extensions[0]) as extension:
                digest = hashlib.file_digest(extension, "sha256").hexdigest()
        if args.builds:
            assert wheel["extension_sha256"] == digest, (version, "extension hash mismatch")
        wheel["extension_sha256"] = digest
    return builds


def load_snapshot():
    # Diagnostics only: no load-based rejection or sample trimming.
    return {
        "loadavg": Path("/proc/loadavg").read_text().strip(),
        "memory": {
            line.split(":", 1)[0]: line.split(":", 1)[1].strip()
            for line in Path("/proc/meminfo").read_text().splitlines()
            if line.startswith(("MemAvailable:", "Dirty:", "Writeback:"))
        },
        "cpu_stat": Path("/proc/stat").read_text().splitlines()[0],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--builds", type=Path, help="Previously generated binary/wheel manifest; verify all pinned hashes"
    )
    for version in ["baseline", "candidate"]:
        parser.add_argument(f"--{version}-binary", type=Path, help="Release daft-shuffles test executable")
        parser.add_argument(f"--{version}-wheel", type=Path, help="Release Daft wheel")
    parser.add_argument("--output", type=Path, required=True, help="New output directory")
    parser.add_argument("--local-root", type=Path, default=Path("/tmp"))
    parser.add_argument("--shared-root", type=Path, required=True)
    parser.add_argument("--cycles", type=int, default=8)
    parser.add_argument("--samples", type=int, default=3)
    parser.add_argument("--writer-cpus", default="2,3")
    args = parser.parse_args()
    if min(args.cycles, args.samples) < 1:
        parser.error("counts must be positive")
    if not all(path.is_dir() for path in [args.local_root, args.shared_root]):
        parser.error("storage roots must exist")
    builds = build_manifest(args, parser)
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    save(out / "builds.json", builds)
    packages = {}
    for version in ["baseline", "candidate"]:
        packages[version] = out / "packages" / version
        with zipfile.ZipFile(builds[version]["wheel"]["path"]) as archive:
            archive.extractall(packages[version])
        extensions = list((packages[version] / "daft").glob("daft*.so"))
        assert len(extensions) == 1
        assert sha(extensions[0]) == builds[version]["wheel"]["extension_sha256"]

    roots = {}
    cluster = None
    jobs = 0
    total = args.cycles * len(ORDER) * (len(WRITERS) + 2)
    all_events = out / "all-events.jsonl"
    workload = Path(__file__).with_name("flight_gather_collect.py").resolve()
    common_env = {**os.environ, "DAFT_PROGRESS_BAR": "0", "RAYON_NUM_THREADS": "2", "OMP_NUM_THREADS": "1"}
    expected_environment = None

    def status(phase, **fields):
        save(out / "status.json", {"phase": phase, "time": time.time(), **fields})
        print(phase, fields, flush=True)

    def writer_env(disk, case, compression, samples):
        return {
            **common_env,
            "TMPDIR": str(roots[disk]),
            "DAFT_WRITE_CASE": case,
            "DAFT_WRITE_CONCURRENCY": "8",
            "DAFT_WRITE_COMPRESSION": compression,
            "DAFT_WRITE_RETRIES": "6",
            "DAFT_WRITE_SAMPLES": str(samples),
        }

    def writer_command(version):
        return [
            "taskset",
            "-c",
            args.writer_cpus,
            builds[version]["binary"]["path"],
            "bench_shuffle_writes",
            "--ignored",
            "--nocapture",
            "--test-threads=1",
        ]

    def run_job(kind, case, compression, cycle, position, command, env):
        nonlocal jobs, expected_environment
        condition = ORDER[position]
        version, disk = condition.split("-", 1)
        name = f"{kind}-{case}-{compression}-{cycle:02d}-{position}-{condition}"
        status("measuring", job=jobs + 1, total=total, name=name)
        started = time.time()
        before = load_snapshot()
        with (out / (name + ".log")).open("w") as log:
            subprocess.run(
                ["/usr/bin/time", "-v", "-o", str(out / (name + ".time")), *command],
                cwd=out,
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
                check=True,
                timeout=600,
            )
        finished = time.time()
        after = load_snapshot()
        resource_usage = {}
        for line in (out / (name + ".time")).read_text().splitlines():
            key, separator, value = line.strip().partition(": ")
            if separator and key in {
                "User time (seconds)",
                "System time (seconds)",
                "Maximum resident set size (kbytes)",
                "File system inputs",
                "File system outputs",
                "Voluntary context switches",
                "Involuntary context switches",
            }:
                resource_usage[key] = float(value)
        lines = (out / (name + ".log")).read_text().splitlines()
        if kind == "writer":
            events = []
            for line in lines:
                if not line.startswith("WRITE_SAMPLE "):
                    continue
                _, observed, warmup, trial, seconds = line.split()
                assert observed == case
                events.append(
                    {
                        "event": "sample",
                        "case": case,
                        "reader": "c8-" + compression,
                        "trial": int(trial),
                        "warmup": warmup == "true",
                        "seconds": float(seconds),
                        "oracle": "PASS",
                    }
                )
            assert not list(roots[disk].glob("daft-write-bench-*")), "writer output cleanup incomplete"
        else:
            events = [json.loads(line) for line in lines if line.startswith("{")]
            assert [e["spilled_bytes"] for e in events if e["event"] == "object_store"] == [0]
            environment = next(e for e in events if e["event"] == "environment")
            expected_hash = builds[version]["wheel"]["extension_sha256"]
            assert environment["extension_sha256"] == expected_hash
            assert all(w["extension_sha256"] == expected_hash for w in environment["workers"])
            identity = {
                "nodes": sorted(environment["nodes"], key=lambda n: n["id"]),
                "dependencies": environment["dependencies"],
                "workers": sorted(
                    [
                        {k: w[k] for k in ["host", "cpu_affinity", "cpu_quota", "dependencies"]}
                        for w in environment["workers"]
                    ],
                    key=lambda worker: json.dumps(worker, sort_keys=True),
                ),
            }
            if expected_environment is None:
                expected_environment = identity
            assert identity == expected_environment, "cluster resources or dependencies changed"
            settings = next(e for e in events if e["event"] == "configuration")["settings"]
            assert all(Path(p).is_relative_to(roots[disk]) for p in settings["flight_shuffle_dirs"])
            assert Path(settings["flight_shuffle_shared_dir"]).is_relative_to(roots[disk])
            assert settings["flight_shuffle_compression"] == compression
            assert "FlightGather" in next(e for e in events if e["event"] == "physical_plan")["plan"]
            input_event = next(e for e in events if e["event"] == "input")
            assert input_event["partitions"] == 32 and input_event["rows"] == 64000
            if "flight_shuffle_eio_local_max_retries" in settings:
                assert settings["flight_shuffle_eio_local_max_retries"] == 6
            for event in events:
                if event["event"] == "sample":
                    event["case"] = "flight-gather-" + compression
                    event["reader"] = "collect"
        samples = [e for e in events if e["event"] == "sample"]
        assert len(samples) == args.samples + 1
        assert {e["trial"] for e in samples} == set(range(args.samples + 1))
        assert all(e["oracle"] == "PASS" and e["warmup"] == (e["trial"] == 0) for e in samples)
        events.insert(
            0,
            {
                "event": "job",
                "name": name,
                "start_time": started,
                "end_time": finished,
                "command": command,
                "disk_root": str(roots[disk]),
                "before": before,
                "after": after,
                "process_resources": resource_usage,
            },
        )
        with all_events.open("a") as records:
            for event in events:
                records.write(
                    json.dumps(
                        {**event, "kind": kind, "condition": condition, "cycle": cycle, "global_position": position}
                    )
                    + "\n"
                )
        jobs += 1

    try:
        for disk, parent in [("local", args.local_root), ("shared", args.shared_root)]:
            roots[disk] = Path(tempfile.mkdtemp(prefix="daft-storage-abba-", dir=parent.resolve()))
            (roots[disk] / "OWNER").write_text(str(out) + "\n")
        manifest = {
            "builds": builds,
            "cycles": args.cycles,
            "samples_per_job": args.samples,
            "warmups_per_job": 1,
            "order": ORDER,
            "views": VIEWS,
            "writer_cases": WRITERS,
            "writer_concurrency": 8,
            "writer_cpu_set": args.writer_cpus,
            "query_cases": ["flight-gather-none", "flight-gather-lz4"],
            "query_cpus_per_worker": 4,
            "regression_budget_pct": 1,
            "roots": {k: str(v) for k, v in roots.items()},
            "mounts": {
                k: json.loads(
                    subprocess.check_output(
                        ["findmnt", "-T", str(v), "-J", "-o", "TARGET,SOURCE,FSTYPE,OPTIONS"], text=True
                    )
                )
                for k, v in roots.items()
            },
            "cpu": subprocess.check_output(["lscpu", "-J"], text=True),
            "harness_sha256": {
                p.name: sha(p)
                for p in [
                    Path(__file__),
                    workload,
                    Path(__file__).with_name("abba_stats.py"),
                    Path(__file__).with_name("min_median_stats.py"),
                    Path(__file__).with_name("storage_stats.py"),
                ]
            },
            "cache": "client/server caches not cleared; no load-based rejection or trimming",
            "durability": "writer and FlightGather normal paths do not fsync; Gather clears shared backend, so selected local dirs use the chosen mount",
            "process_resources_scope": "GNU time covers the launched process and its children; query driver counts exclude the separately launched Ray cluster workers",
            "unique_jobs_expected": total,
            "formal_samples_expected": total * args.samples,
            "warmups_expected": total,
        }
        save(out / "manifest.json", manifest)
        status("path-preflight")
        with (out / "path-preflight.log").open("w") as log:
            subprocess.run(
                [
                    "strace",
                    "-f",
                    "-e",
                    "trace=openat,fsync,fdatasync",
                    "-o",
                    str(out / "path-preflight.strace"),
                    *writer_command("candidate"),
                ],
                env=writer_env("shared", "oneshot-small", "none", 1),
                stdout=log,
                stderr=subprocess.STDOUT,
                check=True,
                timeout=600,
            )
        opened = [
            line
            for line in (out / "path-preflight.strace").read_text().splitlines()
            if str(roots["shared"]) in line and "O_CREAT" in line
        ]
        assert opened and all(" = -1 " not in line for line in opened)
        save(
            out / "path-preflight.json",
            {"created_files": len(opened), "examples": opened[:3], "included_in_statistics": False},
        )

        for cycle in range(args.cycles):
            for case, compression in WRITERS:
                for position, condition in enumerate(ORDER):
                    version, disk = condition.split("-", 1)
                    run_job(
                        "writer",
                        case,
                        compression,
                        cycle,
                        position,
                        writer_command(version),
                        writer_env(disk, case, compression, args.samples),
                    )

        import ray
        from ray.cluster_utils import Cluster
        from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

        status("query-cluster-start")
        os.environ.update(DAFT_PROGRESS_BAR="0", RAYON_NUM_THREADS="2", OMP_NUM_THREADS="1", RAY_TMPDIR="/tmp")
        cluster = Cluster(
            initialize_head=True,
            connect=False,
            head_node_args={"num_cpus": 0, "include_dashboard": False, "object_store_memory": 256 * 1024**2},
        )
        for _ in range(2):
            cluster.add_node(num_cpus=4, object_store_memory=4 * 1024**3)
        cluster.wait_for_nodes()
        ray.init(address=cluster.address)

        @ray.remote(num_cpus=0)
        def storage_probe(path):
            with tempfile.TemporaryFile(dir=path) as file:
                file.write(b"storage probe")
                file.flush()
                os.fsync(file.fileno())
                file.seek(0)
                assert file.read() == b"storage probe"
                return {"root": path, "device": os.fstat(file.fileno()).st_dev, "passed": True}

        probes = []
        for node in ray.nodes():
            if node["Alive"]:
                for disk, path in roots.items():
                    result = ray.get(
                        storage_probe.options(
                            scheduling_strategy=NodeAffinitySchedulingStrategy(node["NodeID"], soft=False)
                        ).remote(str(path))
                    )
                    probes.append({"node": node["NodeID"], "disk": disk, **result})
        save(out / "worker-storage-probes.json", probes)
        ray.shutdown()
        for cycle in range(args.cycles):
            for compression in ["none", "lz4"]:
                for position, condition in enumerate(ORDER):
                    version, disk = condition.split("-", 1)
                    command = [
                        sys.executable,
                        str(workload),
                        "--label",
                        condition,
                        "--case",
                        "flight-gather",
                        "--samples",
                        str(args.samples),
                        "--compression",
                        compression,
                        "--address",
                        cluster.address,
                        "--root",
                        str(roots[disk]),
                    ]
                    run_job(
                        "query",
                        "flight-gather",
                        compression,
                        cycle,
                        position,
                        command,
                        {**common_env, "PYTHONPATH": str(packages[version])},
                    )
        ray.shutdown()
        cluster.shutdown()
        cluster = None
    finally:
        if cluster is not None:
            ray.shutdown()
            cluster.shutdown()
        cleanup = {}
        for disk, path in roots.items():
            assert (path / "OWNER").read_text() == str(out) + "\n"
            shutil.rmtree(path)
            cleanup[disk] = {"root": str(path), "removed": not path.exists()}
        save(out / "cleanup.json", cleanup)
    assert jobs == total
    status("summarizing", jobs=jobs)
    from abba_stats import write_summary
    from storage_stats import write_summary as write_storage_summary

    events = [json.loads(line) for line in all_events.read_text().splitlines()]
    for kind in ["writer", "query"]:
        for view, (positions, baseline, candidate) in VIEWS.items():
            destination = out / (kind + "-" + view)
            destination.mkdir()
            with (destination / "events.jsonl").open("w") as records:
                for event in events:
                    if event["kind"] == kind and event["global_position"] in positions:
                        assert event["condition"] in [baseline, candidate]
                        records.write(
                            json.dumps(
                                {
                                    **event,
                                    "position": positions.index(event["global_position"]),
                                    "label": "baseline" if event["condition"] == baseline else "candidate",
                                }
                            )
                            + "\n"
                        )
            write_summary(destination, regression_budget_pct=1)
    write_storage_summary(out)
    status("complete", jobs=jobs)


if __name__ == "__main__":
    main()
