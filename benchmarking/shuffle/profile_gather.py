"""Profile collect only, including descendant Ray workers on a private cluster.

These instrumented observations are diagnostics, never wall-clock acceptance.
The whole cluster starts below perf so worker processes/threads inherit events.
The workload enables events after warmup and disables them before each oracle.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import zipfile
from pathlib import Path


def sha(path):
    with Path(path).open("rb") as file:
        return hashlib.file_digest(file, "sha256").hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wheel", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--root", default=Path("/tmp"), type=Path)
    parser.add_argument("--samples", type=int, default=32)
    parser.add_argument("--compression", choices=["none", "lz4"], default="none")
    parser.add_argument("--mode", choices=["record", "stat"], default="record")
    parser.add_argument("--under-perf", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    out = args.output.resolve()
    assert args.samples > 0 and args.root.is_dir()
    if args.under_perf:
        import ray
        from ray.cluster_utils import Cluster

        cluster = Cluster(
            initialize_head=True,
            connect=False,
            head_node_args={"num_cpus": 0, "include_dashboard": False, "object_store_memory": 256 * 1024**2},
        )
        try:
            for _ in range(2):
                cluster.add_node(num_cpus=4, object_store_memory=4 * 1024**3)
            cluster.wait_for_nodes()
            subprocess.run(
                [
                    sys.executable,
                    str(Path(__file__).with_name("flight_gather_collect.py")),
                    "--label",
                    "diagnostic",
                    "--case",
                    "flight-gather",
                    "--samples",
                    str(args.samples),
                    "--compression",
                    args.compression,
                    "--root",
                    str(args.root.resolve()),
                    "--address",
                    cluster.address,
                    "--perf-control",
                    str(out / "control"),
                    "--perf-ack",
                    str(out / "ack"),
                ],
                cwd=out,
                check=True,
            )
        finally:
            ray.shutdown()
            cluster.shutdown()
        return

    out.mkdir(parents=True, exist_ok=False)
    wheel = args.wheel.resolve()
    with zipfile.ZipFile(wheel) as archive:
        archive.extractall(out / "package")
    extensions = list((out / "package" / "daft").glob("daft*.so"))
    assert len(extensions) == 1
    expected = sha(extensions[0])
    for pipe in ["control", "ack"]:
        os.mkfifo(out / pipe)
    control = f"--control=fifo:{out / 'control'},{out / 'ack'}"
    if args.mode == "record":
        perf = [
            "perf",
            "record",
            "-q",
            "-F",
            "199",
            "-e",
            "cycles",
            "--call-graph",
            "dwarf,8192",
            "--delay=-1",
            "--no-buildid",
            "--no-buildid-cache",
            control,
            "-o",
            str(out / "perf.data"),
        ]
    else:
        perf = [
            "perf",
            "stat",
            "-x,",
            "-e",
            "cycles,instructions,task-clock,context-switches",
            "--delay=-1",
            control,
            "-o",
            str(out / "counters.csv"),
        ]
    command = [
        *perf,
        "--",
        sys.executable,
        str(Path(__file__).resolve()),
        "--wheel",
        str(wheel),
        "--output",
        str(out),
        "--root",
        str(args.root.resolve()),
        "--samples",
        str(args.samples),
        "--compression",
        args.compression,
        "--mode",
        args.mode,
        "--under-perf",
    ]
    manifest = {
        "command": command,
        "wheel_sha256": sha(wheel),
        "extension_sha256": expected,
        "runner_sha256": sha(__file__),
        "workload_sha256": sha(Path(__file__).with_name("flight_gather_collect.py")),
        "scope": "private cluster descendants, collect after warmup; no oracle/setup/cleanup",
        "acceptance": False,
        "samples": args.samples,
        "root": str(args.root.resolve()),
    }
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    env = {
        **os.environ,
        "PYTHONPATH": str(out / "package"),
        "RAYON_NUM_THREADS": "2",
        "OMP_NUM_THREADS": "1",
        "DAFT_PROGRESS_BAR": "0",
        "RAY_USAGE_STATS_ENABLED": "0",
    }
    with (out / "collect.log").open("w") as log:
        subprocess.run(command, cwd=out, env=env, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=600)
    events = [json.loads(line) for line in (out / "collect.log").read_text().splitlines() if line.startswith("{")]
    samples = [e for e in events if e["event"] == "sample"]
    assert len(samples) == args.samples + 1 and sum(e["profiled"] for e in samples) == args.samples
    assert all(e["oracle"] == "PASS" for e in samples)
    environment = next(e for e in events if e["event"] == "environment")
    assert environment["extension_sha256"] == expected
    assert all(e["extension_sha256"] == expected for e in environment["workers"])
    assert [e["spilled_bytes"] for e in events if e["event"] == "object_store"] == [0]
    if args.mode == "record":
        with (out / "report.txt").open("w") as report:
            subprocess.run(
                [
                    "perf",
                    "report",
                    "--stdio",
                    "--no-children",
                    "--no-inline",
                    "--call-graph",
                    "none",
                    "--sort",
                    "comm,dso,symbol",
                    "--percent-limit",
                    "0.25",
                    "-i",
                    str(out / "perf.data"),
                ],
                stdout=report,
                check=True,
                timeout=180,
            )
    (out / "events.jsonl").write_text("".join(json.dumps(e) + "\n" for e in events))
    print(json.dumps({"status": "PASS", "output": str(out), "profiled_collects": args.samples}), flush=True)


if __name__ == "__main__":
    main()
