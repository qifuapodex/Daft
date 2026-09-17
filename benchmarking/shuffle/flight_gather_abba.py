"""Compare two release wheels using a private, two-worker local Ray cluster.

Dependencies must already be installed in this Python environment. Each job
verifies that its driver and workers loaded the same extension before measuring.
Use --root to select the actual shuffle mount. Both Ray workers run on one host.
The Gather PerPartition backend does not fsync, even with shared durability set.
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

import ray
from message_buffers_stats import write_summary
from ray.cluster_utils import Cluster


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", required=True, type=Path, help="Baseline release wheel")
    parser.add_argument("--candidate", required=True, type=Path, help="Candidate release wheel")
    parser.add_argument("--output", required=True, type=Path, help="A new output directory")
    parser.add_argument("--samples", default=3, type=int)
    parser.add_argument("--cycles", default=8, type=int)
    parser.add_argument("--cpus-per-worker", default=4, type=int)
    parser.add_argument("--root", required=True, type=Path, help="Existing shuffle storage directory")
    parser.add_argument("--compression", choices=["lz4", "none"], default="lz4")
    parser.add_argument("--regression-budget-pct", type=float, default=1.0)
    args = parser.parse_args()
    if min(args.samples, args.cycles, args.cpus_per_worker) < 1:
        parser.error("sample, cycle and CPU counts must be positive")
    if not args.root.is_dir():
        parser.error("--root must be an existing writable directory")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    packages = {}
    builds = {}
    for label in ["baseline", "candidate"]:
        wheel = getattr(args, label)
        packages[label] = output / "packages" / label
        with zipfile.ZipFile(wheel) as archive:
            archive.extractall(packages[label])
        extensions = list((packages[label] / "daft").glob("daft*.so"))
        assert len(extensions) == 1, "expected one Linux Daft extension per wheel"
        builds[label] = {
            "wheel": str(wheel.resolve()),
            "sha256": hashlib.sha256(wheel.read_bytes()).hexdigest(),
            "extension_sha256": hashlib.sha256(extensions[0].read_bytes()).hexdigest(),
        }
    (output / "builds.json").write_text(json.dumps(builds, indent=2) + "\n")
    (output / "manifest.json").write_text(
        json.dumps(
            {
                "regression_budget_pct": args.regression_budget_pct,
                "cycles": args.cycles,
                "samples_per_job": args.samples,
                "warmups_per_job": 1,
                "order": ["baseline", "candidate", "candidate", "baseline"],
                "case": "flight-gather",
                "storage_root": str(args.root.resolve()),
                "compression": args.compression,
                "cpus_per_worker": args.cpus_per_worker,
            },
            indent=2,
        )
        + "\n"
    )

    workload = Path(__file__).with_name("flight_gather_collect.py").resolve()
    os.environ.update(DAFT_PROGRESS_BAR="0", RAYON_NUM_THREADS="2", OMP_NUM_THREADS="1")
    cluster = Cluster(
        initialize_head=True,
        connect=False,
        head_node_args={"num_cpus": 0, "include_dashboard": False, "object_store_memory": 256 * 1024**2},
    )
    try:
        for _ in range(2):
            cluster.add_node(
                num_cpus=args.cpus_per_worker,
                object_store_memory=4 * 1024**3,
            )
        cluster.wait_for_nodes()
        expected_nodes = None
        case = "flight-gather"
        for cycle in range(args.cycles):
            for index, label in enumerate(["baseline", "candidate", "candidate", "baseline"]):
                name = f"collect-{case}-{cycle:02d}-{index}-{label}"
                # The workload propagates this path through the Ray job's
                # runtime_env. Node identities and quotas stay fixed across ABBA.
                env = dict(os.environ, PYTHONPATH=str(packages[label]))
                with (output / f"{name}.log").open("w") as log:
                    subprocess.run(
                        [
                            "/usr/bin/time",
                            "-v",
                            "-o",
                            str(output / f"{name}.time"),
                            sys.executable,
                            str(workload),
                            "--label",
                            label,
                            "--case",
                            case,
                            "--samples",
                            str(args.samples),
                            "--compression",
                            args.compression,
                            "--root",
                            str(args.root.resolve()),
                            "--address",
                            cluster.address,
                        ],
                        cwd=output,
                        env=env,
                        stdout=log,
                        stderr=subprocess.STDOUT,
                        check=True,
                    )
                events = [
                    json.loads(line)
                    for line in (output / f"{name}.log").read_text().splitlines()
                    if line.startswith("{")
                ]
                assert [event["spilled_bytes"] for event in events if event["event"] == "object_store"] == [0]
                environment = next(event for event in events if event["event"] == "environment")
                assert environment["extension_sha256"] == builds[label]["extension_sha256"]
                assert all(
                    worker["extension_sha256"] == builds[label]["extension_sha256"] for worker in environment["workers"]
                )
                nodes = sorted(environment["nodes"], key=lambda n: n["id"])
                if expected_nodes is None:
                    expected_nodes = nodes
                assert nodes == expected_nodes, "node identities or resources changed during the comparison"
                samples = [event for event in events if event["event"] == "sample"]
                assert len(samples) == args.samples + 1
                assert all(event["oracle"] == "PASS" for event in samples)
                with (output / "events.jsonl").open("a") as records:
                    for event in events:
                        records.write(json.dumps({**event, "cycle": cycle, "position": index}) + "\n")
                print(f"Completed {name}", flush=True)
    finally:
        ray.shutdown()
        cluster.shutdown()
    write_summary(output, args.regression_budget_pct)


if __name__ == "__main__":
    main()
