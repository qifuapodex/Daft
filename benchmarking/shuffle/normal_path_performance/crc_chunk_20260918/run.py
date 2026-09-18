"""Serial, symmetric whole/512K/256K real-writer experiments on both disks.

Each pairwise projection is an ABBA cycle. Prepare all builds/tests first.
Keep every warmup and sample; the selected storage roots are never cache-cleared.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import subprocess
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from message_buffers_stats import summarize
from min_median_stats import analyze

ORDER = ["whole", "512k", "256k", "256k", "512k", "whole"]
VIEWS = {"512k": [0, 1, 4, 5], "256k": [0, 2, 3, 5]}


def sha(path):
    with Path(path).open("rb") as file:
        return hashlib.file_digest(file, "sha256").hexdigest()


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def snapshot():
    return {
        "time": time.time(),
        "loadavg": Path("/proc/loadavg").read_text().strip(),
        "cpu_stat": Path("/proc/stat").read_text().splitlines()[0],
        "memory": [
            line
            for line in Path("/proc/meminfo").read_text().splitlines()
            if line.startswith(("MemAvailable:", "Dirty:", "Writeback:"))
        ],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--builds", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--local-root", type=Path, default=Path("/tmp"))
    parser.add_argument("--shared-root", type=Path)
    parser.add_argument("--cycles", type=int, default=8)
    parser.add_argument("--samples", type=int, default=3)
    parser.add_argument("--cpu-set", default="2,3")
    parser.add_argument("--cases", nargs="+", default=["stream-wide"])
    parser.add_argument("--concurrency", nargs="+", type=int, default=[8])
    parser.add_argument("--compression", nargs="+", choices=["none", "lz4"], default=["none"])
    args = parser.parse_args()
    assert min(args.cycles, args.samples, *args.concurrency) > 0
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    builds = json.loads(args.builds.read_text())
    for variant in set(ORDER):
        assert sha(builds[variant]["path"]) == builds[variant]["sha256"]
    roots = {"local": args.local_root}
    if args.shared_root is not None:
        roots["juicefs"] = args.shared_root
    manifest = {
        "arguments": {k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
        "order": ORDER,
        "views": VIEWS,
        "builds": builds,
        "runner_sha256": sha(__file__),
        "warmups_per_job": 1,
        "platform": platform.platform(),
        "cache": "not cleared",
        "crc_retries": 6,
        "timing": "create/encode/write/close; preparation/oracle/cleanup excluded; no fsync",
        "state_before": snapshot(),
    }
    save(out / "manifest.json", manifest)
    all_events = []
    with (out / "all-events.jsonl").open("w") as records:
        for cycle in range(args.cycles):
            for case in args.cases:
                for concurrency in args.concurrency:
                    for compression in args.compression:
                        for disk, parent in roots.items():
                            with tempfile.TemporaryDirectory(prefix="daft-crc-chunk-", dir=parent) as root:
                                for position, variant in enumerate(ORDER):
                                    name = (
                                        f"{disk}-{case}-c{concurrency}-{compression}-{cycle:02d}-{position}-{variant}"
                                    )
                                    command = [
                                        "taskset",
                                        "-c",
                                        args.cpu_set,
                                        builds[variant]["path"],
                                        "bench_shuffle_writes",
                                        "--ignored",
                                        "--nocapture",
                                        "--test-threads=1",
                                    ]
                                    env = {
                                        **os.environ,
                                        "TMPDIR": root,
                                        "RAYON_NUM_THREADS": "2",
                                        "OMP_NUM_THREADS": "1",
                                        "DAFT_WRITE_CASE": case,
                                        "DAFT_WRITE_CONCURRENCY": str(concurrency),
                                        "DAFT_WRITE_COMPRESSION": compression,
                                        "DAFT_WRITE_RETRIES": "6",
                                        "DAFT_WRITE_SAMPLES": str(args.samples),
                                    }
                                    before = snapshot()
                                    save(out / "status.json", {"job": name, "state": "running", **before})
                                    with (out / (name + ".log")).open("w") as log:
                                        subprocess.run(
                                            ["/usr/bin/time", "-v", "-o", str(out / (name + ".time")), *command],
                                            env=env,
                                            stdout=log,
                                            stderr=subprocess.STDOUT,
                                            check=True,
                                            timeout=600,
                                        )
                                    lines = [
                                        line.split()
                                        for line in (out / (name + ".log")).read_text().splitlines()
                                        if line.startswith("WRITE_SAMPLE ")
                                    ]
                                    assert len(lines) == args.samples + 1
                                    assert sum(row[2] == "true" for row in lines) == 1
                                    assert not list(Path(root).iterdir()), "writer cleanup incomplete"
                                    for _, observed, warmup, trial, seconds in lines:
                                        assert observed == case
                                        event = {
                                            "event": "sample",
                                            "case": case,
                                            "disk": disk,
                                            "reader": f"c{concurrency}-{compression}",
                                            "cycle": cycle,
                                            "position": position,
                                            "variant": variant,
                                            "trial": int(trial),
                                            "warmup": warmup == "true",
                                            "seconds": float(seconds),
                                            "oracle": "PASS",
                                        }
                                        all_events.append(event)
                                        records.write(json.dumps(event) + "\n")
                                    records.flush()
                                    save(
                                        out / (name + ".json"),
                                        {"command": command, "before": before, "after": snapshot()},
                                    )
                                    print(f"Completed {name}", flush=True)
    for disk in roots:
        for variant, positions in VIEWS.items():
            events = [
                {
                    **event,
                    "position": positions.index(event["position"]),
                    "label": "baseline" if event["variant"] == "whole" else "candidate",
                }
                for event in all_events
                if event["disk"] == disk and event["position"] in positions
            ]
            directory = out / f"{disk}-{variant}"
            directory.mkdir()
            (directory / "events.jsonl").write_text("".join(json.dumps(event) + "\n" for event in events))
            summary = summarize(events)
            save(directory / "summary.json", summary)
            detailed = analyze(events, summary)
            save(directory / "min_median.json", detailed)
            for row in detailed:
                print(
                    json.dumps(
                        {
                            "disk": disk,
                            "variant": variant,
                            "case": row["case"],
                            "reader": row["reader"],
                            "change_pct": row["change_pct"],
                            "ci": row["previous_median_ci_95pct"],
                        }
                    ),
                    flush=True,
                )
    for variant in set(ORDER):
        assert sha(builds[variant]["path"]) == builds[variant]["sha256"]
    save(out / "status.json", {"state": "complete", "observations": len(all_events), **snapshot()})


if __name__ == "__main__":
    main()
