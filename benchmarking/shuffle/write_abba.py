"""Real IPC writers, pinned release binaries, repeated ABBA with a 1% budget.

The historical source uses the identical test module with only configure() a
no-op, because the pre-EIO version has no local retry policy. Retain warmups and
every observation. CPU pinning applies equally to all threads of both binaries.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import subprocess
from itertools import product
from pathlib import Path

from message_buffers_stats import write_summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", required=True, type=Path)
    parser.add_argument("--candidate", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--cycles", type=int, default=8)
    parser.add_argument("--samples", type=int, default=6)
    parser.add_argument("--cpu-set", default="2,3")
    parser.add_argument(
        "--cases", nargs="+", default=["oneshot-small", "oneshot-wide", "stream-single", "stream-tiny", "stream-wide"]
    )
    parser.add_argument("--concurrency", nargs="+", type=int, default=[1, 8])
    parser.add_argument("--compression", nargs="+", choices=["none", "lz4"], default=["none", "lz4"])
    parser.add_argument("--retries", type=int, default=6)
    parser.add_argument("--baseline-retries", type=int, help="Diagnostic control using the same candidate binary")
    args = parser.parse_args()
    if min(args.cycles, args.samples, *args.concurrency) < 1:
        parser.error("counts must be positive")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    binaries = {label: getattr(args, label).resolve() for label in ["baseline", "candidate"]}
    order = ["baseline", "candidate", "candidate", "baseline"]
    manifest = {
        **{k: v for k, v in vars(args).items() if k not in {"baseline", "candidate", "output"}},
        "order": order,
        "warmups_per_job": 1,
        "regression_budget_pct": 1.0,
        "cache": "warm; not cleared",
        "platform": platform.platform(),
        "binaries": {
            label: {"path": str(binary), "sha256": hashlib.sha256(binary.read_bytes()).hexdigest()}
            for label, binary in binaries.items()
        },
    }
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    with (output / "events.jsonl").open("w") as records:
        for cycle, case, concurrency, compression in product(
            range(args.cycles), args.cases, args.concurrency, args.compression
        ):
            for position, label in enumerate(order):
                name = f"{case}-c{concurrency}-{compression}-{cycle:02d}-{position}-{label}"
                command = [str(binaries[label]), "bench_shuffle_writes", "--ignored", "--nocapture", "--test-threads=1"]
                if args.cpu_set:
                    command = ["taskset", "-c", args.cpu_set, *command]
                env = {
                    **os.environ,
                    "DAFT_WRITE_CASE": case,
                    "DAFT_WRITE_CONCURRENCY": str(concurrency),
                    "DAFT_WRITE_COMPRESSION": compression,
                    "DAFT_WRITE_RETRIES": str(
                        args.baseline_retries
                        if label == "baseline" and args.baseline_retries is not None
                        else args.retries
                    ),
                    "DAFT_WRITE_SAMPLES": str(args.samples),
                    "RAYON_NUM_THREADS": "2",
                    "OMP_NUM_THREADS": "1",
                }
                with (output / f"{name}.log").open("w") as log:
                    subprocess.run(
                        ["/usr/bin/time", "-v", "-o", str(output / f"{name}.time"), *command],
                        env=env,
                        stdout=log,
                        stderr=subprocess.STDOUT,
                        check=True,
                    )
                lines = [
                    line.split()
                    for line in (output / f"{name}.log").read_text().splitlines()
                    if line.startswith("WRITE_SAMPLE ")
                ]
                assert len(lines) == args.samples + 1
                assert sum(line[2] == "true" for line in lines) == 1
                for _, observed_case, warmup, trial, seconds in lines:
                    assert observed_case == case
                    records.write(
                        json.dumps(
                            {
                                "event": "sample",
                                "case": case,
                                "reader": f"c{concurrency}-{compression}",
                                "cycle": cycle,
                                "position": position,
                                "label": label,
                                "warmup": warmup == "true",
                                "trial": int(trial),
                                "seconds": float(seconds),
                                "oracle": "PASS",
                            }
                        )
                        + "\n"
                    )
                records.flush()
                print(f"Completed {name}", flush=True)
    write_summary(output, regression_budget_pct=1.0)


if __name__ == "__main__":
    main()
