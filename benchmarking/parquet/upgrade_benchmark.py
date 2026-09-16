"""Reproducible native Parquet upgrade benchmarks with isolated, alternating workers.

Run ``--help`` for commands. Inputs and validation use PyArrow; only Daft execution
is timed. Keep builds and other benchmarks idle while measuring. Each worker owns
one immutable package snapshot, so alternating versions does not rebuild or import
different extension modules into the same Python process.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import random
import resource
import shutil
import statistics
import subprocess
import sys
import time
from pathlib import Path

CASES = [
    "write_strings_uncompressed",
    "write_strings_snappy",
    "write_strings_zstd",
    "write_dictionary_snappy",
    "write_nested_snappy",
    "write_wide_snappy",
    "read_full_uncompressed",
    "read_full_snappy",
    "read_full_zstd",
    "read_projection",
    "read_split_row_groups",
    "read_selective",
    "read_fragmented",
    "read_limit",
    "read_small_files",
]


def emit(value):
    print(json.dumps(value), flush=True)


def prepare(directory):
    import numpy as np
    import pyarrow as pa
    import pyarrow.parquet as pq

    directory.mkdir(parents=True, exist_ok=False)
    rng = np.random.default_rng(1729)
    n = 262_144
    ids = np.arange(n, dtype=np.int64)
    # Independent pseudo-random strings, with reproducible 128-byte payloads.
    strings = [row.tobytes().hex() for row in rng.integers(0, 256, (n, 64), dtype=np.uint8)]
    table = pa.table({"id": ids, "key": ids % 65_536, "payload": strings, "value": rng.random(n)})
    for codec in ("uncompressed", "snappy", "zstd"):
        pq.write_table(
            table,
            directory / f"strings_{codec}.parquet",
            compression="NONE" if codec == "uncompressed" else codec,
            compression_level=1 if codec == "zstd" else None,
            row_group_size=65_536,
            data_page_size=32_768,
            write_batch_size=256,
            write_page_index=True,
        )
    pq.write_table(
        pa.table({"id": ids, "payload": [f"category-{i % 256:04d}" for i in ids]}), directory / "dictionary.parquet"
    )
    lists = pa.ListArray.from_arrays(
        pa.array(np.arange(0, n * 4 + 1, 4, dtype=np.int32)), pa.array(rng.integers(0, 10_000, n * 4, dtype=np.int64))
    )
    pq.write_table(pa.table({"id": ids, "items": lists}), directory / "nested.parquet")
    pq.write_table(
        pa.table({f"c{i}": rng.integers(0, 100_000, 32_768, dtype=np.int64) for i in range(128)}),
        directory / "wide.parquet",
    )
    # Small projected reads from a wide footer isolate redundant metadata work.
    pq.write_table(
        pa.table({f"c{i}": np.arange(16_384, dtype=np.int64) + i for i in range(128)}),
        directory / "wide_split.parquet",
        row_group_size=256,
        write_page_index=True,
    )
    small = directory / "small"
    small.mkdir()
    for i in range(32):
        pq.write_table(
            table.slice(i * 1024, 1024), small / f"part-{i:03d}.parquet", row_group_size=256, write_page_index=True
        )
    manifest = {
        str(p.relative_to(directory)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(directory.rglob("*.parquet"))
    }
    (directory / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")


def snapshot(repo, destination, label):
    destination.mkdir(parents=True, exist_ok=False)
    shutil.copytree(repo / "daft", destination / "daft", ignore=shutil.ignore_patterns("__pycache__"))
    import tomllib

    lock = tomllib.loads((repo / "Cargo.lock").read_text())
    metadata = {
        "label": label,
        "commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip(),
        "diff": subprocess.check_output(["git", "diff", "HEAD"], cwd=repo, text=True),
        "arrow_versions": {
            p["name"]: p["version"] for p in lock["package"] if p["name"].startswith("arrow") or p["name"] == "parquet"
        },
        "rustc": subprocess.check_output(["rustc", "-Vv"], cwd=repo, text=True),
        "extension_sha256": {
            p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in (destination / "daft").glob("*.so")
        },
        "build": "CARGO_BUILD_JOBS=8 make build-release",
    }
    assert metadata["extension_sha256"], "Build the release extension before snapshotting"
    (destination / "build.json").write_text(json.dumps(metadata, indent=2) + "\n")


def io_counters():
    path = Path("/proc/self/io")
    if not path.exists():
        return {}
    return {k: int(v) for k, v in (line.split(":") for line in path.read_text().splitlines())}


def worker(args):
    sys.path.insert(0, str(args.package.resolve()))
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    import daft

    assert Path(daft.__file__).resolve().is_relative_to(args.package.resolve())
    daft.set_execution_config(local_write_buffer_size_bytes=4 * 1024 * 1024)
    pa.set_cpu_count(4)
    case = args.case
    if case == "read_split_row_groups":
        daft.set_execution_config(scan_tasks_min_size_bytes=1, scan_tasks_max_size_bytes=1)
    out = args.output / f"{args.package.name}-{case}.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    if case.startswith("write_"):
        _, shape, codec = case.split("_")
        filename = "strings_uncompressed" if shape == "strings" else shape
        expected = pq.read_table(args.data / f"{filename}.parquet")
        df = daft.from_arrow(expected).collect()

        def execute():
            return df.write_parquet(str(out), compression=codec, single_file=True)

        def validate(result):
            actual = pq.read_table(out)
            assert actual.cast(expected.schema).equals(expected), case

        byte_count = expected.nbytes
    else:
        filename = "strings_" + case.removeprefix("read_full_") if case.startswith("read_full_") else "strings_snappy"
        if case == "read_split_row_groups":
            filename = "wide_split"
        paths = [args.data / f"{filename}.parquet"]
        if case == "read_small_files":
            paths = sorted((args.data / "small").glob("*.parquet"))
        expected = pq.read_table([str(p) for p in paths])
        if case == "read_projection":
            expected = expected.select(["id", "value"])
        elif case == "read_split_row_groups":
            expected = expected.select(["c0"])
        elif case == "read_selective":
            expected = expected.filter(pc.less(expected["key"], 512))
        elif case == "read_fragmented":
            expected = expected.filter(pa.array([(i % 65_536) % 97 == 0 for i in range(len(expected))]))
        elif case == "read_limit":
            expected = expected.slice(0, 100)

        def execute():
            df = daft.read_parquet([str(p) for p in paths])
            if case == "read_projection":
                df = df.select("id", "value")
            elif case == "read_split_row_groups":
                df = df.select("c0")
            elif case == "read_selective":
                df = df.where(daft.col("key") < 512)
            elif case == "read_fragmented":
                df = df.where(daft.col("key") % 97 == 0)
            elif case == "read_limit":
                df = df.limit(100)
            return df.collect()

        def validate(result):
            actual = result.to_arrow().cast(expected.schema)
            if case in ("read_small_files", "read_split_row_groups"):
                actual = actual.sort_by("c0" if case == "read_split_row_groups" else "id")
            assert actual.equals(expected), case

        byte_count = sum(p.stat().st_size for p in paths)
    emit({"ready": True, "case": case, "input_bytes": byte_count, "expected_rows": len(expected)})
    for command in sys.stdin:
        if command.strip() == "stop":
            return
        if out.exists():
            out.unlink()
        before_io = io_counters()
        before_cpu = time.process_time_ns()
        start = time.perf_counter_ns()
        result = execute()
        elapsed = time.perf_counter_ns() - start
        cpu = time.process_time_ns() - before_cpu
        after_io = io_counters()
        # Conversion and full independent validation are outside the timed region.
        validate(result)
        emit(
            {
                "wall_ns": elapsed,
                "cpu_ns": cpu,
                "peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                "io": {k: after_io[k] - before_io[k] for k in before_io},
                "output_bytes": out.stat().st_size if out.exists() else None,
                "warmup": command.strip() == "warmup",
            }
        )
        del result
        if out.exists():
            out.unlink()


def run(args):
    variants = [Path(p).resolve() for p in args.variants]
    cases = args.cases or CASES
    env = dict(
        os.environ,
        DAFT_RUNNER="native",
        DAFT_PROGRESS_BAR="0",
        DAFT_ANALYTICS_ENABLED="0",
        RAYON_NUM_THREADS="4",
        OMP_NUM_THREADS="4",
        OPENBLAS_NUM_THREADS="4",
    )
    metadata = {
        "platform": platform.platform(),
        "python": sys.version,
        "affinity": sorted(os.sched_getaffinity(0)),
        "warmups": args.warmups,
        "samples_per_variant": args.samples,
        "cache": "warm OS cache; no drop_caches or fsync",
        "measurement": "plan construction + collect / write_parquet return; validation outside timing",
        "rss": "process lifetime peak including input and validation, not operation-only peak",
        "io": "process /proc/self/io deltas, not target-file-only physical IO",
        "variants": [json.loads((v / "build.json").read_text()) for v in variants],
        "input_manifest": json.loads((args.data / "manifest.json").read_text()),
        "environment": {
            k: env[k] for k in ("DAFT_RUNNER", "RAYON_NUM_THREADS", "OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS")
        },
    }
    args.results.parent.mkdir(parents=True, exist_ok=True)
    args.results.with_suffix(".environment.json").write_text(json.dumps(metadata, indent=2) + "\n")
    with args.results.open("x") as output:
        for case in cases:
            workers = []
            try:
                for variant in variants:
                    cmd = [
                        sys.executable,
                        str(Path(__file__).resolve()),
                        "worker",
                        "--package",
                        str(variant),
                        "--case",
                        case,
                        "--data",
                        str(args.data.resolve()),
                        "--output",
                        str(args.output.resolve()),
                    ]
                    proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, env=env)
                    workers.append(proc)
                    ready = json.loads(proc.stdout.readline())
                    assert ready["ready"]
                for phase, rounds in (("warmup", args.warmups), ("sample", args.samples)):
                    for sample in range(rounds):
                        # Two variants give ABBA ABBA ...; all measurements run sequentially.
                        order = range(len(workers)) if sample % 2 == 0 else reversed(range(len(workers)))
                        for i in order:
                            proc = workers[i]
                            proc.stdin.write(phase + "\n")
                            proc.stdin.flush()
                            row = json.loads(proc.stdout.readline())
                            row.update(variant=variants[i].name, case=case, sample=sample)
                            output.write(json.dumps(row) + "\n")
                            output.flush()
                print(f"Completed {case}", flush=True)
            finally:
                for proc in workers:
                    if proc.poll() is None:
                        proc.stdin.write("stop\n")
                        proc.stdin.flush()
                for proc in workers:
                    assert proc.wait() == 0, f"Benchmark worker failed for {case}"


def summarize(paths):
    groups = {}
    for path in paths:
        for line in Path(path).read_text().splitlines():
            row = json.loads(line)
            if not row["warmup"]:
                groups.setdefault((row["case"], row["variant"]), []).append(row)
    print("case,variant,n,wall_median_ms,wall_min_ms,wall_max_ms,cpu_median_ms,rchar_median,peak_rss_kib")
    for (case, variant), rows in sorted(groups.items()):
        wall = [r["wall_ns"] / 1e6 for r in rows]
        print(
            f"{case},{variant},{len(rows)},{statistics.median(wall):.3f},{min(wall):.3f},{max(wall):.3f},"
            f"{statistics.median(r['cpu_ns'] / 1e6 for r in rows):.3f},"
            f"{statistics.median(r['io'].get('rchar', 0) for r in rows):.0f},"
            f"{max(r['peak_rss_kib'] for r in rows)}"
        )


def compare(path, baseline, candidate):
    groups = {}
    for line in Path(path).read_text().splitlines():
        row = json.loads(line)
        if not row["warmup"]:
            groups.setdefault(row["case"], {}).setdefault(row["variant"], {})[row["sample"]] = row
    rng = random.Random(1729)
    print("case,wall_change_pct,bootstrap_95_lo,bootstrap_95_hi,cpu_change_pct,rchar_change_pct")
    for case, variants in sorted(groups.items()):
        before, after = variants[baseline], variants[candidate]
        samples = sorted(before.keys() & after.keys())

        def change(indices, metric, before=before, after=after):
            a = statistics.median(metric(before[i]) for i in indices)
            b = statistics.median(metric(after[i]) for i in indices)
            return 100 * (b / a - 1) if a else 0

        # Paired resampling keeps the alternating rounds together. The interval
        # describes sample variation, not all system or workload uncertainty.
        boot = sorted(change(rng.choices(samples, k=len(samples)), lambda r: r["wall_ns"]) for _ in range(2000))
        wall = change(samples, lambda r: r["wall_ns"])
        cpu = change(samples, lambda r: r["cpu_ns"])
        rchar = change(samples, lambda r: r["io"].get("rchar", 0))
        print(f"{case},{wall:.2f},{boot[50]:.2f},{boot[1949]:.2f},{cpu:.2f},{rchar:.2f}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("prepare")
    p.add_argument("data", type=Path)
    p = sub.add_parser("snapshot")
    p.add_argument("destination", type=Path)
    p.add_argument("--repo", type=Path, default=Path.cwd())
    p.add_argument("--label", required=True)
    p = sub.add_parser("run")
    p.add_argument("--variants", nargs="+", required=True)
    p.add_argument("--data", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--results", type=Path, required=True)
    p.add_argument("--samples", type=int, default=10)
    p.add_argument("--warmups", type=int, default=2)
    p.add_argument("--cases", nargs="+", choices=CASES)
    p = sub.add_parser("worker")
    p.add_argument("--package", type=Path, required=True)
    p.add_argument("--data", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--case", choices=CASES, required=True)
    p = sub.add_parser("summarize")
    p.add_argument("paths", nargs="+")
    p = sub.add_parser("compare")
    p.add_argument("path")
    p.add_argument("baseline")
    p.add_argument("candidate")
    args = parser.parse_args()
    if args.command == "prepare":
        prepare(args.data)
    elif args.command == "snapshot":
        snapshot(args.repo, args.destination, args.label)
    elif args.command == "worker":
        worker(args)
    elif args.command == "run":
        run(args)
    elif args.command == "compare":
        compare(args.path, args.baseline, args.candidate)
    else:
        summarize(args.paths)


if __name__ == "__main__":
    main()
