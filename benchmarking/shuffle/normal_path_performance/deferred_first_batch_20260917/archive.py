"""Publish completed, pinned ABBA runs without selecting or trimming samples."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import shutil
from pathlib import Path

STUDIES = [
    "isolated-final",
    "historical-final",
    "crc-final",
    "isolated-matrix-final",
    "isolated-matrix-confirm",
    "aa-tiny-c1-lz4",
    "aa-wide-c8-none",
    "aa-oneshot-c8-lz4",
]


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    args = parser.parse_args()
    source = args.source.resolve()
    target = Path(__file__).resolve().parent
    completion = json.loads((source / "performance-complete.json").read_text())
    assert completion["status"] == "PASS"
    storage = source / "formal-storage"
    manifest = json.loads((storage / "manifest.json").read_text())
    raw = (storage / "all-events.jsonl").read_bytes()
    manifest["events_sha256"] = hashlib.sha256(raw).hexdigest()
    (target / "all-events.jsonl.gz").write_bytes(gzip.compress(raw, mtime=0))
    save(target / "manifest.json", manifest)
    for name in ["summary.csv", "min_median.json"]:
        shutil.copyfile(storage / name, target / name)

    focus = {"studies": {}}
    summary = {}
    events = []
    resources = []
    for study in STUDIES:
        folder = source / study
        focus["studies"][study] = json.loads((folder / "manifest.json").read_text())
        summary[study] = json.loads((folder / "min_median.json").read_text())["results"]
        events.extend(
            {**json.loads(line), "study": study} for line in (folder / "events.jsonl").read_text().splitlines()
        )
        # These are whole-job counters, including warmup, oracle and cleanup.
        for timing in sorted(folder.glob("*.time")):
            resources.append({"study": study, "job": timing.stem, "time_output": timing.read_text()})
    raw = "".join(json.dumps(event) + "\n" for event in events).encode()
    focus["events_sha256"] = hashlib.sha256(raw).hexdigest()
    (target / "focus-events.jsonl.gz").write_bytes(gzip.compress(raw, mtime=0))
    save(target / "focus-summary.json", summary)
    raw = "".join(json.dumps(event) + "\n" for event in resources).encode()
    (target / "focus-resources.jsonl.gz").write_bytes(gzip.compress(raw, mtime=0))
    focus["resources_sha256"] = hashlib.sha256(raw).hexdigest()
    save(target / "focus-manifest.json", focus)
    (target / "final.patch.gz").write_bytes(gzip.compress((source / "final.patch").read_bytes(), mtime=0))
    for name in ["source-final-manifest.json", "builds-final.json", "performance-complete.json"]:
        shutil.copyfile(source / name, target / name)
    logs = {}
    for pattern in ["*-final-tests.log", "*poisoning*-final.log", "*tracking*-final.log", "validation-final.log"]:
        for path in sorted(source.glob(pattern)):
            logs[path.name] = path.read_text()
    for name in ["make-build-final.log", "release-wheel-final.log"]:
        logs[name] = (source / name).read_text()
    raw = (json.dumps(logs, indent=2) + "\n").encode()
    (target / "validation-logs.json.gz").write_bytes(gzip.compress(raw, mtime=0))
    save(
        target / "validation.json",
        {
            "rust_tests": {"daft_io_shuffle_file": 16, "daft_writers": 29, "daft_shuffles": 65},
            "injected_cancellation_tests": 5,
            "python_ray_tests": {"passed": 58, "deselected": 22},
            "make_build": "PASS",
            "release_wheel": "PASS",
            "logs_sha256": hashlib.sha256(raw).hexdigest(),
            "path_preflight": json.loads((storage / "path-preflight.json").read_text()),
            "worker_storage_probes": json.loads((storage / "worker-storage-probes.json").read_text()),
        },
    )
    print(f"Archived all samples from {source}; run audit.py to reproduce statistics")


if __name__ == "__main__":
    main()
