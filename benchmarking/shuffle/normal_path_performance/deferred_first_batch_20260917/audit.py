"""Recompute the retained small-file and full-storage ABBA statistics."""

from __future__ import annotations

import gzip
import hashlib
import json
import re
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from message_buffers_stats import summarize
from min_median_stats import analyze
from storage_stats import compute, render_csv


def main():
    folder = Path(__file__).resolve().parent
    manifest, storage = compute(folder)
    assert render_csv(storage) == (folder / "summary.csv").read_text()
    assert storage == json.loads((folder / "min_median.json").read_text())

    focus = json.loads((folder / "focus-manifest.json").read_text())
    resources = gzip.decompress((folder / "focus-resources.jsonl.gz").read_bytes())
    assert hashlib.sha256(resources).hexdigest() == focus["resources_sha256"]
    builds = json.loads((folder / "builds-final.json").read_text())
    patch = gzip.decompress((folder / "final.patch.gz").read_bytes())
    assert hashlib.sha256(patch).hexdigest() == builds["candidate"]["patch_sha256"]
    validation = json.loads((folder / "validation.json").read_text())
    logs = gzip.decompress((folder / "validation-logs.json.gz").read_bytes())
    assert hashlib.sha256(logs).hexdigest() == validation["logs_sha256"]
    test_logs = json.loads(logs)
    for package, expected in [("daft_io", 16), ("daft_writers", 29), ("daft_shuffles", 65)]:
        assert re.search(rf"test result: ok\. {expected} passed; 0 failed;", test_logs[f"{package}-final-tests.log"])
    assert "58 passed, 22 deselected" in test_logs["ray-final-tests.log"]
    injected = [name for name in test_logs if "poisoning" in name or "tracking" in name]
    assert len(injected) == validation["injected_cancellation_tests"] == 5
    assert all("test result: ok. 1 passed; 0 failed;" in test_logs[name] for name in injected)
    raw = gzip.decompress((folder / "focus-events.jsonl.gz").read_bytes())
    assert hashlib.sha256(raw).hexdigest() == focus["events_sha256"]
    events = [json.loads(line) for line in raw.splitlines()]
    stored = json.loads((folder / "focus-summary.json").read_text())
    total = 0
    for study, params in focus["studies"].items():
        assert params["binaries"]["candidate"]["sha256"] == builds["candidate"]["binary"]["sha256"]
        if study.startswith("aa-"):
            assert params["binaries"]["baseline"] == params["binaries"]["candidate"]
        selected = [event for event in events if event["study"] == study]
        jobs = defaultdict(list)
        for event in selected:
            assert event["oracle"] == "PASS" and event["warmup"] == (event["trial"] == 0)
            jobs[(event["case"], event["reader"], event["cycle"], event["position"])].append(event)
        expected_cells = {
            (case, f"c{c}-{compression}")
            for case in params["cases"]
            for c in params["concurrency"]
            for compression in params["compression"]
        }
        assert set(jobs) == {
            (*cell, cycle, position)
            for cell in expected_cells
            for cycle in range(params["cycles"])
            for position in range(4)
        }
        assert all(
            sorted(event["trial"] for event in job) == list(range(params["samples"] + 1)) for job in jobs.values()
        )
        assert all(event["label"] == params["order"][event["position"]] for event in selected)
        assert analyze(selected, summarize(selected)) == stored[study]
        total += sum(not event["warmup"] for event in selected)
    print(
        f"PASS: {total} focused/isolated formal samples and {manifest['formal_samples_expected']} "
        f"storage/query formal samples; all warmups retained; min/median/max reproduced"
    )


if __name__ == "__main__":
    main()
