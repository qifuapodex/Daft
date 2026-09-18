"""Audit archived symmetric Gather samples without requiring build artifacts."""

from __future__ import annotations

import gzip
import hashlib
import importlib
import json
import sys
from pathlib import Path

root = Path(sys.argv[1])


def read(name):
    path = root / name
    return (
        path.read_text()
        if path.exists()
        else gzip.decompress(path.with_suffix(path.suffix + ".gz").read_bytes()).decode()
    )


manifest = json.loads(read("manifest.json"))
events = [json.loads(s) for s in read("events.jsonl").splitlines()]
cycles = manifest["cycles"]
order = manifest["order"]
n = manifest["samples_per_job"]
assert order == order[::-1]
for cycle in range(cycles):
    for pos, (disk, version) in enumerate(order):
        records = [e for e in events if e["cycle"] == cycle and e["position"] == pos]
        assert records and all(e["disk"] == disk and e["version"] == version for e in records)
        samples = [e for e in records if e["event"] == "sample"]
        assert len(samples) == n + 1
        assert [e["trial"] for e in samples] == list(range(n + 1))
        assert [e["warmup"] for e in samples] == [True] + [False] * n
        assert all(e["oracle"] == "PASS" and not e["profiled"] for e in samples)
        assert all(e["label"] == version for e in samples)
        assert [e["spilled_bytes"] for e in records if e["event"] == "object_store"] == [0]
        (env,) = [e for e in records if e["event"] == "environment"]
        expected = manifest["builds"][version]["extension_sha256"]
        assert env["extension_sha256"] == expected
        assert all(w["extension_sha256"] == expected for w in env["workers"])
        assert all(w["cpu_affinity"] == manifest["cpu_affinity"] for w in env["workers"])
        (config,) = [e for e in records if e["event"] == "configuration"]
        if version != "old":
            assert config["settings"]["flight_shuffle_eio_local_max_retries"] == 6
assert len([e for e in events if e["event"] == "sample"]) == cycles * len(order) * (n + 1)
for field, name in [("runner_sha256", "run_screen.py"), ("workload_sha256", "flight_gather_collect.py")]:
    assert hashlib.sha256(read(name).encode()).hexdigest() == manifest[field]
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
summarize = importlib.import_module("message_buffers_stats").summarize
analyze = importlib.import_module("min_median_stats").analyze
saved = json.loads(read("summary.json"))
for disk in manifest["roots"]:
    for baseline in ["old", "current"]:
        positions = [i for i, (d, v) in enumerate(order) if d == disk and v in [baseline, "final"]]
        selected = [
            {
                **e,
                "label": "baseline" if e["version"] == baseline else "candidate",
                "position": positions.index(e["position"]),
            }
            for e in events
            if e["disk"] == disk and e["version"] in [baseline, "final"]
        ]
        assert analyze(selected, summarize(selected)) == saved[disk + "-" + baseline]
print(
    json.dumps(
        {
            "jobs": cycles * len(order),
            "formal_samples": cycles * len(order) * n,
            "warmups": cycles * len(order),
            "audit": "PASS",
        }
    )
)
