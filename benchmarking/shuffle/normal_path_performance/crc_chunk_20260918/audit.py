"""Recompute both ABBA projections from the complete retained sample stream."""

from __future__ import annotations

import argparse
import gzip
import json
import math
import sys
from collections import defaultdict
from itertools import product
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from message_buffers_stats import summarize
from min_median_stats import analyze

ORDER = ["whole", "512k", "256k", "256k", "512k", "whole"]
VIEWS = {"512k": [0, 1, 4, 5], "256k": [0, 2, 3, 5]}


def audit(root):
    manifest = json.loads((root / "manifest.json").read_text())
    args = manifest["arguments"]
    assert manifest["order"] == ORDER and manifest["views"] == VIEWS
    raw = root / "all-events.jsonl"
    text = raw.read_text() if raw.exists() else gzip.decompress(raw.with_suffix(".jsonl.gz").read_bytes()).decode()
    events = [json.loads(line) for line in text.splitlines()]
    jobs = defaultdict(list)
    disks = ["local", "juicefs"] if args["shared_root"] is not None else ["local"]
    for e in events:
        assert e["event"] == "sample" and e["oracle"] == "PASS"
        assert math.isfinite(e["seconds"]) and e["seconds"] > 0
        assert e["variant"] == ORDER[e["position"]]
        assert e["disk"] in disks and e["case"] in args["cases"]
        assert 0 <= e["cycle"] < args["cycles"]
        jobs[e["disk"], e["case"], e["reader"], e["cycle"], e["position"]].append(e)
    readers = [f"c{c}-{compression}" for c, compression in product(args["concurrency"], args["compression"])]
    expected_jobs = set(product(disks, args["cases"], readers, range(args["cycles"]), range(6)))
    assert set(jobs) == expected_jobs
    for job in jobs.values():
        assert [e["trial"] for e in job] == list(range(args["samples"] + 1))
        assert [e["warmup"] for e in job] == [True] + [False] * args["samples"]
    rows = []
    for disk in disks:
        for variant, positions in VIEWS.items():
            projected = [
                {
                    **e,
                    "position": positions.index(e["position"]),
                    "label": "baseline" if e["variant"] == "whole" else "candidate",
                }
                for e in events
                if e["disk"] == disk and e["position"] in positions
            ]
            summary = summarize(projected)
            detailed = analyze(projected, summary)
            directory = root / f"{disk}-{variant}"
            assert summary == json.loads((directory / "summary.json").read_text())
            assert detailed == json.loads((directory / "min_median.json").read_text())
            rows.extend({"disk": disk, "variant": variant, **row} for row in detailed)
    return {
        "status": "PASS",
        "jobs": len(jobs),
        "formal_samples": sum(not e["warmup"] for e in events),
        "warmups": sum(e["warmup"] for e in events),
        "rows": rows,
        "meaning": "sample/statistic consistency only; not performance acceptance",
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    result = audit(args.root)
    if args.write:
        (args.root / "audit.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({key: value for key, value in result.items() if key != "rows"}))
