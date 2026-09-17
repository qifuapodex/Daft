"""Recompute all published min/median/max and paired-cycle intervals."""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import io
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from message_buffers_stats import summarize
from min_median_stats import analyze

OUT = Path(__file__).resolve().parent
ORDER = ["old-local", "v7-local", "old-juicefs", "v7-juicefs", "v7-juicefs", "old-juicefs", "v7-local", "old-local"]
VIEWS = {
    "versions-local": ([0, 1, 6, 7], "old-local", "v7-local"),
    "versions-juicefs": ([2, 3, 4, 5], "old-juicefs", "v7-juicefs"),
    "disks-old": ([0, 2, 5, 7], "old-local", "old-juicefs"),
    "disks-v7": ([1, 3, 4, 6], "v7-local", "v7-juicefs"),
}


def compute():
    manifest = json.loads((OUT / "evidence.json").read_text())
    packed = (OUT / "samples.jsonl.gz").read_bytes()
    assert hashlib.sha256(packed).hexdigest() == manifest["samples_sha256"]
    events = [json.loads(line) for line in gzip.decompress(packed).splitlines()]
    assert sum(not e["warmup"] for e in events) == manifest["formal_samples"]
    assert sum(e["warmup"] for e in events) == manifest["warmups"]
    assert all(e["oracle"] == "PASS" and e["warmup"] == (e["trial"] == 0) for e in events)
    groups = {}
    for wave, source in manifest["sources"].items():
        selected = [e for e in events if e["wave"] == wave]
        assert sum(not e["warmup"] for e in selected) == source["formal_samples"]
        assert sum(e["warmup"] for e in selected) == source["warmups"]
        assert {e["cycle"] for e in selected} == set(range(8))
        if not wave.startswith("juicefs-"):
            groups[wave] = selected
            continue
        for kind in sorted({e["kind"] for e in selected}):
            for view, (positions, baseline, candidate) in VIEWS.items():
                rows = []
                for event in selected:
                    assert event["condition"] == ORDER[event["global_position"]]
                    if event["kind"] != kind or event["global_position"] not in positions:
                        continue
                    assert event["condition"] in {baseline, candidate}
                    rows.append(
                        {
                            **event,
                            "position": positions.index(event["global_position"]),
                            "label": "baseline" if event["condition"] == baseline else "candidate",
                        }
                    )
                groups[f"{wave}/{kind}/{view}"] = rows
    # summarize checks complete ABBA cycles and equal sample counts. analyze
    # independently recomputes the minima, medians, maxima, counts and variance.
    result = {name: analyze(rows, summarize(rows, regression_budget_pct=1)) for name, rows in sorted(groups.items())}
    assert len(result) == 14
    return result


def render_csv(result):
    output = io.StringIO()
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(
        [
            "comparison",
            "case",
            "reader",
            "n_each",
            "baseline_min_s",
            "candidate_min_s",
            "min_change_pct",
            "baseline_median_s",
            "candidate_median_s",
            "median_change_pct",
            "baseline_max_s",
            "candidate_max_s",
            "baseline_cv_pct",
            "candidate_cv_pct",
            "median_ci_lower_pct",
            "median_ci_upper_pct",
            "median_ci_gate",
        ]
    )
    for name, rows in result.items():
        for row in rows:
            a, b = row["baseline"], row["candidate"]
            assert a["n"] == b["n"]
            numeric = [
                a["min_s"],
                b["min_s"],
                row["change_pct"]["min"],
                a["median_s"],
                b["median_s"],
                row["change_pct"]["median"],
                a["max_s"],
                b["max_s"],
                a["cv_pct"],
                b["cv_pct"],
                *row["previous_median_ci_95pct"],
            ]
            writer.writerow(
                [
                    name,
                    row["case"],
                    row["reader"],
                    a["n"],
                    *[f"{v:.9g}" for v in numeric],
                    row["previous_median_ci_gate"],
                ]
            )
    return output.getvalue()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true", help="Regenerate the summary from the retained samples")
    args = parser.parse_args()
    result = compute()
    destination = OUT / "summary.csv"
    rendered = render_csv(result)
    if args.write:
        destination.write_text(rendered)
    else:
        assert rendered == destination.read_text(), "published statistics differ from retained samples"
    print(f"PASS: 6,400 retained samples; {len(result)} comparisons; ABBA order and all statistics matched")


if __name__ == "__main__":
    main()
