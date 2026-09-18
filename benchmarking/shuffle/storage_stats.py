"""Audit storage_abba output and report min/median/max without trimming samples.

Accepts either the original all-events.jsonl or its published gzip archive.
Bootstrap resampling uses whole paired cycles. Observed minima are descriptive;
the median interval does not replace the separate minimum comparison.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import io
import json
from collections import defaultdict
from itertools import pairwise
from pathlib import Path

from abba_stats import summarize
from min_median_stats import analyze
from storage_abba import ORDER, VIEWS


def compute(folder):
    manifest = json.loads((folder / "manifest.json").read_text())
    source = folder / "all-events.jsonl"
    if source.exists():
        raw = source.read_bytes()
    else:
        raw = gzip.decompress(source.with_suffix(".jsonl.gz").read_bytes())
    if "events_sha256" in manifest:
        assert hashlib.sha256(raw).hexdigest() == manifest["events_sha256"]
    events = [json.loads(line) for line in raw.splitlines()]
    assert manifest["order"] == ORDER
    jobs = [e for e in events if e["event"] == "job"]
    assert len(jobs) == manifest["unique_jobs_expected"]
    assert len({job["name"] for job in jobs}) == len(jobs)
    assert all(left["end_time"] <= right["start_time"] for left, right in pairwise(jobs))
    samples = [e for e in events if e["event"] == "sample"]
    assert sum(not e["warmup"] for e in samples) == manifest["formal_samples_expected"]
    assert sum(e["warmup"] for e in samples) == manifest["warmups_expected"]
    assert all(e["oracle"] == "PASS" and e["warmup"] == (e["trial"] == 0) for e in samples)
    groups = defaultdict(list)
    for event in samples:
        assert event["condition"] == ORDER[event["global_position"]]
        assert event["seconds"] > 0
        groups[(event["kind"], event["case"], event["reader"], event["cycle"], event["global_position"])].append(event)
    expected_cells = {("writer", case, "c8-" + compression) for case, compression in manifest["writer_cases"]}
    expected_cells |= {("query", case, "collect") for case in manifest["query_cases"]}
    assert set(groups) == {
        (*cell, cycle, position)
        for cell in expected_cells
        for cycle in range(manifest["cycles"])
        for position in range(8)
    }
    for group in groups.values():
        assert sorted(e["trial"] for e in group) == list(range(manifest["samples_per_job"] + 1))
    result = {}
    for kind in ["writer", "query"]:
        for view, (positions, baseline, candidate) in VIEWS.items():
            selected = [
                {
                    **e,
                    "position": positions.index(e["global_position"]),
                    "label": "baseline" if e["condition"] == baseline else "candidate",
                }
                for e in samples
                if e["kind"] == kind and e["global_position"] in positions
            ]
            assert all(e["condition"] in [baseline, candidate] for e in selected)
            result[kind + "/" + view] = analyze(selected, summarize(selected, manifest["regression_budget_pct"]))
    return manifest, result


def render_csv(result):
    output = io.StringIO()
    writer = csv.writer(output, lineterminator="\n")
    metrics = ["min_s", "median_s", "max_s", "sample_variance_s2", "cv_pct"]
    writer.writerow(
        [
            "comparison",
            "case",
            "configuration",
            "n_each",
            *[label + "_" + metric for metric in metrics for label in ["baseline", "candidate"]],
            "min_change_pct",
            "median_change_pct",
            "median_ci_lower_pct",
            "median_ci_upper_pct",
            "median_ci_gate",
            "cycles_min_over_1pct",
            "cycle_minima_median_change_pct",
        ]
    )
    for comparison, rows in result.items():
        for row in rows:
            assert row["baseline"]["n"] == row["candidate"]["n"]
            values = [row[label][metric] for metric in metrics for label in ["baseline", "candidate"]]
            values += [
                row["change_pct"]["min"],
                row["change_pct"]["median"],
                *(row["previous_median_ci_95pct"] or [None, None]),
            ]
            writer.writerow(
                [
                    comparison,
                    row["case"],
                    row["reader"],
                    row["baseline"]["n"],
                    *[f"{v:.9g}" if v is not None else "" for v in values],
                    row["previous_median_ci_gate"],
                    row["cycle_minima_changes_above_1pct"],
                    f"{row['cycle_minima_median_change_pct']:.9g}",
                ]
            )
    return output.getvalue()


def write_summary(folder):
    manifest, result = compute(folder)
    (folder / "summary.csv").write_text(render_csv(result))
    (folder / "min_median.json").write_text(json.dumps(result, indent=2) + "\n")
    return manifest, result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("folder", type=Path)
    parser.add_argument("--write", action="store_true", help="Write summary.csv and full per-cycle statistics")
    args = parser.parse_args()
    if args.write:
        manifest, result = write_summary(args.folder)
    else:
        manifest, result = compute(args.folder)
        assert render_csv(result) == (args.folder / "summary.csv").read_text(), "summary differs from raw events"
    print(
        f"PASS: {manifest['unique_jobs_expected']} serial jobs; {manifest['formal_samples_expected']} formal samples; "
        f"{manifest['warmups_expected']} warmups; {len(result)} comparisons"
    )


if __name__ == "__main__":
    main()
