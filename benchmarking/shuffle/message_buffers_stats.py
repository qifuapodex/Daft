"""Summarize retained ABBA samples without trimming slow observations.

Bootstrap whole paired cycles, not individual correlated samples within jobs.
The interval describes this local experiment; it is not a release guarantee.
"""

from __future__ import annotations

import argparse
import json
import random
import statistics
from collections import defaultdict
from pathlib import Path


def percentile(values, fraction):
    values = sorted(values)
    position = (len(values) - 1) * fraction
    lower = int(position)
    upper = min(lower + 1, len(values) - 1)
    return values[lower] + (values[upper] - values[lower]) * (position - lower)


def distribution(values):
    middle = statistics.median(values)
    return {
        "n": len(values),
        "min": min(values),
        "median": middle,
        "max": max(values),
        "p95": percentile(values, 0.95),
        "mad": statistics.median(abs(value - middle) for value in values),
    }


def summarize(events, regression_budget_pct=1.0):
    groups = defaultdict(list)
    for event in events:
        if event["event"] == "sample" and not event["warmup"]:
            groups[(event["case"], event.get("reader", "collect"))].append(event)
    results = []
    for (case, reader), samples in sorted(groups.items()):
        cycles = defaultdict(lambda: defaultdict(list))
        positions = defaultdict(lambda: defaultdict(list))
        for sample in samples:
            cycles[sample["cycle"]][sample["label"]].append(sample["seconds"])
            positions[sample["cycle"]][sample["position"]].append(sample)
        for jobs in positions.values():
            assert set(jobs) == {0, 1, 2, 3}, "incomplete ABBA cycle"
            for position, label in enumerate(["baseline", "candidate", "candidate", "baseline"]):
                assert all(sample["label"] == label for sample in jobs[position]), "incorrect ABBA order"
            assert len({len(job) for job in jobs.values()}) == 1, "unequal job sample counts"
        for cycle in cycles.values():
            assert set(cycle) == {"baseline", "candidate"}, "incomplete ABBA cycle"
            assert len(cycle["baseline"]) == len(cycle["candidate"]), "unequal sample counts"
        values = {
            label: [value for cycle in cycles.values() for value in cycle[label]] for label in ["baseline", "candidate"]
        }
        per_cycle = []
        for index, cycle in sorted(cycles.items()):
            medians = {label: statistics.median(timings) for label, timings in cycle.items()}
            per_cycle.append(
                {"cycle": index, **medians, "change_pct": (medians["candidate"] / medians["baseline"] - 1) * 100}
            )
        result = {
            "case": case,
            "reader": reader,
            **{label: distribution(timings) for label, timings in values.items()},
            "change_pct": (statistics.median(values["candidate"]) / statistics.median(values["baseline"]) - 1) * 100,
            "cycles": per_cycle,
            "cycle_change_pct": distribution([cycle["change_pct"] for cycle in per_cycle]),
            "regression_budget_pct": regression_budget_pct,
            "cycles_above_budget": sum(cycle["change_pct"] > regression_budget_pct for cycle in per_cycle),
        }
        # Keep the historical change_pct and gate fields as median-only values.
        # Report each observed statistic separately; a median interval cannot
        # establish that the observed minimum meets the regression budget.
        result["observed_change_pct"] = {
            metric: (result["candidate"][metric] / result["baseline"][metric] - 1) * 100
            for metric in ["min", "median", "max"]
        }
        if len(cycles) > 1:
            rng = random.Random(20260916)
            indices = list(cycles)
            changes = []
            for _ in range(10000):
                selected = rng.choices(indices, k=len(indices))
                medians = {
                    label: statistics.median(value for index in selected for value in cycles[index][label])
                    for label in ["baseline", "candidate"]
                }
                changes.append((medians["candidate"] / medians["baseline"] - 1) * 100)
            result["paired_cycle_bootstrap_95pct"] = [percentile(changes, 0.025), percentile(changes, 0.975)]
        interval = result.get("paired_cycle_bootstrap_95pct")
        result["gate"] = (
            "PASS"
            if interval and interval[1] <= regression_budget_pct
            else "REGRESSION"
            if interval and interval[0] > regression_budget_pct
            else "INCONCLUSIVE"
        )
        results.append(result)
    return results


def write_summary(output, regression_budget_pct=1.0):
    events = [json.loads(line) for line in (output / "events.jsonl").read_text().splitlines()]
    results = summarize(events, regression_budget_pct)
    (output / "summary.json").write_text(json.dumps(results, indent=2) + "\n")
    for result in results:
        print(json.dumps(result), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--regression-budget-pct", type=float, default=1.0)
    args = parser.parse_args()
    write_summary(args.output, args.regression_budget_pct)
