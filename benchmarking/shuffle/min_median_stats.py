"""Compare observed minima and medians without changing earlier median-CI gates.

Use all retained non-warmup observations. Minima are descriptive extremes, not
confidence bounds. Cycle minima provide a check against a single unusually fast
observation dominating the pooled minimum.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import statistics
from collections import defaultdict
from pathlib import Path

from abba_stats import percentile


def change(old, new):
    return (new / old - 1) * 100


def timing_stats(values):
    assert len(values) > 1 and min(values) > 0
    mean = statistics.mean(values)
    median = statistics.median(values)
    stdev = statistics.stdev(values)
    mad = statistics.median(abs(value - median) for value in values)
    return {
        "n": len(values),
        "min_s": min(values),
        "median_s": median,
        "max_s": max(values),
        "mean_s": mean,
        "sample_variance_s2": statistics.variance(values),
        "sample_stddev_s": stdev,
        "cv_pct": 100 * stdev / mean,
        "mad_s": mad,
        "mad_over_median_pct": 100 * mad / median,
        "p10_s": percentile(values, 0.1),
        "p95_s": percentile(values, 0.95),
    }


def analyze(events, previous):
    groups = defaultdict(lambda: defaultdict(list))
    for event in events:
        if event["event"] == "sample" and not event["warmup"]:
            groups[(event["case"], event.get("reader", "collect"))][event["cycle"]].append(event)
    prior = {(row["case"], row["reader"]): row for row in previous}
    assert set(groups) == set(prior)
    results = []
    for key, cycles in sorted(groups.items()):
        pooled = {label: [] for label in ["baseline", "candidate"]}
        cycle_minima = []
        for cycle, samples in sorted(cycles.items()):
            jobs = defaultdict(list)
            for sample in samples:
                assert sample["oracle"] == "PASS"
                jobs[sample["position"]].append(sample)
            assert set(jobs) == {0, 1, 2, 3}
            assert len({len(job) for job in jobs.values()}) == 1
            for position, label in enumerate(["baseline", "candidate", "candidate", "baseline"]):
                assert all(sample["label"] == label for sample in jobs[position])
            values = {label: [sample["seconds"] for sample in samples if sample["label"] == label] for label in pooled}
            for label, timings in pooled.items():
                timings.extend(values[label])
            minima = {label: min(timings) for label, timings in values.items()}
            cycle_minima.append(
                {"cycle": cycle, **minima, "change_pct": change(minima["baseline"], minima["candidate"])}
            )
        stats = {label: timing_stats(values) for label, values in pooled.items()}
        for label in stats:
            for metric in ["min", "median", "max"]:
                assert stats[label][f"{metric}_s"] == prior[key][label][metric]
            assert stats[label]["n"] == prior[key][label]["n"]
        changes = {
            metric: change(stats["baseline"][f"{metric}_s"], stats["candidate"][f"{metric}_s"])
            for metric in ["min", "median", "p10", "p95"]
        }
        assert abs(changes["median"] - prior[key]["change_pct"]) < 1e-10
        cycle_min_medians = {label: statistics.median(row[label] for row in cycle_minima) for label in pooled}
        results.append(
            {
                "case": key[0],
                "reader": key[1],
                **stats,
                "change_pct": changes,
                "cycle_minima": cycle_minima,
                "cycle_minima_median_s": cycle_min_medians,
                "cycle_minima_median_change_pct": change(cycle_min_medians["baseline"], cycle_min_medians["candidate"]),
                "cycle_minima_changes_above_1pct": sum(row["change_pct"] > 1 for row in cycle_minima),
                "previous_median_ci_gate": prior[key]["gate"],
                "previous_median_ci_95pct": prior[key].get("paired_cycle_bootstrap_95pct"),
            }
        )
    return results


def report_table(name, rows):
    is_query = all(row["reader"] == "collect" for row in rows)
    unit, scale = ("s", 1) if is_query else ("ms", 1000)
    precision = 6 if is_query else 3
    counts = sorted({row[label]["n"] for row in rows for label in ["baseline", "candidate"]})
    lines = [
        f"## {name}",
        "",
        f"单位：{unit}；每版样本数：{', '.join(map(str, counts))}。每格时间为旧版 → 新版。"
        "CV 为旧版 → 新版的标准差 / 均值。",
        "",
        "| 场景 / 配置 | min 旧 → 新 | Δ min | median 旧 → 新 | Δ median | CV 旧 → 新 | Δ 每轮 min 的中位数 |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in rows:
        old, new = row["baseline"], row["candidate"]
        name = row["case"] + ("" if is_query else " / " + row["reader"])
        lines.append(
            f"| {name} | {old['min_s'] * scale:.{precision}f} → {new['min_s'] * scale:.{precision}f} | "
            f"{row['change_pct']['min']:+.2f}% | "
            f"{old['median_s'] * scale:.{precision}f} → {new['median_s'] * scale:.{precision}f} | "
            f"{row['change_pct']['median']:+.2f}% | "
            f"{old['cv_pct']:.2f}% → {new['cv_pct']:.2f}% | "
            f"{row['cycle_minima_median_change_pct']:+.2f}% |"
        )
    return "\n".join(lines)


def write_analysis(run):
    raw = (run / "events.jsonl").read_bytes()
    previous = (run / "summary.json").read_bytes()
    rows = analyze([json.loads(line) for line in raw.splitlines()], json.loads(previous))
    payload = {
        "source_events_sha256": hashlib.sha256(raw).hexdigest(),
        "source_summary_sha256": hashlib.sha256(previous).hexdigest(),
        "analysis_script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "results": rows,
    }
    (run / "min_median.json").write_text(json.dumps(payload, indent=2) + "\n")
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("runs", type=Path, nargs="+")
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    sections = [
        "# Min / median 分列复核",
        "分别计算 Δ min = 新 min / 旧 min − 1，Δ median = 新 median / 旧 median − 1；"
        "正值表示变慢。所有正式样本均保留，warmup 单独保留但不进入这两项统计。",
        "min 是整组样本中最快的一次；median 是典型样本。"
        "min 对采样数量和偶然的快速样本敏感，不能视为无噪声基线或统计显著性判定。"
        "两边使用相同样本数；额外展示先取每轮 ABBA 的 min、再取这些 min 的中位数，"
        "检查单次极值是否主导结论。该列仍是描述统计，不是置信区间。",
        "CV = 样本标准差 / 均值，方便跨负载比较波动；它对极值敏感。"
        "JSON 同时记录方差（s²）、标准差（s）、MAD / median、p10 和 p95，"
        "以及逐轮 min。旧 summary.json 和其中只针对 median 的置信区间判定均保持原样。",
        "组件时间涵盖整个批量写入用例，不是单个文件耗时；wide 的 c8 配置实际最多并行两个文件。",
    ]
    for run in args.runs:
        rows = write_analysis(run)
        sections.append(report_table(run.name, rows))
        print(f"{run.name}: {len(rows)} cells; original min/median/max/counts matched")
    args.report.write_text("\n\n".join(sections) + "\n")


if __name__ == "__main__":
    main()
