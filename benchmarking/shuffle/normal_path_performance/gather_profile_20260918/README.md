# Collect-scoped FlightGather profiling

Status: instrumentation and an initial current-version capture are complete.
Controlled baseline/current phase attribution remains on [TODO P2](../TODO.md).
These instrumented observations are diagnostics, not performance acceptance.

## Scope and checks

The private Ray cluster starts below `perf`, so its worker processes and threads
inherit the events. The workload enables counters after warmup, immediately
before collect, and disables them before result conversion/verification.
Control handshakes surround the measured interval and have their own overhead.
Asynchronous background work in this private cluster can overlap a collect;
the capture is not a per-query CPU ownership accounting system.

- Unchanged production revision `20fe7b0b05646418d84433f6613a13e46762becc`, pinned
  release wheel/extension, CRC enabled, local `/tmp`, no compression.
- Same actual FlightGather case: 64,000 rows, 32 materialized input partitions,
  two worker nodes with four logical CPUs each, Rayon threads 2, OMP threads 1.
- 128 profiled collects plus one unprofiled warmup; every complete data/ordinal
  oracle passed, all driver/worker extension hashes matched, and no spill occurred.
- About 23K cycle samples, zero reported lost samples. The full raw capture is
  retained locally at the path and SHA-256 in [provenance.json](provenance.json).
  [Events](events.jsonl.gz), [logs](collect.log.gz), [manifest](manifest.json),
  and exact captured scripts are retained here; script hashes were verified.
- Separate two-collect counter-mode smoke checks passed for both the current
  wheel and pre-EIO `c6217a8c09cbfe773f88374adf46ff2a7fcb1195`. Their directories
  retain all events and counters. They are not ABBA comparisons and are too
  small to support a performance conclusion.

## Initial observations

The [library aggregation](report-dso.txt) locates sampled cycles as follows:

| Library | Sampled-cycle share |
|---|---:|
| Kernel | 27.21% |
| Python | 21.63% |
| `_raylet.so` | 20.23% |
| libc | 10.43% |
| `daft.abi3.so` | 8.74% |
| raylet executable | 4.17% |
| gcs_server | 1.85% |

These are code locations, **not write/read/metadata/scheduling phase shares**.
For example, work requested by Daft can execute in libc or the kernel. They
cannot establish which change caused the existing full-version regression.
The [self-symbol view](report-compact.txt) is diffuse and dominated by Python
execution and allocation among entries above its 0.25% display threshold.
An omitted symbol does not prove that its cost is zero.

Next: compare the pre-EIO and current paths under matching conditions, classify
call stacks/syscalls by phase, then isolate the measured fixed costs. The query's
approximately 32 KiB logical input partitions also warrant checking against the
current 8 KiB first-input deferral threshold before a separate threshold experiment.

## Tooling notes and reproduction

The first control probe rejected perf 6.17's NUL-terminated `ack` reply. That
warmup-only failed trace was not analyzed; the workload now accepts the actual
reply format. The completed 128-collect capture's initial inline-symbol report
was interrupted because it was slow; the retained views were regenerated from
the completed raw trace with inline expansion disabled. Normal recording and
the output oracles completed successfully.

The maintained runner disables optional build-ID collection/cache work and
uses `--no-inline --call-graph none` for its initial self-symbol report. This
avoids expensive post-capture symbol expansion; raw call stacks are still recorded.
Historical captured scripts remain separate so their manifest hashes stay valid.

```sh
.venv/bin/python benchmarking/shuffle/profile_gather.py \
  --wheel /path/to/pinned-release.whl --output /tmp/new-gather-profile \
  --root /tmp --samples 32 --compression none --mode record

# Same phase boundaries, with hardware/software counters instead of stacks:
.venv/bin/python benchmarking/shuffle/profile_gather.py \
  --wheel /path/to/pinned-release.whl --output /tmp/new-gather-counters \
  --root /tmp --samples 32 --compression none --mode stat
```

Run profiles separately from clean ABBA timing, builds and functional tests.
Min and median elapsed time remain the acceptance criteria.
