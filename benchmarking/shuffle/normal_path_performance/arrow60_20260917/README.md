# Latest-source storage comparison, 2026-09-17

This experiment compares the pre-EIO version
`c6217a8c09cbfe773f88374adf46ff2a7fcb1195` (Arrow-rs 59) with
`3ec21f0ec3a1bbaf622833992ac2da6646f1dca5` (Arrow-rs 60). The candidate
contains the shuffle optimizations and release changes through `ffe64ff10`,
including the merged Parquet V2 work. This measures the complete version change;
it cannot attribute a difference solely to CRC, retry logic or Arrow.

Both writer executables use the release profile: opt-level 3, no debug assertions
or overflow checks, and no debug information. The old writer workload is identical
to the current workload except that configure() is a no-op because its retry API
does not exist. The pre-EIO binary and wheel were preserved from the previous
verified build; their hashes were rechecked. The candidate executable and wheel
were rebuilt and pinned to the source above.

## Results

**The 1% regression requirement is not met.** Four of six local-storage cells and two of six JuiceFS cells exceed +1% in observed min or median. Each cell has 48 formal observations per version; positive changes mean slower. The local regressions have median intervals entirely above +1%, while the two observed JuiceFS regressions remain statistically inconclusive.

[summary.csv](summary.csv) includes all 24 comparison rows, variance, CV and cycle-minimum diagnostics. [all-events.jsonl.gz](all-events.jsonl.gz) retains every sample and the serial job timing/resource records. [manifest.json](manifest.json) pins the builds, sampling plan, mount information and hashes; [validation.json](validation.json) records the independent checks. Ephemeral cluster addresses and full environment logs remain in the original local run; their verification is summarized in validation.json.

### Local ext4

Times are milliseconds for the complete batch or collect, excluding warmups.

| Case | Old min / median / max | New min / median / max | Δ min | Δ median | CV old → new | Median 95% CI |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 4 KiB single-batch files / none | 5.217 / 5.925 / 8.389 | 6.397 / 7.304 / 9.506 | +22.62% | +23.27% | 12.40% → 9.39% | [+17.81%, +28.58%] |
| One-shot small / none | 3.902 / 4.807 / 6.541 | 3.652 / 4.204 / 5.290 | -6.42% | -12.54% | 11.34% → 9.55% | [-15.72%, -8.59%] |
| Wide streaming / none | 23.989 / 25.460 / 40.631 | 21.995 / 27.144 / 44.820 | -8.32% | +6.61% | 12.13% → 15.37% | [+4.99%, +8.59%] |
| Wide streaming / LZ4 | 54.344 / 58.409 / 74.350 | 14.258 / 22.321 / 52.096 | -73.76% | -61.78% | 7.08% → 36.10% | [-64.65%, -48.96%] |
| FlightGather / none | 67.878 / 71.561 / 80.874 | 68.894 / 73.758 / 82.034 | +1.50% | +3.07% | 3.79% → 4.27% | [+1.58%, +4.08%] |
| FlightGather / LZ4 | 66.246 / 72.166 / 79.664 | 70.056 / 75.401 / 84.883 | +5.75% | +4.48% | 4.34% → 4.41% | [+2.18%, +6.08%] |

### JuiceFS mount path

Times are milliseconds for the complete batch or collect, excluding warmups.

| Case | Old min / median / max | New min / median / max | Δ min | Δ median | CV old → new | Median 95% CI |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 4 KiB single-batch files / none | 3274.819 / 3598.500 / 3842.010 | 3131.060 / 3496.711 / 3751.314 | -4.39% | -2.83% | 3.57% → 3.55% | [-4.36%, -1.14%] |
| One-shot small / none | 398.410 / 510.923 / 613.633 | 460.261 / 527.676 / 581.009 | +15.52% | +3.28% | 8.71% → 6.65% | [-0.63%, +6.39%] |
| Wide streaming / none | 2780.631 / 6111.568 / 9174.482 | 1763.068 / 6086.036 / 8899.872 | -36.59% | -0.42% | 25.32% → 27.06% | [-10.31%, +12.03%] |
| Wide streaming / LZ4 | 197.442 / 245.239 / 289.915 | 132.240 / 208.704 / 231.916 | -33.02% | -14.90% | 8.52% → 8.19% | [-18.19%, -11.43%] |
| FlightGather / none | 1891.738 / 2020.821 / 2378.220 | 1833.522 / 2049.478 / 2259.175 | -3.08% | +1.42% | 4.47% → 4.95% | [-0.73%, +2.18%] |
| FlightGather / LZ4 | 1819.729 / 2023.972 / 2403.749 | 1834.119 / 2029.622 / 2252.155 | +0.79% | +0.28% | 4.61% → 4.38% | [-1.61%, +1.28%] |

### Interpretation and remaining instability

- Local small-file streaming is slower in both min (+22.62%) and median (+23.27%). Its per-cycle minimum exceeds +1% in all eight cycles; the median of cycle minima increases 18.11%. This is not explained by one unusually fast baseline observation.
- Local wide uncompressed streaming illustrates why the columns must remain separate: min improves 8.32%, but median regresses 6.61%, with its median interval [+4.99%, +8.59%]. Both local FlightGather modes also regress beyond 1%.
- JuiceFS one-shot small output has min +15.52% and median +3.28%. Its cycle minimum exceeds +1% in 5/8 cycles, and the median of cycle minima increases 4.42%. The median interval [-0.63%, +6.39%] does not establish the exact regression size.
- JuiceFS wide uncompressed output has min -36.59% and median -0.42%, but CV is 25.32% → 27.06%, and its median interval spans [-10.31%, +12.03%]. These observations do not demonstrate stability or equivalence within 1%. The historical V7 minimum regression of +54.23% must not be reused as this latest-source result.
- Local wide LZ4 output improves substantially in min and median, but the candidate CV is 36.10% (old 7.08%). Improvement and low variance are separate claims. The comparison includes Arrow and release changes, so the gain cannot be assigned exclusively to the CRC implementation.
- JuiceFS is much slower in absolute time: candidate median ratios versus local are 478.75× for single-batch small files, 125.51× for one-shot small output, 224.21× for wide uncompressed output, 9.35× for wide LZ4 and about 27× for FlightGather. That does not mean the new version regresses more on JuiceFS.

The small-file case has only one batch per file, so grouping ready batches cannot amortize work across batches in that file. Whole-job local voluntary context-switch medians increase from 870 to 1,623 (involuntary: 151.5 to 627). This supports investigating the remaining blocking-I/O scheduling cost. These counters include warmups, readback and cleanup; profiling the timed phase on a common Arrow 60 baseline is needed for precise attribution.

Current-source validation passed: make build, release executable/wheel builds, 108 Rust component tests, four injected cancellation checks, 20 targeted Ray EIO tests, and dev/release FlightGather checks. All 384 formal jobs passed their output checks; every query job verified driver/worker extension identities and no object-store spill. All 1,490 pinned source/config files and binary/wheel hashes were rechecked after measurement. Both private storage roots were removed.

## Sampling and workload scope

The sampling plan was fixed before measurement: eight cycles, one warmup and
three formal observations per job. Each cycle uses this order:

```
old-local, new-local, old-JuiceFS, new-JuiceFS,
new-JuiceFS, old-JuiceFS, new-local, old-local
```

That gives 48 formal samples per version, storage and case. The full plan has
384 jobs, 1,152 formal samples and 384 warmups. Writer jobs run first, followed
by queries on one private Ray cluster with two worker nodes on the same host.
Each node has four Ray CPUs; writer processes are pinned to CPUs 2,3. Both
versions use RAYON_NUM_THREADS=2 and OMP_NUM_THREADS=1.

| Case | Work per timed observation |
| --- | --- |
| stream-single / c8-none | 128 files, each containing one 4 KiB logical batch |
| stream-wide / c8-none | Two files, each containing sixteen 4 MiB batches |
| oneshot-small / c8-none | Sixteen files, each containing 128 partitions of 4 KiB |
| stream-wide / c8-lz4 | The same wide workload with LZ4 compression |
| flight-gather-none | 64,000 rows from 32 pre-materialized inputs, global ordered row_number(), uncompressed IPC |
| flight-gather-lz4 | The same real FlightGather query with LZ4 IPC |

c8 is the requested concurrency limit. The wide writer has only two files, so
at most two can write concurrently. File sizes include IPC overhead; 4 KiB is
the logical batch size. Writer input is repeated UInt8 data and compresses well.

Writer timing includes file creation, encoding, writes, close/drain and task
scheduling. Full readback validation and removal occur outside its timer. Query
timing covers collect(); input materialization, result validation and telemetry
are outside its timer. Every value and output ordinal is checked, with one output
partition and an asserted FlightGather physical plan. Loaded driver and worker
extension hashes, dependencies and node resources are checked across jobs.

## Storage and noise controls

Local output uses ext4 under /tmp. Shared output uses private temporary directories
under `/hdd-01/dev/qi.f`, exposed through fuse.bindfs over JuiceFS. A separate
strace preflight verifies actual file creation on that path, and each Ray node
probes both storage roots. No other build, fault test or benchmark job from this
experiment overlaps measurement.

Client and server caches are not cleared. These writer and FlightGather normal
paths do not fsync. Gather clears shared placement internally, so its local output
directory is explicitly placed on the selected mount. Results describe this mount
chain and workload, not raw backend durability or a universal JuiceFS property.

All formal observations are retained; no slow samples are trimmed or rejected
based on machine load. Per-job load, dirty/writeback memory and aggregate CPU
counters are retained. GNU time records process resources, but its query-driver
counts exclude separately launched Ray workers. These diagnostics do not directly
measure backend queue depth and cannot alone establish the cause of a regression.

Min, median and max are reported separately, alongside variance and CV. Median
intervals bootstrap whole paired cycles rather than correlated observations
within a job. Per-cycle minima are retained to check whether one unusually fast
observation dominates a pooled minimum. Minima are descriptive extremes, not
confidence bounds. Storage/version projections reuse samples and are not
independent experiments. Do not directly compare these 48-sample minima against
the earlier 16-sample study.

## Reproduction

Use Python 3.11 and the dependency versions recorded in validation.json.
Prepare a builds.json with the two source revisions and pinned binary/wheel paths,
hashes and extension hashes, then run:

```sh
python benchmarking/shuffle/storage_abba.py \
  --builds /path/to/builds.json --output /tmp/new-storage-abba \
  --local-root /tmp --shared-root /hdd-01/dev/qi.f \
  --cycles 8 --samples 3 --writer-cpus 2,3
python benchmarking/shuffle/storage_stats.py /tmp/new-storage-abba --write
```

The driver refuses an existing output directory and cleans only its own private
storage directories. It retains raw job logs, all events, resource counters and
the manifest. The auditor checks serial execution, all expected cells and trials,
complete ABBA cycles, equal sample counts, and independently recomputes the
min/median/max statistics. A preliminary release query check on JuiceFS is kept
separate from the formal samples.

To verify the committed compact evidence without rebuilding or accessing either disk:

```sh
python benchmarking/shuffle/storage_stats.py \
  benchmarking/shuffle/normal_path_performance/arrow60_20260917
```
