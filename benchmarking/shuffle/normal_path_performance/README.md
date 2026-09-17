# Shuffle normal-path performance work

The implementation preserves local EIO recovery and cancellation safety while reducing
normal-path IPC work. **The pre-EIO baseline's 1% regression target is not met.**
This change is suitable for code review; the historical data below is not a release acceptance claim.

The latest [deferred-first-batch comparison](deferred_first_batch_20260917/README.md)
keeps CRC enabled and avoids an async forwarding task for a single input of at
most 8 KiB. Against the pre-EIO version, the 4 KiB single-batch writer improves
min/median by 2.70%/3.31% locally and 0.96%/1.69% on the JuiceFS mount path.
The complete 1% target remains unmet: wider writes and queries retain regressions
or statistical uncertainty, and local small-file maximum latency increases.
The report retains separate incremental comparisons and same-binary noise controls.

The preceding [Arrow 60 comparison](arrow60_20260917/README.md) measures commit
`3ec21f0ec` after integrating release `ffe64ff10` (Arrow-rs 60), with 48 formal
samples per version/storage/case. It also does not meet the 1% target; see its
separate min/median/max tables and current-source validation.

The historical measurements below used the V7 prototype on `5833cac55` and
Arrow-rs 59. They must not be presented as measurements of the Arrow 60 build.
[validation.json](validation.json) records the earlier functional checks on the
initial Arrow 60 rebase at `42b8b9f3c`; the new report records the latest checks.

## Implementation and recovery invariants

- Flight bodies of at least 64 KiB use a bounded append read, avoiding an extra
  zero-fill. Metadata and small bodies keep the previous path. Reads cannot
  consume the next message; yielded buffers retain independent ownership.
- One-shot output reuses the header, partition and EOS CRCs for recovery. CRC
  combination runs only after a recovered write EIO, instead of scanning every
  normal write twice. The independently committed index prefix is excluded.
- Streaming recovery keeps IEEE CRC32. The compact checksum state uses crc-fast
  on x86_64 CPUs with SSE4.1, PCLMULQDQ, AVX-512VL and VPCLMULQDQ; other CPUs
  retain crc32fast. The polynomial, initial state, final XOR and file format are unchanged.
- IPC buffers small writes in 8 KiB. Its buffer never flushes from Drop, so a
  cancelled future cannot perform untracked I/O after its blocking task drains.
- Once running, the writer drains ready input without waiting to fill a batch. Groups stop at
  32 inputs or after reaching the 32 MiB threshold. A final write and close can
  share one blocking operation. Queue and blocking work share an active-write guard.
- A first input of at most 8 KiB stays in that queue until another input or close.
  Close consumes a single pending batch directly, while IPC still offloads its
  blocking work. Large or subsequent inputs start the background writer. Deferred
  state retains active-write tracking, and started writers avoid the state mutex.

Blocking file operations stay off the async executor. Dropping a pending operation
poisons the writer, and cleanup waits for owned blocking operations to finish.
A recovered write EIO still triggers full readback, validation, rewrite and sync
on the original descriptor. This fault-path I/O has not been removed.

The RetryReader zero-initialization optimization was already merged in #15 and
is part of the PR's base; it is not a new change here.
The initial Parquet error-wording workaround was removed after the latest
release corrected its test. No Parquet implementation change remains in this PR.

## Why retain a checksum?

A write of B can report a delayed writeback error affecting previously accepted A.
A successful retry of B, correct file length or valid IPC framing cannot establish
that A still contains the intended bytes. The current recovery protocol does not
retain the full original output, so it needs an independent content reference
computed from those bytes before a fault. Computing the expected digest only after
EIO from the same file could validate already-corrupted data.

CRC detects corruption; it does not repair it and can collide. CRC32 is not the
only possible algorithm. Removing the reference would require a different recovery
strategy, such as discarding the output and re-executing the task, or retaining
reliable original bytes for replay.

## Retained performance evidence (2026-09-16/17)

[summary.csv](summary.csv) separates min, median and max, includes CV, and reports
the paired 95% interval for median. Positive changes are slower. A median interval
never substitutes for the observed-minimum comparison.

[samples.jsonl.gz](samples.jsonl.gz) retains 5,120 formal samples and 1,280 warmups
across four campaigns. [evidence.json](evidence.json) records provenance, source
and artifact hashes. Samples were not trimmed. Full ABBA cycles, rather than
individual samples within a job, are the bootstrap resampling unit.

- Every case ran eight cycles. The earlier writer studies used one warmup plus
  six formal samples/job (96 samples per version/cell).
- The storage campaign used the symmetric order old-local, V7-local, old-JuiceFS,
  V7-JuiceFS, V7-JuiceFS, old-JuiceFS, V7-local, old-local. It used one warmup plus
  one formal sample/job (16 per version/storage/cell). The four projections
  reuse the same samples; they are not independent experiments.
- Builds, fault checks and benchmark jobs did not overlap. The CPU was a Xeon
  Platinum 8488C; fixed writer runs used CPUs 2,3. Query tests used two Ray workers
  on the same host. Package identity and no object-store spill were verified.
- `/tmp` was ext4; `/hdd-01/dev/qi.f` was exposed through fuse.bindfs over the
  user's JuiceFS storage. Results describe that complete mount path, not every
  JuiceFS deployment. Caches were not cleared.
- Writer and Gather timings do not include fsync. Gather's PerPartition backend
  clears shared placement even when shared_only/sync is configured; its actual
  local directory was also on the selected storage. Separate traces confirmed
  32 file creations and 128 successful reads per gather collect.
- 4 KiB refers to input size, not exact IPC file size or per-file latency. The
  writer timings cover a whole batch. stream-wide writes 128 MiB logical data
  in two files (about 144 MiB uncompressed IPC); c8 can run only those two files
  concurrently. LZ4 input is highly compressible repeated UInt8 data.

### CRC backend V5 → V7

These incremental results compare the earlier optimized writer with the final
compact CRC implementation, not with the pre-EIO acceptance baseline.

| Case / configuration | Baseline min / median / max (ms) | Candidate min / median / max (ms) | Δ min | Δ median | CV baseline → candidate | Median 95% CI |
|---|---:|---:|---:|---:|---:|---:|
| stream-single / c1-none | 12.755 / 71.889 / 115.887 | 13.066 / 82.072 / 117.773 | +2.44% | +14.17% | 52.50% → 45.98% | [-37.40%, +71.68%] |
| stream-single / c8-none | 6.387 / 10.804 / 18.136 | 6.431 / 10.679 / 17.653 | +0.69% | -1.15% | 24.40% → 23.57% | [-5.57%, +7.62%] |
| stream-wide / c1-none | 56.385 / 58.855 / 75.784 | 55.631 / 57.308 / 62.124 | -1.34% | -2.63% | 3.90% → 1.97% | [-3.11%, -1.95%] |
| stream-wide / c8-none | 28.951 / 30.367 / 44.313 | 28.428 / 29.763 / 49.639 | -1.80% | -1.99% | 5.72% → 12.92% | [-3.23%, -0.53%] |

### Same JuiceFS path: pre-EIO → V7

| Case / configuration | Baseline min / median / max (ms) | Candidate min / median / max (ms) | Δ min | Δ median | CV baseline → candidate | Median 95% CI |
|---|---:|---:|---:|---:|---:|---:|
| oneshot-small / c8-none | 426.878 / 538.585 / 621.899 | 411.014 / 523.030 / 621.066 | -3.72% | -2.89% | 10.24% → 9.70% | [-9.72%, +6.05%] |
| stream-single / c8-none | 3282.796 / 3741.763 / 4898.173 | 3180.237 / 3605.684 / 3743.891 | -3.12% | -3.64% | 9.55% → 4.36% | [-5.72%, +0.11%] |
| stream-wide / c8-lz4 | 214.451 / 259.485 / 307.038 | 205.256 / 222.052 / 239.916 | -4.29% | -14.43% | 8.27% → 4.54% | [-17.56%, -11.91%] |
| stream-wide / c8-none | 2941.571 / 6791.962 / 8706.854 | 4536.818 / 6979.999 / 8830.529 | +54.23% | +2.77% | 24.47% → 18.12% | [-14.44%, +24.72%] |

| Case / configuration | Baseline min / median / max (ms) | Candidate min / median / max (ms) | Δ min | Δ median | CV baseline → candidate | Median 95% CI |
|---|---:|---:|---:|---:|---:|---:|
| flight-gather-lz4 / collect | 1971.822 / 2109.496 / 2182.292 | 2047.530 / 2117.777 / 2210.706 | +3.84% | +0.39% | 2.25% → 2.41% | [-1.60%, +2.22%] |
| flight-gather-none / collect | 1967.046 / 2110.326 / 2196.038 | 2049.480 / 2132.264 / 2183.232 | +4.19% | +1.04% | 2.56% → 1.96% | [-1.20%, +2.84%] |

Three of these six cells exceed +1% in min or median. The uncompressed wide writer
has min +54.23% and median +2.77%, with V7 CV 18.12%. Its per-cycle minima are slower
by more than 1% in 5/8 cycles; the median of those cycle minima increases 15.79%.
All unfavorable observations remain included. The wide-writer median interval is
[-14.44%, +24.72%], so the data cannot establish equivalence within 1% or attribute
that entire difference specifically to CRC.

V7's JuiceFS/local median ratios are 497.24× for tiny streaming files, 106.21× for
one-shot small files, 219.23× for wide uncompressed writes, 3.33× for wide LZ4 writes,
and about 28× for the real gather queries. Reducing small-file and small-I/O costs
is a more promising storage-specific direction than CRC throughput alone.
The simultaneous local controls also contain regressions; see all comparisons
in summary.csv. ABBA does not eliminate nonlinear storage load or asynchronous
writeback effects, and no backend-queue measurements were taken.

### Correction to the earlier “gather” workload

The historical `source.into_partitions(1)` over 32 already materialized inputs
coalesces Ray references. Its timed collect does not exercise FlightGather disk
writes or the recovery CRC. Those samples remain as `coalesce-control-*` and are
excluded from the disk-shuffle conclusions above. The corrected query computes
`row_number().over(Window().order_by("v"))`, which requires a real FlightGather.
The physical plan and every input value/output ordinal were checked.

## Reproduce and verify

Recompute the published statistics using only the retained samples and standard
Python libraries:

```sh
python benchmarking/shuffle/normal_path_performance/verify_evidence.py
```

Build and run the affected Rust tests:

```sh
make build
cargo test -p daft-io --lib shuffle_file:: -- --test-threads=2
cargo test -p daft-writers -p daft-shuffles --lib -- --test-threads=2
```

The ignored cancellation tests use the Linux injector in
[shuffle_eio_injection.c](../../../tests/ray/shuffle_eio_injection.c).
Normal `cargo test` does not run the manual performance or injector-dependent tests.

For new measurements, build separate release test executables/wheels for each
version. The pre-EIO writer copy uses the same write benchmark with only
`configure()` made a no-op because its retry API does not exist. For example:

```sh
python benchmarking/shuffle/write_abba.py \
  --baseline "$BASELINE_WRITER" --candidate "$CANDIDATE_WRITER" \
  --cycles 8 --samples 6 --cpu-set 2,3 --output /tmp/new-writer-abba
python benchmarking/shuffle/flight_gather_abba.py \
  --baseline "$BASELINE_WHEEL" --candidate "$CANDIDATE_WHEEL" \
  --root /path/to/storage --compression none --cycles 8 --samples 1 \
  --output /tmp/new-gather-abba
```

The query driver compares versions in ABBA order on the chosen storage path;
repeat separately for LZ4 and each storage path. Use the same CPU, dependencies,
cache policy and sample counts for both sides. Do not compare new minima directly
with the historical 96-sample minima. Run benchmarks separately from builds and
fault tests. Both drivers refuse an existing output directory and retain every sample.
Their summary.json reports min, median and max changes separately under
observed_change_pct; its gate field applies only to the median confidence interval.
