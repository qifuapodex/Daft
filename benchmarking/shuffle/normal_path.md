# Normal-path shuffle performance investigation (A7-006)

A7-006 is an unconfirmed performance observation. The original two-worker
comparison found overlapping ranges and median collect times below the
predeclared 10% regression investigation threshold:

| Workload | apodex.6 median / min–max (s) | apodex.7 median / min–max (s) | Median change |
| --- | --- | --- | --- |
| Sparse aggregation | 4.128992 / 3.576096–4.601487 | 4.312106 / 3.503753–4.610544 | +4.43% |
| Wide rows | 4.063333 / 3.729877–4.969212 | 4.271699 / 3.976129–4.484067 | +5.13% |

The baseline is `83898cdce391c07406e33b4ac3662660208a0015`
(`0.7.24+apodex.6`); the candidate is
`6a845f7a0ca48dcbe09003ec8435aa874734ed5b` (`0.7.24+apodex.7`).
Each workload has six measured samples per version from two independent jobs,
scheduled ABBA, with one warmup per job excluded. Min–max is not a confidence
interval. Neither those samples nor a small writer microbenchmark establish
that the new recovery and cleanup paths are free of overhead.

## Original collect benchmark

Use matching Python 3.11.11, Ray 2.55.1, PyArrow 23.0.1, NumPy 2.2.6, and
pandas 2.3.3 environments that differ only in Daft. The original workers each
advertised 128 Ray CPUs with a 56-CPU cgroup quota. Keep worker IDs, quotas,
shared storage, and workload configuration fixed throughout a comparison.

- **Sparse:** materialize 64,000 rows in 32 input partitions with `k = id % 128`;
  then `repartition(128, 'k')` and grouped sum/count. Independently validate
  each key against the count and sum of `range(k, 64000, 128)`.
- **Wide:** materialize 1,048,576 rows with
  `payload = id.to_bytes(8, 'little') * 128`, then `repartition(256, 'id')`.
  Validate every ID and payload. Including the eight-byte ID, this is
  1,082,130,432 logical bytes.
- Use Flight shuffle with shared-only placement, shared reads, synchronous
  durability, AQE off, producer replay off, and scan split/merge off. Leave
  local EIO retries at the version's default; apodex.7 defaults to six retries.
- Time only `collect`, excluding input materialization and output validation.
  Run one warmup and three measurements per job, two independent jobs per
  version. Preserve every valid sample and the actual task/partition counts.
- Pause when external jobs appear; wait at least one hour before checking
  again. Reject topology changes and contaminated timings. Use a fresh run ID
  and retain logs; do not overwrite the original evidence.

The [runtime validation repository](https://github.com/ApodexAI/apodex-pipeline-runtime-validation)
contains the original workloads, archived evidence, and orchestration. From a
checkout with access to that archive and the configured environments:

```bash
REGRESSION_PY=/fast/env/dev/qi.f/ray_cpu_env/env/bin/python
REGRESSION_CONFIG=archive/2026-09-15-daft-apodex-7/resume-configs/performance.json
"$REGRESSION_PY" -m runtime_validation plan --config "$REGRESSION_CONFIG" --cases sparse,wide
"$REGRESSION_PY" -m runtime_validation run --config "$REGRESSION_CONFIG" --cases sparse,wide --output /tmp/a7-006-runs
```

`plan` is offline. `run` submits cluster work using `APODEX_TOK` through the
configuration's `token_env`; retain `pause_on_external_jobs=true` and
`idle_timeout_s=0`.

### Cluster rerun on 2026-09-16 UTC

Run `20260916T005302Z-46dc9cab` completed both preflights, all eight workload
jobs, and cleanup successfully. Dependency versions and worker identities were
checked, the same two workers were used throughout the comparison, and no
external jobs, topology changes, or telemetry errors were reported. Each worker
advertised 128 Ray CPUs with a 56-CPU cgroup quota. Shared-storage caches were
not globally cleared; polling can miss brief external work.

| Workload | apodex.6 median / min–max (s) | apodex.7 median / min–max (s) | Median change |
| --- | --- | --- | --- |
| Sparse aggregation | 4.183607 / 4.051516–6.373188 | 4.272272 / 3.703908–4.480765 | +2.12% |
| Wide rows | 4.420830 / 3.943596–5.339469 | 4.429316 / 3.659903–5.710468 | +0.19% |

All six measured samples per version/workload and all warmups are retained in
[normal_path_collect.csv](normal_path_collect.csv). All sum/count and full
ID/payload checks passed. The rerun again does not cross the original 10%
investigation threshold. It compares the **original released wheels**; it does
not measure the patch below on the cluster.

## Confirmed redundant initialization in bounded reads

Flight's `CheckedRange` reads through Tokio's `Take` adapter. On every poll,
`Take` passes an uninitialized `ReadBuf` view to its inner reader, even when
`next_flight_data` allocated an initialized message buffer. `RetryReader`
previously called `initialize_unfilled()` on that entire view before polling
the file. This clears memory that Tokio will overwrite, including while the
file read is pending. Short reads and repeated pending polls can clear the
remaining body repeatedly.

Two regression checks fail on the original implementation: a three-byte read
following a six-byte prefix initializes all 4,096 bytes instead of nine, and
a pending file read initializes its entire destination before returning data.

The fix polls the inner reader with the caller's buffer directly, remembers
its filled length, and restores that length before handling the poll result.
Only a successful read advances the filled length and recovery cursor. Failed
and pending reads therefore still expose no new bytes. Initialization performed
by the inner reader remains valid; no unsafe code or extra allocation is needed.
Existing checksum, retry-budget, seek, and write-drain protections are retained.

This is a separately demonstrated unnecessary cost. It does not establish
that the original 4–5% collect differences were caused by this code, or predict
an equivalent end-to-end speedup.

## Reader microbenchmark

The manual [read benchmark](../../src/daft-io/src/shuffle_file/bench.rs) compares
Tokio `File` and `RetryReader`, both wrapped in `Take`, without errors injected:

```bash
DAFT_SHUFFLE_BENCH_DIR=/path/to/the/filesystem/under/test \
  cargo test -p daft-io --release bench_normal_shuffle_reads \
  -- --ignored --nocapture --test-threads=1
```

The directory defaults to the system temporary directory. Each sample performs
256 reads of one file: 4 KiB per read for `small_reads`, or 4 MiB per read for
`wide_reads` (1 MiB and 1 GiB total, respectively). The retry mode allows six
local retries. Both modes validate every byte after every read. File creation,
opens, seeks, destination allocation/reset, validation, and cleanup are outside
the timed interval; timings sum the individual bounded `read_exact` calls.

Each mode has one warmup followed by six measurements in three ABBA blocks.
CSV output retains every sample and warmup. This is a warm-cache benchmark:
the input file is prepared just before reading, caches are not cleared, and
only the benchmark's temporary file is removed. No Ray jobs, IPC decoding,
write recovery, or cleanup RPCs are included. The plain-file control is not
an apodex.6 query. Logical throughput is not physical disk bandwidth.

For before/after comparisons, run the same benchmark on both implementations
in ABBA process order. Keep all valid samples, record the compiler, dependencies,
filesystem and host, and avoid concurrent local builds/workloads. Apply the
10% investigation threshold to controlled collect comparisons, not to these
microbenchmarks.

## Local before/after measurements

Measured on 2026-09-16 UTC on Linux 6.17.0-1013-aws, x86_64 Intel Xeon
Platinum 8488C (16 logical CPUs), using `/tmp` on the host's ext4 filesystem.
Compiler: `rustc 1.91.0-nightly (51ff89506 2025-09-02)`, optimized release
profile; Tokio 1.53.1, crc32fast 1.5.0, tracing 0.1.44, tempfile 3.27.0.

Both source snapshots were compiled into a small standalone crate with those
workspace-locked dependencies and the exact benchmark above. The old snapshot
is the `shuffle_file.rs` from `6a845f7a0`, with only the benchmark module added;
the new snapshot includes this fix. Four fresh processes ran old/new/new/old,
each executing the benchmark's internal File/RetryReader ABBA sequence. Local
builds had completed before measurement. Each cell contains 12 measurements
from two processes, excluding their warmups. Full samples and warmups are in
[normal_path_reads.csv](normal_path_reads.csv).

| Workload / reader | Before median / min–max (s) | After median / min–max (s) |
| --- | --- | --- |
| 256 × 4 KiB / File control | 0.002811 / 0.001489–0.003401 | 0.002499 / 0.001521–0.003820 |
| 256 × 4 KiB / RetryReader | 0.002605 / 0.001479–0.003234 | 0.002966 / 0.001569–0.004821 |
| 256 × 4 MiB / File control | 0.195654 / 0.182142–0.250467 | 0.207665 / 0.180302–0.255847 |
| 256 × 4 MiB / RetryReader | 0.318386 / 0.303673–0.346801 | 0.194425 / 0.187004–0.254367 |

The large-read RetryReader median decreases by 38.93%. Small-read ranges
overlap and their median increases by about 0.36 ms; this measurement supports
no small-read speedup claim. The unchanged File control also fluctuates.
These are local, warm-cache component timings, not cluster collect timings or
proof of zero overhead after the fix.

Validation: the two initialization regressions fail before the fix; all 13
`shuffle_file` unit tests pass after the fix in the actual `daft-io` crate.
The manual release benchmark validates every read in all four processes.
