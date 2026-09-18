# Shuffle normal-path performance

Local EIO retries add checksum and scheduling work to shuffle output. This PR
reduces that work while preserving recovery and cancellation guarantees.
**The complete pre-EIO 1% regression requirement remains unmet.**

## Retained implementation

- Reuse one-shot header, partition and EOS CRCs. Combine them only after a
  recovered write EIO, avoiding a second normal-path checksum traversal.
- Keep IEEE CRC32 for streaming recovery. Use compact crc-fast state on supported
  x86_64 CPUs (SSE4.1, PCLMULQDQ, AVX-512VL and VPCLMULQDQ), with crc32fast as the
  fallback. The checksum values and file format remain compatible.
- Buffer small IPC writes in 8 KiB, group ready inputs, and fuse final write/close.
  Defer a first input of at most 8 KiB until another input or close, avoiding a
  forwarding task for a single small batch. Large/subsequent inputs start the
  background writer; ready groups stop at 32 inputs or after reaching 32 MiB.
- Read Flight bodies of at least 64 KiB into bounded, independently owned buffers
  without first zero-filling them. Metadata and small bodies keep the existing path.

Blocking I/O stays off the async executor. Deferred and active operations retain
write tracking; cancellation poisons the writer and cleanup waits for I/O to
finish. IPC buffers never flush from Drop. After a recovered write EIO, the
writer still reads back, validates, rewrites and syncs on the original descriptor.

CRC is needed by this recovery protocol because retrying the latest write and
checking the file length cannot verify previously accepted bytes after a delayed
writeback error. The expected checksum comes from the original bytes, not the
possibly damaged file. CRC detects corruption; it does not repair it or eliminate
collisions. Removing it requires a different recovery/replay strategy.

## Performance evidence

The [production-version report](deferred_first_batch_20260917/README.md) retains
12,288 formal samples and 2,016 warmups: isolated comparisons, pre-EIO comparisons,
A/A controls, and local/JuiceFS writer and real FlightGather measurements.
All observations, including slow samples, remain retained. Min and median are
separate criteria; full results include max, variance, CV, cycle minima and
paired-cycle median intervals.

The historical baseline is `c6217a8c09cbfe773f88374adf46ff2a7fcb1195` (before EIO
retries). The measured production implementation is `20fe7b0b0`. The full comparison
also includes Arrow 59 -> 60 and earlier changes; it does not isolate CRC cost.
Positive values below mean slower. These are the eight-cycle storage comparisons,
with 48 formal observations per version/storage/case.

| Case | Local Δmin | Local Δmedian | JuiceFS Δmin | JuiceFS Δmedian |
|---|---:|---:|---:|---:|
| 4 KiB single-batch / none | -2.70% | -3.31% | -0.96% | -1.69% |
| One-shot small / none | -8.83% | -12.07% | -4.30% | -1.90% |
| Wide streaming / none | -8.71% | +1.26% | -36.62% | +1.55% |
| Wide streaming / LZ4 | -73.31% | -65.38% | +15.97% | -15.76% |
| FlightGather / none | +2.52% | +3.49% | -5.01% | -0.24% |
| FlightGather / LZ4 | +4.45% | +3.89% | +2.10% | +1.26% |

Single-batch files improve, but local Gather remains slower and wide-write results
retain regressions or substantial uncertainty. Observed minima and median
intervals do not establish equivalence within 1%. The report includes incremental
regressions and A/A drift as well as gains; A/A noise is not subtracted from A/B.

The storage paths were local `/tmp` and `/hdd-01/dev/qi.f` via fuse.bindfs over
JuiceFS, with warm caches. Writer timings cover complete groups, not individual
files; 4 KiB describes logical input. Gather uses two workers on one host and
verifies the physical plan, all values/ordinals, loaded extension hashes and no
object-store spill. These normal-path timings exclude fsync. Builds, fault tests
and profilers did not overlap the measurements.

## Reproduce and audit

Recompute all retained statistics and check sample counts, order, oracles and
provenance with standard Python libraries:

```sh
python3 benchmarking/shuffle/normal_path_performance/deferred_first_batch_20260917/audit.py
```

The report links raw samples, build/source hashes and validation logs. Large JSON
statistics and source manifests are losslessly compressed. Historical reports,
profiler captures and rejected prototype patches remain in the
[pre-cleanup archive](https://github.com/qifuapodex/Daft/tree/a745236736fada5d13157a262761db4b34f1f434/benchmarking/shuffle/normal_path_performance).
[TODO.md](TODO.md) records their conclusions and the outstanding work.

For new measurements, prepare matching release executables/wheels before timing:

```sh
make build
cargo test --release --locked -p daft-io -p daft-writers -p daft-shuffles --no-run

python3 benchmarking/shuffle/write_abba.py \
  --baseline "$BASELINE_WRITER" --candidate "$CANDIDATE_WRITER" \
  --cycles 8 --samples 6 --cpu-set 2,3 --output /tmp/new-writer-abba
python3 benchmarking/shuffle/flight_gather_abba.py \
  --baseline "$BASELINE_WHEEL" --candidate "$CANDIDATE_WHEEL" \
  --root /path/to/storage --compression none --cycles 8 --samples 3 \
  --output /tmp/new-gather-abba
```

Use `storage_abba.py` for interleaved local/JuiceFS comparisons and
`storage_stats.py` / `min_median_stats.py` for the full statistics. Keep compiler,
dependencies, CPU settings and sample counts fixed; repeat for none/LZ4. Output
directories must be new. The historical writer uses the same `write_bench.rs`
with its unavailable retry configuration made a no-op. Fault/cancellation tests
use [shuffle_eio_injection.c](../../../tests/ray/shuffle_eio_injection.c) and are
run separately from timing; ordinary tests do not execute ignored benchmarks.
