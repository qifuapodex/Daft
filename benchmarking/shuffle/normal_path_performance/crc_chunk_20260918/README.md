# CRC chunk locality: real IPC writer experiment

Status (2026-09-18): the preregistered screen and audit are complete. **Neither
chunk cap is adopted.** The production Rust source has been restored unchanged.
Tracked follow-ups: [TODO](../TODO.md).

512 KiB improves both local statistics, but its JuiceFS minimum exceeds the +1%
budget and its median interval is inconclusive. 256 KiB exceeds the observed-min
budget on both disks. This is an isolated comparison against the current
CRC-enabled PR, **not** a fresh pre-EIO acceptance run.

## Results

Each cell has 48 formal observations per variant. Units are milliseconds for
the complete 128 MiB/two-file group; positive changes mean slower. Both chunk
projections reuse the same whole-buffer samples and are not independent studies.

| Disk / cap | Whole min / median / max | Chunked min / median / max | Δ min | Δ median | CV whole → chunked | Median 95% interval |
|---|---:|---:|---:|---:|---:|---:|
| Local / 512 KiB | 22.026 / 26.642 / 38.489 | 20.555 / 25.082 / 33.715 | −6.68% | −5.86% | 13.75% → 12.44% | [−9.76%, −1.47%] |
| Local / 256 KiB | 22.026 / 26.642 / 38.489 | 22.620 / 25.970 / 33.751 | +2.70% | −2.52% | 13.75% → 8.79% | [−4.95%, −1.73%] |
| JuiceFS / 512 KiB | 829.711 / 7069.924 / 9268.152 | 848.280 / 6043.426 / 9171.036 | +2.24% | −14.52% | 55.61% → 56.11% | [−50.44%, +36.34%] |
| JuiceFS / 256 KiB | 829.711 / 7069.924 / 9268.152 | 848.104 / 6021.358 / 9249.084 | +2.22% | −14.83% | 55.61% → 49.36% | [−21.03%, +105.99%] |

The JuiceFS median improvements do not establish a reliable speedup: samples
move between roughly one-second and several-second regimes, and paired-cycle
intervals are very wide. These observations were all retained. Neither the
pooled minimum nor its difference is a noise-free estimate of latency.

The median of per-cycle minima changes by −8.39% / −1.41% for local 512/256 KiB
and −13.47% / +39.42% for JuiceFS 512/256 KiB. The number of cycles whose minimum
regresses more than 1% is respectively 1/8, 4/8, 4/8 and 5/8. These descriptive
checks supplement, rather than replace, the separate pooled min/median columns.

The result supports locality as a useful local optimization direction, but does
not justify a universal short-write cap. Follow up with mechanisms that retain
large writes, and with real Gather scheduling/metadata attribution.

## Separate syscall diagnostic

After clean timings finished, `strace -f -c -w` counted the same workload with
one warmup and one formal trial per job. Counts were identical on both disks:

| Cap | pwrite64 calls | Relative to whole | lseek calls |
|---|---:|---:|---:|
| Whole | 196 | 1.00× | 28 |
| 512 KiB | 644 | 3.29× | 28 |
| 256 KiB | 1,220 | 6.22× | 28 |

These are whole-job userspace syscall counts, including warmup; they are not
remote RPC counts. IPC has multiple buffers and framing, so the complete job
does not have the simple one-buffer microbenchmark's 8×/16× count ratios.
Summed traced syscall wall times are not elapsed-time acceptance results and
cannot establish how much of the noisy JuiceFS difference is caused by calls.
The call increase is confirmed; its latency cost needs clean comparisons.
[Trace manifests, commands and outputs](syscalls/) retain the diagnostic.

## Validation and retained evidence

- Both prototypes passed 111 relevant Rust tests and five injected cancellation
  checks each. `make build` and four real streaming Ray EIO tests passed for the
  256 KiB prototype, covering short writes and original-descriptor recovery sync.
  A Flight test needed its local port outside the sandbox; both the initial
  failure and successful rerun are retained in the validation archive.
- Runtime dispatch is `x86_64-avx512-vpclmulqdq`, using the same crc-fast version
  and features as production. CRC stayed enabled for every timing job.
- [audit.json](audit.json) verifies 96 jobs, 288 formal observations and 96
  warmups, with complete ABBA projections and unchanged statistics.
  [source-audit.json](source-audit.json) verifies all 1,233 pinned source/config
  hashes remained unchanged during measurement. PASS means evidence consistency,
  not compliance with the performance budget.
- [Raw samples](all-events.jsonl.gz), [summary.csv](summary.csv), four projection
  directories (including variance/CV and all cycle minima), [job logs](job-logs.tar.gz),
  [build identities](builds.json), [source identities](sources.json.gz),
  [environment](environment.json) and [validation](validation.json) are retained.
- [512 KiB patch](512k.patch.gz) and [256 KiB patch](256k.patch.gz) retain the rejected
  prototypes and their cross-chunk integrity test for reproduction. The patches
  are not active production changes. Broader controls and pre-EIO acceptance
  remain queued; the existing full-version +1% requirement is still unmet.

## Preregistered first experiment

- Isolate chunking against `20fe7b0b05646418d84433f6613a13e46762becc` (CRC on),
  rather than attributing the full pre-EIO/Arrow-version difference to CRC.
- Compile three release test binaries with identical settings: current whole
  buffer, 512 KiB, and 256 KiB maximum write when CRC/retries are enabled.
- Run the existing real IPC `bench_shuffle_writes`, `stream-wide`, concurrency
  8 (two actual files), no compression: 16 x 4 MiB per file, 128 MiB per sample.
  This existing harness reuses the same input MicroPartition; it is not a
  distinct-buffer cold-memory benchmark. All IPC bytes are read back and checked
  outside timing. Timing includes file creation, IPC encoding, write and close.
- Eight cycles, one warmup and three formal samples per job. On each disk,
  order is whole / 512 KiB / 256 KiB / 256 KiB / 512 KiB / whole, giving 48
  formal observations per variant/disk and two ABBA projections sharing data.
- Local `/tmp` and JuiceFS `/hdd-01/dev/qi.f`, private temporary directories;
  no global cache clearing, fsync or concurrent benchmark/build/profiler jobs.
  CPU affinity 2,3, Rayon threads 2, OMP threads 1. CRC/retry policy stays on (6).
- Record source/build hashes, complete job logs, warmups and observations,
  min/median/max, variance/CV, cycle minima and paired-cycle median intervals.
  Counter profiles are separate and cannot replace wall-clock acceptance.

This screen evaluates whether chunking is worth adopting. It is not the final
pre-EIO 1% acceptance matrix; those outstanding checks remain on the TODO list.
Retain unsuccessful candidates and unfavorable results in the report.

## Reproduction

Build each source variant using the same Cargo toolchain/profile:

```sh
CARGO_TARGET_DIR=/tmp/daft-p3-release CARGO_NET_OFFLINE=true \
  cargo test --locked --release -p daft-io -p daft-writers -p daft-shuffles \
  --no-run --message-format=json
```

Copy the executables before the next build and pin their hashes in `builds.json`.
The runner checks hashes both before and after measuring:

```sh
.venv/bin/python benchmarking/shuffle/normal_path_performance/crc_chunk_20260918/run.py \
  --builds /tmp/daft-crc-chunk-20260918/builds.json \
  --output /tmp/daft-crc-chunk-20260918/wide-both-disks \
  --shared-root /hdd-01/dev/qi.f --cycles 8 --samples 3 \
  --cases stream-wide --concurrency 8 --compression none
```

Recompute the published projections without the original binaries:

```sh
.venv/bin/python benchmarking/shuffle/normal_path_performance/crc_chunk_20260918/audit.py \
  benchmarking/shuffle/normal_path_performance/crc_chunk_20260918
```
