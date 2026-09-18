# Gather final-input experiment

Status: validation and all 48 predeclared jobs completed. **Prototype not adopted:**
the screen does not establish a stable improvement, and the historical 1% gate
remains unmet. Production Rust is restored. This follows [TODO P2](../TODO.md).

## Results and decision

All 192 formal observations and 48 warmups passed the full output oracle; every
driver/worker extension matched its pinned wheel, with zero spill. All 1,232
tracked source/config hashes and the workload/wheels were unchanged during the
study. [Manifest](manifest.json), [complete statistics](summary.json),
[raw events](events.jsonl.gz), and all individual [job logs](logs/) are retained.

Absolute collect times are milliseconds; each row has 32 formal observations.
Warmups and the slower observations are retained, without trimming.

| Disk / version | Min | Median | Max | CV |
|---|---:|---:|---:|---:|
| local / pre-EIO | 57.132 | 60.545 | 67.257 | 4.08% |
| local / current | 58.892 | 61.747 | 68.331 | 3.67% |
| local / prototype | 58.009 | 61.889 | 69.472 | 4.04% |
| JuiceFS / pre-EIO | 1898.599 | 2023.295 | 4495.150 | 22.34% |
| JuiceFS / current | 1921.949 | 2012.971 | 2138.495 | 2.91% |
| JuiceFS / prototype | 1925.212 | 2049.642 | 2191.380 | 3.55% |

Positive changes mean slower. Min and median are independent observed criteria;
the median interval cannot establish whether the observed minimum passes.

| Disk / comparison to prototype | Δmin | Δmedian | Median 95% interval | Cycles with min >+1% |
|---|---:|---:|---:|---:|
| local / pre-EIO | +1.536% | +2.219% | [-1.026%, +3.578%] | 2/4 |
| local / current | -1.498% | +0.229% | [-0.995%, +1.661%] | 1/4 |
| JuiceFS / pre-EIO | +1.402% | +1.302% | [-2.155%, +2.785%] | 2/4 |
| JuiceFS / current | +0.170% | +1.822% | [-1.123%, +3.550%] | 1/4 |

Against the current version, local min improves while median does not; JuiceFS
median exceeds the observed +1% budget. The intervals do not prove a causal
regression, but also do not support a stable gain or equivalence within 1%.
Against pre-EIO, both min and median exceed +1% on both disks. That comparison
includes Arrow 59 -> 60 and earlier EIO changes, not just this prototype or CRC.
The pre-EIO JuiceFS maximum of 4.495 seconds remains in the data; its high CV is
not a reason to remove it or judge this experiment by mean alone.

The current code already uses fused IPC write/close when the queue is closed by
the time the writer consumes its input. Gather's immediate close can already
take that path, so eliminating a forwarding task need not eliminate a second
blocking operation. This is a code-level limitation of the hypothesis, not a
measured operation-count attribution. The screen does not establish that this
handoff is the main cause of the historical Gather regression.

Keep the prototype and tests as a compressed patch, restore production code,
and return to controlled read/write/metadata/scheduling attribution before
expanding this candidate's matrix. LZ4, larger inputs and broader confirmation
were not run for this candidate; it has not been accepted as an optimization.

Audit sample counts, version/CPU pins, script hashes and all retained summary
statistics (including variance, cycle minima and bootstrap intervals):

```sh
.venv/bin/python benchmarking/shuffle/normal_path_performance/gather_final_input_20260918/audit.py \
  benchmarking/shuffle/normal_path_performance/gather_final_input_20260918
```

## Hypothesis and implementation

The existing benchmark's input inspection ([log](shape.log.gz)) confirms 32
materialized partitions, each containing 2,000 rows, 32,000 logical bytes and one
Arrow batch. Its physical plan is `InMemorySource -> FlightGather -> Window`.
`FlightGatherState::push` creates one cache for each received MicroPartition,
pushes it once, and immediately closes the cache. Its current 8 KiB first-input
deferral therefore misses these inputs even though the caller knows they are final.

The [prototype patch](final-input.patch.gz) adds a consuming cache
`write_and_close(partition)` operation. For a first/final input it defers writer
startup at any size, letting close drain the existing queue and use the existing
fused IPC write/close operation. Gather calls this API. Multiple-input callers
keep the original 8 KiB threshold, bounded queue and background writer behavior.
CRC, file format, retry budget, recovery and blocking-I/O cancellation are unchanged.

The source base is `8e7e5e7343c6cb1c78de06860ee58fee1c7f4c99`, whose production Rust
matches `20fe7b0b05646418d84433f6613a13e46762becc`. This is an isolated scheduling
prototype; no global threshold increase or CRC chunking is included.

## Predeclared screen

- Three pinned release wheels: pre-EIO `c6217a8c09cbfe773f88374adf46ff2a7fcb1195`,
  current CRC-enabled version, and current plus this prototype.
- Four cycles. Each cycle runs old-local, current-local, prototype-local,
  old-JuiceFS, current-JuiceFS, prototype-JuiceFS, then the reverse sequence.
  Projecting either baseline against the prototype on one disk gives ABBA.
- Four formal collects and one warmup per job: 48 jobs, 192 formal samples and
  48 warmups; 32 formal samples per version/disk. No sample trimming or pooling
  with historical experiments. The comparison views share observations.
- Real FlightGather, 64,000 rows, 32 materialized partitions, no compression.
  Two worker nodes on one host, four logical Ray CPUs each, Rayon 2, OMP 1.
  The cluster, drivers and descendants share fixed CPU affinity 2-9.
  Nodes/resources, loaded driver/worker extension hashes, output values and
  ordinal columns are checked. Object-store spill invalidates a run.
- `/tmp` and `/hdd-01/dev/qi.f`; warm caches, no explicit clearing. Only collect
  is timed. This PerPartition writer path does not fsync; `shared_only/sync`
  settings do not turn it into a synchronous durability benchmark.
- Builds, functional tests and profiling finish before clean timing starts.
  The input inspection is separate and is not an acceptance measurement.
  The exact [runner](run_screen.py.gz) and [workload](flight_gather_collect.py.gz)
  are captured with their hashes; temporary artifact paths are in the manifest.
- Retain min/median/max, variance, CV, every cycle's minimum and paired-cycle
  median intervals. Four cycles are exploratory, not proof of 1% equivalence.
  LZ4, larger inputs and broader regression confirmation remain required.

## Functional validation

- `make build` passed. Release component tests: 16 IO shuffle tests, 29 writer
  tests and 66 shuffle tests (111 total), with no failures.
- New full readback coverage combines empty, 4 KiB, 32,000-byte and >128 KiB
  final inputs with absent/small/large prior input, none/LZ4 and file rotation.
- Six injected cancellation tests passed, including the new 32,000-byte final
  input case. Active-write registration remains until cancelled I/O drains.
- Four real Ray streaming EIO/short-write/original-FD-sync tests passed.
- The first release-wheel attempt used the default Python interpreter and
  failed because sandbox restrictions prevent the frontend builder from binding
  a local port. The original failure is retained in `wheel-build.log.gz`.
  The host retry explicitly pins the same Python 3.11 environment as the current
  comparison wheel. No timing observations are taken from failed builds.
- After the screen, production Rust was restored and `make build` passed again;
  the prototype is retained only as a patch, not active production code.

Raw temporary working directory: `/tmp/daft-gather-final-20260918`.
