# PR #20 normal-path performance follow-up

Created 2026-09-18 after reviewing
`/root/work/docs/daft/review-pr20-shuffle-normal-path-performance.md`.
Starting production revision: `20fe7b0b05646418d84433f6613a13e46762becc`.

## Acceptance and experiment rules

- Historical acceptance baseline: pre-EIO `c6217a8c09cbfe773f88374adf46ff2a7fcb1195`.
  Also compare each isolated change against the current CRC-enabled version.
- Compare elapsed-time **min and median separately** against the +1% budget.
  Retain max, variance, CV, every cycle, warmups and all observations; do not trim
  slow samples or use median confidence intervals as a substitute for min.
- Predeclare repeated ABBA jobs and sample counts. Keep exploratory screens,
  confirmation runs and A/A controls separate. Pin source/build hashes and CPUs.
- Cover local `/tmp` and JuiceFS `/hdd-01/dev/qi.f`, streaming small/large inputs,
  one-shot, concurrency 1/8, none/LZ4, and actual FlightGather as appropriate.
- CRC stays enabled. Preserve retry budget, partial-write handling, owned prefix,
  cancellation, active-write drain, corruption rejection and recovery sync.
- Builds, functional tests and profilers must not overlap timed experiments.
  `perf` counters explain mechanisms; wall-clock min/median remain acceptance.
- Current full-version +1% acceptance is **unmet**, particularly local Gather.
  Historical V5 CRC costs do not describe the latest version. Arrow 59 -> 60 is
  part of the historical comparison, not an isolated CRC change.

## Work queue

- [ ] **P1 / first screen complete, cap not adopted: CRC locality.** Compare current whole-buffer writes
  with 512 KiB and 256 KiB limits only when write-side CRC/retries are enabled.
  Start with real IPC writer release binaries on both disks; retain one-shot
  and small-file controls. Check syscall costs on JuiceFS before adoption.
  Validate successful-byte CRC accounting, short writes, prefix/final cursor,
  EIO recovery and cancellation. Record actual CRC backend identity.
  [Eight-cycle results](crc_chunk_20260918/README.md): local 512 KiB min/median
  improve 6.68%/5.86%, but JuiceFS min is +2.24% with an inconclusive median
  interval. 256 KiB fails the observed-min budget on both disks. Prototypes are
  retained as patches; production Rust is restored. Consider CRC locality
  without extra writes (e.g. a temporary pre-write digest committed only for
  successful bytes, with a correct short-write fallback) as a separate experiment.
- [ ] **P2 / in progress: Profile real FlightGather.** Attribute write, read, metadata and
  scheduling costs, including worker processes. Profile separately from ABBA.
  Do not attribute all full-process futex calls to one blocking handoff or
  convert aggregate query differences into measured per-file costs.
  Input-shape verification and an explicit final-input experiment are complete
  (below); widening the global streaming threshold is no longer the next step.
  [Initial current-version capture](gather_profile_20260918/README.md) completed
  128 profiled collects with all oracles passing; counter-mode smoke checks
  also passed for current/pre-EIO wheels. Library-level CPU shares are retained,
  but a controlled baseline comparison and phase attribution are still pending.
  Follow-up screen completed: input inspection confirms 32 partitions of exactly 32,000
  logical bytes / 2,000 rows / one Arrow batch. `FlightGatherState::push` creates
  a cache per MP and immediately closes it, so the experiment used an explicit
  final-input operation instead of widening the global streaming threshold.
  [The separate uncompressed screen](gather_final_input_20260918/README.md) ran four symmetric cycles across
  pre-EIO/current/prototype and local/JuiceFS, four formal collects plus one
  warmup per job (32 formal samples per version/disk). Relative to current,
  local min/median change -1.50%/+0.23%, JuiceFS +0.17%/+1.82%; intervals remain
  inconclusive. Relative to pre-EIO both observed min/median exceed +1% on both
  disks. Prototype not adopted; production Rust restored, patch/tests retained.
  Return to controlled phase attribution before expanding this candidate's matrix.
- [ ] **P3: Per-file fixed costs.** Use P2 evidence to choose isolated changes:
  initial/final seeks, cancellation guard/poison allocations, repeated Arrow
  schema conversion and policy/tracking locks. Prior combined cancellation
  prototypes did not show stable c8 gains; avoid assuming each is worthwhile.
- [ ] **P4: Directory metadata.** Measure mkdir/stat cost on JuiceFS. Consider
  moving mkdir into the first blocking write, scoped directory reuse or flatter
  names. Preserve shuffle/attempt isolation, cleanup and directory recreation.
- [ ] **P5: Bounded blocking-side draining.** Investigate consuming ready input
  using `try_recv` with batch/byte limits. Preserve backpressure, fairness,
  close/cancel races and pool capacity. Do not park one thread per cache.
- [ ] **P6: Read-side scheduling/copy.** Profile actual reader paths; Tokio does
  not schedule blocking work for every buffered read, and not all readers use
  an 8 KiB BufReader. Optimize only the demonstrated copy/scheduling bottleneck.
- [ ] **P7: LZ4 allocation.** Measure encoder/table allocations and consider
  reuse if material. Keep Arrow IPC compatibility and compression correctness;
  defer broad encoder rewrites until smaller, better-supported changes finish.
- [ ] **P8: Correctness/coverage follow-ups.** Make Wide vs portable CRC coverage
  explicit on capable hardware. Investigate release schema mismatch behavior
  before claiming corruption; consider a cache-boundary guard if needed.
  Document existing post-finish cancellation and inflation-factor semantics.
- [ ] **P9: Final regression and PR evidence.** Rebuild (`make build` plus
  release artifacts), run relevant functional/fault tests, isolated and
  pre-EIO ABBA matrices, both disks and Gather; update report/PR with independent
  min/median results and unresolved risks. Never mark completion from noise alone.

## Results log

- 2026-09-18: Review checked against current code, historical evidence and
  microbenchmark source. No new confirmed correctness bug. CRC chunking is an
  experiment, not an accepted production optimization. Implementation started.
- 2026-09-18: Whole/512 KiB/256 KiB release artifacts pinned. Both candidates
  passed 111 relevant Rust tests and five injected cancellation checks each;
  `make build` and four real streaming EIO/short-write/recovery-sync Ray tests
  passed for 256 KiB. Runtime probe reports `x86_64-avx512-vpclmulqdq`.
  Builds, tests and profiler capability checks finished before measurements.
- 2026-09-18: The first two-disk screen finished: 96 jobs, 288 formal samples,
  96 warmups, every oracle PASS, and all 1,233 source hashes unchanged. Neither
  cap meets both observed-statistic requirements on both disks, so it is not
  adopted. Full samples, patches, uncertainty and audits are retained in the
  linked report. Gather profiling controls are prepared to exclude setup,
  warmup, result verification and cleanup while including descendant workers.
- 2026-09-18: Separate syscall tracing confirmed 196 / 644 / 1,220 pwrite64
  calls for whole / 512 KiB / 256 KiB in equal two-trial jobs on each disk.
  lseek counts stayed at 28. Initial Gather CPU capture and profiling-tool
  validation completed; no causal or 1% acceptance claim is made from these
  instrumented observations. Production Rust and the development build are restored.
- 2026-09-18: Gather final-input prototype completed 48 three-version/two-disk
  jobs, 192 formal collects and 48 warmups, all output/hash/spill checks passing.
  Four-cycle exploratory results do not support adoption or satisfy the historical
  1% budget. 111 relevant Rust tests, six injected cancellation tests and four
  real Ray streaming fault tests passed; source patch, raw observations, source
  hashes and reproducible statistics are retained. Production Rust is restored.
