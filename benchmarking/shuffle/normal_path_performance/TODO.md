# Shuffle performance follow-ups

Production implementation: `20fe7b0b05646418d84433f6613a13e46762becc`.
The complete pre-EIO 1% requirement remains unmet. This list preserves the review
follow-ups; unchecked items are not completed optimizations.

## Experiment rules

- Acceptance baseline: pre-EIO `c6217a8c09cbfe773f88374adf46ff2a7fcb1195`.
  Also compare isolated changes against the current CRC-enabled implementation.
- Predeclare repeated ABBA and retain every sample/warmup, min/median/max,
  variance/CV and cycle minima. Compare min and median independently against +1%.
  Pin builds, sources and CPUs; keep A/A and confirmation studies separate.
- Cover local and JuiceFS, small/large streaming input, one-shot, concurrency 1/8,
  none/LZ4 and real FlightGather as appropriate. Profile separately from timing;
  counters do not replace elapsed-time acceptance or prove phase attribution.
- Preserve CRC, retry budget, short-write handling, owned prefix, corruption
  detection, cancellation/drain and original-descriptor recovery sync.

## Work queue

- [ ] **P1: CRC locality.** Whole/512 KiB/256 KiB screen completed; neither cap
  adopted. Local 512 KiB min/median improved 6.68%/5.86%, but JuiceFS observed min
  was +2.24% with an inconclusive median interval. Whole-job pwrite counts were
  3.29x/6.22x the original for the two caps. Consider locality without extra syscalls only as
  a separate experiment with correct successful-byte/short-write accounting.
  [Archived evidence](https://github.com/qifuapodex/Daft/tree/a745236736fada5d13157a262761db4b34f1f434/benchmarking/shuffle/normal_path_performance/crc_chunk_20260918).
- [ ] **P2: Real Gather phase attribution — next.** Initial worker-inclusive
  profile completed; a controlled baseline comparison and write/read/metadata/
  scheduling attribution remain pending. Input inspection confirms 32,000-byte,
  single-batch partitions. An explicit final-input prototype completed four
  symmetric cycles on both disks but was not adopted: versus current, local
  min/median changed -1.50%/+0.23%, JuiceFS +0.17%/+1.82%, with inconclusive
  intervals. Do not widen the global streaming threshold based on this result.
  [Profile archive](https://github.com/qifuapodex/Daft/tree/a745236736fada5d13157a262761db4b34f1f434/benchmarking/shuffle/normal_path_performance/gather_profile_20260918);
  [prototype evidence](https://github.com/qifuapodex/Daft/tree/a745236736fada5d13157a262761db4b34f1f434/benchmarking/shuffle/normal_path_performance/gather_final_input_20260918).
- [ ] **P3: Per-file fixed costs.** Use P2 to select isolated changes to seeks,
  cancellation/poison allocations, schema conversion and policy/tracking locks.
  Prior combined cancellation prototypes did not show stable concurrency-8 gains.
- [ ] **P4: Directory metadata.** Measure mkdir/stat on JuiceFS; consider moving
  directory creation into blocking I/O, scoped reuse or flatter names. Preserve
  shuffle/attempt isolation, cleanup and directory recreation.
- [ ] **P5: Bounded blocking-side draining.** Consider consuming ready inputs
  within blocking work, with batch/byte bounds and no idle thread per cache.
  Preserve backpressure, fairness, close/cancel behavior and pool capacity.
- [ ] **P6: Read-side scheduling/copy.** Measure actual readers before changing
  buffering or offloading. Buffered reads do not always schedule blocking work.
- [ ] **P7: LZ4 allocation.** Measure encoder/table allocation and possible reuse;
  retain Arrow IPC compatibility and defer broad encoder rewrites.
- [ ] **P8: Correctness/coverage.** Make Wide/portable CRC coverage explicit;
  investigate release schema mismatch behavior before claiming a bug. Document
  the existing post-finish cancellation and inflation-factor semantics.
- [ ] **P9: Final regression.** Rebuild changed Rust, run relevant functional and
  fault tests, isolated/pre-EIO matrices, both disks and Gather. Keep independent
  min/median results and unresolved risks in the PR; noise alone cannot pass it.

Completed production measurements and validation are in the
[retained report](deferred_first_batch_20260917/README.md). Rejected prototypes
remain outside the production code; their archived results are not pooled with
that report or substituted for its regression results.
