# Flotilla shared shuffle recovery

Status: opt-in, restricted map reconstruction. This is not full coverage of A5-012
or general stage rollback. In particular, **`read_parquet → repartition` is not
covered**: retaining an external scan task does not guarantee an input snapshot.

## Problem and scope

An attempt-specific shared shuffle file can disappear after its map succeeds.
Retrying a reduce task with the same references cannot restore it. Flotilla retains
eligible producer recipes, reconstructs unavailable maps, and resolves consumers to
the selected replacement immediately before dispatch.

The recovery coordinator belongs to Flotilla. Ray itself and the legacy Ray runner
need no changes. Shared Rust readers, exception conversion, and the local executor
preserve structured errors and isolate failed pipeline state. Worker loss alone
does not invalidate shared storage.

## Spark comparison

The reference is Apache Spark v4.0.1:

* [DAGScheduler](https://github.com/apache/spark/blob/v4.0.1/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala)
  distinguishes fetch failures from ordinary task failures, consolidates missing
  producer work, and rejects obsolete attempt reports. Indeterminate output may
  require producer and descendant rollback.
* [MapOutputTracker](https://github.com/apache/spark/blob/v4.0.1/core/src/main/scala/org/apache/spark/MapOutputTracker.scala)
  separates logical map identities from physical locations; executor caches use an
  epoch to observe changed output registrations.
* [RDD determinism](https://github.com/apache/spark/blob/v4.0.1/core/src/main/scala/org/apache/spark/rdd/RDD.scala)
  distinguishes equal ordered output, equal unordered output, and indeterminate
  output. Producer replay equivalence and consumer restart safety differ.

Flotilla adopts those responsibilities using its existing coordinator and dispatch
boundary. It does not add a per-file metadata RPC. Binding at submission is too
early: an entire reduce stage can already be queued when a map is reconstructed.
A worker-side epoch service remains an extension if worker-side queueing becomes a
measured bottleneck. Tasks already dispatched before reconstruction can still fail
and restart; queued coordinator tasks observe the replacement on first execution.

## Correctness contract

1. One consumer execution reads one immutable selected attempt per logical map.
   Readers never switch map attempts after emitting part of a stream.
2. A late report for an old attempt cannot invalidate a replacement.
3. Concurrent reports for a map share one reconstruction owner and completion budget.
4. A failed consumer commits no task result and restarts with fresh operator state.
   A fetch can fail after earlier maps emitted batches or expressions ran; this is
   why external side effects remain excluded despite ordinary worker-loss retries.
5. Reconstructed producers must preserve partition contents. Consumers need safe
   restart semantics, not the producer's stronger replay-equivalence allowlist.
6. Replacement publication updates the directory; it never re-emits a producer's
   original result or notification token.
7. Cancellation releases ownership and stops new submissions/publication. All
   retained state belongs to the plan and is dropped before normal shuffle cleanup.

## Eligibility

Eligible producers are shared hash/range repartition writes over retained in-memory
partitions or retained Flight shuffle inputs, with pure projections/filters and
batching. Range boundaries are retained, not sampled again. Primitive and nested
List/FixedSizeList/Struct/Map schemas are supported. Expressions include primitive
operators and an audited subset of synchronous deterministic numeric/string builtins.
Arbitrary functions are not assumed side-effect-free merely because they are builtins.

External scans/globs, Python/UDF expressions, random repartition, sampling, joins,
aggregates, sort/limit, and external writes remain ineligible **as producers**.
Indeterminate producers require a future stage/descendant commit and rollback protocol.

Consumers additionally admit limit, sort/top-N, joins, explode, gather/partitioning,
concatenation, and selected builtin aggregates when their expressions are safe.
Unknown functions, UDFs and external writes remain excluded. All submissions bind
at dispatch regardless of consumer retry eligibility, including direct submissions
from sort, into-partitions and as-of join. Unsupported consumers propagate failures;
missing unretained producers return an explicit unsupported-recovery error.

## Structured errors and fallback

`ShuffleFetchFailure` identifies shuffle, physical input, attempt, reduce partition,
and diagnostic path. Only a shared map open returning `NotFound` creates it.
Permission/configuration errors and transient transport errors stay distinct.
The payload survives local pipeline broadcasts, Flight status details and Python/Ray
serialization; classification does not parse error strings.

If the alternate route fails transiently, preserve that transient error for ordinary
scheduler retries. Otherwise retain the original missing-output identity when the
fallback has no structured fetch identity of its own.

## Output directory, retention and binding

A recipe retains the producer task, configuration and input partition references.
Original and replacement physical identities resolve to the same logical producer.
Registration occurs only after successful output validation. Invalid bookkeeping or
an exhausted retention cap logs a warning and skips the recipe; a successful map
still succeeds normally.

Recovery is disabled by default. Enabling it extends input/ObjectRef lifetimes until
plan completion. Retention has separate map-count and estimated-input-byte caps.
The byte charge conservatively sums retained partition sizes and Flight map descriptors;
shared inputs may be counted repeatedly. It is not an exact coordinator heap limit:
plans, hash tables, aliases and binding-cache overhead are additional, with map count
and attempt limits bounding recipe/alias counts. No eviction invalidates an existing
recipe; new recipes are skipped when a cap is reached.

The directory caches rebinding by `(shuffle_id, original Arc address)` for the current
directory version. Holding the original Arc prevents address reuse. All reducers
sharing an original list also share one replacement `Arc<BTreeMap>`. Unchanged lists
reuse their original Arc. Binding acquires the directory lock once, rather than once
per map; a cache hit does not scan the map list.

A reconstruction marks its failed output as being repaired before scheduling work.
Dispatch defers consumers referencing that output until replacement publication.
This also covers the interval between physical task completion and the recovery
owner processing its result. Blocked binding results are cached. Publication and
ownership release clear the cache, including on cancellation or failure.

## Scheduling, budgets and cancellation

Reconstruction runs through the existing resource scheduler without blocking its
event loop. Within a query, reconstruction tasks outrank recovering consumers, which
outrank ordinary tasks; existing node/task ordering breaks ties. Earlier queries
retain their priority. Tasks awaiting a dependency or execution permit are deferred.
A reconstruction permit is acquired only at actual dispatch and released with the
physical result, before recursive dependency recovery. Queueing holds no permit.

There is **no recovery deadline on producer or consumer queueing/execution**. An
optional timeout applies only while waiting for another reconstruction owner; it is
also disabled by default. Query cancellation remains the mechanism for stopping
long-running physical work. Ownership wait timeout and dropped unfinished recovery
futures do not consume a completed-attempt budget. Completed infrastructure failures
consume budget, but transient errors do not mark a producer permanently terminal.
Ordinary dispatcher transient/worker-loss retries still run first.

Configuration uses `DaftExecutionConfig` and `daft.set_execution_config`:

| Option | Default | Meaning |
| --- | ---: | --- |
| `flight_shuffle_recovery_max_attempts` | 0 | Completed reconstruction attempts per map; 0 disables recovery/retention |
| `flight_shuffle_recovery_max_inflight` | 4 | Concurrent physical reconstruction executions |
| `flight_shuffle_recovery_max_consumer_failures` | 64 | Fetch recovery rounds per logical consumer |
| `flight_shuffle_recovery_max_depth` | 16 | Maximum reconstruction dependency chain |
| `flight_shuffle_recovery_wait_timeout_ms` | 0 | Ownership wait timeout; 0 disables it |
| `flight_shuffle_recovery_max_retained_maps` | 10000 | Retained producer recipes per plan |
| `flight_shuffle_recovery_max_retained_bytes` | 268435456 | Estimated retained input bytes per plan |

```python
daft.set_execution_config(flight_shuffle_recovery_max_attempts=2)
```

Dependency traversal rejects cycles and excessive depth. All aliases share the
producer's attempt budget. Successful replacement selection is atomic under the
directory lock. Partial/failed reconstruction outputs are never published.

## Worker generations and lifecycle

The release branch's general `DaftError::Shared` broadcast reaches every unfinished
and queued pipeline input. A shared failure cell publishes the same error before
closing enqueue; an input rejected during closure retains that identity and still
runs `try_finish`.

Each `PlanState` has an execution generation distinct from its logical fingerprint.
A new task replaces a dead cached plan immediately. Unfinished receivers retain the
old generation, and `try_finish(fingerprint, input_id, generation)` can only remove
that execution. A late old finisher cannot remove the replacement. Retired states
are released by finishing inputs or plan cancellation. The statistics manager closes
its request channel before the final drain, preventing late snapshot requests from
blocking failed-input finalization. Retries preserve the logical
fingerprint so healthy same-plan tasks can reuse pipelines.

Original notifications complete once after the logical submission finishes. Physical
attempts use child cancellation tokens and new task IDs. A completion reservation
keeps consumer operators alive between attempts without firing `OperatorStart` before
the scheduler's Submitted event. Reconstruction task events identify the original
producer and node without reopening completed operators. Ordinary submissions store
an inline oneshot receiver; only recovery futures require a Box allocation.

Remote cancellation and directory deletion follow the existing Ray protocol. This
design does not add a distributed fence against an already-running worker completing
a write after cancellation; stronger cleanup barriers are a separate extension.

## Validation

Regression coverage includes exact row multisets across concurrent consumers,
all durability modes, stale reports, repeated deletion and budget exhaustion,
cancellation/notifications, recursive dependencies, unsupported producers and
recovery disabled. Additional review regressions cover:

* 128 consumers queued with stale references, with consumer retries disabled,
  followed by prioritized repair and first-dispatch replacement binding.
* Reconstruction queueing longer than the configured ownership timeout, and a
  separate ownership timeout that leaves the producer budget usable.
* 8,000 bindings of a shared 10,000-map list retaining one replacement Arc.
* Retention caps preserving successful map execution.
* Replacement pipeline generation created before the old receiver finishes.
* Transient fallback classification, configuration serialization, and rejection of
  snapshot requests after the statistics manager's final drain.

End-to-end tests inject deletion in the Flotilla scheduler actor after real Ray
workers publish output, across `auto/rpc/shared` and `none/background/sync`.
These are single-node tests. No multi-node storage-failure validation or large-scale
throughput/heap benchmark is claimed. Extending scan recovery requires snapshot
validation; indeterminate output requires stage/descendant rollback.

Final review validation (2026-09-10): 246 Rust tests passed across distributed
(105), local execution (95), shuffle (44), and common error (2), with four existing
unit-test ignores and one ignored doctest. Python/Ray validation covered 97 passing
cases across shuffle, generator retry, transient error, exception serialization and
configuration tests; the nine nested cases passed after correcting the injection
test's assumptions about physical map count. The repeated-deletion regression also
passed 100 isolated runs after the statistics shutdown fix. `make build` passed with
`DAFT_DASHBOARD_SKIP_BUILD=1`; Rust formatting and scoped Python lint checks passed.
