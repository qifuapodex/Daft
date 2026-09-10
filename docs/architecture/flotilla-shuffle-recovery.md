# Flotilla shared shuffle recovery

Status: initial implementation; validation results below. This is a restricted
reconstruction path, not general stage rollback or full coverage of A5-012.

## 设计摘要

借鉴 Spark 的职责划分：读取层报告带身份的 FetchFailure，调度层保留生产任务，
输出目录维护逻辑 Map 到当前物理 attempt 的映射。文件丢失后，只重建对应 Map，
再用新引用重新执行失败的消费者。同一个 Map 的并发失败合并为一次重建。

首版只允许能够证明分区内容等价的生产任务，以及可安全重新执行的消费者。
不确定性任务需要完整 stage/下游回滚；目前缺少这样的提交与回滚协议，因此明确
拒绝自动重建这类生产任务。不会仅凭固定随机种子或 hash 分区器就认定整个任务可重放。

实现放在 Flotilla 中。无需修改 Ray 项目或旧 Ray runner；共享 Rust 读取、异常转换
和本地执行器需要配合，才能把文件身份和失败状态完整传回 Flotilla。正常执行保留
pipeline 复用，不新增逐文件元数据 RPC、不扫描目录、不预先排序全部数据。

## Problem and scope

A successful map task publishes an immutable, attempt-specific shared shuffle file.
If the file subsequently disappears, retrying a consumer with its original input
references cannot restore it. Flotilla needs to invalidate the physical output,
reconstruct its producer, and bind consumers to the replacement output.

This design applies to Flotilla and its Ray worker adapter. It does not modify Ray
itself or introduce recovery in the legacy Ray runner. Shared error and reader code
must continue to fail normally when used without the Flotilla recovery coordinator.
Worker loss alone does not invalidate shared storage.

## Spark comparison

The reference baseline is Apache Spark v4.0.1, with the development branch consulted
for subsequent work on nondeterministic output and query-level rollback.

* [FetchFailed](https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/TaskEndReason.scala)
  separates unavailable shuffle output from ordinary task failure.
* [DAGScheduler](https://github.com/apache/spark/blob/v4.0.1/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala)
  invalidates output registrations and resubmits missing work, consolidates failures,
  and rejects stale attempts. Indeterminate output can require complete producer and
  descendant recomputation; a result stage that cannot be rolled back aborts.
* [MapOutputTracker](https://github.com/apache/spark/blob/v4.0.1/core/src/main/scala/org/apache/spark/MapOutputTracker.scala)
  separates logical partitions from current physical output and invalidates metadata
  caches using an epoch.
* [RDD determinism](https://github.com/apache/spark/blob/v4.0.1/core/src/main/scala/org/apache/spark/rdd/RDD.scala)
  distinguishes equal ordered output, equal unordered output, and indeterminate output.
* [ShuffleExchangeExec](https://github.com/apache/spark/blob/v4.0.1/sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/ShuffleExchangeExec.scala)
  can sort before round-robin partitioning to stabilize partition membership.

Flotilla adopts these responsibilities, not Spark's service topology. Initially,
the coordinator binds an immutable output snapshot before submitting a task; workers
do not need an additional metadata RPC service. The reader never switches to another
map attempt after emitting part of a stream.

## Correctness contract

1. Each consumer execution reads one immutable selected attempt per logical map.
2. A missing-output report only invalidates the physical attempt it names. A late
   report for a replaced attempt cannot invalidate the replacement.
3. Concurrent failures for the same output share a reconstruction. Different
   outputs have bounded reconstruction concurrency and independent budgets.
4. A consumer that already consumed batches restarts from the beginning with fresh
   operator state. Its failed execution contributes no committed task result.
5. Partial recovery is allowed only when producer partition contents are replay
   equivalent and the failed consumer can execute again without external effects.
6. A completed producer is not emitted a second time through the original map result
   stream. Replacement publication changes the output directory, not stream cardinality.
7. Cancellation and exhausted recovery budgets terminate recovery. References and
   replay descriptors are owned by the plan run and released before shuffle cleanup.

Replay equivalence concerns the full producer plan and inputs, not just its hash
partitioner. A fixed random seed is insufficient when row order or batching changes.
An expression's determinism flag alone is not a guarantee of side-effect freedom.

## Initial supported subset

The initial implementation uses a conservative allowlist of replayable local plans
and expressions. Shared hash/range repartition writes over retained stable inputs
and pure row transformations can be replayed. The same checks apply to consumers;
arbitrary UDFs, external writes, sampling, order-sensitive selection, and unknown
operators must not be automatically replayed. Range boundaries must be retained,
not sampled again during recovery. External scan inputs, if admitted, require an
explicit stability contract; retaining a path alone does not create a snapshot.

Unproven/indeterminate producers report an actionable unsupported recovery error.
Unsupported consumers propagate the fetch error without replay. Neither enters an
output-reconstruction loop. No full result buffering, global
commit barrier, or general nondeterministic stage rollback is included initially.
This restriction is a deliberate first implementation boundary, not full coverage
of A5-012. Allowlist additions require evidence and tests of replay semantics.

## Components

### Structured shuffle failure

A shared error payload identifies `shuffle_id`, physical `input_id`, map `attempt`,
`partition_idx`, and diagnostic path/message. The exception type identifies the
missing-output category; only `File::open` returning `NotFound` creates it. Missing output is
distinct from permission/configuration failure and transient transport/access failure.
Integrity failures can use the same protocol but are not automatically considered
transient. Recovery classification must not use error-string/path parsing.

The payload survives Arrow Flight conversion and Python/Ray serialization. A Python
exception carries structured fields and is pickle-safe; the driver reconstructs the
payload by exception type and attributes. RPC status details must preserve the same
identity if the RPC path reports the failure. Route fallback must not discard the
original missing-output identity when the fallback also fails.

### Replay descriptors and output directory

Each recoverable producer retains its local plan, input descriptors/partition refs,
configuration, and original logical identity. Physical output is recorded only after
successful task completion. Combined map files need one directory entry per map,
not one entry per map/reduce pair. Retained inputs remain live until the plan ends;
this can extend object lifetime and is part of the cost of enabling recovery.

The directory resolves original references and references from replacement attempts
to the same logical producer. Its selected output includes physical input identity,
attempt, and server address. A replacement can therefore run as a new physical task
without changing the logical dependency. New attempts preserve file-name isolation.

### Recovery coordinator

The coordinator sits above resource scheduling. It submits reconstruction tasks
through the existing scheduler, so it never blocks the scheduler event loop waiting
for work that only that loop can dispatch. Per-output synchronization consolidates
reports; a bounded global semaphore limits concurrent reconstructions. Dependency
recovery is bounded and rejects cycles. Recursive recovery uses the same protocol
and terminates at retained inputs or an unsupported producer.

The implemented coordinator supports recursive recovery of retained shuffle inputs.
`DAFT_SHUFFLE_RECOVERY_MAX_ATTEMPTS` defaults to 2 per logical map; 0 disables
reconstruction. Additional bounds are 4 simultaneous producer executions per plan,
64 further fetch failures per consumer execution loop, dependency depth 16, and a
120-second deadline starting at the first fetch failure. Waiting for a repair lock
or execution slot counts toward that deadline. Ordinary execution has no new timeout.
Cancellation releases reconstruction ownership; an interrupted attempt consumes budget.

An output follows this state machine:

```text
Available(selected attempt)
  -> reconstructing(selected attempt, retry budget)
  -> Available(replacement attempt)
  or TerminalFailure(reason)
```

After acquiring reconstruction ownership, recheck the selected attempt: another
consumer may already have repaired it. Publish only a complete, validated task result.
Transient retries within reconstruction retain the existing scheduler policy; output
recovery has its own finite budget so many consumers cannot multiply it indefinitely.

### Consumer submission and lifecycle

Before each execution, resolve shuffle dependencies to the selected immutable output
snapshot. On a typed failure, check consumer replay eligibility, reconstruct the
producer, rebind, and resubmit. Original pipeline notification tokens must complete
exactly once, after final success/failure/cancellation, not after the failed execution.

Binding the first execution does not require consumer retry eligibility. A consumer
outside the retry allowlist still uses a replacement that another task has already
published. Ordinary tasks and node-local shuffles bypass replay analysis and the
logical-completion guard entirely.

Physical execution attempts and logical pipeline completion are distinct. Recovery
must not prematurely end an operator, double-decrement running-task counters, or
re-emit a completed producer's original completion token. Reconstructed producers
must have distinguishable task execution identities. Recovery events/logs should
include the logical producer, failed/replacement attempts, consumer, and budget.

The implementation holds original notification tokens on a logical submission future.
Physical attempts use child cancellation tokens and new task IDs/fingerprints.
A statistics completion guard keeps consumer operators alive between attempts;
reconstructed producers emit task events without reopening completed operator nodes.

### Shared local pipeline failure

Several tasks can share a local pipeline. Recovery depends on every unfinished or
queued input receiving the pipeline error before it can commit successful metadata.
The release branch provides this through `finish_input_streams` and
`DaftError::Shared(Arc<DaftError>)` (#4). This change reuses that mechanism and unwraps
`Shared` when classifying or serializing shuffle failures; no shuffle-specific failure
cell is added. Each input retains its error independently of cached-plan lifetime.
The pipeline also publishes this same error Arc in a `OnceLock` before closing its
enqueue channel. Inputs rejected during closure receive a failed result handle with
that original error, so they preserve recovery classification and still execute
`try_finish`. Failed cached plans remain registered until all tracked inputs finish;
late finishers cannot remove a new pipeline instance using the same fingerprint.
New recovery executions use fresh fingerprints so failed local operator state is not
reused. Normal executions continue to share pipelines.

### Cleanup and cancellation

The plan owns replay state and recovery futures. Dropping/cancelling the plan stops
submission and cancels outstanding reconstruction attempts before normal cleanup.
Old files may remain until plan cleanup; deletion during active recovery is avoided.
No worker failure handler should delete still-valid shared files.

Cancellation here guarantees that the coordinator stops submitting and publishing
replacement references. Remote task cancellation and directory deletion use the existing
Ray cleanup protocol; this change does not introduce a distributed fence against a
worker finishing a write after cancellation. The cancellation test covers a coordinator
waiting for reconstruction capacity, not an interrupted remote write. A stronger
worker-completion/cleanup barrier remains a separate extension.

## Extension points

* Add explicit stage dependency and attempt state for indeterminate rollback; invalidate
  affected descendants and refuse rollback across externally committed results.
* Add source snapshot/version validation and additional verified pure operators.
* Add worker metadata caching by output-directory version if submission payloads become
  a measured bottleneck. Do not add a per-file coordinator round trip.
* Consider deterministic repartition separately. Sorting every map input changes normal
  execution cost; runtime checksums alone are not a general proof of semantic equality.
* Add reconstruction checkpoints or replicas for expensive/non-replayable producers.

The current allowlist admits scalar primitive schemas, retained partition references
and Flight shuffle inputs, scans of those retained inputs, projections/filters of pure
scalar expressions, batching, and shared hash/range writes with retained boundaries.
Consumers additionally admit selected builtin aggregates (count, distinct count, sum,
min/max, mean, boolean and/or). Aggregates are excluded from replayable producers.
External scan tasks/globs, Python/complex schemas, functions/UDFs, random repartition,
sampling, sort/limit, joins, and external writes are outside the initial allowlist.
Specialized submission paths such as sort and into-partitions are not recovery-enabled.
Supporting those paths requires a separate semantic and lifecycle review.

## Validation and acceptance

Tests must cover payload preservation through local Flight and Python/Ray, missing
files after publication, simultaneous failures for one map, stale reports after
replacement, fresh consumer state after partial reads, budgets, cancellation, and
unsupported replay plans. Check row multisets with unique IDs against an independent
oracle, not only row counts. Check original map output is not emitted twice.

Use dedicated temporary directories for fault injection. Rust state-machine tests
provide deterministic scheduling coverage; an end-to-end Flotilla/Ray test must prove
actual reconstruction and reference replacement. Run the relevant existing shuffle,
scheduler, lifecycle, and cleanup regressions. Test sync/background/none publication
without confusing normal visibility with persistence under storage failure.

Validation after rebasing onto release #4 and self-review fixes (2026-09-10):

* Distributed Rust suite: 99 passed, 1 existing ignored test. New tests cover exact
  row multisets across concurrent consumers and two maps, all durability modes, stale
  reports, repeated deletion/budget exhaustion, cancellation while waiting for capacity,
  exactly-once completion notification, recursive dependency reconstruction, and rejection
  of random repartition, disabled recovery, and first-execution binding for consumers
  outside the retry allowlist.
* Shuffle Rust suite: 43 passed, 3 existing ignored tests, including typed Flight
  status/header round trips and rejection of ordinary NotFound/permission errors.
* Common-error Rust suite: 2 passed. The local execution regression for finished,
  partial, queued and rejected inputs passed.
* Flotilla/Ray: 87 tests passed across the complete shuffle suite, generator retry,
  transient errors and exception serialization. This includes 18 shared-shuffle
  tests and 9 actual
  deletion/reconstruction tests (`auto/rpc/shared` ×
  `none/background/sync`). The hook runs inside the scheduler actor after real worker
  publication, deletes the referenced file, and checks exact rows and exactly one fresh
  replacement output. A local monkeypatch alone would not reach this actor.
  Existing placement, durability, normal/failure cleanup, and node-local routing
  regressions also passed.
* Python exception pickle and Ray exception serialization round trip passed, including
  maximum-width unsigned identities.
* `make build` passed with the repository-supported `DAFT_DASHBOARD_SKIP_BUILD=1`;
  dashboard frontend assets are unrelated and were not built.

These are single-node tests using temporary directories and real local Ray workers.
They do not validate multi-node mount failures, worker power loss, storage corruption,
or recovery of external scans. Retained lineage can extend input memory/object-store
lifetimes until plan completion; no large-scale memory or throughput benchmark is claimed.
