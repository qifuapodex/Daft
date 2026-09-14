# Managed shared-cluster scheduling

Flotilla can use an injected cluster-runtime client instead of writing Ray's
global autoscaling request. This is an experimental engine-side protocol, version
1. The external runtime must implement capacity aggregation, node protection,
reconciliation, and Ray's final idle/drain checks. Daft never deletes machines.

## Configuration and identity

Configure the client before the first Ray query:

```python
import daft
from daft.runners.cluster_scheduling import ClusterSchedulingConfig

# Defined in an importable runtime adapter, installed on the head.
from my_cluster_runtime import make_daft_client

daft.set_runner_ray(
    cluster_scheduling=ClusterSchedulingConfig(
        client_factory=make_daft_client,
        cluster_instance_id="actual-cluster-instance",
        job_attempt_id="submission-and-ray-job-attempt",
        requested_cpus=1000,
    ),
)
```

The factory receives an `ExecutionIdentity` on a background thread on the head. Each query gets a new
execution UUID, client session, worker manager, and actor instances. Concurrent
queries share Ray nodes and do not reserve exclusive CPUs. Worker instance UUIDs
change when actors are replaced; node IDs remain Ray node IDs.

`requested_cpus` is a fixed **total execution demand**, not an increment or the
observed cluster size. A runtime that already counts a job's capacity declaration
must deduplicate/aggregate execution records by `job_attempt_id` according to its
job policy. Setting this value to zero allows job-level capacity registration to
be the sole source of fixed demand. Adaptive demand aggregation is not enabled.

Without this configuration, the existing standalone autoscaling and best-effort
cleanup paths remain available. Standalone queries do not share the managed
UNKNOWN latch or wait for other queries' Flight reads and fsyncs. In managed mode both the scheduler's scale-up path and the legacy
idle-downscale path bypass global `request_resources`, including global zero.
Client failure never switches the execution back to that path.

## Runtime client contract

See `daft.runners.cluster_scheduling.ClusterSchedulingClient` for the Python
protocol. Clients must provide bounded RPCs and may be called from background
threads. No control RPC runs under the Rust worker-manager state mutex.

| Method | Acknowledgement |
| --- | --- |
| `register_execution(identity, capabilities)` | Return integer protocol version 1. Capabilities include the Ray `control_actor` handle and `execution_worker_retirement=True`. |
| `update_execution_demand(execution_id, revision, requested_total_cpus)` | Persist this execution's total demand. |
| `register_worker(execution_id, worker_instance_id, node_id)` | Return exactly `True` only after atomically registering the participant and establishing node protection; return `False` when admission is closed. |
| `report_node_usage(execution_id, node_id, revision, usage)` | Record the summary and verify protection for nonterminal participants. A terminal `RETIRED`/`FAILED` worker report acknowledges confirmed actor death; recording it may release only that participant's protection. Raise if recording or protection verification fails. |
| `finish_execution(execution_id, revision, cleanup_status)` | Withdraw this execution's demand while retaining unresolved participants/data protection. |

Updates use monotonically increasing revisions within each execution. Reports
are grouped by node, by default every two seconds. Local dispatch checks a
failure latch; an exception or expired control acknowledgements blocks new work.
`report_timeout_seconds` defaults to 30 seconds. A client that hangs cannot cause
new dispatch to continue indefinitely after that interval.

Missing or stale reports, failed admission acknowledgements, and lost actors
require runtime reconciliation. The runtime must retain protection during those
states, including after a driver dies. A failed session stays failed; there is
no automatic recovery that silently resets the ledger.

## Drain control

The registered `control_actor` exposes these Ray actor methods:

| Method | Meaning |
| --- | --- |
| `get_node_usage_snapshot()` | Per-execution worker state, instance IDs, logical CPU usage, active tasks, query data holds, discovery status and errors. |
| `prepare_node_drain(node_id, drain_epoch)` | Persist an exclusion and stop ordinary admission under the same local barrier used by dispatch. |
| `get_node_drain_status(node_id, drain_epoch)` | Return readiness and blockers for this runner's participants. |
| `cancel_node_drain(node_id, drain_epoch)` | Reopen admission before retirement is committed. The epoch cannot be reused for a later drain. |
| `retire_node_workers(node_id, drain_epoch)` | Recheck readiness and wait for the worker actors' death acknowledgements. |

Epochs are positive, monotonically increasing per node. Prepare and retirement
are idempotent for their current epochs. Retired nodes retain tombstones and
cannot be rediscovered or restored through an old cancel request. Discovery in
progress blocks a retirement acknowledgement. Finished discovery is collected
by control snapshots as well as by the running scheduler. Control failures only
block nodes that execution registered or attempted to register; discovery remains
a barrier until its result is known.

A task selected before prepare is rechecked at final submission. Rejected tasks
return to the pending queue without consuming retry attempts. Existing hard
affinity work can finish on a draining worker with retained dependencies.
Current Ray-backed affinity inputs can move to another worker once the target
has no local dependencies or is UNKNOWN. This fallback moves only the existing
Ray-owned affinity inputs; it does not release any UNKNOWN worker's protection.
Unknown and retired workers receive no new work.

The runtime must close node participant admission before prepare, aggregate all
executions/jobs, and only release node protection after all participant retirement acknowledgements.
A READY snapshot or successful prepare is not permission to remove a node.
Ray's own resources, objects and final drain checks still apply.

## Dependency retention and cancellation

Flotilla records compute before submission and establishes a query-scoped data
hold before a Flight-producing task can publish output. The hold spans all
consumers, future task builders, retry attempts, and shuffle reconstruction.
Shared-mount shuffle files retain their original service dependency conservatively.
Workers that never produced local output can become ready while a query is active.

On query completion the manager closes dispatch for that query and verifies
quiescence. Worker cleanup removes Flight registrations, waits for existing local
and remote response streams and background fsyncs, and removes local files.
Only after all worker acknowledgements does it remove shared files, using a
worker node with the shared mount, and release query data holds. Ordinary Ray
cleanup tasks also sweep surviving nodes of actors that died during the query.
Any partial failure preserves protection and is exposed as `cleanup_error`.
The process-wide read/fsync counters are used only by managed actors, which are
exclusive to one execution.

After an execution finishes, its dependency-free actors are retired automatically,
without draining their nodes or excluding those nodes from new queries. Actor
death must be acknowledged first, followed by a terminal report to the runtime.
Only after that report succeeds is the execution removed from the head's maps.
Reports continue while retirement is in progress. Failed cleanup or ambiguous
remote work keeps the execution available for external reconciliation.

`active_tasks` describes dispatched computation, not an exact count of Flight IO.
Data holds and `cleanup_error` are separate blockers; the protocol does not expose
an `inflight_operations` counter or authorize retirement from a zero task count.

Cancellation closes admission, stops task production, and waits for the scheduler
to account for task cancellations before query cleanup. It does not abort the
coordinator JoinSet and abandon shared worker accounting. Requesting
remote cancellation is not proof of termination: unconfirmed tasks become an
UNKNOWN blocker. Transport ambiguity cannot be interpreted as normal retirement. Confirmed actor
death is recorded as FAILED, allows a freshly protected replacement instance,
and follows the existing retry/shuffle-recovery path. Error-path dependencies may consequently need
external reconciliation; this version does not automatically clear UNKNOWN.

Returned results use native Ray references and survive retirement of Daft actors.
No partition-reference serialization format changes are required. Custom UDF
files or services outside Daft's tracked shuffle lifecycle need their own runtime
participant/protection; returning a filename is not a lifetime declaration.
Driver, head and worker must use the same updated Daft wheel.

## Validation and deployment boundary

Rust tests cover drain epochs, data holds, scheduler affinity, refresh exclusions,
and Flight stream lifetimes. Python tests use a fake runtime ledger to validate
capacity/participant separation, protocol failure and strict cleanup. The local
Ray integration test exercises concurrent Flight queries and actor retirement:

```bash
DAFT_RUNNER=native .venv/bin/pytest tests/ray/test_cluster_scheduling.py
DAFT_RUNNER=ray .venv/bin/pytest -m integration tests/ray/test_cluster_scheduling_integration.py
```

The integration client acknowledges simulated protection. It does not implement
a node agent or test physical autoscaler removal. Production enablement still
requires a runtime adapter, deployed node protection, Ray Data coordination if
used, and fault/drain validation against the pinned Ray version in an isolated
cluster.
