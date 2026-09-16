"""Unit tests for the distributed-Limit state machine.

Exercises `_LimitCounterImpl` directly (no Ray cluster). The interesting
invariants here are the retry-rewind path in `start_task` — failed
SwordfishTask attempts must release their claimed budget back so the retry
emits the right total. Dataframe-level limit tests don't fail tasks, so this
path is otherwise uncovered.
"""

from __future__ import annotations

import asyncio
import threading
from collections import Counter
from itertools import product

import pytest

ray = pytest.importorskip("ray")

import daft
from daft import DataType, col, func
from daft.execution.ray_distributed_limit import _LimitCounterImpl
from tests.conftest import get_tests_daft_runner_name


def test_claim_basic():
    actor = _LimitCounterImpl(limit=10, offset=0)
    actor.start_task("t1")
    assert actor.claim("t1", 100) == (0, 10, True)
    assert actor.claim("t1", 100) == (0, 0, True)


def test_claim_with_offset():
    actor = _LimitCounterImpl(limit=10, offset=5)
    actor.start_task("t1")
    # 5 rows go to skip, 10 to take, 5 discarded.
    assert actor.claim("t1", 20) == (5, 10, True)


def test_claim_offset_spans_multiple_calls():
    actor = _LimitCounterImpl(limit=10, offset=15)
    actor.start_task("t1")
    # First batch fully consumed by offset.
    assert actor.claim("t1", 10) == (10, 0, False)
    # Second batch: 5 more to skip, then 10 to take.
    assert actor.claim("t1", 20) == (5, 10, True)


def test_start_task_rewinds_prior_claim():
    """A retried task gets its prior take/skip refunded to the global budget."""
    actor = _LimitCounterImpl(limit=100, offset=0)
    actor.start_task("t1")
    assert actor.claim("t1", 60) == (0, 60, False)
    assert actor.remaining_take == 40

    # Simulate retry: same input_id calls start_task again.
    actor.start_task("t1")
    assert actor.remaining_take == 100, "budget should be restored after rewind"
    # The retry can now claim up to the full limit again.
    assert actor.claim("t1", 80) == (0, 80, False)


def test_start_task_rewinds_offset_claim():
    """Rewind must restore offset progress too, not just take."""
    actor = _LimitCounterImpl(limit=10, offset=20)
    actor.start_task("t1")
    assert actor.claim("t1", 15) == (15, 0, False)
    assert actor.remaining_skip == 5

    actor.start_task("t1")
    assert actor.remaining_skip == 20, "offset progress should rewind"
    assert actor.remaining_take == 10


def test_start_task_rewind_isolated_per_task():
    """Rewinding t1 must not affect t2's claims."""
    actor = _LimitCounterImpl(limit=100, offset=0)
    actor.start_task("t1")
    actor.start_task("t2")
    actor.claim("t1", 30)  # t1 takes 30
    actor.claim("t2", 40)  # t2 takes 40
    assert actor.remaining_take == 30

    # Retry t1 only.
    actor.start_task("t1")
    # t1's 30 should be refunded; t2's 40 stays claimed.
    assert actor.remaining_take == 60
    # t2's bookkeeping should be intact.
    assert actor.input_claims["t2"] == (0, 40)


def test_double_start_task_is_idempotent():
    """Calling start_task twice with no intervening claim must not rewind twice."""
    actor = _LimitCounterImpl(limit=100, offset=0)
    actor.start_task("t1")
    actor.claim("t1", 30)
    assert actor.remaining_take == 70

    actor.start_task("t1")  # first rewind: refund 30
    actor.start_task("t1")  # second call: nothing to refund, must be a no-op
    assert actor.remaining_take == 100


def test_zero_claim_entries_dropped():
    """Tasks that never consume budget shouldn't accumulate in input_claims."""
    actor = _LimitCounterImpl(limit=5, offset=0)
    actor.start_task("t1")
    actor.claim("t1", 10)  # claims all 5
    assert actor.is_done()

    # Subsequent tasks past the limit get (0, 0, True) and shouldn't be retained.
    for i in range(100):
        tid = f"past_limit_{i}"
        actor.start_task(tid)
        assert actor.claim(tid, 50) == (0, 0, True)
        assert tid not in actor.input_claims, "past-limit task should not be retained"

    # Only the one boundary task remains.
    assert set(actor.input_claims.keys()) == {"t1"}


def test_is_done_transitions():
    actor = _LimitCounterImpl(limit=10, offset=0)
    assert not actor.is_done()
    actor.start_task("t1")
    actor.claim("t1", 5)
    assert not actor.is_done()
    actor.claim("t1", 5)
    assert actor.is_done()


@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
@pytest.mark.parametrize("start", [0, 1])
def test_distributed_limit_retries_after_worker_death(tmp_path, start):
    """`.limit(N)` must still produce N rows when a SwordfishTask crashes mid-claim.

    Without the rewind in `_LimitCounterImpl.start_task`, the failed attempt's
    claim stays charged against the global budget while its slice never reaches
    downstream — the retry then sees a smaller budget and the output undercounts.
    """
    import json
    import os

    # On a multi-node cluster, use --basetemp on a filesystem shared by the
    # driver and all workers so they contend for the same one-shot marker.
    marker = str(tmp_path / "crashed_once")

    @func(return_dtype=DataType.int64())
    def crash_once(v: int) -> int:
        # LIMIT is unordered: crash the first invocation that gets any row.
        # Exclusive creation ensures concurrent invocations kill one worker.
        try:
            fd = os.open(marker, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError:
            return v
        with os.fdopen(fd, "w") as f:
            json.dump({"node": ray.get_runtime_context().get_node_id(), "pid": os.getpid(), "value": v}, f)
        # Hard-exit the swordfish actor process after closing the marker.
        # Ray surfaces ActorDiedError; flotilla replaces the worker and retries
        # the failed task. DistributedLimitSink then calls start_task(input_id)
        # to refund the crashed attempt's claim before claiming again.
        os._exit(1)

    # The nonzero start deterministically guards against a value-specific
    # crash condition, even when local scheduling happens to pick row 0 first.
    df = daft.range(start, start + 15, partitions=15).limit(3).select(crash_once(col("id")))
    result = df.to_pydict()["id"]

    assert os.path.exists(marker), "UDF never crashed — retry path not exercised"
    with open(marker) as f:
        fault = json.load(f)
    assert fault["node"] and fault["pid"] > 0, fault
    assert fault["value"] in range(start, start + 15), fault
    # A distributed, unordered LIMIT promises cardinality, not particular rows
    # or contributor order. Missing claim refunds would undercount the output.
    assert len(result) == len(set(result)) == 3, (fault, result)
    assert set(result) <= set(range(start, start + 15)), (fault, result)

    # A separate query must still run after the worker has been replaced.
    assert daft.from_pydict({"x": [1, 2, 3]}).agg(col("x").sum()).to_pydict() == {"x": [6]}


def test_claim_signals_done_event():
    """`claim` must wake `await_limit_completion` rather than have it poll."""

    async def run():
        actor = _LimitCounterImpl(limit=2, offset=0)
        waiter = asyncio.create_task(actor.await_limit_completion())
        # Let the waiter reach `Event.wait()` before the limit is satisfied, so
        # the test covers the wake-up rather than the already-done shortcut.
        await asyncio.sleep(0)
        assert not waiter.done()

        actor.start_task("t1")
        actor.claim("t1", 2)
        return await asyncio.wait_for(waiter, timeout=5)

    assert asyncio.run(run()) == ["t1"]


def test_done_event_set_before_any_waiter():
    """A limit reached before the first `await_limit_completion` still resolves.

    The event is created lazily — `__init__` runs before the actor has an event
    loop — so the first waiter has to notice a limit that was already satisfied
    instead of blocking forever.
    """

    async def run():
        actor = _LimitCounterImpl(limit=1, offset=0)
        actor.start_task("t1")
        actor.claim("t1", 1)
        return await asyncio.wait_for(actor.await_limit_completion(), timeout=5)

    assert asyncio.run(run()) == ["t1"]


def test_retry_rewind_reopens_done_event():
    """A refund that lifts the budget back above zero must un-signal `done`.

    `start_task` rewinds a crashed attempt's claims. If the event stayed set,
    a waiter would conclude the limit was satisfied while the rewound rows had
    never reached downstream.
    """

    async def run():
        actor = _LimitCounterImpl(limit=1, offset=0)
        actor.start_task("t1")
        actor.claim("t1", 1)
        assert actor.is_done()
        # Force the event into existence and into the set state.
        assert await asyncio.wait_for(actor.await_limit_completion(), timeout=5) == ["t1"]

        actor.start_task("t1")  # retry: refunds the claim
        assert not actor.is_done()
        waiter = asyncio.create_task(actor.await_limit_completion())
        await asyncio.sleep(0)
        assert not waiter.done(), "refund left the done event set"

        actor.claim("t2", 1)
        return await asyncio.wait_for(waiter, timeout=5)

    assert asyncio.run(run()) == ["t2"]


def test_refund_between_set_and_resume_does_not_release_waiter():
    """A refund landing after `set()` but before the waiter resumes must not release it.

    `asyncio.Event.clear()` does not un-resolve a future that `set()` already
    resolved, so a waiter that returned on the first wakeup would hand the
    coordinator a contributor set for a limit that is no longer satisfied. The
    coordinator consumes that set exactly once and then cancels the
    non-contributors, so the refunded rows could be cancelled away before the
    retry re-claims them and the query would return fewer rows than requested.

    This is the ordering that the previous `while not is_done(): sleep(0.01)`
    poll could not get wrong, because it re-checked the condition on every
    wakeup. `await_limit_completion` has to keep doing that.
    """

    async def run():
        actor = _LimitCounterImpl(limit=1, offset=0)
        waiter = asyncio.create_task(actor.await_limit_completion())
        await asyncio.sleep(0)  # park the waiter inside Event.wait()

        actor.start_task("t1")
        actor.claim("t1", 1)  # satisfied: resolves the parked waiter's future
        actor.start_task("t1")  # retry refunds before the waiter ever resumes
        assert not actor.is_done(), "refund should have reopened the budget"

        # Give the waiter every chance to resume on a stale wakeup.
        for _ in range(3):
            await asyncio.sleep(0)
        assert not waiter.done(), (
            "await_limit_completion returned while remaining_take > 0; it must "
            "re-check is_done() instead of trusting a single Event wakeup"
        )

        actor.claim("t2", 1)  # the retry supplies the row
        return await asyncio.wait_for(waiter, timeout=5)

    assert asyncio.run(run()) == ["t2"]


@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
def test_limit_under_cross_join_keeps_the_other_side_whole():
    """Satisfying a `LIMIT` must not stop the *other* side of a cross join.

    Previously the limited scan was fused with the other scan, so cancellation
    had to be scoped to the limit's subtree. The limited input is now
    materialized before joining; its early stop must still leave the other
    input whole.

    Asserted on shape rather than on *which* rows survive: a distributed limit
    claims rows first-come-first-served across tasks, so the surviving set is
    not deterministic. The cross product's shape is: every row the limit let
    through must pair with every one of the right side's rows.
    """
    limit = 100
    right_rows = 5

    @func(return_dtype=DataType.int64())
    def identity(v: int) -> int:
        # Blocks limit pushdown into the scan, forcing the path where the
        # counter actor actually reports `done` mid-stream.
        return v

    left = daft.range(0, 400, partitions=4).select(identity(col("id")).alias("lid")).limit(limit)
    right = daft.range(0, right_rows, partitions=1).select(col("id").alias("rid"))

    result = left.join(right, how="cross").to_pydict()

    assert len(result["lid"]) == limit * right_rows, (
        f"cross join produced {len(result['lid'])} rows, expected {limit * right_rows}. "
        "A short count means the limit's early stop reached the join's other side."
    )
    assert Counter(result["rid"]) == {r: limit for r in range(right_rows)}, (
        f"right-side rows are not evenly paired: {Counter(result['rid'])}. "
        "The right scan was truncated by the limit's early stop."
    )


@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
def test_limit_under_broadcast_join_emits_every_limited_row():
    """A satisfied `LIMIT` under a broadcast join must not drop probe rows.

    `PushDownLimit` leaves the `limit` on the join's receiver side, and
    `map_plan` fuses both into one task plan. The build side covers every
    possible key here, so each of the `limit` rows has exactly one match and the
    inner join has to emit exactly `limit` rows — no assumption about *which*
    rows the distributed limit let through, which is not deterministic.
    """
    limit = 200
    universe = 400

    @func(return_dtype=DataType.int64())
    def identity(v: int) -> int:
        return v

    big = daft.range(0, universe, partitions=8).select(identity(col("id")).alias("id"))
    small = daft.from_pydict({"id": list(range(universe)), "tag": [f"t{i}" for i in range(universe)]})

    result = big.limit(limit).join(small, on="id").to_pydict()

    assert len(result["id"]) == limit, (
        f"join emitted {len(result['id'])} rows, expected {limit}. "
        "Rows the limit let through failed to find their match."
    )
    assert len(set(result["id"])) == limit, f"join duplicated rows: {result['id']}"
    # Each row must carry its own tag, not another row's — a partially built
    # hash table can match while pairing the wrong side.
    assert all(tag == f"t{i}" for i, tag in zip(result["id"], result["tag"]))


@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
def test_limit_stops_upstream_work():
    """A satisfied `LIMIT` must stop upstream production, not just discard it.

    Regression test for the distributed limit doing no work-saving at all: the
    counter actor's `done` flag was dropped on the floor, so every task drained
    its whole partition through the (non-pushdown-able) UDF and the query cost
    the same as having no `LIMIT`.

    Asserts on rows actually fed to the UDF rather than on wall clock, so it
    does not depend on machine speed. The bound is deliberately loose — how much
    is saved depends on scheduling, and the counter increments are
    fire-and-forget so the total can undercount slightly — but the pre-fix
    behavior feeds the UDF *every* row, which is far outside it.
    """
    total_rows = 8 * 20_000

    @ray.remote(num_cpus=0)
    class RowCounter:
        def __init__(self) -> None:
            self.n = 0

        def add(self, n: int) -> None:
            self.n += n

        def get(self) -> int:
            return self.n

    counter = RowCounter.options(name="limit_row_counter", lifetime=None).remote()

    @func(return_dtype=DataType.bool())
    def keep_and_count(a: int) -> bool:
        import ray as _ray

        _ray.get_actor("limit_row_counter").add.remote(1)
        return a % 1_000 == 0

    try:
        # Small morsels so the early-stop signal has a chance to land partway
        # through a partition; the default morsel is larger than these
        # partitions, which would make the test measure nothing.
        daft.set_execution_config(default_morsel_size=2_000)
        df = daft.range(0, total_rows, partitions=8).where(keep_and_count(col("id"))).limit(4)
        result = df.to_pydict()
        assert len(result["id"]) == 4

        processed = ray.get(counter.get.remote())
        assert processed < total_rows * 0.9, (
            f"UDF saw {processed} of {total_rows} rows; the limit saved almost nothing. "
            "The DistributedLimitSink is not honoring the counter actor's `done` flag, "
            "or the cancellation is not reaching the source."
        )
    finally:
        daft.set_execution_config(default_morsel_size=None)
        ray.kill(counter)


@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
def test_limit_larger_than_input_reads_everything():
    """`limit > total rows` never satisfies the counter, so nothing is cancelled.

    The complement of `test_limit_stops_upstream_work`: this query legitimately
    has to read all of its input, and early stopping must not truncate it.
    """

    @func(return_dtype=DataType.bool())
    def keep(a: int) -> bool:
        return a % 3 == 0

    df = daft.range(0, 300, partitions=8).where(keep(col("id"))).limit(10_000)
    assert len(df.to_pydict()["id"]) == 100


def _materialize_with_deadline(df, timeout_s=120):
    """Materialize `df` in a daemon thread, failing the test if it doesn't finish.

    `@pytest.mark.timeout` can't guard the deadlock these tests cover: the query
    blocks inside the Rust runtime holding the GIL, so a SIGALRM handler never
    gets a chance to run and pytest-timeout's signal method hangs right along
    with it. A watchdog in a separate thread is the only one that can report.
    The thread is a daemon so a regression fails this test and lets the rest of
    the session continue instead of wedging the run.
    """
    outcome: dict = {}

    def target():
        try:
            outcome["result"] = df.to_pydict()
        except BaseException as e:
            outcome["error"] = e

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    thread.join(timeout_s)
    if thread.is_alive():
        pytest.fail(f"query did not finish within {timeout_s}s — the distributed limit deadlocked instead of returning")
    if "error" in outcome:
        raise outcome["error"]
    return outcome["result"]


@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
@pytest.mark.parametrize("num_partitions", [1, 4, 16])
def test_limit_under_into_partitions_does_not_hang(num_partitions):
    """`into_partitions` on top of a `Limit` must not deadlock the query.

    `PushDownLimit` commutes `Limit-IntoPartitions` into `IntoPartitions-Limit`,
    so `IntoPartitionsNode` ends up consuming `LimitNode`'s task-builder stream.
    It has to count its input tasks before it can decide whether to coalesce or
    split, so it drains that stream to exhaustion before submitting anything —
    while `LimitNode`'s loop only finishes once the tasks it emitted have run
    and claimed rows from the counter actor. If `LimitNode` holds its output
    channel open until its loop ends, neither side can move: no task is ever
    submitted, no `claim` ever arrives, and the query hangs forever with the
    actor spinning in `_LimitCounterImpl.await_limit_completion`.

    `num_partitions` covers all three `IntoPartitionsNode` shapes against the
    8-task input: coalesce to one task, coalesce to four, and split to sixteen.
    """
    df = daft.range(0, 10_000, partitions=8).into_partitions(num_partitions).limit(10)
    result = _materialize_with_deadline(df)

    assert len(result["id"]) == 10
    assert len(set(result["id"])) == 10, f"limit returned duplicate rows: {result['id']}"


@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
def test_limit_under_into_partitions_offset_and_overshoot():
    """The same deadlock, on the paths that don't early-stop.

    With `limit > total rows` the counter never reaches zero, so the loop has to
    end by draining its input rather than by the actor reporting completion —
    the output channel has to be released in that case too. `limit(0)` is the
    other no-contributor path.
    """
    df = daft.range(0, 100, partitions=8).into_partitions(3).offset(10).limit(20)
    assert len(_materialize_with_deadline(df)["id"]) == 20

    df = daft.range(0, 100, partitions=8).into_partitions(3).limit(1000)
    assert len(_materialize_with_deadline(df)["id"]) == 100

    df = daft.range(0, 100, partitions=8).into_partitions(3).limit(0)
    assert len(_materialize_with_deadline(df)["id"]) == 0


@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
@pytest.mark.parametrize("limit_on_left", [True, False])
def test_limit_under_cross_join_does_not_hang(limit_on_left):
    """A `LIMIT` on either side of a cross join must not deadlock the query.

    `combine_with` originally lost completion/cancellation tokens when fusing
    limited builders into cross-join tasks, stranding `LimitNode`. Preserving
    those tokens fixed the hang but left duplicated LIMIT claims. Materializing
    the limited input before fan-out must preserve both liveness and row counts.

    Asserted on shape rather than on which rows survive: a distributed limit
    claims rows first-come-first-served across tasks, so the surviving set is
    not deterministic. The cross product's shape is: every row the limit let
    through pairs with every one of the other side's rows.
    """
    limit = 100
    other_rows = 5

    @func(return_dtype=DataType.int64())
    def identity(v: int) -> int:
        # Blocks limit pushdown into the scan, so the limit is a real
        # `distributed_limit` below the cross join.
        return v

    limited = daft.range(0, 400, partitions=4).select(identity(col("id")).alias("lid")).limit(limit)
    whole = daft.range(0, other_rows, partitions=1).select(col("id").alias("rid"))

    joined = limited.join(whole, how="cross") if limit_on_left else whole.join(limited, how="cross")
    result = _materialize_with_deadline(joined)

    assert len(result["lid"]) == limit * other_rows, (
        f"cross join produced {len(result['lid'])} rows, expected {limit * other_rows}"
    )
    assert Counter(result["rid"]) == {r: limit for r in range(other_rows)}, (
        f"rows are not evenly paired: {Counter(result['rid'])}"
    )


@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
def test_limit_under_cross_join_natural_drain():
    """The cross-join deadlock on the path where the limit is never hit.

    With `limit > total rows` the counter actor never reports completion, so the
    limit loop can only end by draining: input exhausted *and* nothing derived
    from a forwarded builder still running. Cross join's templates used to
    derive more limited tasks after input exhaustion, risking premature actor
    teardown. Materialization must also drain this path without hanging.
    """
    limited = daft.range(0, 20, partitions=4).limit(1000)
    whole = daft.range(0, 3, partitions=1).select(col("id").alias("rid"))

    result = _materialize_with_deadline(limited.join(whole, how="cross"))
    assert len(result["id"]) == 20 * 3


def _assert_unique_cross_product(result, left_rows, right_rows):
    # An unordered distributed LIMIT may select any rows. Check cardinality and
    # every pair's multiplicity without prescribing which rows win the limit.
    left, right = set(result["lid"]), set(result["rid"])
    assert len(left) == (left_rows if right_rows else 0)
    assert len(right) == (right_rows if left_rows else 0)
    assert Counter(zip(result["lid"], result["rid"])) == Counter(product(left, right))


@pytest.mark.parametrize("limit_on_left", [True, False])
@pytest.mark.parametrize("other_partitions", [1, 2, 5])
@pytest.mark.parametrize("limit", [0, 100, 500])
@pytest.mark.parametrize("with_udf", [False, True])
def test_limit_under_cross_join_pairs_with_every_partition_of_the_other_side(
    limit_on_left, other_partitions, limit, with_udf
):
    """Every row a `LIMIT` lets through must pair with the *whole* other side.

    With multiple partitions on the other side, fusing a limited builder into
    each pair of partitions used to make those copies compete for one counter
    budget. The selected rows must instead be reused for every pairing.
    """
    other_rows = 50

    @func(return_dtype=DataType.int64())
    def identity(v: int) -> int:
        return v

    limited = daft.range(0, 400, partitions=4)
    if with_udf:
        limited = limited.select(identity(col("id")).alias("id"))
    limited = limited.limit(limit).select(col("id").alias("lid"))
    whole = daft.range(0, other_rows, partitions=other_partitions).select(col("id").alias("rid"))

    joined = limited.join(whole, how="cross") if limit_on_left else whole.join(limited, how="cross")
    result = _materialize_with_deadline(joined)
    _assert_unique_cross_product(result, min(limit, 400), other_rows)


@pytest.mark.parametrize("right_partitions", [1, 2, 5])
@pytest.mark.parametrize("limits", [(20, 10), (500, 100), (0, 10), (20, 0)])
def test_limits_on_both_cross_join_inputs(right_partitions, limits):
    @func(return_dtype=DataType.int64())
    def identity(v: int) -> int:
        return v

    left_limit, right_limit = limits
    left = daft.range(0, 40, partitions=4).select(identity(col("id")).alias("lid")).limit(left_limit)
    right = daft.range(0, 25, partitions=right_partitions).select(identity(col("id")).alias("rid")).limit(right_limit)

    result = _materialize_with_deadline(left.join(right, how="cross"))
    _assert_unique_cross_product(result, min(left_limit, 40), min(right_limit, 25))


def test_cross_join_limits_preserve_duplicate_multiplicity():
    left = daft.range(0, 40, partitions=4).select((col("id") // 2).alias("lid")).limit(100)
    right = daft.range(0, 20, partitions=5).select((col("id") // 2).alias("rid")).limit(100)

    result = _materialize_with_deadline(left.join(right, how="cross"))
    assert Counter(zip(result["lid"], result["rid"])) == dict.fromkeys(product(range(20), range(10)), 4)


def test_cross_join_limit_with_offset_below_projection():
    @func(return_dtype=DataType.int64())
    def identity(v: int) -> int:
        return v

    left = (
        daft.range(0, 40, partitions=4)
        .select(identity(col("id")))
        .offset(10)
        .limit(20)
        .select((col("id") + 100).alias("lid"))
    )
    right = daft.range(0, 25, partitions=5).select(col("id").alias("rid"))
    result = _materialize_with_deadline(left.join(right, how="cross"))
    _assert_unique_cross_product(result, 20, 25)
    assert all(100 <= value < 140 for value in result["lid"])


@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
@pytest.mark.parametrize("failure_stage", ["input", "join"])
def test_cross_join_limits_survive_worker_retry(tmp_path, failure_stage):
    import os

    marker = str(tmp_path / "crashed_once")

    @func(return_dtype=DataType.int64())
    def identity(v: int) -> int:
        return v

    @func(return_dtype=DataType.int64())
    def crash_once(v: int) -> int:
        # Exclusively create the marker so concurrent tasks kill just one worker.
        try:
            fd = os.open(marker, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError:
            return v
        os.close(fd)
        os._exit(1)

    left = daft.range(0, 40, partitions=4).select(identity(col("id")).alias("lid")).limit(20)
    right = daft.range(0, 25, partitions=5).select(identity(col("id")).alias("rid")).limit(10)
    if failure_stage == "input":
        # Crash after a LIMIT claim, before its output is materialized.
        left = left.select(crash_once(col("lid")).alias("lid"))
    joined = left.join(right, how="cross")
    if failure_stage == "join":
        # A retried join must read the same fixed input without claiming again.
        # Depend on both sides so this UDF cannot be pushed below the join.
        joined = joined.select(col("lid"), col("rid"), crash_once(col("lid") + col("rid")).alias("value"))

    result = _materialize_with_deadline(joined)
    assert os.path.exists(marker), "worker retry was not exercised"
    _assert_unique_cross_product(result, 20, 10)
