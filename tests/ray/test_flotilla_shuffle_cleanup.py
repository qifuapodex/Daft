from __future__ import annotations

import asyncio
import logging
from types import SimpleNamespace

import pytest

from daft.runners import flotilla


def test_standalone_drain_waits_for_only_the_requested_shuffles(monkeypatch):
    actor_type = flotilla.RaySwordfishActor.__ray_metadata__.modified_class
    counts = iter([2, 1, 0])
    events = []

    class Native:
        def shuffle_active_writes(self, shuffle_ids):
            assert shuffle_ids == [7, 9]
            count = next(counts)
            events.append(("writes", count))
            return count

        def shuffle_active_operations(self):
            raise AssertionError("another query's I/O must not block this cleanup")

        def unregister_shuffles(self, shuffle_ids):
            events.append(("unregister", shuffle_ids))

    async def tick(_delay):
        events.append(("wait", None))

    monkeypatch.setattr(flotilla.asyncio, "sleep", tick)
    actor = SimpleNamespace(native_executor=Native())
    asyncio.run(actor_type.drain_shuffle_writes(actor, [7, 9]))
    assert events == [
        ("writes", 2),
        ("wait", None),
        ("writes", 1),
        ("wait", None),
        ("writes", 0),
        ("unregister", [7, 9]),
    ]


def test_standalone_drain_timeout_does_not_unregister(monkeypatch):
    actor_type = flotilla.RaySwordfishActor.__ray_metadata__.modified_class

    class Native:
        def shuffle_active_writes(self, _ids):
            return 1

        def unregister_shuffles(self, _ids):
            raise AssertionError("write completion has not been acknowledged")

    times = iter([0.0, 31.0])
    monkeypatch.setattr(flotilla, "time", SimpleNamespace(monotonic=lambda: next(times)))
    with pytest.raises(RuntimeError, match="have not drained"):
        asyncio.run(actor_type.drain_shuffle_writes(SimpleNamespace(native_executor=Native()), [7]))


def test_write_drain_requires_acknowledgement_except_for_confirmed_actor_death():
    async def fail(error):
        raise error

    async def run():
        await flotilla.await_flight_shuffle_write_drain([fail(flotilla.ray.exceptions.ActorDiedError())])
        for error in [TimeoutError("unknown worker state"), RuntimeError("write did not finish")]:
            with pytest.raises(RuntimeError, match="preserving shuffle directories"):
                await flotilla.await_flight_shuffle_write_drain([fail(error)])

    asyncio.run(run())


def test_missing_write_drain_acknowledgement_times_out(monkeypatch):
    monkeypatch.setattr(flotilla, "_SHUFFLE_WRITE_DRAIN_RPC_TIMEOUT", 0.01)

    async def run():
        with pytest.raises(RuntimeError, match="acknowledgement timed out; preserving"):
            await flotilla.await_flight_shuffle_write_drain([asyncio.get_running_loop().create_future()])

    asyncio.run(run())


class _FakeActor:
    """Stand-in for the Ray actor handle wrapped by ``RaySwordfishActorHandle``."""

    def __init__(self) -> None:
        self.calls: list[list[int]] = []
        self.unregister_shuffles = self

    def remote(self, shuffle_ids: list[int]) -> str:
        self.calls.append(shuffle_ids)
        return f"ref-for-{shuffle_ids}"


def test_actor_handle_starts_the_unregistration_without_waiting():
    """The handle hands back the pending call so the coordinator can wait once.

    Awaiting per worker would serialize the fan-out behind whichever worker is
    slowest to answer, on a path that runs after the query's results are already
    delivered.
    """
    actor = _FakeActor()
    handle = flotilla.RaySwordfishActorHandle(actor)

    ref = handle.unregister_shuffles([7, 9])

    assert actor.calls == [[7, 9]]
    assert ref == "ref-for-[7, 9]"


async def _count(n: int) -> int:
    return n


async def _boom() -> int:
    raise RuntimeError("worker went away")


def test_awaiting_unregistrations_tolerates_a_dead_worker(caplog):
    """One unreachable worker must not fail the others or the query.

    A worker that cannot answer has already lost the registry we were asking it
    to trim, so the failure is logged and the remaining workers still report.
    """
    with caplog.at_level(logging.DEBUG, logger="daft.runners.flotilla"):
        asyncio.run(flotilla.await_flight_shuffle_unregistrations([_count(3), _boom(), _count(5)]))

    assert "worker went away" in caplog.text
    assert "Dropped 8 flight shuffle registration(s) across 3 worker(s)" in caplog.text


def test_awaiting_no_unregistrations_is_a_noop():
    """No shuffles, or no live workers, means nothing to wait for."""
    asyncio.run(flotilla.await_flight_shuffle_unregistrations([]))


# Ray validates node ids as 28-byte hex, so the fakes need well-formed ones.
_NODE_A = "a" * 56
_NODE_B = "b" * 56


class _BoundRemote:
    """One `.options(...)`-configured handle, remembering how it was pinned."""

    def __init__(self, parent: _FakeClearDirs, hard: bool) -> None:
        self.parent = parent
        self.hard = hard

    def remote(self, dirs: list[str]):
        return self.parent.call(self.hard, dirs)


class _FakeClearDirs:
    """Stand-in for the ``_clear_flight_shuffle_dirs`` Ray remote function.

    Node-local deletes arrive pinned hard (`soft=False`); the shared delete
    arrives pinned soft, which is how the fake tells them apart.
    """

    def __init__(self, fail_hard: bool = False, fail_soft: bool = False) -> None:
        self.fail_hard = fail_hard
        self.fail_soft = fail_soft
        self.ran: list[tuple[bool, tuple[str, ...]]] = []

    def options(self, scheduling_strategy=None):
        return _BoundRemote(self, getattr(scheduling_strategy, "soft", None) is False)

    def remote(self, dirs: list[str]):
        return self.call(False, dirs)

    def call(self, hard: bool, dirs: list[str]):
        async def _run() -> None:
            if hard and self.fail_hard:
                raise RuntimeError("TaskUnschedulableError: node is gone")
            if not hard and self.fail_soft:
                raise RuntimeError("mount went away")
            self.ran.append((hard, tuple(dirs)))

        return _run()


def _two_nodes() -> list[dict]:
    return [
        {"NodeID": _NODE_A, "Alive": True, "Resources": {"CPU": 8}},
        {"NodeID": _NODE_B, "Alive": True, "Resources": {"CPU": 8}},
    ]


def test_shared_cleanup_is_awaited_even_when_a_node_local_delete_fails(monkeypatch, caplog):
    """A churned node must not strand the shared delete.

    The node-local deletes are pinned hard, so a node that goes away makes one of
    them unschedulable. There is one copy of the shared tree and nothing else will
    ever remove it, so that failure must not take the shared delete with it.
    """
    fake = _FakeClearDirs(fail_hard=True)
    monkeypatch.setattr(flotilla, "_clear_flight_shuffle_dirs", fake)
    monkeypatch.setattr(flotilla.ray, "nodes", _two_nodes)

    with caplog.at_level(logging.DEBUG, logger="daft.runners.flotilla"):
        asyncio.run(
            flotilla.clear_flight_shuffle_dirs_on_all_nodes(["/local/daft_shuffle/1"], ["/mnt/shared/daft_shuffle/1"])
        )

    assert fake.ran == [(False, ("/mnt/shared/daft_shuffle/1",))], (
        f"the shared delete must run and be awaited, ran: {fake.ran}"
    )
    assert "unreachable node" in caplog.text
    # A node that is gone took its local disk with it, so that is not a warning.
    assert "Failed to clear shared" not in caplog.text


def test_a_failed_shared_cleanup_is_reported_loudly(monkeypatch, caplog):
    """A failed shared delete is worth a warning.

    Shared data outliving its query can only be cleared by a human now, so unlike
    the node-local case this one is not something to pass over quietly.
    """
    fake = _FakeClearDirs(fail_soft=True)
    monkeypatch.setattr(flotilla, "_clear_flight_shuffle_dirs", fake)
    monkeypatch.setattr(flotilla.ray, "nodes", _two_nodes)

    with caplog.at_level(logging.DEBUG, logger="daft.runners.flotilla"):
        asyncio.run(
            flotilla.clear_flight_shuffle_dirs_on_all_nodes(["/local/daft_shuffle/2"], ["/mnt/shared/daft_shuffle/2"])
        )

    # The local deletes still happened; only the shared one failed.
    assert [hard for hard, _ in fake.ran] == [True, True]
    assert "Failed to clear shared flight shuffle directories" in caplog.text
    assert "must be deleted by hand" in caplog.text
