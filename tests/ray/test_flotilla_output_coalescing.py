from __future__ import annotations

import asyncio
from collections.abc import Callable
from types import SimpleNamespace
from typing import Any

import pytest

import daft
from daft.recordbatch.micropartition import MicroPartition
from daft.runners import flotilla

TARGET_BYTES = 64 * 1024 * 1024


@pytest.fixture
def run_plan() -> Callable[..., Any]:
    """Exercise the production worker generator without starting a Ray cluster."""
    return flotilla.RaySwordfishActor.__ray_metadata__.modified_class.run_plan


async def consume(
    run_plan: Callable[..., Any], parts: list[MicroPartition], partitioned: bool = False
) -> list[MicroPartition]:
    """Feed native partitions through the real output and metadata handling."""

    class Handle:
        def __init__(self) -> None:
            self.parts = iter(parts)

        def __aiter__(self) -> Handle:
            return self

        async def __anext__(self) -> Any:
            try:
                return next(self.parts)._micropartition
            except StopIteration:
                raise StopAsyncIteration from None

        async def try_finish(self) -> SimpleNamespace:
            return SimpleNamespace(encode=lambda: b"test-stats")

    async def execute(*args: Any) -> Handle:
        return Handle()

    async def resolve(*args: Any) -> tuple[dict, int]:
        return {}, 1

    worker = SimpleNamespace(native_executor=SimpleNamespace(run=execute), _resolve_inputs=resolve)
    plan = SimpleNamespace(has_partitioned_output=lambda: partitioned)
    cfg = daft.context.get_context().daft_execution_config
    emitted = [x async for x in run_plan(worker, plan, cfg, None)]
    metadata = emitted.pop()
    assert isinstance(metadata, flotilla.SwordfishTaskMetadata)
    assert metadata.stats == b"test-stats"
    assert not metadata.is_flight_shuffle
    assert len(metadata.partition_metadatas) == len(emitted)
    assert [m.num_rows for m in metadata.partition_metadatas] == [len(p) for p in emitted]
    assert [m.size_bytes for m in metadata.partition_metadatas] == [p.size_bytes() for p in emitted]
    return emitted


def test_empty_buckets_coalesce_with_data(run_plan: Callable[..., Any]) -> None:
    data = MicroPartition.from_pydict({"x": [11, 12]})
    empty = data.slice(0, 0)
    assert empty.size_bytes() == 0
    out = asyncio.run(consume(run_plan, [empty] * 64 + [data] + [empty] * 63))
    assert len(out) == 1
    assert out[0].to_pydict() == {"x": [11, 12]}


def test_all_empty_preserves_one_schema_bearing_result(run_plan: Callable[..., Any]) -> None:
    data = MicroPartition.from_pydict({"x": [11]})
    out = asyncio.run(consume(run_plan, [data.slice(0, 0)] * 128))
    assert len(out) == 1
    assert len(out[0]) == 0
    assert out[0].schema() == data.schema()


def test_fixed_partition_slots_are_not_coalesced(run_plan: Callable[..., Any]) -> None:
    data = MicroPartition.from_pydict({"x": [11]})
    out = asyncio.run(consume(run_plan, [data.slice(0, 0), data, data.slice(0, 0)], True))
    assert [p.to_pydict() for p in out] == [{"x": []}, {"x": [11]}, {"x": []}]


def test_unknown_size_flushes_immediately(run_plan: Callable[..., Any], monkeypatch: pytest.MonkeyPatch) -> None:
    data = MicroPartition.from_pydict({"x": [11]})
    monkeypatch.setattr(MicroPartition, "size_bytes", lambda self: None)
    out = asyncio.run(consume(run_plan, [data] * 3))
    assert len(out) == 3


def test_byte_threshold_still_applies(run_plan: Callable[..., Any], monkeypatch: pytest.MonkeyPatch) -> None:
    data = MicroPartition.from_pydict({"x": [11]})
    monkeypatch.setattr(MicroPartition, "size_bytes", lambda self: TARGET_BYTES // 2)
    out = asyncio.run(consume(run_plan, [data] * 5))
    assert [len(p) for p in out] == [2, 2, 1]


def test_empty_buffers_have_a_count_limit(run_plan: Callable[..., Any]) -> None:
    data = MicroPartition.from_pydict({"x": [11]}).slice(0, 0)
    out = asyncio.run(consume(run_plan, [data] * 1025))
    assert len(out) == 2
    assert all(len(p) == 0 for p in out)
    assert all(p.schema() == data.schema() for p in out)


def test_small_buffers_have_a_count_limit(run_plan: Callable[..., Any]) -> None:
    data = MicroPartition.from_pydict({"x": [11]})
    out = asyncio.run(consume(run_plan, [data] * 1025))
    assert [len(p) for p in out] == [1024, 1]
    assert MicroPartition.concat(out).to_pydict() == {"x": [11] * 1025}
