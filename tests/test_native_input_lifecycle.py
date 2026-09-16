from __future__ import annotations

import asyncio
import gc

import pytest

from daft.daft import LocalPhysicalPlan, NativeExecutor, PyDaftContext
from daft.logical.builder import LogicalPlanBuilder
from daft.recordbatch import MicroPartition
from daft.runners.partitioning import LocalPartitionSet, PartitionCacheEntry


@pytest.fixture
def native_plan():
    partition = MicroPartition.from_pydict({"x": [1, 2, 3]})
    parts = LocalPartitionSet()
    parts.set_partition_from_table(0, partition)
    entry = PartitionCacheEntry("cancel-test", parts)
    builder = LogicalPlanBuilder.from_in_memory_scan(entry, partition.schema(), 1, partition.size_bytes(), 3)
    return LocalPhysicalPlan.from_logical_plan_builder(builder._builder, {entry.key: [partition._micropartition]})


async def wait_for_no_plans(executor):
    async def wait():
        while executor.active_plan_count():
            await asyncio.sleep(0.01)

    await asyncio.wait_for(wait(), timeout=5)


@pytest.mark.parametrize("release", ["cancel", "drop", "cancel_enqueue"])
def test_abandoned_native_input_releases_its_pipeline(native_plan, release):
    async def run():
        executor = NativeExecutor(False, "")
        plan, inputs = native_plan
        pending = executor.run(plan, PyDaftContext(), 1, inputs, {"plan_fingerprint": "123"})
        if release == "cancel_enqueue":
            pending.cancel()
            with pytest.raises(asyncio.CancelledError):
                await pending
        else:
            receiver = await pending
            del pending  # The completed asyncio Future also owns its result.
            if release == "cancel":
                receiver.cancel()
                receiver.cancel()
            else:
                del receiver
                gc.collect()
        await wait_for_no_plans(executor)

    asyncio.run(run())


@pytest.mark.parametrize("finish_sibling", [True, False])
def test_cancel_preserves_other_inputs_in_the_same_pipeline(native_plan, finish_sibling):
    async def run():
        executor = NativeExecutor(False, "")
        plan, inputs = native_plan
        ctx = PyDaftContext()
        context = {"plan_fingerprint": "123"}
        first = await executor.run(plan, ctx, 1, inputs, context)
        second = await executor.run(plan, ctx, 2, inputs, context)
        first.cancel()
        assert executor.active_plan_count() == 1
        if finish_sibling:
            values = []
            async for partition in second:
                if partition is None:
                    break
                values.extend(MicroPartition._from_pymicropartition(partition).to_pydict()["x"])
            assert values == [1, 2, 3]
            await second.try_finish()
        else:
            second.cancel()
        await wait_for_no_plans(executor)

    asyncio.run(run())


def test_late_cancel_does_not_cancel_replacement_generation(native_plan):
    async def run():
        executor = NativeExecutor(False, "")
        plan, inputs = native_plan
        ctx = PyDaftContext()
        context = {"plan_fingerprint": "123"}
        first = await executor.run(plan, ctx, 1, inputs, context)
        executor.cancel_plan(123)
        second = await executor.run(plan, ctx, 1, inputs, context)
        first.cancel()
        del first
        assert executor.active_plan_count() == 1
        values = []
        async for partition in second:
            if partition is None:
                break
            values.extend(MicroPartition._from_pymicropartition(partition).to_pydict()["x"])
        assert values == [1, 2, 3]
        await second.try_finish()
        assert executor.active_plan_count() == 0

    asyncio.run(run())
