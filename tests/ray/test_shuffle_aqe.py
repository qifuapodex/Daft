from __future__ import annotations

import io

import pytest

import daft
from daft.context import get_context
from tests.conftest import get_tests_daft_runner_name

pytestmark = pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="AQE requires Ray")


def source():
    return daft.from_pydict({"k": list(range(64)) * 4, "v": list(range(256))}).into_partitions(8).collect()


def plan_text(df):
    stream = io.StringIO()
    df.explain(show_all=True, file=stream)
    return stream.getvalue()


def test_shuffle_aqe_config_defaults_and_validation():
    config = get_context().daft_execution_config
    assert config.experimental_shuffle_aqe_min_partitions is None
    assert config.experimental_shuffle_aqe is False
    assert config.experimental_shuffle_aqe_target_bytes == 256 * 1024 * 1024
    with (
        pytest.raises(ValueError, match="greater than 0"),
        daft.execution_config_ctx(experimental_shuffle_aqe_target_bytes=0),
    ):
        pass
    with (
        pytest.raises(ValueError, match="greater than 0"),
        daft.execution_config_ctx(experimental_shuffle_aqe_min_partitions=0),
    ):
        pass
    with daft.execution_config_ctx(
        experimental_shuffle_aqe=True,
        experimental_shuffle_aqe_min_partitions=1,
        experimental_shuffle_aqe_target_bytes=1024,
    ):
        assert get_context().daft_execution_config.experimental_shuffle_aqe is True
        assert get_context().daft_execution_config.experimental_shuffle_aqe_target_bytes == 1024
    assert get_context().daft_execution_config.experimental_shuffle_aqe is False


@pytest.mark.parametrize("placement", ["local_only", "shared_only"])
@pytest.mark.parametrize("operation", ["aggregate", "distinct"])
def test_shuffle_aqe_opt_in_coalesces_and_preserves_rows(tmp_path, operation, placement):
    df = source()
    answers = []
    counts = []
    storage = (
        {}
        if placement == "local_only"
        else {
            "flight_shuffle_placement": "shared_only",
            "flight_shuffle_shared_dir": str(tmp_path / "shared"),
            "flight_shuffle_read_source": "shared",
        }
    )
    for enabled in [False, True]:
        with daft.execution_config_ctx(
            shuffle_algorithm="flight_shuffle",
            flight_shuffle_dirs=[str(tmp_path)],
            experimental_shuffle_aqe=enabled,
            experimental_shuffle_aqe_min_partitions=1,
            **storage,
        ):
            result = df.groupby("k").agg(daft.col("v").sum()) if operation == "aggregate" else df.select("k").distinct()
            text = plan_text(result)
            assert ("Experimental AQE: coalesce" in text) == enabled
            if not enabled:
                assert "Experimental AQE:" not in text
            result.collect()
            counts.append(result._result_cache.num_partitions())
            answers.append(sorted(zip(*result.to_pydict().values())))
    assert answers[0] == answers[1]
    expected = [(k, 4 * k + 384) for k in range(64)] if operation == "aggregate" else [(k,) for k in range(64)]
    assert answers[1] == expected
    assert counts[0] > counts[1]
    assert counts[1] == 1


def test_shuffle_aqe_explicit_partition_count_is_preserved(tmp_path):
    df = source()
    with daft.execution_config_ctx(
        shuffle_algorithm="flight_shuffle", flight_shuffle_dirs=[str(tmp_path)], experimental_shuffle_aqe=True
    ):
        result = df.repartition(7, "k")
        assert "user_partition_contract" in plan_text(result)
        result.collect()
        assert result._result_cache.num_partitions() == 7
        assert sorted(zip(*result.to_pydict().values())) == sorted(zip(*df.to_pydict().values()))


def test_shuffle_aqe_skips_hash_join_and_its_inputs(tmp_path):
    df = source()
    answers = []
    for enabled in [False, True]:
        with daft.execution_config_ctx(
            shuffle_algorithm="flight_shuffle",
            flight_shuffle_dirs=[str(tmp_path)],
            experimental_shuffle_aqe=enabled,
            experimental_shuffle_aqe_min_partitions=1,
            broadcast_join_size_bytes_threshold=0,
        ):
            left = df.groupby("k").agg(daft.col("v").sum())
            right = df.select("k").distinct()
            result = left.join(right, on="k", strategy="hash")
            text = plan_text(result)
            if enabled:
                assert "join_input" in text
                assert "Experimental AQE: coalesce" not in text
            answers.append(sorted(zip(*result.to_pydict().values())))
    assert answers[0] == answers[1]
    assert len(answers[0]) == 64


def test_shuffle_aqe_empty_aggregate_and_repartition_after_coalescing(tmp_path):
    df = source()
    with daft.execution_config_ctx(
        shuffle_algorithm="flight_shuffle",
        flight_shuffle_dirs=[str(tmp_path)],
        experimental_shuffle_aqe=True,
        experimental_shuffle_aqe_min_partitions=1,
    ):
        empty = df.where(daft.col("k") < 0).groupby("k").agg(daft.col("v").sum())
        assert empty.to_pydict() == {"k": [], "v": []}
        grouped = df.groupby("k").agg(daft.col("v").sum())
        expected = {k: k * 4 + 384 for k in range(64)}
        # The explicit downstream count remains exact even if its child emits
        # fewer tasks than the planning-time upper bound.
        result = grouped.repartition(7, "k").collect()
        assert result._result_cache.num_partitions() == 7
        assert dict(zip(result.to_pydict()["k"], result.to_pydict()["v"])) == expected


@pytest.mark.parametrize("aligned", [False, True])
def test_shuffle_aqe_skips_asof_join_inputs(tmp_path, aligned):
    df = source()
    for enabled in [False, True]:
        with daft.execution_config_ctx(
            shuffle_algorithm="flight_shuffle",
            flight_shuffle_dirs=[str(tmp_path)],
            experimental_shuffle_aqe=enabled,
            experimental_shuffle_aqe_min_partitions=1,
            enable_scan_task_split_and_merge=False,
        ):
            left = df.groupby("k").agg(daft.col("v").sum())
            right = df.select("k").distinct().with_column("w", daft.col("k"))
            # One partition on both sides makes the private alignment contract
            # unambiguous, while retaining the upstream adaptive exchanges in plan.
            if aligned:
                left = left.into_partitions(1).sort("k")
                right = right.into_partitions(1).sort("k")
            result = left.join_asof(right, on="k", _assume_sorted_and_aligned=aligned)
            if enabled:
                text = plan_text(result)
                assert "join_input" in text
                assert "Experimental AQE: coalesce" not in text
            rows = result.to_pydict()
            assert sorted(zip(rows["k"], rows["v"], rows["w"])) == [(k, 4 * k + 384, k) for k in range(64)]


@pytest.mark.parametrize("downstream", ["into_partitions", "sort", "window", "distinct_window"])
def test_shuffle_aqe_shared_downstream_operators(tmp_path, downstream):
    df = source()
    with daft.execution_config_ctx(
        shuffle_algorithm="flight_shuffle",
        flight_shuffle_dirs=[str(tmp_path / "local")],
        flight_shuffle_placement="shared_only",
        flight_shuffle_shared_dir=str(tmp_path / "shared"),
        flight_shuffle_read_source="shared",
        experimental_shuffle_aqe=True,
        experimental_shuffle_aqe_min_partitions=1,
    ):
        grouped = df.groupby("k").agg(daft.col("v").sum())
        if downstream == "into_partitions":
            result = grouped.into_partitions(7).collect()
            assert result._result_cache.num_partitions() == 7
        elif downstream == "sort":
            result = grouped.sort("k").collect()
            assert result.to_pydict()["k"] == list(range(64))
        else:
            if downstream == "distinct_window":
                grouped = grouped.distinct()
            window = (
                daft.Window()
                .partition_by("k")
                .order_by("v")
                .rows_between(daft.Window.unbounded_preceding, daft.Window.current_row)
            )
            result = grouped.with_column("w", daft.col("v").first_value().over(window)).collect()
            rows = result.to_pydict()
            assert rows["w"] == rows["v"]
        rows = result.to_pydict()
        assert sorted(zip(rows["k"], rows["v"])) == [(k, 4 * k + 384) for k in range(64)]


@pytest.mark.parametrize("minimum", [None, 3, 100000])
def test_shuffle_aqe_task_floor(tmp_path, minimum):
    df = source().into_partitions(8)
    counts = []
    for enabled in [False, True]:
        with daft.execution_config_ctx(
            shuffle_algorithm="flight_shuffle",
            flight_shuffle_dirs=[str(tmp_path)],
            experimental_shuffle_aqe=enabled,
            experimental_shuffle_aqe_min_partitions=minimum,
        ):
            result = df.groupby("k").agg(daft.col("v").sum()).collect()
            counts.append(result._result_cache.num_partitions())
    # The automatic floor uses worker-manager capacity, not Ray's advertised
    # resource total; only capacity-independent bounds are asserted here.
    floor = minimum if minimum is not None else 1
    assert min(floor, counts[0]) <= counts[1] <= counts[0]


@pytest.mark.parametrize("class_name", ["PyDaftExecutionConfig", "Input", "DistributedPhysicalPlan"])
def test_shuffle_pickle_rejects_legacy_and_malformed_payloads(class_name):
    import daft.daft as native

    cls = getattr(native, class_name)
    with pytest.raises(ValueError, match="Legacy .* pickle is incompatible"):
        cls._from_serialized(b"old positional payload")
    with pytest.raises(ValueError, match="Invalid versioned"):
        cls._from_serialized_shuffle_aqe_v1(b"")


def test_shuffle_config_versioned_pickle_roundtrip():
    import pickle

    with daft.execution_config_ctx(experimental_shuffle_aqe=True, experimental_shuffle_aqe_min_partitions=3):
        config = get_context().daft_execution_config
        factory, (payload,) = config.__reduce__()
        assert factory.__name__ == "_from_serialized_shuffle_aqe_v1"
        restored = pickle.loads(pickle.dumps(config))
        assert restored.experimental_shuffle_aqe is True
        assert restored.experimental_shuffle_aqe_min_partitions == 3
        assert restored.pre_shuffle_merge_threshold == config.pre_shuffle_merge_threshold
        with pytest.raises(ValueError, match="Trailing bytes"):
            factory(payload + b"extra")
