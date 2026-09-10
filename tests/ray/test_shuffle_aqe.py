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
    assert config.experimental_shuffle_aqe is False
    assert config.experimental_shuffle_aqe_target_bytes == 256 * 1024 * 1024
    with (
        pytest.raises(ValueError, match="greater than 0"),
        daft.execution_config_ctx(experimental_shuffle_aqe_target_bytes=0),
    ):
        pass
    with daft.execution_config_ctx(experimental_shuffle_aqe=True, experimental_shuffle_aqe_target_bytes=1024):
        assert get_context().daft_execution_config.experimental_shuffle_aqe is True
        assert get_context().daft_execution_config.experimental_shuffle_aqe_target_bytes == 1024
    assert get_context().daft_execution_config.experimental_shuffle_aqe is False


@pytest.mark.parametrize("operation", ["aggregate", "distinct"])
def test_shuffle_aqe_opt_in_coalesces_and_preserves_rows(tmp_path, operation):
    df = source()
    answers = []
    counts = []
    for enabled in [False, True]:
        with daft.execution_config_ctx(
            shuffle_algorithm="flight_shuffle",
            flight_shuffle_dirs=[str(tmp_path)],
            experimental_shuffle_aqe=enabled,
        ):
            result = df.groupby("k").agg(daft.col("v").sum()) if operation == "aggregate" else df.select("k").distinct()
            text = plan_text(result)
            assert ("Experimental AQE: coalesce" if enabled else "Experimental AQE: skipped (disabled)") in text
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
