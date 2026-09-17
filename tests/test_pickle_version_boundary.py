from __future__ import annotations

import base64
import json
import pickle
from pathlib import Path

import pytest

import daft
from daft.context import get_context
from daft.daft import DistributedPhysicalPlan, LocalPhysicalPlan, PyDaftExecutionConfig


def expected_factory(kind):
    return "_from_serialized_local_write_buffer_v3" if kind == "config" else "_from_serialized_parquet_data_page_v4"


@pytest.mark.parametrize("kind", ["config", "plan"])
@pytest.mark.parametrize("version", ["0.7.24+apodex.6", "0.7.24+apodex.7"])
def test_previous_pickle_reports_incompatible_format(kind, version):
    fixture = json.loads((Path(__file__).parent / "assets" / "pickle" / f"{version}-{kind}.json").read_text())
    with pytest.raises(ValueError, match=f"pickle format {fixture['factory']} is incompatible") as exc:
        pickle.loads(base64.b64decode(fixture["payload"]))
    message = str(exc.value)
    assert f"expected {expected_factory(kind)}" in message
    assert "Use identical Daft builds on driver and workers" in message
    assert "recreate persisted configs/plans with this build" in message


@pytest.fixture(params=["config", "plan"])
def versioned_object(request):
    with daft.execution_config_ctx(
        experimental_shuffle_aqe=True,
        experimental_shuffle_aqe_min_partitions=3,
        flight_shuffle_eio_max_retries=7,
        flight_shuffle_eio_local_max_retries=2,
        local_write_buffer_size_bytes=128 * 1024,
    ):
        config = get_context().daft_execution_config
        if request.param == "config":
            yield config
        else:
            frame = daft.range(0, 16, partitions=2).repartition(3, "id")
            yield DistributedPhysicalPlan.from_logical_plan_builder(
                frame._builder.optimize(config)._builder, "pickle-boundary", config
            )


def test_current_pickle_roundtrip(versioned_object):
    factory, (payload,) = versioned_object.__reduce__()
    kind = "config" if isinstance(versioned_object, PyDaftExecutionConfig) else "plan"
    assert factory.__name__ == expected_factory(kind)
    restored = pickle.loads(pickle.dumps(versioned_object))
    assert restored.__reduce__()[1] == (payload,)
    if isinstance(restored, PyDaftExecutionConfig):
        assert restored.experimental_shuffle_aqe is True
        assert restored.experimental_shuffle_aqe_min_partitions == 3
        assert restored.flight_shuffle_eio_max_retries == 7
        assert restored.flight_shuffle_eio_local_max_retries == 2
        assert restored.local_write_buffer_size_bytes == 128 * 1024
    else:
        assert restored.idx() == versioned_object.idx()
    with pytest.raises(ValueError, match="Trailing bytes"):
        factory(payload + b"extra")
    with pytest.raises(ValueError, match="Invalid versioned"):
        factory(b"")


@pytest.mark.parametrize("payload_kind", ["empty", "malformed", "current"])
@pytest.mark.parametrize("factory_name", ["_from_serialized_shuffle_aqe_v1", "_from_serialized_shuffle_eio_v2"])
def test_retired_factory_never_decodes_payload(versioned_object, payload_kind, factory_name):
    # Even valid current-format bytes must fail if routed through the old factory.
    payload = {"empty": b"", "malformed": b"invalid", "current": versioned_object.__reduce__()[1][0]}[payload_kind]
    with pytest.raises(ValueError, match=f"pickle format {factory_name} is incompatible"):
        getattr(type(versioned_object), factory_name)(payload)


def test_unversioned_factory_remains_rejected(versioned_object):
    with pytest.raises(ValueError, match="Legacy .* pickle is incompatible"):
        type(versioned_object)._from_serialized(versioned_object.__reduce__()[1][0])


def test_arrow60_v3_execution_config_remains_compatible():
    fixture = json.loads((Path(__file__).parent / "assets/pickle/arrow60-v3-config.json").read_text())
    payload = base64.b64decode(fixture["payload"])
    restored = pickle.loads(payload)
    assert isinstance(restored, PyDaftExecutionConfig)
    assert restored.__reduce__()[0].__name__ == fixture["factory"]
    assert pickle.dumps(restored) == payload


def test_arrow60_v3_write_plan_is_rejected_before_decoding():
    fixture = json.loads((Path(__file__).parent / "assets/pickle/arrow60-v3-plan.json").read_text())
    with pytest.raises(ValueError, match="expected _from_serialized_parquet_data_page_v4"):
        pickle.loads(base64.b64decode(fixture["payload"]))
    # Retired factory must also refuse current or malformed bytes.
    for payload in [b"", b"invalid"]:
        with pytest.raises(ValueError, match="pickle format _from_serialized_local_write_buffer_v3 is incompatible"):
            DistributedPhysicalPlan._from_serialized_local_write_buffer_v3(payload)


def test_arrow60_v3_local_write_plan_is_rejected_before_decoding():
    fixture = json.loads((Path(__file__).parent / "assets/pickle/arrow60-v3-local-plan.json").read_text())
    with pytest.raises(ValueError, match="Legacy PyLocalPhysicalPlan pickle is incompatible"):
        pickle.loads(base64.b64decode(fixture["payload"]))
    for payload in [b"", b"invalid"]:
        with pytest.raises(ValueError, match="Legacy PyLocalPhysicalPlan pickle is incompatible"):
            LocalPhysicalPlan._from_serialized(payload)
