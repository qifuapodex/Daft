from __future__ import annotations

import base64
import json
import pickle
from pathlib import Path

import pytest

import daft
from daft.context import get_context
from daft.daft import DistributedPhysicalPlan, PyDaftExecutionConfig


@pytest.mark.parametrize("kind", ["config", "plan"])
def test_apodex_6_pickle_reports_incompatible_format(kind):
    fixture = json.loads((Path(__file__).parent / "assets" / "pickle" / f"0.7.24+apodex.6-{kind}.json").read_text())
    with pytest.raises(ValueError, match="pickle format _from_serialized_shuffle_aqe_v1 is incompatible") as exc:
        pickle.loads(base64.b64decode(fixture["payload"]))
    message = str(exc.value)
    assert "expected _from_serialized_shuffle_eio_v2" in message
    assert "Use identical Daft builds on driver and workers" in message
    assert "recreate persisted configs/plans with this build" in message


@pytest.mark.parametrize("kind", ["config", "plan"])
def test_apodex_7_pickle_remains_readable(kind):
    fixture = json.loads((Path(__file__).parent / "assets" / "pickle" / f"0.7.24+apodex.7-{kind}.json").read_text())
    restored = pickle.loads(base64.b64decode(fixture["payload"]))
    expected_type = PyDaftExecutionConfig if kind == "config" else DistributedPhysicalPlan
    assert isinstance(restored, expected_type)
    factory, args = restored.__reduce__()
    assert factory.__name__ == fixture["factory"] == "_from_serialized_shuffle_eio_v2"
    assert pickle.loads(pickle.dumps(restored)).__reduce__()[1] == args
    if kind == "config":
        assert restored.flight_shuffle_eio_max_retries == 3
        assert restored.flight_shuffle_eio_local_max_retries == 6
    else:
        assert restored.idx() == "pickle-cross"


@pytest.fixture(params=["config", "plan"])
def versioned_object(request):
    with daft.execution_config_ctx(
        experimental_shuffle_aqe=True,
        experimental_shuffle_aqe_min_partitions=3,
        flight_shuffle_eio_max_retries=7,
        flight_shuffle_eio_local_max_retries=2,
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
    assert factory.__name__ == "_from_serialized_shuffle_eio_v2"
    restored = pickle.loads(pickle.dumps(versioned_object))
    assert restored.__reduce__()[1] == (payload,)
    if isinstance(restored, PyDaftExecutionConfig):
        assert restored.experimental_shuffle_aqe is True
        assert restored.experimental_shuffle_aqe_min_partitions == 3
        assert restored.flight_shuffle_eio_max_retries == 7
        assert restored.flight_shuffle_eio_local_max_retries == 2
    else:
        assert restored.idx() == versioned_object.idx()
    with pytest.raises(ValueError, match="Trailing bytes"):
        factory(payload + b"extra")
    with pytest.raises(ValueError, match="Invalid versioned"):
        factory(b"")


@pytest.mark.parametrize("payload_kind", ["empty", "malformed", "current"])
def test_retired_factory_never_decodes_payload(versioned_object, payload_kind):
    # Even valid current-format bytes must fail if routed through the old factory.
    payload = {"empty": b"", "malformed": b"invalid", "current": versioned_object.__reduce__()[1][0]}[payload_kind]
    with pytest.raises(ValueError, match="pickle format _from_serialized_shuffle_aqe_v1 is incompatible"):
        type(versioned_object)._from_serialized_shuffle_aqe_v1(payload)


def test_unversioned_factory_remains_rejected(versioned_object):
    with pytest.raises(ValueError, match="Legacy .* pickle is incompatible"):
        type(versioned_object)._from_serialized(versioned_object.__reduce__()[1][0])
