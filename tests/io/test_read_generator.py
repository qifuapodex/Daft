from __future__ import annotations

import pytest

from daft import exceptions
from daft.io._generator import read_generator
from daft.recordbatch.recordbatch import RecordBatch
from tests.conftest import get_tests_daft_runner_name


@pytest.mark.skipif(get_tests_daft_runner_name() != "native", reason="Checks exceptions before Ray serialization")
@pytest.mark.parametrize(
    "exc_type",
    [
        exceptions.ConnectTimeoutError,
        exceptions.ReadTimeoutError,
        exceptions.ByteStreamError,
        exceptions.SocketError,
        exceptions.ThrottleError,
        exceptions.MiscTransientError,
        ValueError,
        TypeError,
    ],
)
@pytest.mark.parametrize("phase", ["call", "first_next", "after_batch"])
def test_read_generator_preserves_exception(exc_type, phase):
    batch = RecordBatch.from_pydict({"x": [1, 2]})
    error = exc_type("generator failure")
    cause = RuntimeError("original cause")

    def generate():
        if phase == "after_batch":
            yield batch
        raise error from cause

    def factory():
        if phase == "call":
            raise error from cause
        return generate()

    with pytest.raises(exc_type, match="generator failure") as raised:
        read_generator(iter([factory]), batch.schema()).to_pydict()

    assert raised.value is error
    assert raised.value.__cause__ is cause
