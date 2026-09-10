from __future__ import annotations

import pickle

import pytest

from daft.exceptions import DaftShuffleFetchError


def test_shuffle_failure_survives_pickle_and_ray_exception_wrapping():
    ray = pytest.importorskip("ray")
    original = DaftShuffleFetchError(2**64 - 1, 2**32 - 1, 2**64 - 1, 7, "/test/map.arrow", "missing")
    wrapped = ray.exceptions.RayTaskError("shuffle_read", "test traceback", original)
    for restored in [
        pickle.loads(pickle.dumps(original)),
        ray.exceptions.RayError.from_bytes(wrapped.to_bytes()).cause,
    ]:
        assert isinstance(restored, DaftShuffleFetchError)
        assert vars(restored) == vars(original)
        assert str(restored) == str(original)
