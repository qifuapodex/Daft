"""Contract tests for the transient-error retry path (T2).

`DaftError::is_transient()` in `src/common/error/src/error.rs` decides whether the
scheduler retries a failed task. On the Ray path it cannot match on the Rust error
variant: a worker-side `DaftError` is converted to a Python exception, wrapped by Ray,
and arrives at the driver as an opaque `DaftError::PyO3Error`. So it classifies by the
Python exception class instead, and that only works because of three properties of Ray
and `daft.exceptions` that nothing else in the test suite pins down.

If any of these break, retries silently stop happening -- the query fails on the first
network blip exactly as it did before T2, with no error and no failing Rust test to say
so. Hence testing them here.

None of this needs a Ray cluster; the exception wrapping is pure Python.
"""

from __future__ import annotations

import asyncio

import pytest
import ray
from ray.exceptions import RayTaskError

from daft import exceptions as daft_exceptions
from daft.exceptions import DaftTransientError, SocketError
from daft.runners.flotilla import RaySwordfishTaskHandle

# The six variants `src/common/error/src/python.rs` maps onto `DaftTransientError`
# subclasses, which is also exactly the whitelist `DaftError::is_transient()` matches on.
TRANSIENT_EXCEPTIONS = [
    daft_exceptions.ConnectTimeoutError,
    daft_exceptions.ReadTimeoutError,
    daft_exceptions.ByteStreamError,
    daft_exceptions.SocketError,
    daft_exceptions.ThrottleError,
    daft_exceptions.MiscTransientError,
]


def _wrap_like_ray(cause: Exception) -> RayTaskError:
    """Reproduce how Ray delivers a worker-side exception to the driver.

    The traceback string is only ever formatted into the message, so a placeholder is
    enough; what matters here is what `as_instanceof_cause()` does to the type.
    """
    return RayTaskError("run_plan", "<traceback>", cause).as_instanceof_cause()


@pytest.mark.parametrize("exc_type", TRANSIENT_EXCEPTIONS)
def test_ray_preserves_transient_exception_type(exc_type):
    """Ray's `as_instanceof_cause()` keeps the original class in the MRO.

    This is the property the Rust classifier's primary check relies on. Ray builds a
    synthetic class deriving from both `RayTaskError` and the original exception, so an
    `isinstance` check against `DaftTransientError` still succeeds on the driver even
    though the object is nominally a Ray error.
    """
    wrapped = _wrap_like_ray(exc_type("transient failure"))

    assert isinstance(wrapped, DaftTransientError)
    assert isinstance(wrapped, RayTaskError)


def test_ray_exposes_the_original_exception_on_cause():
    """`.cause` carries the original exception even without `as_instanceof_cause()`.

    Ray falls back to returning the plain `RayTaskError` when it cannot synthesise the
    dual class, and then the `isinstance` check above fails. The Rust classifier checks
    `.cause` as well for exactly this case, so the fallback needs pinning too.
    """
    bare = RayTaskError("run_plan", "traceback", SocketError("connection reset"))

    assert not isinstance(bare, DaftTransientError)
    assert isinstance(bare.cause, DaftTransientError)


@pytest.mark.parametrize(
    "exc",
    [
        ValueError("bad schema"),
        MemoryError("out of memory"),
        daft_exceptions.DaftTypeError("cannot cast"),
    ],
)
def test_non_transient_errors_are_not_classified_as_transient(exc):
    """Retrying these cannot change the outcome, and retrying an OOM kills another worker.

    `DaftTypeError` is in here deliberately: it shares the `DaftCoreException` base with
    `DaftTransientError`, so a classifier that checked the base class would wrongly retry it.
    """
    wrapped = _wrap_like_ray(exc)

    assert not isinstance(wrapped, DaftTransientError)
    assert not isinstance(getattr(wrapped, "cause", None), DaftTransientError)


def test_transient_whitelist_matches_the_exception_hierarchy():
    """The Rust and Python whitelists have to stay in sync.

    `python.rs` maps six `DaftError` variants onto these classes and
    `DaftError::is_transient()` whitelists the same six. Adding a subclass here without
    the matching Rust variant (or vice versa) means an error that is transient on one
    side of the boundary and permanent on the other.
    """
    declared = {
        obj
        for obj in vars(daft_exceptions).values()
        if isinstance(obj, type) and issubclass(obj, DaftTransientError) and obj is not DaftTransientError
    }

    assert declared == set(TRANSIENT_EXCEPTIONS)


def test_get_result_lets_transient_errors_through():
    """A transient error must reach Rust as a raised exception.

    `_get_result` converts some Ray failures into `worker_died` / `worker_unavailable`
    results and re-raises everything else. Only the re-raised path becomes
    `TaskStatus::Failed`, which is what `is_transient()` is consulted for -- a transient
    error caught by one of the earlier `except` clauses would be retried as a worker
    loss instead, against the wrong (much larger) retry budget.
    """

    class _RaisingHandle:
        async def completed(self):
            raise _wrap_like_ray(SocketError("connection reset"))

    handle = RaySwordfishTaskHandle(result_handle=_RaisingHandle())

    with pytest.raises(DaftTransientError):
        asyncio.run(handle._get_result())


def test_actor_death_is_not_classified_as_transient():
    """Worker loss goes down the `worker_died` path, not the transient-retry path.

    The two have separate retry budgets in the dispatcher, so they must not be confused.
    """
    actor_died = ray.exceptions.ActorDiedError()

    assert not isinstance(actor_died, DaftTransientError)
