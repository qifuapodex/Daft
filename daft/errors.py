from __future__ import annotations

import pickle
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from traceback import TracebackException


class ExpressionTypeError(Exception):
    pass


def _rebuild_udf_exception(
    message: str, tb_info: TracebackException | None, cause: BaseException | None
) -> UDFException:
    """Reconstruct a `UDFException`, restoring the `__cause__` that pickling drops."""
    exc = UDFException(message, tb_info)
    exc.__cause__ = cause
    return exc


class UDFException(Exception):
    """An Daft exception raised when a UDF raises an exception.

    Also provides additional context about the error. The original exception is either available as:
    - The original exception via `__cause__` if running in the same process
    - A replica & rendered traceback via `tb_info` if running in a different process
    Both are accessible via `original_exception`
    """

    def __init__(self, message: str, tb_info: TracebackException | None = None):
        super().__init__(message)
        self.message = message
        self.tb_info = tb_info

    @property
    def original_exception(self) -> BaseException | None:
        """The original exception that was raised by the UDF."""
        # We except every creation of UDFException to be `raise UDFException(...) from ...`
        return self.__cause__

    def __reduce__(self) -> tuple[Any, ...]:
        # `__cause__` is a C-level slot that `BaseException.__reduce__` does not carry, so
        # without this a UDFException that crosses a process boundary -- which is every Ray
        # task -- arrives with `original_exception` == None and the original error's type
        # gone. Two things depend on it surviving: `original_exception` / `__str__` here,
        # and `DaftError::is_transient()` on the Rust side, which walks this chain to decide
        # whether a failed task is worth retrying. Dropping it turns a retryable blip inside
        # a UDF (a throttled S3 request, say) into a whole-query failure.
        cause = self.__cause__
        if cause is not None:
            try:
                pickle.dumps(cause)
            except Exception:
                # An unpicklable cause is dropped rather than allowed to fail the enclosing
                # error's serialisation, which would replace the user's traceback with a
                # pickling error. This mirrors what the process-pool path already does for
                # its base exception (`daft/execution/udf.py`).
                cause = None
        return (_rebuild_udf_exception, (self.message, self.tb_info, cause))

    def __str__(self) -> str:
        if self.tb_info:
            return (
                "\n".join(self.tb_info.format())
                + "\nThe above exception was the direct cause of the following exception:\n"
                + f"\n{self.message}"
            )
        else:
            return self.message


class RetryAfterError(Exception):
    """Retryable error carrying the requested wait time (in seconds)."""

    def __init__(self, retry_after: float, original: Exception | None = None) -> None:
        super().__init__(str(original) if original else "RetryAfterError")
        self.retry_after = retry_after
        self.__cause__ = original
