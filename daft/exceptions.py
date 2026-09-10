# Do not modify or delete these exceptions before checking where they are used in rust
# src/common/error/src/python.rs
from __future__ import annotations


class DaftCoreException(ValueError):
    """DaftCore Base Exception."""

    pass


class DaftTypeError(DaftCoreException):
    """Type Error that occurred in Daft Core."""

    pass


class DaftShuffleFetchError(DaftCoreException):
    """An unavailable shuffle output, including its exact execution identity."""

    def __init__(self, shuffle_id: int, input_id: int, attempt: int, partition_idx: int, path: str, message: str):
        self.shuffle_id = shuffle_id
        self.input_id = input_id
        self.attempt = attempt
        self.partition_idx = partition_idx
        self.path = path
        self.message = message
        super().__init__(
            f"Shuffle {shuffle_id} map {input_id} attempt {attempt:#x} partition {partition_idx}: {message} ({path})"
        )

    def __reduce__(self):
        return type(self), (self.shuffle_id, self.input_id, self.attempt, self.partition_idx, self.path, self.message)


class DaftTransientError(DaftCoreException):
    """Daft Transient Error.

    This is typically raised when there is a network issue such as timeout or throttling. This can usually be retried.
    """

    pass


class ConnectTimeoutError(DaftTransientError):
    """Daft Connection Timeout Error.

    Daft client was not able to make a connection to the server in the connect timeout time.
    """

    pass


class ReadTimeoutError(DaftTransientError):
    """Daft Read Timeout Error.

    Daft client was not able to read bytes from server under the read timeout time.
    """

    pass


class ByteStreamError(DaftTransientError):
    """Daft Byte Stream Error.

    Daft client had an error while reading bytes in a stream from the server.
    """

    pass


class SocketError(DaftTransientError):
    """Daft Socket Error.

    Daft client had a socket error while reading bytes in a stream from the server.
    """

    pass


class ThrottleError(DaftTransientError):
    """Daft Throttle Error.

    Daft client had a throttle error while making request to server.
    """

    pass


class MiscTransientError(DaftTransientError):
    """Daft Misc Transient Error.

    Daft client had a Misc Transient Error while making request to server.
    """

    pass
