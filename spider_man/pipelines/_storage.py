"""Internal storage error classification shared by pipelines and buffering."""

from pymongo.errors import (
    AutoReconnect, ConnectionFailure, NetworkTimeout, OperationFailure,
    ServerSelectionTimeoutError, WTimeoutError,
)

from spider_man.exceptions import StorageError

_RETRYABLE_CODES = frozenset({6, 7, 64, 89, 91, 189, 262, 9001, 10107, 11600, 11602, 13435, 13436})


def _recoverable(error):
    if isinstance(error, StorageError):
        return bool(getattr(error, "retryable", False))
    if isinstance(error, (AutoReconnect, ConnectionFailure, NetworkTimeout,
                          ServerSelectionTimeoutError, WTimeoutError)):
        return True
    if isinstance(error, OperationFailure):
        return error.code in _RETRYABLE_CODES or error.has_error_label("RetryableWriteError")
    return False


def _storage_error(message, *, retryable=False):
    return StorageError(message, retryable=retryable)
