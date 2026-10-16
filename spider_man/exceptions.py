class SpiderManError(Exception):
    """Framework errors carry an explicit failure policy."""


class ConfigurationError(SpiderManError):
    pass


class ItemValidationError(SpiderManError):
    pass


class LeaseLost(SpiderManError):
    pass


class PermanentError(SpiderManError):
    retryable = False


class RetryableError(SpiderManError):
    retryable = True

    def __init__(self, message, retry_after=None):
        super().__init__(message)
        self.retry_after = retry_after


class DownloadError(SpiderManError):
    def __init__(self, message, error_type="network", retryable=True, retry_after=None, status_code=None):
        super().__init__(message)
        self.error_type = error_type
        self.retryable = retryable
        self.retry_after = retry_after
        self.status_code = status_code


class StorageError(SpiderManError):
    def __init__(self, message, retryable=False):
        super().__init__(message)
        self.retryable = retryable


class DropItem(SpiderManError):
    def __init__(self, reason):
        super().__init__(reason)
        self.reason = reason

