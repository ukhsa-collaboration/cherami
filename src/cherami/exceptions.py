"""Cherami Specific Exceptions"""


class CheramiError(RuntimeError):
    """Base exception."""


class WorkerError(CheramiError):
    """Error in worker; either config or set up, will enter sleep state,
    cannot continue."""


class RetryableError(CheramiError):
    """Error is likely intermittent, and could be solved on trying again,
    e.g. Onyx connection errors."""


class NonRetryableError(CheramiError):  # noqa: N818
    """Error occurs during handling of a sample."""


class RetryablePipelineError(CheramiError):
    """Pipeline error eligible for retry."""


class NonRetryablePipelineError(CheramiError):
    """Pipeline error not eligible for retry."""
