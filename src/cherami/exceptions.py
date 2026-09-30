"""Cherami Specific Exceptions"""


class CheramiError(RuntimeError):
    """Base exception."""


class ConfigurationError(CheramiError):
    """Error in set up somewhere in the worker."""


class RetryableError(CheramiError):
    """Error is likely intermittent, and could be solved on trying again,
    e.g. Onyx connection errors."""


class SampleError(CheramiError):
    """
    Error sits within a sample, which can occur in the worker or pipeline.
    """


class RetryablePipelineError(CheramiError):
    """Pipeline error eligible for retry."""


class NonRetryablePipelineError(CheramiError):
    """Pipeline error not eligible for retry."""
