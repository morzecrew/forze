"""SQS lifecycle steps (client pool startup and shutdown)."""

from .pool import (
    SQSShutdownHook,
    SQSStartupHook,
    sqs_lifecycle_step,
)

# ----------------------- #

__all__ = [
    "SQSShutdownHook",
    "SQSStartupHook",
    "sqs_lifecycle_step",
]
