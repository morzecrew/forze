"""Lifecycle wiring for the inference HTTP client."""

from .pool import (
    InferenceHttpShutdownHook,
    InferenceHttpStartupHook,
    inference_http_lifecycle_step,
)

# ----------------------- #

__all__ = [
    "InferenceHttpShutdownHook",
    "InferenceHttpStartupHook",
    "inference_http_lifecycle_step",
]
