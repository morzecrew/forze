"""Dependency keys owned by the inference HTTP integration."""

from forze.application.contracts.deps import DepKey

from ...kernel import InferenceHttpClientPort

# ----------------------- #

InferenceHttpClientDepKey = DepKey[InferenceHttpClientPort]("inference_http_client")
"""Key for the pre-constructed endpoint client (initialized via the lifecycle step)."""
