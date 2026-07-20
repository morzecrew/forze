"""Execution wiring for served-model inference over HTTP."""

from .deps import (
    ConfigurableHttpInference,
    HttpInferenceConfig,
    HttpInferenceDepsModule,
    InferenceHttpClientDepKey,
    InferenceWireProtocolName,
)
from .lifecycle import (
    InferenceHttpShutdownHook,
    InferenceHttpStartupHook,
    inference_http_lifecycle_step,
    routed_inference_http_lifecycle_step,
)

# ----------------------- #

__all__ = [
    "ConfigurableHttpInference",
    "HttpInferenceConfig",
    "HttpInferenceDepsModule",
    "InferenceHttpClientDepKey",
    "InferenceHttpShutdownHook",
    "InferenceHttpStartupHook",
    "InferenceWireProtocolName",
    "inference_http_lifecycle_step",
    "routed_inference_http_lifecycle_step",
]
