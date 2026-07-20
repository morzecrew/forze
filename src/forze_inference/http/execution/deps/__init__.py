"""Deps wiring for served-model inference over HTTP."""

from .configs import HttpInferenceConfig, InferenceWireProtocolName
from .factories import ConfigurableHttpInference
from .keys import InferenceHttpClientDepKey
from .module import HttpInferenceDepsModule

# ----------------------- #

__all__ = [
    "ConfigurableHttpInference",
    "HttpInferenceConfig",
    "HttpInferenceDepsModule",
    "InferenceHttpClientDepKey",
    "InferenceWireProtocolName",
]
