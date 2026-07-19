"""Served-model inference over HTTP wire protocols (KServe V2 / MLflow).

Requires the ``forze[inference-http]`` extra. One :class:`HttpInferenceDepsModule` binds
inference routes to a model-serving endpoint; the wire dialect is per-route config, so
the same handler code scores against mlserver, KServe, Seldon, Triton, or a legacy
MLflow ``/invocations`` server.
"""

from ._compat import require_inference_http

require_inference_http()

# ....................... #

from .adapters import HttpInferenceAdapter
from .execution import (
    ConfigurableHttpInference,
    HttpInferenceConfig,
    HttpInferenceDepsModule,
    InferenceHttpClientDepKey,
    InferenceHttpShutdownHook,
    InferenceHttpStartupHook,
    InferenceWireProtocolName,
    inference_http_lifecycle_step,
)
from .kernel import (
    DEFAULT_REQUEST_TIMEOUT_S,
    InferenceHttpClient,
    InferenceHttpClientPort,
)
from .protocols import KserveV2Protocol, MlflowProtocol, WireProtocol

# ----------------------- #

__all__ = [
    "DEFAULT_REQUEST_TIMEOUT_S",
    "ConfigurableHttpInference",
    "HttpInferenceAdapter",
    "HttpInferenceConfig",
    "HttpInferenceDepsModule",
    "InferenceHttpClient",
    "InferenceHttpClientDepKey",
    "InferenceHttpClientPort",
    "InferenceHttpShutdownHook",
    "InferenceHttpStartupHook",
    "InferenceWireProtocolName",
    "KserveV2Protocol",
    "MlflowProtocol",
    "WireProtocol",
    "inference_http_lifecycle_step",
]
