"""Served-model inference over HTTP wire protocols (KServe V2 / MLflow / OpenAI chat /
Anthropic messages).

Requires the ``forze[inference-http]`` extra. One :class:`HttpInferenceDepsModule` binds
inference routes to a model-serving endpoint; the wire dialect is per-route config, so
the same handler code scores against mlserver, KServe, Seldon, Triton, a legacy
MLflow ``/invocations`` server, anything speaking OpenAI's ``/v1/chat/completions``, or
Anthropic's native ``/v1/messages``.
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
    InferenceHttpRoutingCredentials,
    RoutedInferenceHttpClient,
)
from .protocols import (
    AnthropicMessagesProtocol,
    InferenceOutputMode,
    KserveV2Protocol,
    MlflowProtocol,
    OpenAiChatProtocol,
    PromptTemplate,
    WireProtocol,
)
from .settings import InferenceHttpSettings

# ----------------------- #

__all__ = [
    "InferenceHttpSettings",
    "DEFAULT_REQUEST_TIMEOUT_S",
    "AnthropicMessagesProtocol",
    "ConfigurableHttpInference",
    "HttpInferenceAdapter",
    "HttpInferenceConfig",
    "HttpInferenceDepsModule",
    "InferenceHttpClient",
    "InferenceHttpClientDepKey",
    "InferenceHttpClientPort",
    "InferenceHttpRoutingCredentials",
    "InferenceHttpShutdownHook",
    "InferenceHttpStartupHook",
    "InferenceOutputMode",
    "InferenceWireProtocolName",
    "KserveV2Protocol",
    "MlflowProtocol",
    "OpenAiChatProtocol",
    "PromptTemplate",
    "RoutedInferenceHttpClient",
    "WireProtocol",
    "inference_http_lifecycle_step",
]
