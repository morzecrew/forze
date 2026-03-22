from .._utils import facade_dependency
from .composition import attach_http_endpoint, build_http_endpoint_spec
from .contracts import (
    HttpEndpointContext,
    HttpEndpointHandlerPort,
    HttpEndpointSpec,
    HttpFeaturesSpec,
    HttpMetadataSpec,
    HttpRequestDTO,
    HttpRequestSpec,
    HttpSpec,
)
from .features import (
    ETAG_HEADER_KEY,
    IDEMPOTENCY_KEY_HEADER,
    IF_NONE_MATCH_HEADER_KEY,
    ETagFeature,
    ETagProviderPort,
    IdempotencyFeature,
)
from .mapping import BodyAsIsMapper, QueryAsIsBodyAssignMapper, QueryAsIsMapper

# ----------------------- #

__all__ = [
    "attach_http_endpoint",
    "build_http_endpoint_spec",
    "HttpEndpointContext",
    "HttpEndpointHandlerPort",
    "HttpEndpointSpec",
    "HttpFeaturesSpec",
    "HttpMetadataSpec",
    "HttpRequestDTO",
    "HttpRequestSpec",
    "HttpSpec",
    "IdempotencyFeature",
    "IDEMPOTENCY_KEY_HEADER",
    "ETagFeature",
    "ETagProviderPort",
    "ETAG_HEADER_KEY",
    "IF_NONE_MATCH_HEADER_KEY",
    "BodyAsIsMapper",
    "QueryAsIsBodyAssignMapper",
    "QueryAsIsMapper",
    "facade_dependency",
]
