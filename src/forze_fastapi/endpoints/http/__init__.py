from .._utils import facade_dependency
from .composition import (
    attach_http_endpoint,
    attach_http_endpoints,
    build_http_endpoint_spec,
)
from .contracts import (
    HttpEndpointContext,
    HttpEndpointHandlerPort,
    HttpEndpointSpec,
    HttpMetadataSpec,
    HttpRequestDTO,
    HttpRequestSpec,
    HttpSpec,
    SimpleHttpEndpointSpec,
)
from .features import (
    ETAG_HEADER_KEY,
    IDEMPOTENCY_KEY_HEADER,
    IF_NONE_MATCH_HEADER_KEY,
    ETagFeature,
    ETagProviderPort,
    IdempotencyFeature,
)
from .mapping import (
    BodyAsIsMapper,
    NullMapper,
    QueryAsIsBodyAssignMapper,
    QueryAsIsMapper,
)

# ----------------------- #

__all__ = [
    "attach_http_endpoint",
    "attach_http_endpoints",
    "build_http_endpoint_spec",
    "HttpEndpointContext",
    "HttpEndpointHandlerPort",
    "HttpEndpointSpec",
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
    "NullMapper",
    "facade_dependency",
    "SimpleHttpEndpointSpec",
]
