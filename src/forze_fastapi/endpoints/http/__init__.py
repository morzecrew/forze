from .._utils import facade_dependency
from .composition import (
    attach_http_endpoint,
    attach_http_endpoints,
    build_http_endpoint_spec,
)
from .contracts import (
    AuthnRequirement,
    HttpBodyMode,
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
from .features.security import (
    RequireAuthnFeature,
    RequireTenantFeature,
)
from .mapping import (
    BodyAsIsMapper,
    DocumentUpdateResDataMapper,
    EmptyMapper,
    QueryAsIsBodyAssignMapper,
    QueryAsIsMapper,
)
from .policy import (
    AnyFeature,
    apply_authn_requirement,
    merge_http_endpoint_features,
    with_default_http_features,
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
    "RequireAuthnFeature",
    "RequireTenantFeature",
    "AnyFeature",
    "apply_authn_requirement",
    "AuthnRequirement",
    "merge_http_endpoint_features",
    "with_default_http_features",
    "BodyAsIsMapper",
    "DocumentUpdateResDataMapper",
    "QueryAsIsBodyAssignMapper",
    "QueryAsIsMapper",
    "facade_dependency",
    "SimpleHttpEndpointSpec",
    "EmptyMapper",
    "HttpBodyMode",
]
