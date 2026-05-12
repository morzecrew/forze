from .document import attach_document_endpoints
from .http import (
    AuthnRequirement,
    attach_http_endpoint,
    attach_http_endpoints,
    build_authn_requirement_dependency,
    build_http_endpoint_spec,
)
from .search import attach_search_endpoints

# ----------------------- #

__all__ = [
    "attach_document_endpoints",
    "attach_search_endpoints",
    "attach_http_endpoint",
    "attach_http_endpoints",
    "build_http_endpoint_spec",
    "AuthnRequirement",
    "build_authn_requirement_dependency",
]
