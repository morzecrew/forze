"""Function-first HTTP transport for Forze operations.

Layer guide:

- **Catalog** (``forze.application.composition.*.catalog``): which operations exist,
  presets, and capability checks — protocol-agnostic.
- **Bindings** (``transport.http.bindings``): HTTP method, path, response model, and
  handler construction for each operation.
- **Options** (``transport.http.options``): per-route ``RouteOpts`` and feature config
  (ETag, idempotency, token transport).
- **Attach** (``transport.http.attach``): ``enable`` loop, policies, registration on
  ``APIRouter`` via :func:`~forze_fastapi.transport.http.register.register_route`.

Real-time commands use :mod:`forze_socketio`, not this package.
"""

from .attach import (
    attach_authn_routes,
    attach_document_routes,
    attach_search_routes,
    attach_storage_routes,
)
from .auth import AuthnRequirement, AuthnRequirementSchemeName
from .etag import (
    ETAG_HEADER_KEY,
    IF_NONE_MATCH_HEADER_KEY,
    ETagProviderPort,
    document_etag,
)
from .facade import make_facade_dep
from .idempotency import IDEMPOTENCY_KEY_HEADER, run_idempotent
from .options import (
    AuthnConfigSpec,
    AuthnPreset,
    DocumentConfigSpec,
    DocumentPreset,
    RouteOpts,
    SearchPreset,
    StorageConfigSpec,
    StoragePreset,
)
from .policies import (
    ETagPolicy,
    IdempotentPolicy,
    MergedPolicies,
    Policy,
    RequirePrincipal,
    build_require_principal_dependency,
    merge_policies,
)
from .register import RouteRegistration, register_route
from .router import ForzeAPIRoute, ForzeRouter, HttpMethod, forze_route

# ----------------------- #

__all__ = [
    "AuthnConfigSpec",
    "AuthnPreset",
    "AuthnRequirement",
    "AuthnRequirementSchemeName",
    "DocumentConfigSpec",
    "DocumentPreset",
    "ETAG_HEADER_KEY",
    "ETagPolicy",
    "ETagProviderPort",
    "ForzeAPIRoute",
    "ForzeRouter",
    "HttpMethod",
    "IDEMPOTENCY_KEY_HEADER",
    "IF_NONE_MATCH_HEADER_KEY",
    "IdempotentPolicy",
    "MergedPolicies",
    "Policy",
    "RequirePrincipal",
    "RouteOpts",
    "RouteRegistration",
    "SearchPreset",
    "StorageConfigSpec",
    "StoragePreset",
    "attach_authn_routes",
    "attach_document_routes",
    "attach_search_routes",
    "attach_storage_routes",
    "build_require_principal_dependency",
    "document_etag",
    "forze_route",
    "make_facade_dep",
    "merge_policies",
    "register_route",
    "run_idempotent",
]
