"""Project a configured :class:`AuthnRequirement` onto the OpenAPI schema.

The middleware (:class:`~forze_fastapi.middlewares.SecurityContextMiddleware`) already
knows the transport's auth scheme — header/cookie names, bearer vs API key — from the
same :class:`AuthnRequirement` the app author passes it. Without this the generated
schema (and Scalar/Swagger UI) shows every protected endpoint as open: no Authorize
button, no lock, no scheme. :func:`apply_openapi_security` derives the OpenAPI
``securitySchemes`` from that one source of truth and attaches a ``security``
requirement to exactly the operations the catalog flagged ``x-requires-authn`` (the
route generators emit it from :attr:`OperationCatalogEntry.requires_authn`), so token-
minting routes like ``/login`` stay public and protected routes advertise the scheme.
"""

from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from collections.abc import Callable
from collections.abc import Set as AbstractSet
from typing import Any

from fastapi import FastAPI

from .value_objects import AuthnRequirement

# ----------------------- #

_HTTP_METHODS = frozenset({"get", "put", "post", "delete", "options", "head", "patch", "trace"})
"""OpenAPI path-item keys that are operations (the rest are metadata)."""

_APPLIED_MARKER = "x-forze-security-applied"
"""Idempotency sentinel stamped on the schema once security has been attached."""

_REQUIRES_AUTHN_EXTENSION = "x-requires-authn"
"""Per-operation vendor extension emitted by the route generators."""


# ....................... #


def apply_openapi_security(
    app: FastAPI,
    requirement: AuthnRequirement,
    *,
    exclude: AbstractSet[str] = frozenset(),
) -> None:
    """Document *requirement*'s auth scheme in *app*'s OpenAPI and guard the schema.

    Registers one ``securityScheme`` per ingress method of *requirement* (bearer for a
    token on ``Authorization``; ``apiKey`` in header/cookie otherwise) and attaches a
    ``security`` requirement — the ingress methods as **alternatives** (any one
    satisfies it) — to every operation the catalog flagged as requiring a bound
    principal (``x-requires-authn``, from :attr:`OperationCatalogEntry.requires_authn`).
    Operations without that flag (e.g. the authn ``/login`` and ``/refresh`` routes
    that *mint* tokens) are left open.

    This documents auth; it does **not** enforce it — enforcement stays in the engine
    (the ``AuthnRequired`` / authz hooks) and identity extraction in
    :class:`~forze_fastapi.middlewares.SecurityContextMiddleware`. Call it once after
    every router is attached. It wraps ``app.openapi`` and is idempotent.

    :param app: The FastAPI application whose schema to enrich.
    :param requirement: The same ingress requirement passed to the security middleware.
    :param exclude: Operation ids to leave open even if flagged ``x-requires-authn``.
    """

    schemes = dict(ingress.openapi_scheme() for ingress in requirement.ingress)

    # Ingress methods are tried in order and any one authenticates the caller, so
    # they are OR alternatives: separate single-key objects in the security array
    # (AND would be multiple keys within one object).
    security: list[dict[str, list[str]]] = [{name: []} for name in schemes]

    original: Callable[[], dict[str, Any]] = app.openapi

    def _openapi() -> dict[str, Any]:
        schema = original()

        if schema.get(_APPLIED_MARKER):
            return schema

        components = schema.setdefault("components", {})
        components.setdefault("securitySchemes", {}).update(schemes)

        for path_item in schema.get("paths", {}).values():
            for method, operation in path_item.items():
                if method not in _HTTP_METHODS:
                    continue

                if operation.get("operationId") in exclude:
                    continue

                if operation.get(_REQUIRES_AUTHN_EXTENSION):
                    operation["security"] = security

        schema[_APPLIED_MARKER] = True

        return schema

    app.openapi = _openapi  # type: ignore[method-assign]
