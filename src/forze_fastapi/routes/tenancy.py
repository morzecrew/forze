"""Generated FastAPI routes for the tenant selector (list memberships + switch active tenant).

Projects the tenancy self-service operations of a frozen registry (built with
:func:`forze_kits.aggregates.tenancy.build_tenancy_registry`) onto a user-owned
:class:`fastapi.APIRouter`:

- ``GET /tenants`` → ``list_tenants`` (the principal's active memberships)
- ``POST /tenants/{id}/activate`` → ``switch_tenant`` (re-mint a token pair scoped to the
  selected tenant — the same OAuth2-shaped body as ``/login``)
- ``DELETE /tenants/{id}`` → ``leave_tenant`` (204 — drop the caller's *own* membership)

All require a bound identity (``AuthnRequired`` — a 401 without one) and are **tenant-unaware**
(you are *selecting* the tenant). ``switch_tenant`` validates the selection against the
principal's membership before minting (``tenant_mismatch`` / ``tenant_inactive``). The
``/activate`` response carries token material in the body by design — the client swaps to the
new token — so keep any custom access logging body-blind (the stock
:class:`~forze_fastapi.middlewares.LoggingMiddleware` already is).
"""

from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from collections.abc import Awaitable, Callable, Mapping
from collections.abc import Set as AbstractSet
from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel

from forze.application.execution.context import ExecutionContextFactory
from forze.application.execution.operations import FrozenOperationRegistry
from forze.base.primitives import StrKeyNamespace
from forze_kits.aggregates.tenancy import TenancyKernelOp

from ._attach import (
    OperationRunner,
    RouteBinding,
    attach_operation_routes,
    id_endpoint,
    resolve_namespace,
)

# ----------------------- #


def _no_body_endpoint(
    runner: OperationRunner,
    input_type: type[BaseModel] | None,
    op: str,
) -> Callable[..., Awaitable[Any]]:
    """Endpoint for an input-less operation — no request payload at all."""

    _ = input_type, op  # list_tenants takes no input; identity comes from the context

    async def endpoint() -> Any:
        return await runner(None)

    return endpoint


# ....................... #

_TENANCY_BINDINGS: Mapping[str, RouteBinding] = {
    TenancyKernelOp.LIST_TENANTS: RouteBinding(
        method="GET", path="/tenants", build=_no_body_endpoint
    ),
    TenancyKernelOp.SWITCH_TENANT: RouteBinding(
        method="POST", path="/tenants/{id}/activate", build=id_endpoint
    ),
    TenancyKernelOp.LEAVE_TENANT: RouteBinding(
        method="DELETE", path="/tenants/{id}", build=id_endpoint, status_code=204
    ),
}


# ....................... #


def attach_tenancy_routes(
    router: APIRouter,
    *,
    registry: FrozenOperationRegistry,
    ns: StrKeyNamespace | None = None,
    ctx_dep: ExecutionContextFactory,
    include: AbstractSet[TenancyKernelOp | str] | None = None,
    resource: str | None = None,
    path_overrides: Mapping[TenancyKernelOp | str, str] | None = None,
    exclude_none: bool = True,
) -> APIRouter:
    """Attach the tenant-selector operations under *ns* to *router*.

    - ``GET /tenants`` → ``list_tenants`` (the principal's active memberships)
    - ``POST /tenants/{id}/activate`` → ``switch_tenant`` (200, a fresh token pair scoped to
      the selected tenant — same shape as ``/login``)
    - ``DELETE /tenants/{id}`` → ``leave_tenant`` (204, drops the caller's own membership)

    All require a bound identity (``AuthnRequired``) and are tenant-unaware. Each route's
    ``operation_id`` is the operation key verbatim; request/response schemas come from the
    operation descriptors; every call dispatches through ``run_operation`` (plans + hooks
    apply, no bypass).

    Args:
        router (APIRouter): A plain FastAPI router the caller owns.
        registry (FrozenOperationRegistry): Frozen registry holding the tenancy
            operations.
        ns (StrKeyNamespace | None): Namespace the operations were registered under
            (e.g. ``spec.default_namespace``). Mutually exclusive with *resource* —
            provide exactly one.
        ctx_dep (ExecutionContextFactory): Factory yielding the current execution
            context per request.
        include (AbstractSet | None): Optional narrowing to a subset of operations.
        resource (str | None): Convenience alternative to *ns* — a prefix string the
            namespace is built from; must equal the prefix the operations were
            registered under. Mutually exclusive with *ns* — provide exactly one.
        path_overrides (Mapping | None): Optional per-operation route-path replacements
            (keyed like *include*); only the path changes, the ``operation_id`` stays
            verbatim. An override must bind exactly the ``{id}`` placeholder the
            default path binds.

    Returns:
        APIRouter: The same *router*, for chaining.

    Raises:
        CoreException: On a configuration error — an unknown *include*/override
            operation, both or neither of *ns*/*resource*, or a path override that
            drops or adds a placeholder.
    """

    return attach_operation_routes(
        router,
        registry=registry,
        ns=resolve_namespace(ns, resource),
        ctx_dep=ctx_dep,
        bindings=_TENANCY_BINDINGS,
        include=include,
        path_overrides=path_overrides,
        exclude_none=exclude_none,
    )
