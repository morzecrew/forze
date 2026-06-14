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

from typing import AbstractSet, Any, Awaitable, Callable, Mapping

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
    ns: StrKeyNamespace,
    ctx_dep: ExecutionContextFactory,
    include: AbstractSet[TenancyKernelOp | str] | None = None,
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

    :param router: A plain FastAPI router the caller owns.
    :param registry: Frozen registry holding the tenancy operations.
    :param ns: Namespace the operations were registered under (e.g. ``spec.default_namespace``).
    :param ctx_dep: Factory yielding the current execution context per request.
    :param include: Optional narrowing to a subset of operations.
    :returns: *router*, for chaining.
    """

    return attach_operation_routes(
        router,
        registry=registry,
        ns=ns,
        ctx_dep=ctx_dep,
        bindings=_TENANCY_BINDINGS,
        include=include,
    )
