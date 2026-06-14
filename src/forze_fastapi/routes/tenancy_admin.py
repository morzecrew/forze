"""Generated FastAPI routes for tenancy admin (provision tenants, manage members).

Projects the tenancy-admin operations of a frozen registry (built with
:func:`forze_kits.aggregates.tenancy_admin.build_tenancy_admin_registry`, then guarded by the
app) onto a user-owned :class:`fastapi.APIRouter`:

- ``POST /tenants`` → ``create_tenant`` (201, returns the new tenant)
- ``GET /tenants/{id}/members`` → ``list_members`` (the tenant's principal ids)
- ``POST /tenants/{id}/deactivate`` → ``deactivate_tenant`` (204)
- ``POST /memberships`` → ``invite_member`` (204, body: ``tenant_id`` + ``principal_id``)
- ``DELETE /memberships`` → ``remove_member`` (204, body: ``tenant_id`` + ``principal_id``)

These are **privileged**. Unlike the self-service selector, they manage arbitrary tenants and
memberships, so the framework ships them unguarded — bind ``AuthnRequired`` **and** an
``AuthzBeforeAuthorize`` on every operation (or narrow with ``include=``) before mounting this
router. ``build_tenancy_admin_registry``'s docstring shows the binding loop.
"""

from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import AbstractSet, Mapping

from fastapi import APIRouter

from forze.application.execution.context import ExecutionContextFactory
from forze.application.execution.operations import FrozenOperationRegistry
from forze.base.primitives import StrKeyNamespace
from forze_kits.aggregates.tenancy_admin import TenancyAdminKernelOp

from ._attach import (
    RouteBinding,
    attach_operation_routes,
    body_endpoint,
    id_endpoint,
)

# ----------------------- #

_TENANCY_ADMIN_BINDINGS: Mapping[str, RouteBinding] = {
    TenancyAdminKernelOp.CREATE_TENANT: RouteBinding(
        method="POST", path="/tenants", build=body_endpoint, status_code=201
    ),
    TenancyAdminKernelOp.LIST_MEMBERS: RouteBinding(
        method="GET", path="/tenants/{id}/members", build=id_endpoint
    ),
    TenancyAdminKernelOp.DEACTIVATE_TENANT: RouteBinding(
        method="POST", path="/tenants/{id}/deactivate", build=id_endpoint, status_code=204
    ),
    TenancyAdminKernelOp.INVITE_MEMBER: RouteBinding(
        method="POST", path="/memberships", build=body_endpoint, status_code=204
    ),
    TenancyAdminKernelOp.REMOVE_MEMBER: RouteBinding(
        method="DELETE", path="/memberships", build=body_endpoint, status_code=204
    ),
}


# ....................... #


def attach_tenancy_admin_routes(
    router: APIRouter,
    *,
    registry: FrozenOperationRegistry,
    ns: StrKeyNamespace,
    ctx_dep: ExecutionContextFactory,
    include: AbstractSet[TenancyAdminKernelOp | str] | None = None,
) -> APIRouter:
    """Attach the tenancy-admin operations under *ns* to *router*.

    - ``POST /tenants`` → ``create_tenant`` (201)
    - ``GET /tenants/{id}/members`` → ``list_members``
    - ``POST /tenants/{id}/deactivate`` → ``deactivate_tenant`` (204)
    - ``POST /memberships`` → ``invite_member`` (204)
    - ``DELETE /memberships`` → ``remove_member`` (204)

    The registry ships these **unguarded** — bind ``AuthnRequired`` + ``AuthzBeforeAuthorize``
    on each operation (see :func:`~forze_kits.aggregates.tenancy_admin.build_tenancy_admin_registry`)
    before mounting, or narrow exposure with ``include=``. Each route's ``operation_id`` is the
    operation key verbatim; every call dispatches through ``run_operation``.

    :param router: A plain FastAPI router the caller owns.
    :param registry: Frozen registry holding the tenancy-admin operations.
    :param ns: Namespace the operations were registered under.
    :param ctx_dep: Factory yielding the current execution context per request.
    :param include: Optional narrowing to a subset of operations.
    :returns: *router*, for chaining.
    """

    return attach_operation_routes(
        router,
        registry=registry,
        ns=ns,
        ctx_dep=ctx_dep,
        bindings=_TENANCY_ADMIN_BINDINGS,
        include=include,
    )
