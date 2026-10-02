"""Tenant memberships for listing tests, and the row-by-row listing as an oracle.

``TenantManagementAdapter.list_principal_tenants`` reads a principal's tenants in one batch. The
oracle is the listing it replaced — one read per membership — so a test can ask both the same
question on any backend.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

from forze.application.contracts.tenancy import TenantIdentity
from forze_identity.tenancy.adapters.management import (
    _BINDING_PAGE_SIZE,  # pyright: ignore[reportPrivateUsage]
    TenantManagementAdapter,
)
from forze_identity.tenancy.application.specs import principal_tenant_binding_spec, tenant_spec
from forze_identity.tenancy.domain.models.principal_tenant_binding import (
    CreatePrincipalTenantBindingCmd,
)
from forze_identity.tenancy.domain.models.tenant import (
    CreateTenantCmd,
    ReadTenant,
    UpdateTenantCmd,
)

TENANCY_SPECS = (tenant_spec, principal_tenant_binding_spec)
"""The documents tenant management reads and writes."""


def management(ctx: Any, *, wrap: Any = None) -> TenantManagementAdapter:
    """The adapter over *ctx*'s tenancy documents; *wrap* (port, name) wraps each query port."""

    def query(spec: Any, name: str) -> Any:
        port = ctx.doc.query(spec)
        return port if wrap is None else wrap(port, name)

    return TenantManagementAdapter(
        tenant_qry=query(tenant_spec, "tenant"),
        tenant_cmd=ctx.doc.command(tenant_spec),
        binding_qry=query(principal_tenant_binding_spec, "binding"),
        binding_cmd=ctx.doc.command(principal_tenant_binding_spec),
    )


async def oracle_principal_tenants(
    adapter: TenantManagementAdapter, principal_id: UUID
) -> list[TenantIdentity]:
    """The listing as it was: every membership, then one tenant read per membership."""

    bindings = []

    async for batch in adapter.binding_qry.find_stream(
        filters={"$values": {"principal_id": principal_id}}, chunk_size=_BINDING_PAGE_SIZE
    ):
        bindings.extend(batch)

    out: list[TenantIdentity] = []

    for bind in bindings:
        tenant = await adapter.tenant_qry.get(bind.tenant_id)

        if tenant.is_active:
            out.append(TenantIdentity(tenant_id=tenant.id, tenant_key=tenant.tenant_key))

    return out


async def create_tenant(ctx: Any, key: str, *, active: bool = True) -> ReadTenant:
    """A tenant, deactivated after it is made when *active* is false (create takes no flag)."""

    cmd = ctx.doc.command(tenant_spec)
    row = await cmd.create(CreateTenantCmd(tenant_key=key))

    if not active:
        row = await cmd.update(row.id, row.rev, UpdateTenantCmd(is_active=False))

    return row


async def join(ctx: Any, principal_id: UUID, tenant_id: UUID) -> None:
    await ctx.doc.command(principal_tenant_binding_spec).create(
        CreatePrincipalTenantBindingCmd(principal_id=principal_id, tenant_id=tenant_id)
    )


async def wide_memberships(ctx: Any) -> tuple[UUID, set[str]]:
    """A principal in 35 tenants — past Firestore's 30 ids in one read — 3 of them inactive,
    one joined twice, and the keys of the active ones. Another principal's membership must not
    show.

    The tenants are joined newest first, so the order memberships are read in differs from the
    order of the tenants' ids.
    """

    principal_id = uuid4()
    tenants = [await create_tenant(ctx, f"tenant-{i}", active=i >= 3) for i in range(35)]

    for tenant in reversed(tenants):
        await join(ctx, principal_id, tenant.id)

    await join(ctx, principal_id, tenants[-1].id)
    active = {tenant.tenant_key for tenant in tenants if tenant.is_active and tenant.tenant_key}

    await join(ctx, uuid4(), (await create_tenant(ctx, "someone-elses")).id)

    return principal_id, active


async def list_both_ways(ctx: Any, principal_id: UUID) -> list[TenantIdentity]:
    """*principal_id*'s tenants, after checking the batched and the row-by-row listing agree on
    them, order and repeats included."""

    adapter = management(ctx)
    listed = list(await adapter.list_principal_tenants(principal_id))

    if listed != await oracle_principal_tenants(adapter, principal_id):
        raise AssertionError("the batched and the row-by-row listing disagree")

    return listed
