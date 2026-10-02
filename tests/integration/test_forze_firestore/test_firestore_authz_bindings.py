"""Identity-plane binding scans read every row on Firestore, past one page.

The grant resolver reads a principal's bindings through ``fetch_all_document_hits``, and tenancy
management drains a tenant's memberships. Firestore refuses offset pagination, so a scan that
pages by offset fails on the second page: a principal with more bindings than one page holds
could not be authorized, and a tenant with more members than one page could not be listed.
"""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest

from forze.application.contracts.document import DocumentCommandDepKey, DocumentQueryDepKey
from forze.application.execution import Deps
from forze.application.integrations.authz import ConfigGrants, ConfigGrantsProvider
from forze.base.exceptions import CoreException
from forze_firestore.execution.deps import (
    ConfigurableFirestoreDocument,
    FirestoreDocumentConfig,
)
from forze_firestore.execution.deps.keys import FirestoreClientDepKey
from forze_firestore.kernel.client import FirestoreClient
from forze_identity.authz.application.specs import (
    permission_definition_spec,
    principal_permission_binding_spec,
)
from forze_identity.authz.domain.models.bindings import CreatePrincipalPermissionBindingCmd
from forze_identity.authz.domain.models.permission_definition import (
    CreatePermissionDefinitionCmd,
)
from forze_identity.authz.services.grants import check_declared_keys, fetch_all_document_hits
from forze_identity.tenancy.adapters.management import (
    _BINDING_PAGE_SIZE,  # pyright: ignore[reportPrivateUsage]
    TenantManagementAdapter,
)
from forze_identity.tenancy.application.specs import principal_tenant_binding_spec, tenant_spec
from forze_identity.tenancy.domain.models.principal_tenant_binding import (
    CreatePrincipalTenantBindingCmd,
)
from tests.support.authz_grants import GRANT_SPECS, resolve_both_ways, wide_catalog
from tests.support.execution_context import context_from_deps

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


def _context(client: FirestoreClient, collection: str) -> Any:
    configurable = ConfigurableFirestoreDocument(
        config=FirestoreDocumentConfig(
            read=("(default)", collection),
            write=("(default)", collection),
        ),
    )

    return context_from_deps(
        Deps.plain(
            {
                FirestoreClientDepKey: client,
                DocumentQueryDepKey: configurable,
                DocumentCommandDepKey: configurable,
            }
        )
    )


async def test_a_scan_past_one_page_reads_every_binding(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    ctx = _context(firestore_client, f"pp_bindings_{unique_collection}")
    principal = uuid4()
    command = ctx.document.command(principal_permission_binding_spec)

    for _ in range(25):
        await command.create(
            CreatePrincipalPermissionBindingCmd(principal_id=principal, permission_id=uuid4())
        )

    # Someone else's row, which the filter must leave out.
    await command.create(
        CreatePrincipalPermissionBindingCmd(principal_id=uuid4(), permission_id=uuid4())
    )

    rows = await fetch_all_document_hits(
        ctx.document.query(principal_permission_binding_spec),
        filters={"$values": {"principal_id": principal}},
        page_size=10,
    )

    assert len(rows) == 25
    assert len({row.id for row in rows}) == 25
    assert {row.principal_id for row in rows} == {principal}


async def test_a_tenant_with_more_members_than_one_page_lists_them_all(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    ctx = _context(firestore_client, f"pt_bindings_{unique_collection}")
    tenant = uuid4()
    members = {uuid4() for _ in range(_BINDING_PAGE_SIZE + 1)}
    bindings = ctx.document.command(principal_tenant_binding_spec)

    for member in members:
        await bindings.create(
            CreatePrincipalTenantBindingCmd(principal_id=member, tenant_id=tenant)
        )

    adapter = TenantManagementAdapter(
        tenant_qry=ctx.document.query(tenant_spec),
        tenant_cmd=ctx.document.command(tenant_spec),
        binding_qry=ctx.document.query(principal_tenant_binding_spec),
        binding_cmd=bindings,
    )

    assert set(await adapter.list_tenant_principals(tenant)) == members


async def test_a_principal_past_one_in_batch_resolves_in_full(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    # Firestore takes at most 30 values in one `in`: 39 roles to expand and 31 active groups
    # make every batched read split, and the grants must still match the row-by-row reads.
    routes = {
        spec.name: ConfigurableFirestoreDocument(
            config=FirestoreDocumentConfig(
                read=("(default)", f"{unique_collection}_{spec.name}"),
                write=("(default)", f"{unique_collection}_{spec.name}"),
            ),
        )
        for spec in GRANT_SPECS
    }
    ctx = context_from_deps(
        Deps.plain({FirestoreClientDepKey: firestore_client}).merge(
            Deps.routed({DocumentQueryDepKey: routes, DocumentCommandDepKey: routes})
        )
    )
    principal_id, roles, permissions = await wide_catalog(ctx)

    assert await resolve_both_ways(ctx, principal_id) == (roles, permissions)


async def test_a_provider_declaring_more_keys_than_one_in_batch_is_checked(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    ctx = _context(firestore_client, f"perms_{unique_collection}")
    keys = [f"ops.key_{i}" for i in range(31)]

    for key in keys:
        await ctx.document.command(permission_definition_spec).create(
            CreatePermissionDefinitionCmd(permission_key=key)
        )

    def provider(*declared: str) -> ConfigGrantsProvider:
        return ConfigGrantsProvider(keys=frozenset(declared), grants=ConfigGrants())

    query = ctx.document.query(permission_definition_spec)
    await check_declared_keys(query, [provider(*keys)])

    with pytest.raises(CoreException) as refused:
        await check_declared_keys(query, [provider(*keys, "ops.missing")])

    assert refused.value.code == "authz_provider_unknown_keys"
    assert "ops.missing" in str(refused.value)
