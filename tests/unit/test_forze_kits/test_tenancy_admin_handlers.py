"""Unit tests for the tenancy-admin handlers (tenant + membership management)."""

from __future__ import annotations

from uuid import uuid4

import pytest

from forze.application.contracts.tenancy import TenantIdentity
from forze_kits.aggregates.tenancy_admin import (
    CreateTenant,
    CreateTenantRequestDTO,
    DeactivateTenant,
    InviteMember,
    ListMembers,
    MembershipDTO,
    RemoveMember,
    TenantRefDTO,
)

# ----------------------- #


class _FakeManagement:
    def __init__(self, *, members: list | None = None) -> None:
        self._members = members or []
        self.provisioned: str | None = None
        self.listed_for = None
        self.attached: tuple | None = None
        self.detached: tuple | None = None
        self.deactivated = None

    async def provision_tenant(self, *, tenant_key=None):  # noqa: ANN001, ANN202
        self.provisioned = tenant_key
        return TenantIdentity(tenant_id=uuid4(), tenant_key=tenant_key)

    async def list_tenant_principals(self, tenant_id):  # noqa: ANN001, ANN202
        self.listed_for = tenant_id
        return self._members

    async def attach_principal(self, principal_id, tenant_id):  # noqa: ANN001, ANN202
        self.attached = (principal_id, tenant_id)

    async def detach_principal(self, principal_id, tenant_id):  # noqa: ANN001, ANN202
        self.detached = (principal_id, tenant_id)

    async def deactivate_tenant(self, tenant_id):  # noqa: ANN001, ANN202
        self.deactivated = tenant_id


# ....................... #


class TestCreateTenant:
    @pytest.mark.asyncio
    async def test_provisions_and_returns_identity(self) -> None:
        mgmt = _FakeManagement()
        handler = CreateTenant(tenant_management=mgmt)

        dto = await handler(CreateTenantRequestDTO(tenant_key="acme"))

        assert mgmt.provisioned == "acme"
        assert dto.tenant_key == "acme"
        assert dto.tenant_id is not None


class TestListMembers:
    @pytest.mark.asyncio
    async def test_lists_member_principal_ids(self) -> None:
        a, b = uuid4(), uuid4()
        tenant = uuid4()
        mgmt = _FakeManagement(members=[a, b])
        handler = ListMembers(tenant_management=mgmt)

        dto = await handler(TenantRefDTO(id=tenant))

        assert mgmt.listed_for == tenant
        assert [m.principal_id for m in dto.members] == [a, b]


class TestMembership:
    @pytest.mark.asyncio
    async def test_invite_attaches_principal(self) -> None:
        principal, tenant = uuid4(), uuid4()
        mgmt = _FakeManagement()
        handler = InviteMember(tenant_management=mgmt)

        result = await handler(
            MembershipDTO(tenant_id=tenant, principal_id=principal)
        )

        assert result is None
        assert mgmt.attached == (principal, tenant)

    @pytest.mark.asyncio
    async def test_remove_detaches_principal(self) -> None:
        principal, tenant = uuid4(), uuid4()
        mgmt = _FakeManagement()
        handler = RemoveMember(tenant_management=mgmt)

        result = await handler(
            MembershipDTO(tenant_id=tenant, principal_id=principal)
        )

        assert result is None
        assert mgmt.detached == (principal, tenant)


class TestDeactivateTenant:
    @pytest.mark.asyncio
    async def test_deactivates_by_id(self) -> None:
        tenant = uuid4()
        mgmt = _FakeManagement()
        handler = DeactivateTenant(tenant_management=mgmt)

        result = await handler(TenantRefDTO(id=tenant))

        assert result is None
        assert mgmt.deactivated == tenant
