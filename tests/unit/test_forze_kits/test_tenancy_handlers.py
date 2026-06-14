"""Unit tests for the tenant-selector self-service handlers."""

from __future__ import annotations

from uuid import uuid4

import pytest

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.authn.value_objects.credentials import (
    AccessTokenCredentials,
)
from forze.application.contracts.authn.value_objects.tokens import (
    IssuedAccessToken,
    IssuedTokens,
)
from forze.application.contracts.tenancy import TenantIdentity
from forze.base.exceptions import CoreException, exc
from forze_kits.aggregates.tenancy import (
    ListTenants,
    SwitchTenant,
    TenantSwitchRequestDTO,
)

# ----------------------- #

_USER = AuthnIdentity(principal_id=uuid4())


def _resolver(identity: AuthnIdentity | None):
    return lambda: identity


class _FakeManagement:
    def __init__(self, tenants: list[TenantIdentity]) -> None:
        self._tenants = tenants
        self.listed_for = None

    async def list_principal_tenants(self, principal_id):  # noqa: ANN001, ANN202
        self.listed_for = principal_id
        return self._tenants


class _FakeResolver:
    def __init__(self, *, raises: Exception | None = None) -> None:
        self.calls: list[tuple] = []
        self._raises = raises

    async def resolve_from_principal(self, principal_id, *, requested_tenant_id=None):  # noqa: ANN001, ANN202
        self.calls.append((principal_id, requested_tenant_id))
        if self._raises is not None:
            raise self._raises
        return TenantIdentity(tenant_id=requested_tenant_id)


class _FakeTokenLifecycle:
    def __init__(self) -> None:
        self.issued: tuple | None = None

    async def issue_tokens(self, identity, *, tenant_id=None):  # noqa: ANN001, ANN202
        self.issued = (identity, tenant_id)
        return IssuedTokens(
            access=IssuedAccessToken(
                token=AccessTokenCredentials(token="new-access", scheme="Bearer"),
            ),
        )


# ....................... #


class TestListTenants:
    @pytest.mark.asyncio
    async def test_lists_active_memberships_and_flags_current(self) -> None:
        a, b = uuid4(), uuid4()
        mgmt = _FakeManagement(
            [
                TenantIdentity(tenant_id=a, tenant_key="acme"),
                TenantIdentity(tenant_id=b, tenant_key="globex"),
            ]
        )
        handler = ListTenants(
            resolver=_resolver(_USER),
            current_tenant=lambda: TenantIdentity(tenant_id=b),
            tenant_management=mgmt,
        )

        dto = await handler(None)

        assert mgmt.listed_for == _USER.principal_id
        assert [(t.tenant_id, t.tenant_key, t.is_current) for t in dto.tenants] == [
            (a, "acme", False),
            (b, "globex", True),
        ]

    @pytest.mark.asyncio
    async def test_no_identity_is_401(self) -> None:
        handler = ListTenants(
            resolver=_resolver(None),
            current_tenant=lambda: None,
            tenant_management=_FakeManagement([]),
        )

        with pytest.raises(CoreException, match="auth_required"):
            await handler(None)


class TestSwitchTenant:
    @pytest.mark.asyncio
    async def test_validates_membership_then_mints_scoped_token(self) -> None:
        target = uuid4()
        resolver = _FakeResolver()
        lifecycle = _FakeTokenLifecycle()
        handler = SwitchTenant(
            resolver=_resolver(_USER),
            tenant_resolver=resolver,
            token_lifecycle=lifecycle,
        )

        dto = await handler(TenantSwitchRequestDTO(id=target))

        # Validated against membership with the requested tenant, then minted scoped to it.
        assert resolver.calls == [(_USER.principal_id, target)]
        assert lifecycle.issued == (_USER, target)
        assert dto.access_token == "new-access"

    @pytest.mark.asyncio
    async def test_rejects_non_member_without_minting(self) -> None:
        resolver = _FakeResolver(
            raises=exc.authentication("nope", code="tenant_mismatch")
        )
        lifecycle = _FakeTokenLifecycle()
        handler = SwitchTenant(
            resolver=_resolver(_USER),
            tenant_resolver=resolver,
            token_lifecycle=lifecycle,
        )

        with pytest.raises(CoreException, match="tenant_mismatch"):
            await handler(TenantSwitchRequestDTO(id=uuid4()))

        assert lifecycle.issued is None  # never minted a token for a non-member

    @pytest.mark.asyncio
    async def test_no_identity_is_401(self) -> None:
        handler = SwitchTenant(
            resolver=_resolver(None),
            tenant_resolver=_FakeResolver(),
            token_lifecycle=_FakeTokenLifecycle(),
        )

        with pytest.raises(CoreException, match="auth_required"):
            await handler(TenantSwitchRequestDTO(id=uuid4()))
