"""In-memory tenant resolver and management stubs."""

from __future__ import annotations

from typing import final
from uuid import UUID

import attrs

from forze.application.contracts.tenancy import (
    TenantIdentity,
    TenantManagementPort,
    TenantResolverPort,
)
from forze.base.exceptions import exc
from forze.base.primitives import utcnow, uuid7
from forze_mock.state import MockState

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True)
class MockTenantResolverPort(TenantResolverPort):
    state: MockState
    route: str = "main"

    def _tenants(self) -> dict[str, dict[str, object]]:
        identity = self.state.identity
        tenants = identity.setdefault("tenants", {})
        assert isinstance(tenants, dict)  # nosec: B101
        return tenants.setdefault(self.route, {})  # type: ignore[return-value]

    async def resolve_from_principal(
        self,
        principal_id: UUID,
        *,
        requested_tenant_id: UUID | None = None,
    ) -> TenantIdentity | None:
        _ = principal_id
        with self.state.lock:
            if requested_tenant_id is not None:
                entry = self._tenants().get(str(requested_tenant_id))
                if entry is None:
                    return None
                return TenantIdentity(
                    tenant_id=requested_tenant_id,
                    tenant_key=str(entry.get("tenant_key", requested_tenant_id)),
                )
            for tid, entry in self._tenants().items():
                members = entry.get("principals", [])
                if principal_id in members:  # type: ignore[operator]
                    return TenantIdentity(
                        tenant_id=UUID(tid),
                        tenant_key=str(entry.get("tenant_key", tid)),
                    )
        return None


@final
@attrs.define(slots=True, kw_only=True)
class MockTenantManagementPort(TenantManagementPort):
    state: MockState
    route: str = "main"

    def _tenants(self) -> dict[str, dict[str, object]]:
        identity = self.state.identity
        tenants = identity.setdefault("tenants", {})
        assert isinstance(tenants, dict)  # nosec: B101
        return tenants.setdefault(self.route, {})  # type: ignore[return-value]

    async def provision_tenant(
        self,
        *,
        tenant_key: str | None = None,
    ) -> TenantIdentity:
        tid = uuid7()
        key = tenant_key or str(tid)
        with self.state.lock:
            self._tenants()[str(tid)] = {
                "tenant_key": key,
                "principals": [],
                "active": True,
                "created_at": utcnow(),
            }
        return TenantIdentity(tenant_id=tid, tenant_key=key)

    async def attach_principal(
        self,
        principal_id: UUID,
        tenant_id: UUID,
    ) -> None:
        with self.state.lock:
            entry = self._tenants().get(str(tenant_id))
            if entry is None:
                raise exc.not_found(f"Tenant {tenant_id!r} not found")
            principals = entry.setdefault("principals", [])
            if isinstance(principals, list) and principal_id not in principals:
                principals.append(principal_id)  # type: ignore[arg-type]

    async def detach_principal(
        self,
        principal_id: UUID,
        tenant_id: UUID,
    ) -> None:
        with self.state.lock:
            entry = self._tenants().get(str(tenant_id))

            if entry is None:
                return

            principals = entry.get("principals", [])

            if isinstance(principals, list) and principal_id in principals:
                principals.remove(principal_id)  # type: ignore[arg-type]

    async def deactivate_tenant(self, tenant_id: UUID) -> None:
        with self.state.lock:
            entry = self._tenants().get(str(tenant_id))
            if entry is not None:
                entry["active"] = False
