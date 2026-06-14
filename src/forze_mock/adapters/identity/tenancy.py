"""In-memory tenant resolver and management stubs."""

from __future__ import annotations

from typing import Sequence, final
from uuid import UUID

import attrs

from forze.application.contracts.tenancy import (
    TenantIdentity,
    TenantManagementPort,
    TenantProvisionerPort,
    TenantResolverPort,
)
from forze.base.exceptions import exc
from forze.base.primitives import utcnow, uuid7
from forze_mock.state import MockState

# ----------------------- #


@attrs.define(slots=True, kw_only=True)
class _TenantRouteStore:
    """Shared per-route tenant store accessor for the mock tenancy ports."""

    state: MockState
    route: str = "main"

    def _tenants(self) -> dict[str, dict[str, object]]:
        identity = self.state.identity
        tenants = identity.setdefault("tenants", {})
        assert isinstance(tenants, dict)  # nosec: B101
        return tenants.setdefault(self.route, {})  # type: ignore[return-value]


# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True)
class MockTenantResolverPort(_TenantRouteStore, TenantResolverPort):
    """Resolve tenants from seeded principal→tenant memberships.

    Mirrors the real document-backed resolver
    (:class:`forze_identity.tenancy.adapters.resolver.TenantResolverAdapter`):

    - ``requested_tenant_id`` resolves only when the principal is a member of
      that tenant; otherwise raises ``tenant_mismatch``;
    - ambiguous membership (no request, >1 tenant) raises ``tenant_ambiguous``;
    - inactive tenants raise ``tenant_inactive``.

    Seed memberships via :meth:`MockTenantManagementPort.attach_principal`.
    """

    async def resolve_from_principal(
        self,
        principal_id: UUID,
        *,
        requested_tenant_id: UUID | None = None,
    ) -> TenantIdentity | None:
        with self.state.lock:
            tenants = self._tenants()

            member_of = [
                (tid, entry)
                for tid, entry in tenants.items()
                if principal_id in entry.get("principals", [])  # type: ignore[operator]
            ]

            if requested_tenant_id is not None:
                match = next(
                    (
                        (tid, entry)
                        for tid, entry in member_of
                        if tid == str(requested_tenant_id)
                    ),
                    None,
                )

                if match is None:
                    raise exc.authentication(
                        "Requested tenant does not match principal membership",
                        code="tenant_mismatch",
                    )

                tid, entry = match

            else:
                if not member_of:
                    return None

                if len(member_of) > 1:
                    raise exc.authentication(
                        "Tenant identity is ambiguous for this principal",
                        code="tenant_ambiguous",
                    )

                tid, entry = member_of[0]

            if not entry.get("active", True):
                raise exc.authentication(
                    "Tenant is inactive",
                    code="tenant_inactive",
                )

            return TenantIdentity(
                tenant_id=UUID(tid),
                tenant_key=str(entry.get("tenant_key", tid)),
            )


@final
@attrs.define(slots=True, kw_only=True)
class MockTenantManagementPort(_TenantRouteStore, TenantManagementPort):
    provisioner: TenantProvisionerPort | None = None
    """Optional per-tenant infrastructure provisioner run on :meth:`provision_tenant`."""

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
        identity = TenantIdentity(tenant_id=tid, tenant_key=key)

        if self.provisioner is not None:
            await self.provisioner.provision(identity)

        return identity

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

    async def list_principal_tenants(
        self,
        principal_id: UUID,
    ) -> Sequence[TenantIdentity]:
        with self.state.lock:
            return [
                TenantIdentity(
                    tenant_id=UUID(tid),
                    tenant_key=str(entry.get("tenant_key", tid)),
                )
                for tid, entry in self._tenants().items()
                if entry.get("active", True)
                and principal_id in entry.get("principals", [])  # type: ignore[operator]
            ]

    async def list_tenant_principals(
        self,
        tenant_id: UUID,
    ) -> Sequence[UUID]:
        with self.state.lock:
            entry = self._tenants().get(str(tenant_id))

            if entry is None:
                return []

            principals = entry.get("principals", [])

            return list(principals) if isinstance(principals, list) else []

    async def deactivate_tenant(self, tenant_id: UUID) -> None:
        with self.state.lock:
            entry = self._tenants().get(str(tenant_id))
            if entry is not None:
                entry["active"] = False

    async def deprovision_tenant(self, tenant_id: UUID) -> None:
        if self.provisioner is None:
            return

        with self.state.lock:
            entry = self._tenants().get(str(tenant_id))
            if entry is None:
                # Mirror the real adapter, which loads the tenant (a document ``get``
                # that raises on a missing row) before tearing down its infrastructure.
                raise exc.not_found(f"Tenant {tenant_id!r} not found")
            key = str(entry["tenant_key"])

        await self.provisioner.deprovision(
            TenantIdentity(tenant_id=tenant_id, tenant_key=key)
        )
