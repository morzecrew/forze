"""In-memory secret stores for routed-client integration tests."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any
from uuid import UUID

from forze.application.contracts.secrets import SecretRef
from forze.base.exceptions import exc


def tenant_secret_ref(tenant_id: UUID, resource_suffix: str) -> SecretRef:
    """Build a tenant-scoped secret path (e.g. ``tenants/{id}/s3``)."""

    return SecretRef(path=f"tenants/{tenant_id}/{resource_suffix}")


def tenant_holder() -> tuple[Callable[[], UUID | None], Callable[[UUID | None], None]]:
    """Mutable tenant slot for routed client ``tenant_provider`` wiring."""

    slot: list[UUID | None] = [None]

    def getter() -> UUID | None:
        return slot[0]

    def setter(value: UUID | None) -> None:
        slot[0] = value

    return getter, setter


class MemSecretsByPath:
    """Maps secret paths to string payloads (DSN, JSON, etc.)."""

    def __init__(
        self,
        path_to_value: dict[str, str],
        *,
        missing_path: str | None = None,
        broken_path: str | None = None,
    ) -> None:
        self._paths = path_to_value
        self._missing_path = missing_path
        self._broken_path = broken_path

    async def resolve_str(self, ref: SecretRef) -> str:
        if self._broken_path is not None and ref.path == self._broken_path:
            raise RuntimeError("vault unavailable")
        if self._missing_path is not None and ref.path == self._missing_path:
            raise exc.not_found(
                f"No secret for {ref.path!r}",
                details={"ref": ref.path},
            )
        try:
            return self._paths[ref.path]
        except KeyError as e:
            raise exc.not_found(
                f"No secret for {ref.path!r}",
                details={"ref": ref.path},
            ) from e

    async def exists(self, ref: SecretRef) -> bool:
        return ref.path in self._paths


class MemSecretsTenantByPath(MemSecretsByPath):
    """Tenant paths ``tenants/{id}/{suffix}`` mapping to string secrets."""

    def __init__(
        self,
        *,
        resource_suffix: str,
        values_by_tenant: dict[UUID, str],
        missing_tenant: UUID | None = None,
        broken_tenant: UUID | None = None,
    ) -> None:
        paths = {
            f"tenants/{tid}/{resource_suffix}": value
            for tid, value in values_by_tenant.items()
        }
        missing_path = (
            f"tenants/{missing_tenant}/{resource_suffix}"
            if missing_tenant is not None
            else None
        )
        broken_path = (
            f"tenants/{broken_tenant}/{resource_suffix}"
            if broken_tenant is not None
            else None
        )
        super().__init__(paths, missing_path=missing_path, broken_path=broken_path)


class MemSecretsTenantJson(MemSecretsByPath):
    """Tenant paths with JSON-serialized dict payloads per tenant."""

    def __init__(
        self,
        *,
        resource_suffix: str,
        payloads_by_tenant: dict[UUID, dict[str, Any]],
        missing_tenant: UUID | None = None,
        broken_tenant: UUID | None = None,
    ) -> None:
        paths = {
            f"tenants/{tid}/{resource_suffix}": json.dumps(payload)
            for tid, payload in payloads_by_tenant.items()
        }
        missing_path = (
            f"tenants/{missing_tenant}/{resource_suffix}"
            if missing_tenant is not None
            else None
        )
        broken_path = (
            f"tenants/{broken_tenant}/{resource_suffix}"
            if broken_tenant is not None
            else None
        )
        super().__init__(paths, missing_path=missing_path, broken_path=broken_path)
