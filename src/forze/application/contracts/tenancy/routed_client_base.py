"""Shared tenant-routed client pooling for integration packages."""

from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Callable, Generic, Mapping, Protocol, TypeVar
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.secrets import SecretRef, SecretsPort

from .helpers import (
    ensure_dsn_fingerprint,
    ensure_structured_fingerprint,
    require_tenant_id,
    resolve_dsn_for_tenant,
    resolve_structured_for_tenant,
)
from .registry import TenantClientRegistry

# ----------------------- #


class _CloseableClient(Protocol):
    async def close(self) -> None: ...


C = TypeVar("C", bound=_CloseableClient)


# ....................... #


@attrs.define(slots=True, kw_only=True)
class RoutedTenantClientBase(Generic[C]):
    """LRU tenant pool with fingerprint dedup and optional guarded eviction."""

    secrets: SecretsPort
    secret_ref_for_tenant: Callable[[UUID], SecretRef] | Mapping[UUID, SecretRef]
    tenant_provider: Callable[[], UUID | None]
    max_cached_tenants: int = 100
    guarded: bool = False
    tenant_required_message: str = "Tenant ID is required for routed access"

    _pool: TenantClientRegistry[C, str] = attrs.field(init=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        self._pool = TenantClientRegistry(
            max_entries=self.max_cached_tenants,
            create=self._create_client,
            dispose=lambda client: client.close(),
            guarded=self.guarded,
        )

    async def startup(self) -> None:
        await self._pool.startup()

    async def close(self) -> None:
        await self._pool.close()

    async def evict_tenant(self, tenant_id: UUID) -> None:
        await self._pool.evict(tenant_id)

    async def resolve_credentials(self, tenant_id: UUID) -> Any:
        raise NotImplementedError

    async def initialize_client(self, tenant_id: UUID, creds: Any) -> C:
        raise NotImplementedError

    async def ensure_access_fingerprint(self, tenant_id: UUID) -> None:
        raise NotImplementedError

    # ....................... #

    async def _create_client(self, tenant_id: UUID) -> C:
        creds = await self.resolve_credentials(tenant_id)

        return await self.initialize_client(tenant_id, creds)

    # ....................... #

    def _require_tenant_id(self) -> UUID:
        return require_tenant_id(
            self.tenant_provider,
            message=self.tenant_required_message,
        )

    # ....................... #

    @asynccontextmanager
    async def _client_scope(self) -> AsyncGenerator[C, None]:
        tenant_id = self._require_tenant_id()
        await self.ensure_access_fingerprint(tenant_id)

        async with self._pool.use(tenant_id) as client:
            yield client

    # ....................... #

    async def _get_client(self) -> C:
        tenant_id = self._require_tenant_id()
        await self.ensure_access_fingerprint(tenant_id)

        return await self._pool.get(tenant_id)

    # ....................... #

    def _peek_client(self, tenant_id: UUID | None = None) -> C | None:
        tid = tenant_id if tenant_id is not None else self.tenant_provider()

        if tid is None:
            return None

        return self._pool.peek(tid)


# ....................... #


@attrs.define(slots=True, kw_only=True)
class DsnRoutedTenantClientBase(RoutedTenantClientBase[C]):
    """DSN-backed routed client (Postgres, Redis, …)."""

    dsn_backend: str

    # ....................... #

    async def resolve_credentials(self, tenant_id: UUID) -> str:
        return await resolve_dsn_for_tenant(
            tenant_id=tenant_id,
            secrets=self.secrets,
            ref_for_tenant=self.secret_ref_for_tenant,
            backend=self.dsn_backend,
        )

    # ....................... #

    async def ensure_access_fingerprint(self, tenant_id: UUID) -> None:
        await ensure_dsn_fingerprint(
            self._pool.get_fingerprint,
            self._pool.set_fingerprint,
            tenant_id=tenant_id,
            secrets=self.secrets,
            ref_for_tenant=self.secret_ref_for_tenant,
            backend=self.dsn_backend,
        )


# ....................... #


@attrs.define(slots=True, kw_only=True)
class StructuredSecretRoutedTenantClientBase(RoutedTenantClientBase[C]):
    """Structured-secret routed client (JSON credentials resolved per tenant).

    Integration packages implement :meth:`credential_fingerprint` for their secret
    shape (workspace id, key file path, inline key material, etc.).
    """

    creds_type: type[BaseModel]
    backend: str

    # ....................... #

    async def resolve_credentials(self, tenant_id: UUID) -> BaseModel:
        return await resolve_structured_for_tenant(
            self.creds_type,
            tenant_id=tenant_id,
            secrets=self.secrets,
            ref_for_tenant=self.secret_ref_for_tenant,
            backend=self.backend,
        )

    # ....................... #

    def credential_fingerprint(self, creds: BaseModel) -> str:
        raise NotImplementedError

    # ....................... #

    async def ensure_access_fingerprint(self, tenant_id: UUID) -> None:
        async def _fingerprint() -> str:
            resolved = await self.resolve_credentials(tenant_id)

            return self.credential_fingerprint(resolved)

        await ensure_structured_fingerprint(
            self._pool.get_fingerprint,
            self._pool.set_fingerprint,
            tenant_id=tenant_id,
            fingerprint=_fingerprint,
        )
