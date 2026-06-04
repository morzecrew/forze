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
from .value_objects import TenantIdentity
from .registry import TenantClientRegistry

# ----------------------- #


class _CloseableClient(Protocol):
    async def close(self) -> None: ...


C = TypeVar("C", bound=_CloseableClient)


# ....................... #


@attrs.define(slots=True, kw_only=True)
class RoutedTenantClientBase(Generic[C]):
    """LRU tenant pool with fingerprint dedup and optional guarded eviction.

    **Credential rotation.** A tenant's access fingerprint covers *all* credential
    fields, including secrets (see :meth:`credential_fingerprint` /
    :func:`~forze.base.primitives.build_routing_fingerprint`), so rotated credentials
    produce a different fingerprint. The fingerprint and pooled client are cached until
    explicitly invalidated, so rotation is **signal-driven** by default: wire your
    secret store's rotation notification to :meth:`evict_tenant`, which drops both the
    cached fingerprint and the client so the next access rebuilds with fresh
    credentials. For deployments without such a signal, set :attr:`fingerprint_ttl`
    (seconds) to periodically re-resolve credentials and rebuild only when the
    fingerprint actually changed.
    """

    secrets: SecretsPort
    secret_ref_for_tenant: Callable[[UUID], SecretRef] | Mapping[UUID, SecretRef]
    tenant_provider: Callable[[], UUID | TenantIdentity | None]
    max_cached_tenants: int = 100
    guarded: bool = False
    tenant_required_message: str = "Tenant ID is required for routed access"
    fingerprint_ttl: float | None = None
    """Optional seconds-based TTL for credential-rotation refresh.

    When set, a tenant's cached fingerprint is re-resolved on first access after it ages
    past *fingerprint_ttl*; if the credentials changed, the pooled client is evicted and
    rebuilt. ``None`` (default) keeps fingerprints cached until :meth:`evict_tenant`
    (signal-driven rotation). Adds periodic secret-store load per active tenant, so
    prefer the signal-driven path when a rotation notification is available.
    """

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
        """Drop the cached client and fingerprint for *tenant_id*.

        The credential-rotation hook: call this when a tenant's credentials change so
        the next access re-resolves the secret and rebuilds the client (see the class
        rotation contract).
        """

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

    def _fingerprint_expiry_check(self) -> Callable[[UUID], bool] | None:
        """Return a per-tenant staleness predicate when :attr:`fingerprint_ttl` is set."""

        ttl = self.fingerprint_ttl

        if ttl is None:
            return None

        return lambda tenant_id: self._pool.is_fingerprint_expired(tenant_id, ttl)

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
        if tenant_id is not None:
            return self._pool.peek(tenant_id)

        value = self.tenant_provider()

        if value is None:
            return None

        tid = value.tenant_id if isinstance(value, TenantIdentity) else value

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
            is_expired=self._fingerprint_expiry_check(),
            on_change=self._pool.evict,
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
        """Return the LRU pool dedup key for *creds*.

        Build it with :func:`~forze.base.primitives.build_routing_fingerprint`,
        declaring **every** credential field — including secrets — so that rotating any
        field (a secret in particular) changes the key. Omitting a secret silently
        defeats rotation detection: the pool would keep serving the stale client.
        """

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
            is_expired=self._fingerprint_expiry_check(),
            on_change=self._pool.evict,
        )
