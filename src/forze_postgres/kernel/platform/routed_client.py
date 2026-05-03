"""Postgres client that resolves a DSN per tenant via :class:`AsyncSecretsPort`."""

import asyncio
from collections import OrderedDict
from collections.abc import Callable
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any, AsyncIterator, Literal, Mapping, Sequence, overload
from uuid import UUID

import attrs
from psycopg import AsyncConnection
from psycopg.abc import Params, QueryNoTemplate

from forze.application.contracts.secrets import SecretRef, SecretsPort
from forze.base.errors import CoreError, InfrastructureError, SecretNotFoundError
from forze.base.primitives import JsonDict

from .client import PostgresClient
from .port import PostgresClientPort
from .types import RowFactory
from .value_objects import PostgresConfig, PostgresTransactionOptions

# ----------------------- #


@attrs.define(slots=True)
class RoutedPostgresClient(PostgresClientPort):
    """Routes each call to a lazily created :class:`PostgresClient` for the current tenant.

    The tenant is read from ``tenant_provider`` (typically
    :meth:`forze.application.execution.ExecutionContext.get_tenant_id`).
    DSN strings are loaded via :meth:`SecretsPort.resolve_str` using
    ``secret_ref_for_tenant``.

    Call :meth:`startup` during application startup (see
    :func:`~forze_postgres.execution.lifecycle.routed_postgres_lifecycle_step`)
    before use. Call :meth:`close` on shutdown to drain all per-tenant pools.
    """

    secrets: SecretsPort
    """Backend used to resolve connection strings."""

    secret_ref_for_tenant: Callable[[UUID], SecretRef] | Mapping[UUID, SecretRef]
    """Build a :class:`SecretRef` for a tenant's database DSN."""

    tenant_provider: Callable[[], UUID | None]
    """Return the current tenant id (or ``None`` if unauthenticated)."""

    pool_config: PostgresConfig = attrs.field(factory=PostgresConfig)
    """Pool configuration applied to each per-tenant :class:`PostgresClient`."""

    acquire_timeout: timedelta = attrs.field(default=timedelta(seconds=0.5))
    """Pool checkout timeout passed to each inner client."""

    max_cached_tenants: int = 100
    """Maximum number of tenant pools to retain; LRU eviction closes overflow pools."""

    _lock: asyncio.Lock = attrs.field(factory=asyncio.Lock, init=False)
    _clients: OrderedDict[UUID, PostgresClient] = attrs.field(
        factory=OrderedDict,
        init=False,
    )
    _started: bool = attrs.field(default=False, init=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.max_cached_tenants < 1:
            raise CoreError("max_cached_tenants must be at least 1")

    # ....................... #

    async def startup(self) -> None:
        """Mark the client as ready (idempotent)."""

        self._started = True

    # ....................... #

    async def close(self) -> None:
        """Close all per-tenant pools and reset startup state."""

        async with self._lock:
            to_close = list(self._clients.values())
            self._clients.clear()

        for c in to_close:
            await c.close()

        self._started = False

    # ....................... #

    async def evict_tenant(self, tenant_id: UUID) -> None:
        """Close and remove the pool for one tenant (e.g. after credential rotation)."""

        async with self._lock:
            client = self._clients.pop(tenant_id, None)

        if client is not None:
            await client.close()

    # ....................... #

    def _require_tenant_id(self) -> UUID:
        tid = self.tenant_provider()

        if tid is None:
            raise CoreError(
                "Tenant ID is required for routed Postgres access",
                code="tenant_required",
            )

        return tid

    # ....................... #

    def _get_secret_ref(self, tenant_id: UUID) -> SecretRef:
        if callable(self.secret_ref_for_tenant):
            return self.secret_ref_for_tenant(tenant_id)

        return self.secret_ref_for_tenant[tenant_id]

    # ....................... #

    async def _get_client(self) -> PostgresClient:
        if not self._started:
            raise InfrastructureError("Routed Postgres client is not started")

        tid = self._require_tenant_id()

        async with self._lock:
            if tid in self._clients:
                client = self._clients[tid]
                self._clients.move_to_end(tid)
                return client

            ref = self._get_secret_ref(tid)

            try:
                dsn = await self.secrets.resolve_str(ref)

            except SecretNotFoundError:
                raise

            except Exception as e:
                raise InfrastructureError(
                    f"Failed to resolve database secret for tenant {tid}: {e}",
                ) from e

            client = PostgresClient()
            await client.initialize(
                dsn,
                config=self.pool_config,
                acquire_timeout=self.acquire_timeout,
            )
            self._clients[tid] = client
            self._clients.move_to_end(tid)

            while len(self._clients) > self.max_cached_tenants:
                _, old = self._clients.popitem(last=False)
                await old.close()

            return client

    # ....................... #

    async def health(self) -> tuple[str, bool]:
        inner = await self._get_client()
        return await inner.health()

    # ....................... #

    def is_in_transaction(self) -> bool:
        tid = self.tenant_provider()

        if tid is None:
            return False

        inner = self._clients.get(tid)

        if inner is None:
            return False

        return inner.is_in_transaction()

    # ....................... #

    def query_concurrency_limit(self) -> int:
        tid = self.tenant_provider()

        if tid is not None:
            inner = self._clients.get(tid)

            if inner is not None:
                return inner.query_concurrency_limit()

        cfg = self.pool_config

        if cfg.max_concurrent_queries is not None:
            return cfg.max_concurrent_queries

        return max(1, cfg.max_size - cfg.pool_headroom)

    # ....................... #

    def require_transaction(self) -> None:
        tid = self.tenant_provider()

        if tid is None:
            raise InfrastructureError("Transactional context is required")

        inner = self._clients.get(tid)

        if inner is None:
            raise InfrastructureError("Transactional context is required")

        inner.require_transaction()

    # ....................... #

    @asynccontextmanager
    async def bound_connection(self) -> AsyncIterator[AsyncConnection]:
        inner = await self._get_client()

        async with inner.bound_connection() as conn:
            yield conn

    # ....................... #

    @asynccontextmanager
    async def transaction(
        self,
        *,
        options: PostgresTransactionOptions | None = None,
    ) -> AsyncIterator[AsyncConnection]:
        inner = await self._get_client()

        async with inner.transaction(options=options) as conn:
            yield conn

    # ....................... #

    @overload
    async def execute(
        self,
        query: QueryNoTemplate,
        params: Params | None = None,
        *,
        return_rowcount: Literal[False] = False,
    ) -> None: ...

    @overload
    async def execute(
        self,
        query: QueryNoTemplate,
        params: Params | None = None,
        *,
        return_rowcount: Literal[True],
    ) -> int: ...

    async def execute(
        self,
        query: QueryNoTemplate,
        params: Params | None = None,
        *,
        return_rowcount: bool = False,
    ) -> int | None:
        inner = await self._get_client()

        if return_rowcount:
            return await inner.execute(query, params, return_rowcount=True)

        await inner.execute(query, params, return_rowcount=False)

        return None

    # ....................... #

    async def execute_many(
        self, query: QueryNoTemplate, params: Sequence[Params]
    ) -> None:
        inner = await self._get_client()

        await inner.execute_many(query, params)

    # ....................... #

    @overload
    async def fetch_all(
        self,
        query: QueryNoTemplate,
        params: Params | None = None,
        *,
        row_factory: Literal["dict"] = "dict",
        commit: bool = False,
    ) -> list[JsonDict]: ...

    @overload
    async def fetch_all(
        self,
        query: QueryNoTemplate,
        params: Params | None = None,
        *,
        row_factory: Literal["tuple"] = "tuple",
        commit: bool = False,
    ) -> list[tuple[Any, ...]]: ...

    async def fetch_all(
        self,
        query: QueryNoTemplate,
        params: Params | None = None,
        *,
        row_factory: RowFactory = "dict",
        commit: bool = False,
    ) -> list[JsonDict] | list[tuple[Any, ...]]:
        inner = await self._get_client()

        return await inner.fetch_all(
            query,
            params,
            row_factory=row_factory,
            commit=commit,
        )

    # ....................... #

    @overload
    async def fetch_one(
        self,
        query: QueryNoTemplate,
        params: Params | None = None,
        *,
        row_factory: Literal["dict"] = "dict",
        commit: bool = False,
    ) -> JsonDict | None: ...

    @overload
    async def fetch_one(
        self,
        query: QueryNoTemplate,
        params: Params | None = None,
        *,
        row_factory: Literal["tuple"] = "tuple",
        commit: bool = False,
    ) -> tuple[Any, ...] | None: ...

    async def fetch_one(
        self,
        query: QueryNoTemplate,
        params: Params | None = None,
        *,
        row_factory: RowFactory = "dict",
        commit: bool = False,
    ) -> JsonDict | tuple[Any, ...] | None:
        inner = await self._get_client()

        return await inner.fetch_one(
            query,
            params,
            row_factory=row_factory,
            commit=commit,
        )

    # ....................... #

    async def fetch_value(
        self,
        query: QueryNoTemplate,
        params: Params | None = None,
        *,
        default: Any = None,
    ) -> Any:
        inner = await self._get_client()

        return await inner.fetch_value(query, params, default=default)
