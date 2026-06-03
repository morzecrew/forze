"""Postgres client that resolves a DSN per tenant via :class:`AsyncSecretsPort`."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import (
    Any,
    AsyncGenerator,
    Callable,
    Literal,
    Mapping,
    Sequence,
    overload,
)
from uuid import UUID

import attrs
from psycopg import AsyncConnection
from psycopg.abc import Params, QueryNoTemplate

from forze.application.contracts.secrets import SecretRef, SecretsPort
from forze.application.contracts.tenancy.routed_client_base import DsnRoutedTenantClientBase
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict

from .client import PostgresClient
from .port import PostgresClientPort
from .types import RowFactory
from .value_objects import PostgresConfig, PostgresTransactionOptions

# ----------------------- #


@attrs.define(slots=True, kw_only=True)
class RoutedPostgresClient(DsnRoutedTenantClientBase[PostgresClient], PostgresClientPort):
    """Routes each call to a lazily created :class:`PostgresClient` for the current tenant.

    The tenant is read from ``tenant_provider`` (typically
    :meth:`forze.application.execution.context.ExecutionContext.inv_ctx.get_tenant`).
    DSN strings are loaded via :meth:`SecretsPort.resolve_str` using
    ``secret_ref_for_tenant``.

    Call :meth:`startup` during application startup (see
    :func:`~forze_postgres.execution.lifecycle.pool.routed_postgres_lifecycle_step`)
    before use. Call :meth:`close` on shutdown to drain all per-tenant pools.

    LRU eviction never closes a pool that still has in-flight routed operations:
    evicted tenants move to a draining set until the last in-flight use of that pool
    finishes, then the pool is closed and the tenant slot may be recreated.

    :meth:`_create_client` must not perform routed Postgres calls for the same tenant
    while that tenant's pool is being created (reentrant :meth:`use` raises
    :exc:`~forze.base.errors.exc.internal`).
    """

    secrets: SecretsPort
    """Backend used to resolve connection strings."""

    secret_ref_for_tenant: Callable[[UUID], SecretRef] | Mapping[UUID, SecretRef]
    """Build a :class:`SecretRef` for a tenant's database DSN."""

    tenant_provider: Callable[[], UUID | None]
    """Return the current tenant id (or ``None`` if unauthenticated)."""

    pool_config: PostgresConfig = attrs.field(factory=PostgresConfig)
    """Pool configuration applied to each per-tenant :class:`PostgresClient`."""

    acquire_timeout: timedelta = attrs.field(default=timedelta(seconds=5))
    """Pool checkout timeout passed to each inner client."""

    max_cached_tenants: int = 100
    """Maximum number of tenant pools to retain; LRU eviction closes overflow pools."""

    guarded: bool = attrs.field(default=True, init=False)
    dsn_backend: str = attrs.field(default="database", init=False)
    tenant_required_message: str = attrs.field(
        default="Tenant ID is required for routed Postgres access",
        init=False,
    )

    _gather_sem: asyncio.Semaphore | None = attrs.field(default=None, init=False)

    # ....................... #

    async def startup(self) -> None:
        """Mark the client as ready (idempotent)."""

        cfg = self.pool_config
        lim = (
            cfg.max_concurrent_queries
            if cfg.max_concurrent_queries is not None
            else max(1, cfg.max_size - cfg.pool_headroom)
        )
        self._gather_sem = asyncio.Semaphore(lim)
        await super().startup()

    # ....................... #

    async def close(self) -> None:
        """Close all per-tenant pools and reset startup state."""

        await super().close()
        self._gather_sem = None

    async def initialize_client(self, tenant_id: UUID, creds: str) -> PostgresClient:
        client = PostgresClient()
        await client.initialize(
            creds,
            config=self.pool_config,
            acquire_timeout=self.acquire_timeout,
        )

        return client

    async def health(self) -> tuple[str, bool]:
        async with self._client_scope() as inner:
            return await inner.health()

    # ....................... #

    def is_in_transaction(self) -> bool:
        """Return whether the inner client for the current tenant is in a transaction.

        Best-effort: reads routing dicts without locking and may race with eviction
        or first-time client creation; treat as a hint for diagnostics rather than a
        strict mutex over tenant routing.
        """

        tid = self.tenant_provider()

        if tid is None:
            return False

        client = self._pool.peek(tid)

        if client is None:
            return False

        return client.is_in_transaction()

    # ....................... #

    def query_concurrency_limit(self) -> int:
        tid = self.tenant_provider()

        if tid is not None:
            client = self._pool.peek(tid)

            if client is not None:
                return client.query_concurrency_limit()

        cfg = self.pool_config

        if cfg.max_concurrent_queries is not None:
            return cfg.max_concurrent_queries

        return max(1, cfg.max_size - cfg.pool_headroom)

    # ....................... #

    def gather_concurrency_semaphore(self) -> asyncio.Semaphore:
        if self._gather_sem is None:
            self._pool.require_started()

        if self._gather_sem is None:
            raise exc.internal("Tenant client registry is not started")

        return self._gather_sem

    # ....................... #

    def require_transaction(self) -> None:
        tid = self.tenant_provider()

        if tid is None:
            raise exc.internal("Transactional context is required")

        client = self._pool.peek(tid)

        if client is None:
            raise exc.internal("Transactional context is required")

        client.require_transaction()

    # ....................... #

    @asynccontextmanager
    async def bound_connection(self) -> AsyncGenerator[AsyncConnection]:
        async with self._client_scope() as inner:
            async with inner.bound_connection() as conn:
                yield conn

    # ....................... #

    @asynccontextmanager
    async def transaction(
        self,
        *,
        options: PostgresTransactionOptions | None = None,
    ) -> AsyncGenerator[AsyncConnection]:
        async with self._client_scope() as inner:
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
        async with self._client_scope() as inner:
            if return_rowcount:
                return await inner.execute(query, params, return_rowcount=True)

            await inner.execute(query, params, return_rowcount=False)

            return None

    # ....................... #

    async def execute_many(
        self,
        query: QueryNoTemplate,
        params: Sequence[Params],
    ) -> None:
        async with self._client_scope() as inner:
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
        async with self._client_scope() as inner:
            return await inner.fetch_all(
                query,
                params,
                row_factory=row_factory,
                commit=commit,
            )

    # ....................... #

    async def fetch_all_batched(
        self,
        query: QueryNoTemplate,
        params: Params | None = None,
        *,
        batch_size: int = 2000,
        row_factory: RowFactory = "dict",
        commit: bool = False,
    ) -> AsyncGenerator[list[JsonDict] | list[tuple[Any, ...]]]:
        async with self._client_scope() as inner:
            async for chunk in inner.fetch_all_batched(
                query,
                params,
                batch_size=batch_size,
                row_factory=row_factory,
                commit=commit,
            ):
                yield chunk

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
        async with self._client_scope() as inner:
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
        async with self._client_scope() as inner:
            return await inner.fetch_value(query, params, default=default)
