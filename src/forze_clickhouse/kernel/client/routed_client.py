"""ClickHouse client that resolves connection settings per tenant via :class:`~forze.application.contracts.secrets.SecretsPort`."""

from datetime import timedelta
from typing import Callable, Mapping, final
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.secrets import SecretRef, SecretsPort
from forze.application.contracts.tenancy import (
    TenantClientRegistry,
    ensure_structured_fingerprint,
    require_tenant_id,
    resolve_structured_for_tenant,
)
from forze.base.primitives import JsonDict

from .client import ClickHouseClient
from .port import ClickHouseClientPort
from .routing_credentials import ClickHouseRoutingCredentials, routing_fingerprint
from .value_objects import (
    ClickHouseConfig,
    ClickHouseInsertResult,
    ClickHouseQueryResult,
)

# ----------------------- #


@final
@attrs.define(slots=True)
class RoutedClickHouseClient(ClickHouseClientPort):
    """Routes each operation to a lazily created :class:`ClickHouseClient` for the current tenant.

    Connection settings are JSON secrets (see :class:`ClickHouseRoutingCredentials`) resolved
    via :func:`~forze.application.contracts.secrets.resolve_structured`.

    Register this instance under :data:`~forze_clickhouse.execution.deps.ClickHouseClientDepKey`
    and use :func:`~forze_clickhouse.execution.lifecycle.routed_clickhouse_lifecycle_step` for
    startup/shutdown.

    Do not combine with :func:`~forze_clickhouse.execution.lifecycle.clickhouse_lifecycle_step`
    on the same registered instance.
    """

    secrets: SecretsPort
    secret_ref_for_tenant: Callable[[UUID], SecretRef] | Mapping[UUID, SecretRef]
    tenant_provider: Callable[[], UUID | None]
    connection_defaults: ClickHouseConfig | None = None
    """Optional defaults merged into each tenant secret (tenant fields win)."""

    max_cached_tenants: int = 100

    __pool: TenantClientRegistry[ClickHouseClient, str] = attrs.field(init=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        self.__pool = TenantClientRegistry(
            max_entries=self.max_cached_tenants,
            create=self._create_client,
            dispose=lambda client: client.close(),
            guarded=False,
        )

    # ....................... #

    async def startup(self) -> None:
        await self.__pool.startup()

    # ....................... #

    async def close(self) -> None:
        await self.__pool.close()

    # ....................... #

    async def evict_tenant(self, tenant_id: UUID) -> None:
        await self.__pool.evict(tenant_id)

    # ....................... #

    def _merge_config(self, creds: ClickHouseRoutingCredentials) -> ClickHouseConfig:
        base = self.connection_defaults
        tenant_cfg = creds.to_clickhouse_config()

        if base is None:
            return tenant_cfg

        return ClickHouseConfig(
            host=tenant_cfg.host,
            port=tenant_cfg.port,
            username=tenant_cfg.username,
            password=tenant_cfg.password,
            database=tenant_cfg.database,
            secure=tenant_cfg.secure,
            timeout=base.timeout,
            connector_limit=base.connector_limit,
            connector_limit_per_host=base.connector_limit_per_host,
            keepalive_timeout=base.keepalive_timeout,
            read_retry_attempts=base.read_retry_attempts,
            read_retry_base_delay=base.read_retry_base_delay,
            insert_batch_size=base.insert_batch_size,
            max_append_rows=base.max_append_rows,
        )

    # ....................... #

    async def _fingerprint_for(self, tenant_id: UUID) -> str:
        creds = await resolve_structured_for_tenant(
            ClickHouseRoutingCredentials,
            tenant_id=tenant_id,
            secrets=self.secrets,
            ref_for_tenant=self.secret_ref_for_tenant,
            backend="ClickHouse",
        )

        return routing_fingerprint(creds)

    # ....................... #

    async def _create_client(self, tid: UUID) -> ClickHouseClient:
        creds = await resolve_structured_for_tenant(
            ClickHouseRoutingCredentials,
            tenant_id=tid,
            secrets=self.secrets,
            ref_for_tenant=self.secret_ref_for_tenant,
            backend="ClickHouse",
        )
        client = ClickHouseClient()
        await client.initialize(self._merge_config(creds))

        return client

    # ....................... #

    async def _get_client(self) -> ClickHouseClient:
        tenant_id = require_tenant_id(
            self.tenant_provider,
            message="Tenant ID is required for routed ClickHouse access",
        )

        await ensure_structured_fingerprint(
            self.__pool.get_fingerprint,
            self.__pool.set_fingerprint,
            tenant_id=tenant_id,
            fingerprint=lambda: self._fingerprint_for(tenant_id),
        )

        return await self.__pool.get(tenant_id)

    # ....................... #

    async def health(self) -> tuple[str, bool]:
        inner = await self._get_client()
        return await inner.health()

    async def run_query(
        self,
        sql: str,
        params: BaseModel | JsonDict | None = None,
        *,
        database: str | None = None,
        max_rows: int | None = None,
        limit: int | None = None,
        offset: int | None = None,
        timeout: timedelta | None = None,
    ) -> ClickHouseQueryResult:
        inner = await self._get_client()

        return await inner.run_query(
            sql,
            params,
            database=database,
            max_rows=max_rows,
            limit=limit,
            offset=offset,
            timeout=timeout,
        )

    async def run_query_all_pages(
        self,
        sql: str,
        params: BaseModel | None = None,
        *,
        database: str | None = None,
        max_rows: int | None = None,
        timeout: timedelta | None = None,
        fetch_batch_size: int = 2000,
    ) -> list[JsonDict]:
        inner = await self._get_client()

        return await inner.run_query_all_pages(
            sql,
            params,
            database=database,
            max_rows=max_rows,
            timeout=timeout,
            fetch_batch_size=fetch_batch_size,
        )

    async def insert_rows(
        self,
        database: str,
        table: str,
        rows: list[JsonDict],
        *,
        timeout: timedelta | None = None,
    ) -> ClickHouseInsertResult:
        inner = await self._get_client()

        return await inner.insert_rows(
            database,
            table,
            rows,
            timeout=timeout,
        )

    async def run_command(
        self,
        command: str,
        params: BaseModel | None = None,
        *,
        database: str | None = None,
        timeout: timedelta | None = None,
    ) -> None:
        inner = await self._get_client()

        await inner.run_command(
            command,
            params,
            database=database,
            timeout=timeout,
        )
