"""ClickHouse client that resolves connection settings per tenant via :class:`~forze.application.contracts.secrets.SecretsPort`."""

from collections.abc import AsyncGenerator, Callable, Mapping, Sequence
from datetime import timedelta
from typing import cast, final
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.secrets import SecretRef, SecretsPort
from forze.application.contracts.tenancy.routed_client_base import (
    StructuredSecretRoutedTenantClientBase,
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
@attrs.define(slots=True, kw_only=True)
class RoutedClickHouseClient(
    StructuredSecretRoutedTenantClientBase[ClickHouseClient],
    ClickHouseClientPort,
):
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
    creds_type: type[BaseModel] = attrs.field(
        default=ClickHouseRoutingCredentials,
        init=False,
    )
    backend: str = attrs.field(default="ClickHouse", init=False)
    tenant_required_message: str = attrs.field(
        default="Tenant ID is required for routed ClickHouse access",
        init=False,
    )

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

    def credential_fingerprint(self, creds: BaseModel) -> str:
        return routing_fingerprint(cast(ClickHouseRoutingCredentials, creds))

    # ....................... #

    async def initialize_client(
        self,
        tenant_id: UUID,
        creds: ClickHouseRoutingCredentials,
    ) -> ClickHouseClient:
        client = ClickHouseClient()
        await client.initialize(self._merge_config(creds))

        return client

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
        params: BaseModel | JsonDict | None = None,
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

    async def run_query_streamed(
        self,
        sql: str,
        params: BaseModel | JsonDict | None = None,
        *,
        database: str | None = None,
        max_rows: int | None = None,
        timeout: timedelta | None = None,
        fetch_batch_size: int = 2000,
    ) -> AsyncGenerator[Sequence[JsonDict]]:
        inner = await self._get_client()

        async for batch in inner.run_query_streamed(
            sql,
            params,
            database=database,
            max_rows=max_rows,
            timeout=timeout,
            fetch_batch_size=fetch_batch_size,
        ):
            yield batch

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
