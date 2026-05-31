"""BigQuery client that resolves GCP credentials per tenant via :class:`~forze.application.contracts.secrets.SecretsPort`."""

import tempfile
from datetime import timedelta
from pathlib import Path
from typing import Any, Callable, Mapping, final
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
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze.base.primitives.fingerprint import (
    gcp_credential_dedup_tag,
    stable_fingerprint,
)

from .client import BigQueryClient
from .port import BigQueryClientPort
from .routing_credentials import BigQueryRoutingCredentials
from .value_objects import BigQueryConfig, BigQueryInsertResult, BigQueryQueryResult

# ----------------------- #


def _service_file_for_init(creds: BigQueryRoutingCredentials) -> str | None:
    if creds.service_file is not None:
        return creds.service_file

    if creds.service_account_json is None:
        return None

    fd, path = tempfile.mkstemp(prefix="forze-bq-", suffix=".json")
    Path(path).write_text(creds.service_account_json, encoding="utf-8")

    import os

    os.close(fd)

    return path


@final
@attrs.define(slots=True)
class RoutedBigQueryClient(BigQueryClientPort):
    """Routes each operation to a lazily created :class:`BigQueryClient` for the current tenant.

    Credentials are JSON secrets (see :class:`BigQueryRoutingCredentials`) resolved via
    :func:`~forze.application.contracts.secrets.resolve_structured`.

    Register this instance under :data:`~forze_bigquery.execution.deps.BigQueryClientDepKey` and
    use :func:`~forze_bigquery.execution.lifecycle.routed_bigquery_lifecycle_step` for
    startup/shutdown.

    Do not combine with :func:`~forze_bigquery.execution.lifecycle.bigquery_lifecycle_step` on
    the same registered instance.
    """

    secrets: SecretsPort
    secret_ref_for_tenant: Callable[[UUID], SecretRef] | Mapping[UUID, SecretRef]
    tenant_provider: Callable[[], UUID | None]
    client_config: BigQueryConfig | None = None
    max_cached_tenants: int = 100

    __pool: TenantClientRegistry[BigQueryClient, str] = attrs.field(init=False)

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

    async def _fingerprint_for(self, tenant_id: UUID) -> str:
        creds = await resolve_structured_for_tenant(
            BigQueryRoutingCredentials,
            tenant_id=tenant_id,
            secrets=self.secrets,
            ref_for_tenant=self.secret_ref_for_tenant,
            backend="BigQuery",
        )

        return stable_fingerprint(
            creds.project_id,
            gcp_credential_dedup_tag(
                service_file=creds.service_file,
                service_account_json=creds.service_account_json,
            ),
        )

    # ....................... #

    async def _create_client(self, tid: UUID) -> BigQueryClient:
        creds = await resolve_structured_for_tenant(
            BigQueryRoutingCredentials,
            tenant_id=tid,
            secrets=self.secrets,
            ref_for_tenant=self.secret_ref_for_tenant,
            backend="BigQuery",
        )
        client = BigQueryClient()

        await client.initialize(
            creds.project_id,
            service_file=_service_file_for_init(creds),
            config=self.client_config,
        )

        return client

    # ....................... #

    async def _get_client(self) -> BigQueryClient:
        tenant_id = require_tenant_id(
            self.tenant_provider,
            message="Tenant ID is required for routed BigQuery access",
        )

        await ensure_structured_fingerprint(
            self.__pool.get_fingerprint,
            self.__pool.set_fingerprint,
            tenant_id=tenant_id,
            fingerprint=lambda: self._fingerprint_for(tenant_id),
        )

        return await self.__pool.get(tenant_id)

    # ....................... #

    def _peek_client(self) -> BigQueryClient:
        self.__pool.require_started()

        tenant_id = require_tenant_id(
            self.tenant_provider,
            message="Tenant ID is required for routed BigQuery access",
        )
        inner = self.__pool.peek(tenant_id)

        if inner is None:
            raise exc.internal(
                "Routed BigQuery inner client is not initialized for this tenant; "
                "call an async port method first.",
            )

        return inner

    # ....................... #

    async def health(self) -> tuple[str, bool]:
        inner = await self._get_client()
        return await inner.health()

    async def run_query(
        self,
        sql: str,
        params: BaseModel | None = None,
        *,
        dry_run: bool = False,
        maximum_bytes_billed: int | None = None,
        max_results: int | None = None,
        start_index: int | None = None,
        page_token: str | None = None,
        timeout: timedelta | None = None,
    ) -> BigQueryQueryResult:
        inner = await self._get_client()

        return await inner.run_query(
            sql,
            params,
            dry_run=dry_run,
            maximum_bytes_billed=maximum_bytes_billed,
            max_results=max_results,
            start_index=start_index,
            page_token=page_token,
            timeout=timeout,
        )

    async def run_query_all_pages(
        self,
        sql: str,
        params: BaseModel | None = None,
        *,
        maximum_bytes_billed: int | None = None,
        max_rows: int | None = None,
        timeout: timedelta | None = None,
        fetch_batch_size: int = 2000,
    ) -> list[JsonDict]:
        inner = await self._get_client()

        return await inner.run_query_all_pages(
            sql,
            params,
            maximum_bytes_billed=maximum_bytes_billed,
            max_rows=max_rows,
            timeout=timeout,
            fetch_batch_size=fetch_batch_size,
        )

    async def insert_rows(
        self,
        dataset: str,
        table: str,
        rows: list[JsonDict],
        *,
        insert_id_field: str | None = None,
        timeout: timedelta | None = None,
    ) -> BigQueryInsertResult:
        inner = await self._get_client()

        return await inner.insert_rows(
            dataset,
            table,
            rows,
            insert_id_field=insert_id_field,
            timeout=timeout,
        )

    def table(self, dataset: str, table: str) -> Any:
        return self._peek_client().table(dataset, table)

    def job(self, job_id: str | None = None) -> Any:
        return self._peek_client().job(job_id)
