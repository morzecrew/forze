"""BigQuery client that resolves GCP credentials per tenant via :class:`~forze.application.contracts.secrets.SecretsPort`."""

from datetime import timedelta
from typing import Any, Callable, Mapping, cast, final
from uuid import UUID

import attrs
from forze.application.contracts.secrets import SecretRef, SecretsPort
from forze.application.contracts.tenancy import require_tenant_id
from forze.application.contracts.tenancy.routed_client_base import (
    StructuredSecretRoutedTenantClientBase,
)
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from pydantic import BaseModel

from .client import BigQueryClient
from .port import BigQueryClientPort
from .routing_credentials import (
    BigQueryRoutingCredentials,
    credential_file_for_init,
    routing_fingerprint,
)
from .value_objects import BigQueryConfig, BigQueryInsertResult, BigQueryQueryResult

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True)
class RoutedBigQueryClient(
    StructuredSecretRoutedTenantClientBase[BigQueryClient],
    BigQueryClientPort,
):
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
    creds_type: type[BaseModel] = attrs.field(
        default=BigQueryRoutingCredentials,
        init=False,
    )
    backend: str = attrs.field(default="BigQuery", init=False)
    credential_file_prefix: str = attrs.field(default="forze-bq-", init=False)
    tenant_required_message: str = attrs.field(
        default="Tenant ID is required for routed BigQuery access",
        init=False,
    )

    def credential_fingerprint(self, creds: BaseModel) -> str:
        return routing_fingerprint(cast(BigQueryRoutingCredentials, creds))

    async def initialize_client(
        self,
        tenant_id: UUID,
        creds: BigQueryRoutingCredentials,
    ) -> BigQueryClient:
        client = BigQueryClient()
        credential_path = credential_file_for_init(
            creds,
            prefix=self.credential_file_prefix,
        )

        await client.initialize(
            creds.project_id,
            service_file=credential_path.path,
            service_file_owned=credential_path.owned,
            config=self.client_config,
        )

        return client

    def _peek_client(self, tenant_id: UUID | None = None) -> BigQueryClient:  # type: ignore[override]
        self._pool.require_started()

        if tenant_id is None:
            tenant_id = require_tenant_id(
                self.tenant_provider,
                message="Tenant ID is required for routed BigQuery access",
            )
        inner = self._pool.peek(tenant_id)

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
