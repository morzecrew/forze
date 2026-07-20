"""Integration tests for :class:`~forze_bigquery.kernel.client.RoutedBigQueryClient`."""

from __future__ import annotations

from unittest.mock import patch
from uuid import uuid4

import pytest
from pydantic import BaseModel

pytest.importorskip("gcloud.aio.bigquery")

from forze.application.contracts.analytics import (
    AnalyticsQueryDefinition,
    AnalyticsSpec,
    IngestSpec,
)
from forze.application.contracts.base import CountlessPage
from forze.application.contracts.secrets import SecretRef
from forze.base.exceptions import CoreException
from forze_bigquery.execution import BigQueryAnalyticsConfig, BigQueryDepsModule
from forze_bigquery.execution.deps.configs import BigQueryQueryConfig
from forze_bigquery.kernel.client import BigQueryClient, RoutedBigQueryClient
from tests.support.execution_context import context_from_deps
from tests.support.secrets_fixtures import (
    MemSecretsByPath,
    MemSecretsTenantJson,
    tenant_holder,
    tenant_secret_ref,
)

_BQ_PROJECT = "test"
_BQ_SUFFIX = "bigquery"


def _payload(*, project_id: str = _BQ_PROJECT) -> dict[str, str]:
    return {"project_id": project_id}


def _ref(tenant_id) -> SecretRef:
    return tenant_secret_ref(tenant_id, _BQ_SUFFIX)


def _tenant_json(
    payloads: dict,
    *,
    missing_tenant=None,
    broken_tenant=None,
) -> MemSecretsTenantJson:
    return MemSecretsTenantJson(
        resource_suffix=_BQ_SUFFIX,
        payloads_by_tenant=payloads,
        missing_tenant=missing_tenant,
        broken_tenant=broken_tenant,
    )


class _Row(BaseModel):
    event: str
    value: int


class _Params(BaseModel):
    pass


class _Ingest(BaseModel):
    event: str
    value: int = 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_bigquery_query_and_insert(
    bigquery_emulator_host: str,
    analytics_dataset,
) -> None:
    _ = bigquery_emulator_host
    dataset_id, table_id = analytics_dataset
    t1 = uuid4()
    secrets = _tenant_json({t1: _payload()})
    tenant_get, tenant_set = tenant_holder()

    routed = RoutedBigQueryClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        max_cached_tenants=4,
    )
    tenant_set(t1)
    await routed.startup()
    try:
        assert (await routed.health())[1] is True

        sql = f"SELECT event, value FROM `{_BQ_PROJECT}.{dataset_id}.{table_id}`"
        await routed.insert_rows(
            dataset_id,
            table_id,
            [{"event": "routed", "value": 7}],
        )
        result = await routed.run_query(sql, _Params(), max_results=10)
        assert any(row.get("event") == "routed" for row in result.rows)

        all_rows = await routed.run_query_all_pages(
            sql,
            _Params(),
            max_rows=20,
            fetch_batch_size=5,
        )
        assert len(all_rows) >= 1

        table = routed.table(dataset_id, table_id)
        assert table is not None
        job = routed.job()
        assert job is not None
    finally:
        await routed.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_bigquery_analytics_deps_module(
    bigquery_emulator_host: str,
    analytics_dataset,
) -> None:
    _ = bigquery_emulator_host
    dataset_id, table_id = analytics_dataset
    t1 = uuid4()
    secrets = _tenant_json({t1: _payload()})
    tenant_get, tenant_set = tenant_holder()

    routed = RoutedBigQueryClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        max_cached_tenants=4,
    )
    tenant_set(t1)
    await routed.startup()
    try:
        sql = f"SELECT event, value FROM `{_BQ_PROJECT}.{dataset_id}.{table_id}`"
        spec = AnalyticsSpec(
            name="events",
            read=_Row,
            queries={"all": AnalyticsQueryDefinition(params=_Params)},
            ingest=_Ingest,
        )
        config = BigQueryAnalyticsConfig(
            dataset=dataset_id,
            queries={"all": BigQueryQueryConfig(sql=sql, skip_total=True)},
            ingest=IngestSpec((dataset_id, table_id)),
        )
        ctx = context_from_deps(
            BigQueryDepsModule(client=routed, analytics={"events": config})(),
        )
        await ctx.analytics.ingest(spec).append([_Ingest(event="deps-routed", value=3)])
        page = await ctx.analytics.query(spec).run_page(
            "all",
            _Params(),
            pagination={"limit": 5, "offset": 0},
        )
        assert isinstance(page, CountlessPage)
        assert any(hit.event == "deps-routed" for hit in page.hits)
    finally:
        await routed.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_bigquery_startup_and_tenant_guards(bigquery_emulator_host: str) -> None:
    _ = bigquery_emulator_host
    t1 = uuid4()
    secrets = _tenant_json({t1: _payload()})
    tenant_get, tenant_set = tenant_holder()

    routed = RoutedBigQueryClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        max_cached_tenants=4,
    )
    tenant_set(t1)
    with pytest.raises(CoreException, match="not started"):
        await routed.health()

    await routed.startup()
    try:
        tenant_set(None)
        with pytest.raises(CoreException, match="Tenant ID"):
            await routed.health()
    finally:
        await routed.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_bigquery_invalid_json(bigquery_emulator_host: str) -> None:
    _ = bigquery_emulator_host
    t1 = uuid4()
    secrets = MemSecretsByPath({f"tenants/{t1}/{_BQ_SUFFIX}": "{bad"})
    tenant_get, tenant_set = tenant_holder()

    routed = RoutedBigQueryClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        max_cached_tenants=4,
    )
    tenant_set(t1)
    await routed.startup()
    try:
        with pytest.raises(CoreException, match="BigQueryRoutingCredentials"):
            await routed.health()
    finally:
        await routed.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_bigquery_lru_and_evict(bigquery_emulator_host: str) -> None:
    _ = bigquery_emulator_host
    t1, t2, t3 = uuid4(), uuid4(), uuid4()
    secrets = _tenant_json(
        {
            t1: _payload(project_id="test-a"),
            t2: _payload(project_id="test-b"),
            t3: _payload(project_id="test-c"),
        },
    )
    tenant_get, tenant_set = tenant_holder()

    routed = RoutedBigQueryClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        max_cached_tenants=2,
    )
    await routed.startup()
    closes: list[int] = []
    real_close = BigQueryClient.close

    async def counting_close(self: BigQueryClient) -> None:
        closes.append(1)
        await real_close(self)

    try:
        with patch.object(BigQueryClient, "close", counting_close):
            tenant_set(t1)
            await routed.health()
            tenant_set(t2)
            await routed.health()
            tenant_set(t3)
            await routed.health()
            assert sum(closes) >= 1

        await routed.evict_tenant(t1)
    finally:
        await routed.close()
