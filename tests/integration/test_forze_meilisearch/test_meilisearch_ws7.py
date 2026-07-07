"""Integration coverage for WS7: maxTotalHits provisioning + exact page-mode count."""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import (
    SearchCommandDepKey,
    SearchManagementDepKey,
    SearchQueryDepKey,
    SearchSpec,
)
from forze.application.execution import Deps, ExecutionContext
from forze_meilisearch.execution.deps import (
    ConfigurableMeilisearchSearch,
    ConfigurableMeilisearchSearchCommand,
    MeilisearchClientDepKey,
    MeilisearchSearchConfig,
)
from forze_meilisearch.execution.deps.factories import (
    ConfigurableMeilisearchSearchManagement,
)
from tests.support.execution_context import context_from_deps

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


class Article(BaseModel):
    id: str
    title: str


def _ctx(meilisearch_client, *, index_uid: str, max_total_hits: int) -> ExecutionContext:
    config = MeilisearchSearchConfig(
        index_uid=index_uid,
        max_total_hits=max_total_hits,
        exact_total_count=True,
    )
    return context_from_deps(
        Deps.plain(
            {
                MeilisearchClientDepKey: meilisearch_client,
                SearchQueryDepKey: ConfigurableMeilisearchSearch(config=config),
                SearchCommandDepKey: ConfigurableMeilisearchSearchCommand(config=config),
                SearchManagementDepKey: ConfigurableMeilisearchSearchManagement(config=config),
            }
        )
    )


async def test_ensure_index_provisions_max_total_hits(meilisearch_client) -> None:
    index_uid = "ws7_prov"
    ctx = _ctx(meilisearch_client, index_uid=index_uid, max_total_hits=2500)
    spec = SearchSpec(name="ws7", model_type=Article, fields=["title"])

    await ctx.search.management(spec).ensure_index()

    settings = await meilisearch_client.index(index_uid).get_settings()
    assert settings.pagination.max_total_hits == 2500


async def test_exact_count_returns_correct_total(meilisearch_client) -> None:
    index_uid = "ws7_exact"
    ctx = _ctx(meilisearch_client, index_uid=index_uid, max_total_hits=1000)
    spec = SearchSpec(name="ws7", model_type=Article, fields=["title"])

    mgmt = ctx.search.management(spec)
    await mgmt.ensure_index()
    await mgmt.delete_all()
    await ctx.search.command(spec).upsert(
        [Article(id=str(i), title="widget") for i in range(7)]
    )

    adapter = ctx.search.query(spec)
    assert adapter.search_capabilities.exact_total_count is True

    page = await adapter.search_page("widget")
    assert page.count == 7  # exact page-mode totalHits
