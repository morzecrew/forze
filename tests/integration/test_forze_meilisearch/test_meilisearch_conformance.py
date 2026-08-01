"""Meilisearch against the shared search battery.

The odd leg architecturally: Meilisearch owns a *derived* index rather than searching a
system of record, so its corpus arrives through the search command port, and its totals are
estimates bounded by ``maxTotalHits`` rather than a real count. Both facts are declared —
``exact_total_count=False`` relaxes the count assertions — which is exactly why running the
same battery here is worth doing: it checks the declaration is honest.
"""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from pydantic import BaseModel

from forze.application.contracts.search import (
    SearchCommandDepKey,
    SearchManagementDepKey,
    SearchQueryDepKey,
    SearchSpec,
)
from forze.application.execution import Deps
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
from tests.support.search_conformance import (
    SEARCH_BATTERY,
    SEARCH_WRITE_BATTERY,
    Check,
    SearchHarness,
    SearchWriteHarness,
    WriteCheck,
    corpus_rows,
    searchable_fields,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


class _Row(BaseModel):
    id: UUID
    title: str
    content: str
    category: str = ""


@pytest_asyncio.fixture
async def harness(meilisearch_client) -> SearchHarness:
    config = MeilisearchSearchConfig(
        index_uid=f"search_conf_{uuid4().hex[:10]}",
        filterable_attributes=["category"],
        sortable_attributes=["title", "category"],
    )
    ctx = context_from_deps(
        Deps.plain(
            {
                MeilisearchClientDepKey: meilisearch_client,
                SearchQueryDepKey: ConfigurableMeilisearchSearch(config=config),
                SearchCommandDepKey: ConfigurableMeilisearchSearchCommand(config=config),
                SearchManagementDepKey: ConfigurableMeilisearchSearchManagement(config=config),
            }
        )
    )

    spec = SearchSpec(name="rows", model_type=_Row, fields=searchable_fields())

    management = ctx.search.management(spec)
    await management.ensure_index()
    await management.delete_all()
    await ctx.search.command(spec).upsert(
        [_Row(**row) for row in corpus_rows(uuid4)],
    )

    return SearchHarness(
        query=ctx.search.query(spec),
        backend="meili",
        blank_query_matches_all=True,
    )


@pytest.mark.conformance(plane="search", engine="meilisearch")
@pytest.mark.parametrize("check", SEARCH_BATTERY, ids=lambda check: check.__name__)
async def test_search_battery(check: Check, harness: SearchHarness) -> None:
    await check(harness)


# ....................... #


@pytest.fixture
def write_harness(meilisearch_client) -> SearchWriteHarness:
    # Deliberately *not* provisioned here: the first write check is that wiping an index
    # nothing has created yet succeeds, which a provisioning fixture would hide.
    config = MeilisearchSearchConfig(index_uid=f"write_conf_{uuid4().hex[:10]}")
    ctx = context_from_deps(
        Deps.plain(
            {
                MeilisearchClientDepKey: meilisearch_client,
                SearchQueryDepKey: ConfigurableMeilisearchSearch(config=config),
                SearchCommandDepKey: ConfigurableMeilisearchSearchCommand(config=config),
                SearchManagementDepKey: ConfigurableMeilisearchSearchManagement(config=config),
            }
        )
    )
    spec = SearchSpec(name="rows", model_type=_Row, fields=searchable_fields())

    return SearchWriteHarness(
        command=ctx.search.command(spec),
        management=ctx.search.management(spec),
        query=ctx.search.query(spec),
        backend="meili",
        new_row=lambda title: _Row(id=uuid4(), title=title, content="python"),
    )


@pytest.mark.conformance(plane="search_write", engine="meilisearch")
@pytest.mark.parametrize("check", SEARCH_WRITE_BATTERY, ids=lambda check: check.__name__)
async def test_search_write_battery(check: WriteCheck, write_harness: SearchWriteHarness) -> None:
    await check(write_harness)
