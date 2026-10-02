"""How a Meilisearch page's sort meets the index it runs against.

The sort names index attributes: the id is the primary key, and every attribute must be one
the index declares sortable. An index provisioned before the spec set a ``default_sort``, or
managed outside forze, fails loudly with what to do, rather than in the engine's words.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import (
    SearchCommandDepKey,
    SearchManagementDepKey,
    SearchQueryDepKey,
    SearchSpec,
)
from forze.application.execution import Deps
from forze.base.exceptions import CoreException, ExceptionKind
from forze_meilisearch.execution.deps import (
    ConfigurableMeilisearchSearch,
    ConfigurableMeilisearchSearchCommand,
    MeilisearchClientDepKey,
    MeilisearchSearchConfig,
)
from forze_meilisearch.execution.deps.factories import ConfigurableMeilisearchSearchManagement
from tests.support.execution_context import context_from_deps

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


class _Row(BaseModel):
    id: UUID
    title: str
    rank: int = 0


def _ctx(client: Any, config: MeilisearchSearchConfig) -> Any:
    return context_from_deps(
        Deps.plain(
            {
                MeilisearchClientDepKey: client,
                SearchQueryDepKey: ConfigurableMeilisearchSearch(config=config),
                SearchCommandDepKey: ConfigurableMeilisearchSearchCommand(config=config),
                SearchManagementDepKey: ConfigurableMeilisearchSearchManagement(config=config),
            }
        )
    )


async def _seed(ctx: Any, spec: SearchSpec[_Row]) -> list[_Row]:
    rows = [_Row(id=uuid4(), title=f"t{i}", rank=i) for i in range(3)]
    await ctx.search.management(spec).ensure_index()
    await ctx.search.command(spec).upsert(rows)

    return rows


async def test_a_custom_primary_key_is_what_the_id_sorts_by(meilisearch_client: Any) -> None:
    config = MeilisearchSearchConfig(index_uid=f"sw_{uuid4().hex[:10]}", primary_key="doc_id")
    ctx = _ctx(meilisearch_client, config)
    spec = SearchSpec(name="rows", model_type=_Row, fields=["title"], default_sort={"rank": "desc"})
    await _seed(ctx, spec)

    page = await ctx.search.query(spec).search("", None, {"limit": 10})
    by_id = await ctx.search.query(spec).search("", None, {"limit": 10}, {"id": "asc"})

    assert [hit.rank for hit in page.hits] == [2, 1, 0]
    assert [hit.id for hit in by_id.hits] == sorted(hit.id for hit in by_id.hits)


async def test_an_index_the_previous_ensure_index_provisioned_still_sorts(
    meilisearch_client: Any,
) -> None:
    # It declared the primary key sortable, not the ``id`` attribute a document also carries.
    config = MeilisearchSearchConfig(index_uid=f"sw_{uuid4().hex[:10]}", primary_key="doc_id")
    ctx = _ctx(meilisearch_client, config)
    spec = SearchSpec(name="rows", model_type=_Row, fields=["title"], default_sort={"rank": "desc"})
    await _seed(ctx, spec)
    index = meilisearch_client.index(config.index_uid)
    task = await index.update_sortable_attributes(["doc_id", "title", "rank"])
    await meilisearch_client.wait_for_task(task.task_uid)

    page = await ctx.search.query(spec).search("", None, {"limit": 10})

    assert [hit.rank for hit in page.hits] == [2, 1, 0]


async def test_an_index_without_the_default_sort_says_to_reprovision(
    meilisearch_client: Any,
) -> None:
    config = MeilisearchSearchConfig(index_uid=f"sw_{uuid4().hex[:10]}")
    ctx = _ctx(meilisearch_client, config)
    await _seed(ctx, SearchSpec(name="rows", model_type=_Row, fields=["title"]))
    spec = SearchSpec(name="rows", model_type=_Row, fields=["title"], default_sort={"rank": "desc"})

    with pytest.raises(CoreException, match="ensure_index") as caught:
        await ctx.search.query(spec).search("", None, {"limit": 10})

    assert caught.value.kind is ExceptionKind.CONFIGURATION
    assert "rank" in str(caught.value)

    # A query with search text does not sort by the default, so it still answers.
    assert len((await ctx.search.query(spec).search("t0", None, {"limit": 10})).hits) == 1


async def test_a_requests_own_unsortable_sort_stays_the_requests_error(
    meilisearch_client: Any,
) -> None:
    config = MeilisearchSearchConfig(index_uid=f"sw_{uuid4().hex[:10]}")
    ctx = _ctx(meilisearch_client, config)
    spec = SearchSpec(name="rows", model_type=_Row, fields=["title"])
    await _seed(ctx, spec)

    with pytest.raises(Exception) as caught:  # noqa: PT011 — the engine's own error
        await ctx.search.query(spec).search("", None, {"limit": 10}, {"rank": "asc"})

    assert not (isinstance(caught.value, CoreException) and "ensure_index" in str(caught.value))
