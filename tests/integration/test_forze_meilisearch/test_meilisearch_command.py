"""Integration tests for MeilisearchSearchCommandPort."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import (
    SearchCommandDepKey,
    SearchManagementDepKey,
    SearchQueryDepKey,
    SearchSpec,
)
from forze.application.execution import Deps
from forze.domain.models import ReadDocument
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

# ----------------------- #


class Item(BaseModel):
    id: str
    title: str


class GadgetRead(ReadDocument):
    name: str


@pytest.mark.integration
@pytest.mark.asyncio
async def test_command_delete_round_trip(meilisearch_client) -> None:
    index_uid = "cmd_it"
    spec = SearchSpec(name="items", model_type=Item, fields=["title"])
    ctx = context_from_deps(
        Deps.plain(
            {
                MeilisearchClientDepKey: meilisearch_client,
                SearchCommandDepKey: ConfigurableMeilisearchSearchCommand(
                    config=MeilisearchSearchConfig(index_uid=index_uid),
                ),
                SearchManagementDepKey: ConfigurableMeilisearchSearchManagement(
                    config=MeilisearchSearchConfig(index_uid=index_uid),
                ),
            }
        )
    )

    cmd = ctx.search.command(spec)
    mgmt = ctx.search.management(spec)
    await mgmt.ensure_index()
    await mgmt.delete_all()
    await cmd.upsert([Item(id="a", title="one"), Item(id="b", title="two")])
    await cmd.delete(["a"])
    await mgmt.delete_all()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_upsert_of_a_standard_read_document(meilisearch_client) -> None:
    """The framework's own document read model — ``UUID`` id, ``datetime`` timestamps.

    This is what ``bind_search_sync`` feeds the index on every committed write, and what a
    rebuild sweep feeds it in bulk, so it is the *primary* shape this adapter sees. It is
    also the one shape no test here used to cover: every other model in this suite declares
    ``id: str``, and the mock search adapter stores Python objects in a dict with no JSON
    round-trip — so a mapping full of ``UUID`` / ``datetime`` objects passed both, and only
    a real index (whose SDK calls ``json.dumps`` on it) rejected them.
    """

    index_uid = "read_document_it"
    spec = SearchSpec(name="gadgets", model_type=GadgetRead, fields=["name"])
    config = MeilisearchSearchConfig(index_uid=index_uid, wait_for_tasks=True)
    ctx = context_from_deps(
        Deps.plain(
            {
                MeilisearchClientDepKey: meilisearch_client,
                SearchCommandDepKey: ConfigurableMeilisearchSearchCommand(config=config),
                SearchManagementDepKey: ConfigurableMeilisearchSearchManagement(config=config),
                SearchQueryDepKey: ConfigurableMeilisearchSearch(config=config),
            }
        )
    )

    mgmt = ctx.search.management(spec)
    await mgmt.ensure_index()
    await mgmt.delete_all()

    now = datetime.now(UTC)
    row = GadgetRead(id=uuid4(), rev=1, created_at=now, last_update_at=now, name="alpha")

    await ctx.search.command(spec).upsert([row])

    page = await ctx.search.query(spec).search("alpha", pagination={"offset": 0, "limit": 10})

    # Round-trips as the model, not as whatever JSON shape survived the wire.
    assert [(hit.id, hit.name) for hit in page.hits] == [(row.id, "alpha")]

    await mgmt.delete_all()
