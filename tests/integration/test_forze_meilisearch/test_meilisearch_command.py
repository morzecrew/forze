"""Integration tests for MeilisearchSearchCommandPort."""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import (
    SearchCommandDepKey,
    SearchManagementDepKey,
    SearchSpec,
)
from forze.application.execution import Deps
from forze_meilisearch.execution.deps import MeilisearchClientDepKey
from forze_meilisearch.execution.deps import MeilisearchSearchConfig
from forze_meilisearch.execution.deps import ConfigurableMeilisearchSearchCommand
from forze_meilisearch.execution.deps.factories import (
    ConfigurableMeilisearchSearchManagement,
)
from tests.support.execution_context import context_from_deps

# ----------------------- #


class Item(BaseModel):
    id: str
    title: str


@pytest.mark.integration
@pytest.mark.asyncio
async def test_command_delete_round_trip(meilisearch_client) -> None:
    index_uid = "cmd_it"
    spec = SearchSpec(name="items", model_type=Item, fields=["title"])
    ctx = context_from_deps(Deps.plain(
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
