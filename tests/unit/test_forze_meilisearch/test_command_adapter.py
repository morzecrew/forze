"""Unit tests for MeilisearchSearchCommandAdapter."""

from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import SearchSpec
from forze_meilisearch.adapters.search._command import MeilisearchSearchCommandAdapter

# ----------------------- #


class _Doc(BaseModel):
    id: str
    title: str


@pytest.mark.asyncio
async def test_upsert_calls_add_documents() -> None:
    client = MagicMock()
    index = MagicMock()
    task = MagicMock(task_uid=1)
    index.add_documents = AsyncMock(return_value=task)
    index.update_settings = AsyncMock(return_value=task)
    client.get_or_create_index = AsyncMock(return_value=index)
    client.wait_for_task = AsyncMock()
    client.index = MagicMock(return_value=index)

    spec = SearchSpec(name="items", model_type=_Doc, fields=["title"])
    adapter = MeilisearchSearchCommandAdapter(
        spec=spec,
        config={"index_uid": "items_idx", "wait_for_tasks": False},
        client=client,
    )

    await adapter.upsert([_Doc(id="1", title="A")])

    index.add_documents.assert_awaited_once()
    client.wait_for_task.assert_not_awaited()
