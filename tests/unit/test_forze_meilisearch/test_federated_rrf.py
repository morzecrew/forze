"""Unit tests for Meilisearch RRF federated merge."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import BaseModel

from forze.application.contracts.base import page_from_limit_offset
from forze.application.contracts.search import FederatedSearchSpec, SearchSpec
from forze_meilisearch.adapters.search.federated import MeilisearchFederatedSearchAdapter

# ----------------------- #


class _Hit(BaseModel):
    id: str
    label: str = ""


def _mem(name: str) -> SearchSpec[_Hit]:
    return SearchSpec(name=name, model_type=_Hit, fields=["label"])


@pytest.mark.asyncio
async def test_rrf_merge_calls_each_leg_search() -> None:
    h1 = _Hit(id="1", label="alpha")
    h2 = _Hit(id="2", label="beta")

    leg_a = MagicMock()
    leg_a.index_uid = "idx_a"
    leg_a.spec = _mem("a")
    leg_a.search = AsyncMock(
        return_value=page_from_limit_offset([h1, h2], {"offset": 0, "limit": 10}, total=None)
    )

    leg_b = MagicMock()
    leg_b.index_uid = "idx_b"
    leg_b.spec = _mem("b")
    leg_b.search = AsyncMock(
        return_value=page_from_limit_offset([h2, h1], {"offset": 0, "limit": 10}, total=None)
    )

    adapter = MeilisearchFederatedSearchAdapter(
        federated_spec=FederatedSearchSpec(
            name="fed_rrf",
            members=(_mem("a"), _mem("b")),
        ),
        legs=(("a", leg_a), ("b", leg_b)),
        client=MagicMock(),
        merge="rrf",
        rrf_k=60,
        rrf_per_leg_limit=100,
    )

    page = await adapter.search_page("alpha", pagination={"offset": 0, "limit": 5})

    leg_a.search.assert_awaited_once()
    leg_b.search.assert_awaited_once()
    assert len(page.hits) >= 1
    members = {row.member for row in page.hits}
    assert members <= {"a", "b"}
