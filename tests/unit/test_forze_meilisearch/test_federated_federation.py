"""Unit tests for Meilisearch native federation merge."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import FederatedSearchSpec, SearchSpec
from forze_meilisearch.adapters.search.federated import (
    MeilisearchFederatedSearchAdapter,
    _hit_index_uid,
)

# ----------------------- #


class _Hit(BaseModel):
    id: str
    label: str = ""


def _mem(name: str) -> SearchSpec[_Hit]:
    return SearchSpec(name=name, model_type=_Hit, fields=["label"])


def _leg(name: str, index_uid: str) -> MagicMock:
    leg = MagicMock()
    leg.index_uid = index_uid
    leg._resolved_index_uid = AsyncMock(return_value=index_uid)
    leg.spec = _mem(name)
    leg.field_map = {}
    leg.build_filter = MagicMock(return_value=None)
    leg.from_hit = lambda raw: dict(raw)
    return leg


@pytest.mark.asyncio
async def test_federation_skips_zero_weight_legs() -> None:
    client = MagicMock()
    client.multi_search = AsyncMock(
        return_value=MagicMock(
            hits=[{"id": "1", "label": "x", "_federation": {"indexUid": "idx_a"}}],
            estimated_total_hits=1,
        )
    )

    adapter = MeilisearchFederatedSearchAdapter(
        federated_spec=FederatedSearchSpec(
            name="fed",
            members=(_mem("a"), _mem("b")),
        ),
        legs=(("a", _leg("a", "idx_a")), ("b", _leg("b", "idx_b"))),
        client=client,
        merge="federation",
    )

    page = await adapter.search_page(
        "q",
        options={"member_weights": {"a": 1.0, "b": 0.0}},
    )

    assert page.count == 1
    assert page.hits[0].member == "a"
    queries = client.multi_search.await_args.args[0]
    assert len(queries) == 1
    assert queries[0].index_uid == "idx_a"
    assert queries[0].federation_options is not None
    assert queries[0].federation_options.weight == 1.0


def test_hit_index_uid_reads_federation_metadata() -> None:
    assert _hit_index_uid({"_federation": {"indexUid": "idx_a"}}) == "idx_a"
    assert _hit_index_uid({"_federation": {"index_uid": "idx_b"}}) == "idx_b"
    assert _hit_index_uid({"id": "1"}) is None


@pytest.mark.asyncio
async def test_federation_all_zero_weights_returns_empty() -> None:
    client = MagicMock()
    adapter = MeilisearchFederatedSearchAdapter(
        federated_spec=FederatedSearchSpec(
            name="fed_empty",
            members=(_mem("a"), _mem("b")),
        ),
        legs=(("a", _leg("a", "idx_a")), ("b", _leg("b", "idx_b"))),
        client=client,
        merge="federation",
    )

    page = await adapter.search_page(
        "q",
        options={"member_weights": {"a": 0.0, "b": 0.0}},
        pagination={"offset": 0, "limit": 10},
    )

    assert page.count == 0
    client.multi_search.assert_not_called()


@pytest.mark.asyncio
async def test_federation_resolves_member_via_leg_index_fallback() -> None:
    client = MagicMock()
    client.multi_search = AsyncMock(
        return_value=MagicMock(
            hits=[{"id": "1", "label": "x", "_federation": {"indexUid": "idx_b"}}],
            estimated_total_hits=1,
        )
    )

    adapter = MeilisearchFederatedSearchAdapter(
        federated_spec=FederatedSearchSpec(
            name="fed_resolve",
            members=(_mem("a"), _mem("b")),
        ),
        legs=(("a", _leg("a", "idx_a")), ("b", _leg("b", "idx_b"))),
        client=client,
        merge="federation",
    )

    page = await adapter.search_page("q", options={"member_weights": {"a": 1.0, "b": 1.0}})

    assert page.hits[0].member == "b"
