"""Unit tests for Meilisearch native federation merge."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import FederatedSearchSpec, SearchSpec
from forze_meilisearch.adapters.search.federated import MeilisearchFederatedSearchAdapter

# ----------------------- #


class _Hit(BaseModel):
    id: str
    label: str = ""


def _mem(name: str) -> SearchSpec[_Hit]:
    return SearchSpec(name=name, model_type=_Hit, fields=["label"])


def _leg(name: str, index_uid: str) -> MagicMock:
    leg = MagicMock()
    leg.index_uid = index_uid
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
    params, fed_opts = queries[0]
    assert params.index_uid == "idx_a"
    assert fed_opts == {"weight": 1.0}
