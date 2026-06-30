"""Unit tests for Meilisearch RRF federated merge."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import BaseModel

from forze.application.contracts.base import page_from_limit_offset
from forze.application.contracts.search import (
    FederatedSearchSpec,
    SearchResultSnapshotMeta,
    SearchResultSnapshotSpec,
    SearchSpec,
)
from forze.application.integrations.search import SearchResultSnapshot
from forze_meilisearch.adapters.search.federated import MeilisearchFederatedSearchAdapter
from forze_mock.adapters.search.snapshot import MockSearchResultSnapshotAdapter
from forze_mock.state import MockState

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


@pytest.mark.asyncio
async def test_rrf_all_zero_weights_returns_empty() -> None:
    leg = MagicMock()
    leg.spec = _mem("a")
    leg.search = AsyncMock()

    leg_b = MagicMock()
    leg_b.spec = _mem("b")
    leg_b.search = AsyncMock()

    adapter = MeilisearchFederatedSearchAdapter(
        federated_spec=FederatedSearchSpec(
            name="fed_rrf_empty",
            members=(_mem("a"), _mem("b")),
        ),
        legs=(("a", leg), ("b", leg_b)),
        client=MagicMock(),
        merge="rrf",
    )

    page = await adapter.search_page(
        "q",
        options={"member_weights": {"a": 0.0, "b": 0.0}},
        pagination={"offset": 0, "limit": 5},
    )

    assert page.count == 0
    leg.search.assert_not_called()
    leg_b.search.assert_not_called()


# ----------------------- #
# Shared RRF leg helpers


def _rrf_leg(name: str, hits: list[_Hit]) -> MagicMock:
    leg = MagicMock()
    leg.spec = _mem(name)
    leg.search = AsyncMock(
        return_value=page_from_limit_offset(
            hits, {"offset": 0, "limit": 10}, total=None
        )
    )
    return leg


def _rrf_adapter(
    leg_a: MagicMock,
    leg_b: MagicMock,
    *,
    name: str = "fed_rrf",
    snapshot: SearchResultSnapshotSpec | None = None,
    result_snapshot: SearchResultSnapshot | None = None,
    rrf_per_leg_limit: int = 100,
) -> MeilisearchFederatedSearchAdapter:
    return MeilisearchFederatedSearchAdapter(
        federated_spec=FederatedSearchSpec(
            name=name,
            members=(_mem("a"), _mem("b")),
            snapshot=snapshot,
        ),
        legs=(("a", leg_a), ("b", leg_b)),
        client=MagicMock(),
        merge="rrf",
        rrf_k=60,
        rrf_per_leg_limit=rrf_per_leg_limit,
        result_snapshot=result_snapshot,
    )


# ----------------------- #
# RRF countless empty path (search, no count) with all-zero weights


@pytest.mark.asyncio
async def test_rrf_all_zero_weights_countless_returns_empty() -> None:
    leg_a = _rrf_leg("a", [])
    leg_b = _rrf_leg("b", [])
    adapter = _rrf_adapter(leg_a, leg_b, name="fed_rrf_countless")

    page = await adapter.search("q", options={"member_weights": {"a": 0.0, "b": 0.0}})

    assert list(page.hits) == []
    leg_a.search.assert_not_called()
    leg_b.search.assert_not_called()


# ----------------------- #
# RRF merge offset/limit windowing over merged results


@pytest.mark.asyncio
async def test_rrf_countless_search_returns_merged_hits_without_count() -> None:
    a = [_Hit(id="1", label="alpha")]
    b = [_Hit(id="2", label="beta")]
    adapter = _rrf_adapter(_rrf_leg("a", a), _rrf_leg("b", b))

    page = await adapter.search("q", pagination={"offset": 0, "limit": 10})

    assert {row.member for row in page.hits} == {"a", "b"}
    assert page.snapshot is None


@pytest.mark.asyncio
async def test_rrf_merge_applies_offset_and_limit() -> None:
    a = [_Hit(id=str(i), label=f"a{i}") for i in range(4)]
    b = [_Hit(id=str(10 + i), label=f"b{i}") for i in range(4)]
    adapter = _rrf_adapter(_rrf_leg("a", a), _rrf_leg("b", b))

    page = await adapter.search_page("q", pagination={"offset": 2, "limit": 3})

    # 8 distinct docs across legs; total reflects full merged pool, window is sliced.
    assert page.count == 8
    assert len(page.hits) == 3


# ----------------------- #
# RRF secondary sorts re-rank the merged window


@pytest.mark.parametrize(
    ("direction", "expected_first"),
    [("desc", "zzz"), ("asc", "aaa")],
)
@pytest.mark.asyncio
async def test_rrf_merge_secondary_sort_breaks_score_ties(
    direction: str,
    expected_first: str,
) -> None:
    # Single rank-1 hit per leg => equal RRF scores; the field ``sorts`` breaks the tie.
    a = [_Hit(id="1", label="aaa")]
    b = [_Hit(id="2", label="zzz")]
    adapter = _rrf_adapter(_rrf_leg("a", a), _rrf_leg("b", b))

    page = await adapter.search_page(
        "q",
        sorts={"label": direction},
        pagination={"offset": 0, "limit": 10},
    )

    labels = [row.hit.label for row in page.hits]
    assert labels[0] == expected_first


# ----------------------- #
# RRF select_search (return_type) projection path through _finalize_page


class _Projected(BaseModel):
    hit: _Hit
    member: str


@pytest.mark.asyncio
async def test_rrf_select_search_decodes_return_type() -> None:
    a = [_Hit(id="1", label="alpha")]
    b = [_Hit(id="2", label="beta")]
    adapter = _rrf_adapter(_rrf_leg("a", a), _rrf_leg("b", b))

    page = await adapter.select_search(
        _Projected,
        "q",
        pagination={"offset": 0, "limit": 10},
    )

    assert {row.member for row in page.hits} == {"a", "b"}
    assert all(isinstance(row, _Projected) for row in page.hits)


@pytest.mark.asyncio
async def test_rrf_select_search_page_returns_count() -> None:
    a = [_Hit(id="1", label="alpha")]
    b = [_Hit(id="2", label="beta")]
    adapter = _rrf_adapter(_rrf_leg("a", a), _rrf_leg("b", b))

    page = await adapter.select_search_page(
        _Projected,
        "q",
        pagination={"offset": 0, "limit": 10},
    )

    assert page.count == 2
    assert all(isinstance(row, _Projected) for row in page.hits)


# ----------------------- #
# RRF snapshot read short-circuit and write paths


def _snap_spec() -> SearchResultSnapshotSpec:
    return SearchResultSnapshotSpec(name="snap", enabled=True, max_ids=100)


@pytest.mark.asyncio
async def test_rrf_snapshot_read_short_circuits() -> None:
    hit = _Hit(id="9", label="stored")
    key = SearchResultSnapshot.federated_record_key_string("a", hit)

    store = MagicMock()
    store.get_id_range = AsyncMock(return_value=[key])
    store.get_meta = AsyncMock(
        return_value=SearchResultSnapshotMeta(
            run_id="run-1",
            fingerprint="fp",
            total=1,
            chunk_size=10,
            complete=True,
        )
    )
    result_snapshot = SearchResultSnapshot(store=store)

    leg_a = _rrf_leg("a", [_Hit(id="1", label="x")])
    leg_b = _rrf_leg("b", [_Hit(id="2", label="y")])
    adapter = _rrf_adapter(
        leg_a,
        leg_b,
        name="fed_rrf_snap_read",
        snapshot=_snap_spec(),
        result_snapshot=result_snapshot,
    )

    page = await adapter.search_page("q", snapshot={"id": "run-1"})

    assert page.count == 1
    assert page.hits[0].hit.id == "9"
    leg_a.search.assert_not_called()
    leg_b.search.assert_not_called()


@pytest.mark.asyncio
async def test_rrf_snapshot_read_miss_then_merges_and_writes() -> None:
    rs_spec = _snap_spec()
    store = MockSearchResultSnapshotAdapter(state=MockState(), spec=rs_spec)
    result_snapshot = SearchResultSnapshot(store=store)

    a = [_Hit(id="1", label="alpha")]
    b = [_Hit(id="2", label="beta")]
    adapter = _rrf_adapter(
        _rrf_leg("a", a),
        _rrf_leg("b", b),
        name="fed_rrf_snap_write",
        snapshot=rs_spec,
        result_snapshot=result_snapshot,
    )

    page = await adapter.search_page("q", snapshot={"id": "missing", "mode": True})

    assert page.snapshot is not None
    assert page.snapshot.total == 2
    # The merged keys were streamed into the store and replay can serve them.
    stored = await store.get_id_range(
        page.snapshot.id, 0, 10, expected_fingerprint=page.snapshot.fingerprint
    )
    assert stored is not None
    assert len(stored) == 2
