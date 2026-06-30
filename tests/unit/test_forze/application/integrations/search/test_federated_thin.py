"""Late-materialized (thin) federated RRF: merge, eligibility, and executor parity."""

from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable, Sequence

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import FederatedSearchSpec, SearchSpec
from forze.application.integrations.search import (
    SearchResultSnapshot,
    execute_federated_thin_offset,
    federated_thin_eligible,
)
from forze_mock.adapters.search.command import MockSearchCommandAdapter
from forze_mock.adapters.search.federated import MockFederatedSearchAdapter
from forze_mock.adapters.search.query import MockSearchAdapter
from forze_mock.state import MockState

# ----------------------- #


class _Item(BaseModel):
    id: str
    title: str


class _NoId(BaseModel):
    slug: str
    title: str


class _Out(BaseModel):
    hit: _Item
    member: str


async def _gather(makers: Sequence[Callable[[], Awaitable[Any]]]) -> list[Any]:
    return list(await asyncio.gather(*(maker() for maker in makers)))


# --- weighted_rrf_merge_ids ------------------------------------------------ #


def test_merge_ids_fuses_top_ranks_highest() -> None:
    merged = SearchResultSnapshot.weighted_rrf_merge_ids(
        leg_rows=[("a", ["x", "y"], 1.0), ("b", ["x", "z"], 1.0)],
        k=60,
    )

    keys = [(m, i) for m, i, _ in merged]
    # Each (member, id) is a distinct identity; the two rank-1 hits score highest.
    assert set(keys) == {("a", "x"), ("a", "y"), ("b", "x"), ("b", "z")}
    assert {(m, i) for m, i, _ in merged[:2]} == {("a", "x"), ("b", "x")}


def test_merge_ids_skips_nonpositive_weight() -> None:
    merged = SearchResultSnapshot.weighted_rrf_merge_ids(
        leg_rows=[("a", ["x"], 0.0), ("b", ["y"], 1.0)],
        k=60,
    )

    assert [(m, i) for m, i, _ in merged] == [("b", "y")]


# --- eligibility ----------------------------------------------------------- #


def test_eligibility_gates() -> None:
    members = [
        SearchSpec(name="a", model_type=_Item, fields=["title"]),
        SearchSpec(name="b", model_type=_Item, fields=["title"]),
    ]
    base: dict[str, Any] = {
        "members": members,
        "wants_highlights": False,
        "sorts": None,
        "snapshot_write": False,
    }

    assert federated_thin_eligible(thin_merge=True, **base)
    assert not federated_thin_eligible(thin_merge=False, **base)
    assert not federated_thin_eligible(
        thin_merge=True, **{**base, "wants_highlights": True}
    )
    assert not federated_thin_eligible(
        thin_merge=True, **{**base, "sorts": {"title": "asc"}}
    )
    assert not federated_thin_eligible(
        thin_merge=True, **{**base, "snapshot_write": True}
    )

    no_id = [
        SearchSpec(
            name="a",
            model_type=_NoId,
            fields=["title"],
            default_sort={"slug": "asc"},
        ),
        SearchSpec(
            name="b",
            model_type=_NoId,
            fields=["title"],
            default_sort={"slug": "asc"},
        ),
    ]
    assert not federated_thin_eligible(
        thin_merge=True,
        members=no_id,
        wants_highlights=False,
        sorts=None,
        snapshot_write=False,
    )


# --- executor parity ------------------------------------------------------- #


async def _two_legs(state: MockState) -> tuple[SearchSpec[_Item], SearchSpec[_Item]]:
    leg_a = SearchSpec(name="a", model_type=_Item, fields=["title"])
    leg_b = SearchSpec(name="b", model_type=_Item, fields=["title"])
    await MockSearchCommandAdapter(state=state, spec=leg_a).upsert(
        [
            _Item(id="1", title="alpha shared"),
            _Item(id="2", title="alpha only"),
        ]
    )
    await MockSearchCommandAdapter(state=state, spec=leg_b).upsert(
        [
            _Item(id="1", title="alpha shared"),
            _Item(id="3", title="alpha extra"),
        ]
    )
    return leg_a, leg_b


def _idents(page: Any) -> list[tuple[str, str]]:
    return sorted((h.member, h.hit.id) for h in page.hits)


@pytest.mark.asyncio
async def test_thin_executor_matches_full_merge() -> None:
    state = MockState()
    leg_a, leg_b = await _two_legs(state)
    ports = [
        ("a", MockSearchAdapter(state=state, spec=leg_a)),
        ("b", MockSearchAdapter(state=state, spec=leg_b)),
    ]

    full = MockFederatedSearchAdapter(
        federated_spec=FederatedSearchSpec(name="fed", members=[leg_a, leg_b]),
        legs=ports,
    )
    full_page = await full.search_page("alpha", pagination={"limit": 10})

    active = [("a", ports[0][1], 1.0), ("b", ports[1][1], 1.0)]
    thin_page = await execute_federated_thin_offset(
        legs=active,
        query="alpha",
        filters=None,
        pagination={"limit": 10},
        leg_opts=None,
        rrf_k=60,
        per_leg_limit=5000,
        return_count=True,
        return_type=None,
        run_legs=_gather,
    )

    # Same identities and same total as the full-fetch merge.
    assert _idents(thin_page) == _idents(full_page)
    assert thin_page.count == full_page.count
    # The shared doc "1" appears once per member (distinct federated identities).
    assert ("a", "1") in _idents(thin_page)
    assert ("b", "1") in _idents(thin_page)


@pytest.mark.asyncio
async def test_thin_executor_paginates_window() -> None:
    state = MockState()
    leg_a, leg_b = await _two_legs(state)
    active = [
        ("a", MockSearchAdapter(state=state, spec=leg_a), 1.0),
        ("b", MockSearchAdapter(state=state, spec=leg_b), 1.0),
    ]

    page1 = await execute_federated_thin_offset(
        legs=active,
        query="alpha",
        filters=None,
        pagination={"limit": 2, "offset": 0},
        leg_opts=None,
        rrf_k=60,
        per_leg_limit=5000,
        return_count=True,
        return_type=None,
        run_legs=_gather,
    )

    assert len(page1.hits) == 2
    # total counts the full fused candidate set, not just the page.
    assert page1.count == 4


@pytest.mark.asyncio
async def test_thin_executor_decodes_return_type() -> None:
    state = MockState()
    leg_a, leg_b = await _two_legs(state)
    active = [
        ("a", MockSearchAdapter(state=state, spec=leg_a), 1.0),
        ("b", MockSearchAdapter(state=state, spec=leg_b), 1.0),
    ]

    page = await execute_federated_thin_offset(
        legs=active,
        query="alpha",
        filters=None,
        pagination={"limit": 10},
        leg_opts=None,
        rrf_k=60,
        per_leg_limit=5000,
        return_count=False,
        return_type=_Out,
        run_legs=_gather,
    )

    assert page.hits
    assert all(isinstance(h, _Out) for h in page.hits)
    assert all(h.member in {"a", "b"} for h in page.hits)
