"""Late-materialized (thin) federated RRF: merge, eligibility, and executor parity."""

from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable, Sequence

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import FederatedSearchSpec, SearchSpec
from forze.application.contracts.search import SearchResultSnapshotSpec
from forze.application.integrations.search import (
    SearchResultSnapshot,
    execute_federated_thin_offset,
    federated_snapshot_rehydrator,
    federated_thin_eligible,
    federated_thin_format,
)
from forze_mock.adapters.search.command import MockSearchCommandAdapter
from forze_mock.adapters.search.federated import MockFederatedSearchAdapter
from forze_mock.adapters.search.query import MockSearchAdapter
from forze_mock.adapters.search.snapshot import MockSearchResultSnapshotAdapter
from forze_mock.state import MockState

# ----------------------- #


class _Item(BaseModel):
    id: str
    title: str


class _Meta(BaseModel):
    rank: int


class _NestedItem(BaseModel):
    id: str
    title: str
    meta: _Meta


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


def test_order_federated_secondary_sorts_none_and_dict_direction() -> None:
    # Equal scores so the sort field fully orders; one item's field is None (optional/missing).
    values = {("a", "1"): "x", ("b", "2"): None, ("c", "3"): "y"}

    def _run(sort_value: Any) -> list[str]:
        merged = [("a", "1", 0.5), ("b", "2", 0.5), ("c", "3", 0.5)]
        SearchResultSnapshot.order_federated_secondary_sorts(
            merged,
            {"title": sort_value},
            value_of=lambda it, field: values[(it[0], it[1])],
            score_of=lambda it: -it[2],
        )
        return [m[1] for m in merged]

    # Ascending: a null is the smallest value → sorts first; no TypeError on None vs str.
    assert _run("asc") == ["2", "1", "3"]
    # Descending: a null sorts last (the contract's nulls-last-for-desc default).
    assert _run("desc") == ["3", "1", "2"]
    # Explicit ``{"dir": ...}`` spec resolves direction (previously misread as ascending).
    assert _run({"dir": "desc"}) == ["3", "1", "2"]
    # Explicit ``nulls`` overrides the direction default (absolute placement).
    assert _run({"dir": "asc", "nulls": "last"}) == ["1", "3", "2"]
    assert _run({"dir": "desc", "nulls": "first"}) == ["2", "3", "1"]


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
    }

    assert federated_thin_eligible(thin_merge=True, **base)
    assert not federated_thin_eligible(thin_merge=False, **base)
    assert not federated_thin_eligible(
        thin_merge=True, **{**base, "wants_highlights": True}
    )
    # A top-level sort field present on every member is thin-eligible (projected
    # alongside ``id`` and applied as a tie-break under the RRF score).
    assert federated_thin_eligible(
        thin_merge=True, **{**base, "sorts": {"title": "asc"}}
    )
    # A dotted key whose ROOT field exists on every member is eligible too — both paths
    # resolve nested paths the same way (projected dict vs. ``model_dump`` + ``path_get``).
    nested_members = [
        SearchSpec(name="a", model_type=_NestedItem, fields=["title"]),
        SearchSpec(name="b", model_type=_NestedItem, fields=["title"]),
    ]
    assert federated_thin_eligible(
        members=nested_members,
        thin_merge=True,
        wants_highlights=False,
        sorts={"meta.rank": "asc"},
    )
    # A key whose root is absent on a member falls back to the full-fetch path.
    assert not federated_thin_eligible(
        thin_merge=True, **{**base, "sorts": {"meta.rank": "asc"}}
    )
    assert not federated_thin_eligible(
        thin_merge=True, **{**base, "sorts": {"absent": "asc"}}
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
    )

    # federated_thin_format is spec-level (governs the snapshot key format): set by the
    # opt-in + all-members-have-id, independent of per-request highlights/sorts.
    assert federated_thin_format(members, thin_merge=True)
    assert not federated_thin_format(members, thin_merge=False)
    assert not federated_thin_format(no_id, thin_merge=True)


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


def _ordered(page: Any) -> list[tuple[str, str]]:
    return [(h.member, h.hit.id) for h in page.hits]


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
@pytest.mark.parametrize("direction", ["asc", "desc"])
async def test_thin_executor_matches_full_merge_with_sorts(direction: str) -> None:
    state = MockState()
    leg_a, leg_b = await _two_legs(state)
    ports = [
        ("a", MockSearchAdapter(state=state, spec=leg_a)),
        ("b", MockSearchAdapter(state=state, spec=leg_b)),
    ]
    sorts = {"title": direction}

    full = MockFederatedSearchAdapter(
        federated_spec=FederatedSearchSpec(name="fed", members=[leg_a, leg_b]),
        legs=ports,
    )
    full_page = await full.search_page(
        "alpha", pagination={"limit": 10}, sorts=sorts
    )

    active = [("a", ports[0][1], 1.0), ("b", ports[1][1], 1.0)]
    thin_page = await execute_federated_thin_offset(
        legs=active,
        query="alpha",
        filters=None,
        pagination={"limit": 10},
        sorts=sorts,
        leg_opts=None,
        rrf_k=60,
        per_leg_limit=5000,
        return_count=True,
        return_type=None,
        run_legs=_gather,
    )

    # Identical ORDER (not just identity set): both paths run the same ordering helper
    # (RRF score primary, ``title`` tie-break), so late materialization must not reorder.
    assert _ordered(thin_page) == _ordered(full_page)
    assert thin_page.count == full_page.count


@pytest.mark.asyncio
@pytest.mark.parametrize("direction", ["asc", "desc"])
async def test_thin_executor_matches_full_merge_with_nested_sort(
    direction: str,
) -> None:
    state = MockState()
    leg_a = SearchSpec(name="a", model_type=_NestedItem, fields=["title"])
    leg_b = SearchSpec(name="b", model_type=_NestedItem, fields=["title"])
    await MockSearchCommandAdapter(state=state, spec=leg_a).upsert(
        [_NestedItem(id="1", title="alpha one", meta=_Meta(rank=30))]
    )
    await MockSearchCommandAdapter(state=state, spec=leg_b).upsert(
        [_NestedItem(id="2", title="alpha two", meta=_Meta(rank=10))]
    )
    ports = [
        ("a", MockSearchAdapter(state=state, spec=leg_a)),
        ("b", MockSearchAdapter(state=state, spec=leg_b)),
    ]
    sorts = {"meta.rank": direction}

    full = MockFederatedSearchAdapter(
        federated_spec=FederatedSearchSpec(name="fed", members=[leg_a, leg_b]),
        legs=ports,
    )
    full_page = await full.search_page(
        "alpha", pagination={"limit": 10}, sorts=sorts
    )

    active = [("a", ports[0][1], 1.0), ("b", ports[1][1], 1.0)]
    thin_page = await execute_federated_thin_offset(
        legs=active,
        query="alpha",
        filters=None,
        pagination={"limit": 10},
        sorts=sorts,
        leg_opts=None,
        rrf_k=60,
        per_leg_limit=5000,
        return_count=True,
        return_type=None,
        run_legs=_gather,
    )

    # Dotted sort resolves identically thin (projected nested dict) vs. full (model_dump +
    # path_get). One rank-1 hit per leg → equal RRF score → nested rank decides the order.
    assert _ordered(thin_page) == _ordered(full_page)
    expected = (
        [("b", "2"), ("a", "1")] if direction == "asc" else [("a", "1"), ("b", "2")]
    )
    assert _ordered(thin_page) == expected


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
async def test_thin_snapshot_write_and_replay_round_trip() -> None:
    state = MockState()
    leg_a, leg_b = await _two_legs(state)
    ports_list = [
        ("a", MockSearchAdapter(state=state, spec=leg_a)),
        ("b", MockSearchAdapter(state=state, spec=leg_b)),
    ]
    active = [("a", ports_list[0][1], 1.0), ("b", ports_list[1][1], 1.0)]

    rs_spec = SearchResultSnapshotSpec(name="snap", enabled=True, chunk_size=2)
    result_snapshot = SearchResultSnapshot(
        store=MockSearchResultSnapshotAdapter(state=MockState(), spec=rs_spec)
    )

    # Write: the thin executor stores tiny (member, id) keys and returns a handle.
    first = await execute_federated_thin_offset(
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
        result_snapshot=result_snapshot,
        rs_spec=rs_spec,
        snapshot=None,
        fp_computed="fp",
        write_snapshot=True,
    )
    assert first.snapshot is not None
    handle = first.snapshot

    # Replay: re-fetch the page's hits by id from the legs (current content).
    ports = {name: port for name, port in ports_list}
    replay = await result_snapshot.read_federated_thin_snapshot_page_if_requested(
        rs_spec=rs_spec,
        snapshot={"id": handle.id, "fingerprint": handle.fingerprint},
        fp_computed="fp",
        pagination={"limit": 10},
        return_type=None,
        return_count=True,
        rehydrate=federated_snapshot_rehydrator(
            ports=ports, leg_opts=None, run_legs=_gather
        ),
    )

    assert replay is not None
    assert _idents(replay) == _idents(first)
    assert replay.count == first.count


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
