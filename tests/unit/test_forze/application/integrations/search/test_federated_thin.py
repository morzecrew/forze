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
    # Dotted keys (the full path reads via ``getattr``, no traversal) and keys absent
    # on a member fall back to the full-fetch path.
    assert not federated_thin_eligible(
        thin_merge=True, **{**base, "sorts": {"nested.field": "asc"}}
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
