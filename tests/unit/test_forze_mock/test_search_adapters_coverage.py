"""Coverage for mock search query / hub / federated / offset-only adapters."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import (
    FederatedSearchSpec,
    HubSearchSpec,
    SearchResultSnapshotSpec,
    SearchSpec,
)
from forze.application.integrations.search import SearchResultSnapshot
from forze.base.exceptions import CoreException
from forze_mock.adapters.search.command import MockSearchCommandAdapter
from forze_mock.adapters.search.federated import MockFederatedSearchAdapter
from forze_mock.adapters.search.hub import MockHubSearchAdapter
from forze_mock.adapters.search.query import MockSearchAdapter
from forze_mock.adapters.search.snapshot import MockSearchResultSnapshotAdapter
from forze_mock.state import MockState

pytestmark = pytest.mark.unit

# ----------------------- #


class _Item(BaseModel):
    id: str
    title: str
    body: str = ""


async def _seed(state: MockState, spec: SearchSpec[_Item], items: list[_Item]) -> None:
    await MockSearchCommandAdapter(state=state, spec=spec).upsert(items)


# ----------------------- #
# query.py: field resolution + scoring modes


@pytest.mark.asyncio
async def test_resolve_fields_with_weights_option() -> None:
    state = MockState()
    spec = SearchSpec(name="w", model_type=_Item, fields=["title", "body"])
    await _seed(state, spec, [_Item(id="1", title="alpha", body="zzz")])
    adapter = MockSearchAdapter(state=state, spec=spec)

    # Weight only ``title`` → ``body`` text is ignored for scoring.
    page = await adapter.search(
        "alpha", pagination={"limit": 10}, options={"weights": {"title": 1.0}}
    )
    assert [h.id for h in page.hits] == ["1"]
    # A query matching only the zero-weight field should not rank.
    page2 = await adapter.search(
        "zzz", pagination={"limit": 10}, options={"weights": {"title": 1.0}}
    )
    assert page2.hits == []


@pytest.mark.asyncio
async def test_resolve_fields_with_fields_subset_option() -> None:
    state = MockState()
    spec = SearchSpec(name="fsub", model_type=_Item, fields=["title", "body"])
    await _seed(state, spec, [_Item(id="1", title="cat", body="dog")])
    adapter = MockSearchAdapter(state=state, spec=spec)

    page = await adapter.search(
        "dog", pagination={"limit": 10}, options={"fields": ["body"]}
    )
    assert [h.id for h in page.hits] == ["1"]


@pytest.mark.asyncio
async def test_resolve_fields_weights_all_zero_falls_back() -> None:
    state = MockState()
    spec = SearchSpec(name="z", model_type=_Item, fields=["title"])
    await _seed(state, spec, [_Item(id="1", title="hello")])
    adapter = MockSearchAdapter(state=state, spec=spec)
    # All weights zero → falls back to all allowed fields, but weight 0 → score 0.
    page = await adapter.search(
        "hello", pagination={"limit": 10}, options={"weights": {"title": 0.0}}
    )
    assert page.hits == []


@pytest.mark.asyncio
async def test_default_weights_scoring() -> None:
    state = MockState()
    spec = SearchSpec(
        name="dw",
        model_type=_Item,
        fields=["title", "body"],
        default_weights={"title": 1.0, "body": 0.5},
    )
    await _seed(state, spec, [_Item(id="1", title="match", body="x")])
    adapter = MockSearchAdapter(state=state, spec=spec)
    page = await adapter.search("match", pagination={"limit": 10})
    assert [h.id for h in page.hits] == ["1"]


def test_text_score_exact_and_prefix() -> None:
    state = MockState()
    spec = SearchSpec(name="ts", model_type=_Item, fields=["title"])
    adapter = MockSearchAdapter(state=state, spec=spec)
    doc = {"title": "hello world"}

    assert adapter._text_score("hello world", doc, ["title"], "exact") == 1.0
    assert adapter._text_score("hello", doc, ["title"], "exact") == 0.0
    # prefix: both tokens are prefixes of words present.
    assert adapter._text_score("hel wor", doc, ["title"], "prefix") == 1.0
    assert adapter._text_score("zzz", doc, ["title"], "prefix") == 0.0
    # empty query → 1.0; empty doc text → 0.0
    assert adapter._text_score("", doc, ["title"], "exact") == 1.0
    assert adapter._text_score("hello", {"title": ""}, ["title"], "exact") == 0.0


def test_text_score_whitespace_query_is_full() -> None:
    state = MockState()
    spec = SearchSpec(name="ws", model_type=_Item, fields=["title"])
    adapter = MockSearchAdapter(state=state, spec=spec)
    # whitespace-only query → no tokens → score 1.0 (line 126)
    assert adapter._text_score("   ", {"title": "anything"}, ["title"], "fulltext") == 1.0


def test_document_score_no_fields_is_zero() -> None:
    state = MockState()
    spec = SearchSpec(name="nf", model_type=_Item, fields=["title"])
    adapter = MockSearchAdapter(state=state, spec=spec)
    # no fields → 0.0 (line 173)
    assert adapter._document_score("q", {"title": "q"}, [], None) == 0.0
    # weights total <= 0 → 0.0 early return
    assert (
        adapter._document_score("q", {"title": "q"}, ["title"], {"title": 0.0}) == 0.0
    )


def test_document_score_skips_zero_weight_field_in_loop() -> None:
    state = MockState()
    spec = SearchSpec(name="zwf", model_type=_Item, fields=["title", "body"])
    adapter = MockSearchAdapter(state=state, spec=spec)
    # Positive total weight overall, but ``body`` weight is 0 → loop ``continue`` (line 182).
    score = adapter._document_score(
        "q",
        {"title": "q", "body": "q"},
        ["title", "body"],
        {"title": 1.0, "body": 0.0},
    )
    assert score == pytest.approx(1.0)  # only title contributes; total_w = 1.0


def test_document_score_multi_phrase_empty_terms() -> None:
    state = MockState()
    spec = SearchSpec(name="mp", model_type=_Item, fields=["title"])
    adapter = MockSearchAdapter(state=state, spec=spec)
    # empty terms tuple → falls back to single empty-query score (line 158)
    score = adapter._document_score_multi_phrase(
        (), {"title": "x"}, ["title"], None, combine="any"
    )
    assert score == 1.0


@pytest.mark.asyncio
async def test_multi_phrase_query_combine() -> None:
    state = MockState()
    spec = SearchSpec(name="mpc", model_type=_Item, fields=["title"])
    await _seed(state, spec, [_Item(id="1", title="alpha beta")])
    adapter = MockSearchAdapter(state=state, spec=spec)
    # list query exercises the multi-phrase scoring branch
    page = await adapter.search(["alpha", "beta"], pagination={"limit": 10})
    assert [h.id for h in page.hits] == ["1"]


@pytest.mark.asyncio
async def test_search_with_filter_excludes_docs() -> None:
    state = MockState()
    spec = SearchSpec(name="flt", model_type=_Item, fields=["title"])
    await _seed(
        state,
        spec,
        [_Item(id="1", title="hit one"), _Item(id="2", title="hit two")],
    )
    adapter = MockSearchAdapter(state=state, spec=spec)
    # A filter excludes id 2 → the filter-fail ``continue`` branch (line 207).
    page = await adapter.search(
        "hit",
        filters={"$values": {"id": {"$eq": "1"}}},
        pagination={"limit": 10},
    )
    assert [h.id for h in page.hits] == ["1"]


@pytest.mark.asyncio
async def test_search_page_no_limit_counts_all() -> None:
    state = MockState()
    spec = SearchSpec(name="nl", model_type=_Item, fields=["title"])
    await _seed(state, spec, [_Item(id=str(i), title="hit") for i in range(3)])
    adapter = MockSearchAdapter(state=state, spec=spec)
    # search_page with no limit → counted total path (lines 367, 404, 344->347)
    page = await adapter.search_page("hit")
    assert page.count == 3
    assert len(page.hits) == 3


@pytest.mark.asyncio
async def test_full_ordered_search_with_sorts() -> None:
    state = MockState()
    spec = SearchSpec(name="srt", model_type=_Item, fields=["title"])
    await _seed(
        state,
        spec,
        [
            _Item(id="b", title="hit beta"),
            _Item(id="a", title="hit alpha"),
        ],
    )
    adapter = MockSearchAdapter(state=state, spec=spec)
    page = await adapter.search("hit", pagination={"limit": 10}, sorts={"id": "asc"})
    assert [h.id for h in page.hits] == ["a", "b"]


@pytest.mark.asyncio
async def test_offset_search_with_offset_and_return_fields() -> None:
    state = MockState()
    spec = SearchSpec(name="off", model_type=_Item, fields=["title"])
    await _seed(
        state,
        spec,
        [_Item(id=str(i), title=f"item {i}") for i in range(5)],
    )
    adapter = MockSearchAdapter(state=state, spec=spec)

    # offset applied
    page = await adapter.search(
        "item", pagination={"limit": 2, "offset": 1}, sorts={"id": "asc"}
    )
    assert len(page.hits) == 2

    # return_fields projection combined with offset
    proj = await adapter.project_search(
        ["id"],
        "item",
        pagination={"limit": 2, "offset": 1},
        sorts={"id": "asc"},
    )
    assert all(set(h.keys()) == {"id"} for h in proj.hits)
    # counted projection variant
    proj_p = await adapter.project_search_page(
        ["id"], "item", pagination={"limit": 2}, sorts={"id": "asc"}
    )
    assert proj_p.count == 5


@pytest.mark.asyncio
async def test_select_search_returns_alternate_type() -> None:
    class _Alt(BaseModel):
        id: str
        title: str

    state = MockState()
    spec = SearchSpec(name="sel", model_type=_Item, fields=["title"])
    await _seed(state, spec, [_Item(id="1", title="alpha")])
    adapter = MockSearchAdapter(state=state, spec=spec)

    page = await adapter.select_search(_Alt, "alpha", pagination={"limit": 10})
    assert isinstance(page.hits[0], _Alt)
    page_c = await adapter.select_search_page(_Alt, "alpha", pagination={"limit": 10})
    assert page_c.count == 1


# ----------------------- #
# query.py: cursor overloads


@pytest.mark.asyncio
async def test_cursor_search_overloads() -> None:
    class _Alt(BaseModel):
        id: str
        title: str

    state = MockState()
    spec = SearchSpec(name="cur", model_type=_Item, fields=["title"])
    await _seed(
        state,
        spec,
        [_Item(id=str(i), title=f"row {i}") for i in range(4)],
    )
    adapter = MockSearchAdapter(state=state, spec=spec)

    page = await adapter.search_cursor("row", cursor={"limit": 2}, sorts={"id": "asc"})
    assert len(page.hits) == 2
    assert page.has_more is True
    assert page.next_cursor is not None

    nxt = await adapter.search_cursor(
        "row", cursor={"limit": 2, "after": page.next_cursor}, sorts={"id": "asc"}
    )
    assert nxt.prev_cursor is not None

    # projection cursor (return_fields)
    proj = await adapter.project_search_cursor(
        ["id"], "row", cursor={"limit": 2}, sorts={"id": "asc"}
    )
    assert all(set(h.keys()) == {"id"} for h in proj.hits)

    # typed cursor (return_type)
    typed = await adapter.select_search_cursor(
        _Alt, "row", cursor={"limit": 2}, sorts={"id": "asc"}
    )
    assert isinstance(typed.hits[0], _Alt)


# ----------------------- #
# query.py: snapshot read path


@pytest.mark.asyncio
async def test_offset_search_snapshot_hit() -> None:
    state = MockState()
    snap_spec = SearchResultSnapshotSpec(name="rs", chunk_size=10)
    spec = SearchSpec(
        name="snaphit", model_type=_Item, fields=["title"], snapshot=snap_spec
    )
    await _seed(state, spec, [_Item(id="1", title="alpha")])

    snap_store = MockSearchResultSnapshotAdapter(state=state, spec=snap_spec)
    rs = SearchResultSnapshot(store=snap_store)
    adapter = MockSearchAdapter(state=state, spec=spec, result_snapshot=rs)

    fp = SearchResultSnapshot.simple_search_fingerprint(
        "alpha", None, None, spec_name="snaphit", variant="offset"
    )
    record_key = SearchResultSnapshot.result_record_key_string(
        _Item(id="1", title="alpha")
    )
    await snap_store.put_run(
        run_id="run-1", fingerprint=fp, ordered_ids=[record_key], chunk_size=10
    )

    page = await adapter.search(
        "alpha", pagination={"limit": 10}, snapshot={"id": "run-1"}
    )
    assert [h.id for h in page.hits] == ["1"]


@pytest.mark.asyncio
async def test_offset_search_snapshot_miss_falls_through() -> None:
    # result_snapshot + spec.snapshot are wired, but no snapshot option → live search.
    state = MockState()
    snap_spec = SearchResultSnapshotSpec(name="rs2", chunk_size=10)
    spec = SearchSpec(
        name="snapmiss", model_type=_Item, fields=["title"], snapshot=snap_spec
    )
    await _seed(state, spec, [_Item(id="1", title="alpha")])
    snap_store = MockSearchResultSnapshotAdapter(state=state, spec=snap_spec)
    rs = SearchResultSnapshot(store=snap_store)
    adapter = MockSearchAdapter(state=state, spec=spec, result_snapshot=rs)

    page = await adapter.search("alpha", pagination={"limit": 10})
    assert [h.id for h in page.hits] == ["1"]


# ----------------------- #
# hub.py


def _hub_legs(state: MockState) -> tuple[SearchSpec[_Item], SearchSpec[_Item], list[Any]]:
    leg_a = SearchSpec(name="ha", model_type=_Item, fields=["title"])
    leg_b = SearchSpec(name="hb", model_type=_Item, fields=["title"])
    legs = [
        ("a", MockSearchAdapter(state=state, spec=leg_a)),
        ("b", MockSearchAdapter(state=state, spec=leg_b)),
    ]
    return leg_a, leg_b, legs


@pytest.mark.asyncio
async def test_hub_score_merge_sum_and_offset() -> None:
    state = MockState()
    leg_a, leg_b, legs = _hub_legs(state)
    # Same id in both legs so the merge accumulates / maxes.
    await _seed(state, leg_a, [_Item(id="1", title="shared hit")])
    await _seed(state, leg_b, [_Item(id="1", title="shared hit"), _Item(id="2", title="hit two")])

    hub = HubSearchSpec(name="hubsum", model_type=_Item, members=[leg_a, leg_b])
    adapter = MockHubSearchAdapter(hub_spec=hub, legs=legs, score_merge="sum")

    page = await adapter.search("hit", pagination={"limit": 10})
    ids = [h.id for h in page.hits]
    assert "1" in ids
    # offset trims the leading hit
    page_off = await adapter.search("hit", pagination={"limit": 10, "offset": 1})
    assert len(page_off.hits) == len(ids) - 1

    counted = await adapter.search_page("hit", pagination={"limit": 10})
    assert counted.count == len(ids)


@pytest.mark.asyncio
async def test_hub_score_merge_max_replaces_on_higher_contrib() -> None:
    state = MockState()
    leg_a, leg_b, legs = _hub_legs(state)
    # In leg a, id 1 is rank 2 (contrib 0.5); in leg b it's rank 1 (contrib 1.0).
    # max-merge must replace the stored doc/score with the higher contribution.
    await _seed(
        state,
        leg_a,
        [_Item(id="0", title="hit best a"), _Item(id="1", title="hit")],
    )
    await _seed(state, leg_b, [_Item(id="1", title="hit best b")])
    hub = HubSearchSpec(name="hubmax", model_type=_Item, members=[leg_a, leg_b])
    adapter = MockHubSearchAdapter(hub_spec=hub, legs=legs, score_merge="max")
    page = await adapter.search("hit", pagination={"limit": 10})
    by_id = {h.id: h for h in page.hits}
    assert by_id["1"].title == "hit best b"  # replaced by the higher-scoring leg


@pytest.mark.asyncio
async def test_hub_search_no_limit() -> None:
    state = MockState()
    leg_a, leg_b, legs = _hub_legs(state)
    await _seed(state, leg_a, [_Item(id="1", title="hit a")])
    await _seed(state, leg_b, [_Item(id="2", title="hit b")])
    hub = HubSearchSpec(name="hubnl", model_type=_Item, members=[leg_a, leg_b])
    adapter = MockHubSearchAdapter(hub_spec=hub, legs=legs)
    # no pagination at all → limit None branch (107->109)
    page = await adapter.search("hit")
    assert len(page.hits) == 2
    page_p = await adapter.search_page("hit")
    assert page_p.count == 2


@pytest.mark.asyncio
async def test_hub_member_weight_zero_skips_leg() -> None:
    state = MockState()
    leg_a, leg_b, legs = _hub_legs(state)
    await _seed(state, leg_a, [_Item(id="1", title="only a")])
    await _seed(state, leg_b, [_Item(id="2", title="only b")])
    hub = HubSearchSpec(
        name="hubw",
        model_type=_Item,
        members=[leg_a, leg_b],
        default_member_weights={"ha": 1.0, "hb": 0.0},
    )
    adapter = MockHubSearchAdapter(hub_spec=hub, legs=legs)
    page = await adapter.search("only", pagination={"limit": 10})
    # Leg b is weighted out, so only id 1 ranks.
    assert [h.id for h in page.hits] == ["1"]


@pytest.mark.asyncio
async def test_hub_zero_legs() -> None:
    state = MockState()
    leg_a = SearchSpec(name="solo", model_type=_Item, fields=["title"])
    hub = HubSearchSpec(name="hubsolo", model_type=_Item, members=[leg_a])
    adapter = MockHubSearchAdapter(hub_spec=hub, legs=[])
    page = await adapter.search("x", pagination={"limit": 10})
    assert page.hits == []
    page_p = await adapter.search_page("x", pagination={"limit": 10})
    assert page_p.count == 0


@pytest.mark.asyncio
async def test_hub_offset_only_methods_reject() -> None:
    state = MockState()
    leg_a = SearchSpec(name="rej", model_type=_Item, fields=["title"])
    hub = HubSearchSpec(name="hubrej", model_type=_Item, members=[leg_a])
    adapter = MockHubSearchAdapter(hub_spec=hub, legs=[])

    with pytest.raises(CoreException, match="offset pagination only"):
        await adapter.search_cursor("x")
    with pytest.raises(CoreException, match="offset pagination only"):
        await adapter.project_search(["title"], "x")
    with pytest.raises(CoreException, match="offset pagination only"):
        await adapter.project_search_page(["title"], "x")
    with pytest.raises(CoreException, match="offset pagination only"):
        await adapter.project_search_cursor(["title"], "x")
    with pytest.raises(CoreException, match="offset pagination only"):
        await adapter.select_search(_Item, "x")
    with pytest.raises(CoreException, match="offset pagination only"):
        await adapter.select_search_page(_Item, "x")
    with pytest.raises(CoreException, match="offset pagination only"):
        await adapter.select_search_cursor(_Item, "x")
    _ = state


# ----------------------- #
# federated.py


@pytest.mark.asyncio
async def test_federated_offset_and_rrf_merge() -> None:
    state = MockState()
    leg_a = SearchSpec(name="fa", model_type=_Item, fields=["title"])
    leg_b = SearchSpec(name="fb", model_type=_Item, fields=["title"])
    await _seed(state, leg_a, [_Item(id="1", title="alpha one"), _Item(id="3", title="alpha three")])
    await _seed(state, leg_b, [_Item(id="2", title="alpha two")])

    fed = FederatedSearchSpec(name="fed", members=[leg_a, leg_b])
    legs = [
        ("a", MockSearchAdapter(state=state, spec=leg_a)),
        ("b", MockSearchAdapter(state=state, spec=leg_b)),
    ]
    adapter = MockFederatedSearchAdapter(federated_spec=fed, legs=legs)

    page = await adapter.search("alpha", pagination={"limit": 10})
    assert len(page.hits) == 3
    assert {h.member for h in page.hits} <= {"a", "b"}

    # offset applied on the merged window
    page_off = await adapter.search("alpha", pagination={"limit": 10, "offset": 1})
    assert len(page_off.hits) == 2

    # search_page exposes the total over the full merged set
    counted = await adapter.search_page("alpha", pagination={"limit": 2})
    assert counted.count == 3
    assert len(counted.hits) == 2


@pytest.mark.asyncio
async def test_federated_member_weight_zero_skips_leg() -> None:
    state = MockState()
    leg_a = SearchSpec(name="fwa", model_type=_Item, fields=["title"])
    leg_b = SearchSpec(name="fwb", model_type=_Item, fields=["title"])
    await _seed(state, leg_a, [_Item(id="1", title="beta a")])
    await _seed(state, leg_b, [_Item(id="2", title="beta b")])

    fed = FederatedSearchSpec(name="fedw", members=[leg_a, leg_b])
    legs = [
        ("a", MockSearchAdapter(state=state, spec=leg_a)),
        ("b", MockSearchAdapter(state=state, spec=leg_b)),
    ]
    adapter = MockFederatedSearchAdapter(federated_spec=fed, legs=legs)

    page = await adapter.search(
        "beta", pagination={"limit": 10}, options={"member_weights": {"fwb": 0.0}}
    )
    members = {h.member for h in page.hits}
    assert members == {"a"}  # leg b skipped (weight 0)

    # search_page variant also skips the zero-weight leg (line 120)
    page_p = await adapter.search_page(
        "beta", options={"member_weights": {"fwb": 0.0}}
    )
    assert {h.member for h in page_p.hits} == {"a"}
    assert page_p.count == len(page_p.hits)


@pytest.mark.asyncio
async def test_federated_no_limit() -> None:
    state = MockState()
    leg_a = SearchSpec(name="fnla", model_type=_Item, fields=["title"])
    leg_b = SearchSpec(name="fnlb", model_type=_Item, fields=["title"])
    await _seed(state, leg_a, [_Item(id="1", title="gamma a")])
    await _seed(state, leg_b, [_Item(id="2", title="gamma b")])
    fed = FederatedSearchSpec(name="fednl", members=[leg_a, leg_b])
    legs = [
        ("a", MockSearchAdapter(state=state, spec=leg_a)),
        ("b", MockSearchAdapter(state=state, spec=leg_b)),
    ]
    adapter = MockFederatedSearchAdapter(federated_spec=fed, legs=legs)
    # no pagination → limit None branches (95->97, 139->141)
    page = await adapter.search("gamma")
    assert len(page.hits) == 2
    page_p = await adapter.search_page("gamma")
    assert page_p.count == 2


@pytest.mark.asyncio
async def test_federated_offset_only_methods_reject() -> None:
    state = MockState()
    leg_a = SearchSpec(name="frej", model_type=_Item, fields=["title"])
    leg_b = SearchSpec(name="frej2", model_type=_Item, fields=["title"])
    fed = FederatedSearchSpec(name="fedrej", members=[leg_a, leg_b])
    legs = [
        ("a", MockSearchAdapter(state=state, spec=leg_a)),
        ("b", MockSearchAdapter(state=state, spec=leg_b)),
    ]
    adapter = MockFederatedSearchAdapter(federated_spec=fed, legs=legs)

    with pytest.raises(CoreException, match="offset pagination only"):
        await adapter.search_cursor("x")
    with pytest.raises(CoreException, match="offset pagination only"):
        await adapter.project_search(["title"], "x")
    with pytest.raises(CoreException, match="offset pagination only"):
        await adapter.project_search_page(["title"], "x")
    with pytest.raises(CoreException, match="offset pagination only"):
        await adapter.project_search_cursor(["title"], "x")
    with pytest.raises(CoreException, match="offset pagination only"):
        await adapter.select_search(_Item, "x")
    with pytest.raises(CoreException, match="offset pagination only"):
        await adapter.select_search_page(_Item, "x")
    with pytest.raises(CoreException, match="offset pagination only"):
        await adapter.select_search_cursor(_Item, "x")
