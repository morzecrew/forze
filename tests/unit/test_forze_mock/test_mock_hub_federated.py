"""Hub and federated mock search adapters."""

from typing import Any

import pytest
from pydantic import BaseModel

from forze.application.contracts.querying import OPERATOR_TYPE_MISMATCH_CODE, QueryFilterLimits
from forze.application.contracts.search import (
    FederatedSearchSpec,
    HubSearchSpec,
    SearchSpec,
)
from forze.base.exceptions import CoreException
from forze_mock.adapters.search.command import MockSearchCommandAdapter
from forze_mock.adapters.search.federated import MockFederatedSearchAdapter
from forze_mock.adapters.search.hub import MockHubSearchAdapter
from forze_mock.adapters.search.query import MockSearchAdapter
from forze_mock.state import MockState

# ----------------------- #


class _Item(BaseModel):
    id: str
    title: str


@pytest.mark.asyncio
async def test_hub_search_merges_two_legs() -> None:
    state = MockState()
    leg_a = SearchSpec(name="a", model_type=_Item, fields=["title"])
    leg_b = SearchSpec(name="b", model_type=_Item, fields=["title"])
    await MockSearchCommandAdapter(state=state, spec=leg_a).upsert(
        [_Item(id="1", title="hello world")]
    )
    await MockSearchCommandAdapter(state=state, spec=leg_b).upsert(
        [_Item(id="2", title="hello again")]
    )

    hub = HubSearchSpec(name="hub", model_type=_Item, members=[leg_a, leg_b])
    adapter = MockHubSearchAdapter(
        hub_spec=hub,
        legs=[
            ("a", MockSearchAdapter(state=state, spec=leg_a)),
            ("b", MockSearchAdapter(state=state, spec=leg_b)),
        ],
    )
    page = await adapter.search("hello", pagination={"limit": 10})
    titles = {h.title for h in page.hits}
    assert "hello world" in titles or "hello again" in titles


@pytest.mark.asyncio
async def test_hub_search_surfaces_merged_scores() -> None:
    state = MockState()
    leg_a = SearchSpec(name="a", model_type=_Item, fields=["title"])
    leg_b = SearchSpec(name="b", model_type=_Item, fields=["title"])
    await MockSearchCommandAdapter(state=state, spec=leg_a).upsert(
        [_Item(id="1", title="hello world"), _Item(id="3", title="hello there")]
    )
    await MockSearchCommandAdapter(state=state, spec=leg_b).upsert(
        [_Item(id="2", title="hello again")]
    )

    hub = HubSearchSpec(name="hub", model_type=_Item, members=[leg_a, leg_b])
    adapter = MockHubSearchAdapter(
        hub_spec=hub,
        legs=[
            ("a", MockSearchAdapter(state=state, spec=leg_a)),
            ("b", MockSearchAdapter(state=state, spec=leg_b)),
        ],
    )

    page = await adapter.search_page("hello", pagination={"limit": 10})
    # Merged hub score is surfaced, index-aligned with hits, non-increasing, positive.
    assert page.scores is not None
    assert len(page.scores) == len(page.hits)
    assert all(a >= b for a, b in zip(page.scores, page.scores[1:]))
    assert all(s > 0.0 for s in page.scores)

    # Filter-only browse (empty query) has no score.
    browse = await adapter.search_page("", pagination={"limit": 10})
    assert browse.scores is None


@pytest.mark.asyncio
async def test_hub_fusion_gate_rejects_weighted() -> None:
    from forze.base.exceptions import CoreException, ExceptionKind

    state = MockState()
    leg_a = SearchSpec(name="a", model_type=_Item, fields=["title"])
    leg_b = SearchSpec(name="b", model_type=_Item, fields=["title"])
    await MockSearchCommandAdapter(state=state, spec=leg_a).upsert(
        [_Item(id="1", title="hello world")]
    )
    await MockSearchCommandAdapter(state=state, spec=leg_b).upsert(
        [_Item(id="2", title="hello again")]
    )

    hub = HubSearchSpec(name="hub", model_type=_Item, members=[leg_a, leg_b])
    adapter = MockHubSearchAdapter(
        hub_spec=hub,
        legs=[
            ("a", MockSearchAdapter(state=state, spec=leg_a)),
            ("b", MockSearchAdapter(state=state, spec=leg_b)),
        ],
    )

    # The hub advertises rank-based (rrf) fusion only; the default and explicit rrf work.
    assert adapter.search_capabilities.hybrid_fusion == frozenset({"rrf"})
    assert (await adapter.search_page("hello", options={"fusion": "rrf"})).count == 2
    assert (await adapter.search_page("hello")).count == 2

    # Weighted fusion is a federated concept — refused, not silently the default merge.
    with pytest.raises(CoreException, match="weighted fusion") as ei:
        await adapter.search_page("hello", options={"fusion": "weighted"})
    assert ei.value.kind is ExceptionKind.PRECONDITION


@pytest.mark.asyncio
async def test_federated_search_rrf_merge() -> None:
    state = MockState()
    leg_a = SearchSpec(name="a", model_type=_Item, fields=["title"])
    leg_b = SearchSpec(name="b", model_type=_Item, fields=["title"])
    await MockSearchCommandAdapter(state=state, spec=leg_a).upsert(
        [_Item(id="1", title="alpha")]
    )
    await MockSearchCommandAdapter(state=state, spec=leg_b).upsert(
        [_Item(id="2", title="beta")]
    )

    fed = FederatedSearchSpec(name="fed", members=[leg_a, leg_b])
    adapter = MockFederatedSearchAdapter(
        federated_spec=fed,
        legs=[
            ("a", MockSearchAdapter(state=state, spec=leg_a)),
            ("b", MockSearchAdapter(state=state, spec=leg_b)),
        ],
    )
    page = await adapter.search("a", pagination={"limit": 10})
    assert len(page.hits) >= 1
    assert page.hits[0].member in {"a", "b"}


@pytest.mark.asyncio
async def test_federated_search_surfaces_rrf_scores() -> None:
    state = MockState()
    leg_a = SearchSpec(name="a", model_type=_Item, fields=["title"])
    leg_b = SearchSpec(name="b", model_type=_Item, fields=["title"])
    await MockSearchCommandAdapter(state=state, spec=leg_a).upsert(
        [_Item(id="1", title="alpha match"), _Item(id="3", title="alpha extra")]
    )
    await MockSearchCommandAdapter(state=state, spec=leg_b).upsert(
        [_Item(id="2", title="alpha other")]
    )

    fed = FederatedSearchSpec(name="fed", members=[leg_a, leg_b])
    adapter = MockFederatedSearchAdapter(
        federated_spec=fed,
        legs=[
            ("a", MockSearchAdapter(state=state, spec=leg_a)),
            ("b", MockSearchAdapter(state=state, spec=leg_b)),
        ],
    )

    page = await adapter.search("alpha", pagination={"limit": 10})

    # Fused RRF score is surfaced, index-aligned with hits, and non-increasing (rank order).
    assert page.scores is not None
    assert len(page.scores) == len(page.hits)
    assert all(a >= b for a, b in zip(page.scores, page.scores[1:]))
    assert all(s > 0.0 for s in page.scores)

    # search_page carries the same scores alongside the total count.
    counted = await adapter.search_page("alpha", pagination={"limit": 10})
    assert counted.scores is not None
    assert len(counted.scores) == len(counted.hits)

    # Filter-only browse (empty query) has no meaningful fused score.
    browse = await adapter.search("", pagination={"limit": 10})
    assert browse.scores is None


@pytest.mark.asyncio
async def test_federated_weighted_fusion_supported_on_mock() -> None:
    state = MockState()
    leg_a = SearchSpec(name="a", model_type=_Item, fields=["title"])
    leg_b = SearchSpec(name="b", model_type=_Item, fields=["title"])
    await MockSearchCommandAdapter(state=state, spec=leg_a).upsert(
        [_Item(id="1", title="alpha match")]
    )
    await MockSearchCommandAdapter(state=state, spec=leg_b).upsert(
        [_Item(id="2", title="alpha other")]
    )

    fed = FederatedSearchSpec(name="fed", members=[leg_a, leg_b])
    adapter = MockFederatedSearchAdapter(
        federated_spec=fed,
        legs=[
            ("a", MockSearchAdapter(state=state, spec=leg_a)),
            ("b", MockSearchAdapter(state=state, spec=leg_b)),
        ],
    )

    # The reference adapter advertises both strategies; weighted fusion runs and scores.
    assert {"rrf", "weighted"} <= adapter.search_capabilities.hybrid_fusion
    page = await adapter.search("alpha", pagination={"limit": 10}, options={"fusion": "weighted"})
    assert page.scores is not None
    assert len(page.scores) == len(page.hits)
    assert all(a >= b for a, b in zip(page.scores, page.scores[1:]))


@pytest.mark.asyncio
async def test_federated_unsupported_fusion_fails_closed() -> None:
    from forze.application.contracts.search import (
        SearchCapabilities,
        validate_fusion_supported,
    )
    from forze.base.exceptions import CoreException

    # A backend that only advertises rrf (Postgres/Meilisearch today) rejects weighted.
    caps = SearchCapabilities(hybrid_fusion=frozenset({"rrf"}))
    with pytest.raises(CoreException, match="weighted fusion"):
        validate_fusion_supported(caps, "weighted", backend="postgres_federated")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("default_sort", "sorts"),
    [({"title": "asc"}, None), (None, {"title": "asc"})],
    ids=["hub default", "request"],
)
async def test_a_hub_sort_orders_rows_tied_on_the_hub_score(
    default_sort: dict[str, str] | None, sorts: dict[str, str] | None
) -> None:
    # As the Postgres hub orders a page: the hub score, then the request's sorts or the hub's
    # own default, then the id. Each leg ranks its one match first, so the two tie.
    state = MockState()
    leg_a = SearchSpec(name="a", model_type=_Item, fields=["title"])
    leg_b = SearchSpec(name="b", model_type=_Item, fields=["title"])
    await MockSearchCommandAdapter(state=state, spec=leg_a).upsert(
        [_Item(id="1", title="hello world")]
    )
    await MockSearchCommandAdapter(state=state, spec=leg_b).upsert(
        [_Item(id="2", title="hello again")]
    )
    hub = HubSearchSpec(
        name="hub", model_type=_Item, members=[leg_a, leg_b], default_sort=default_sort
    )
    adapter = MockHubSearchAdapter(
        hub_spec=hub,
        legs=[
            ("a", MockSearchAdapter(state=state, spec=leg_a)),
            ("b", MockSearchAdapter(state=state, spec=leg_b)),
        ],
    )

    page = await adapter.search_page("hello", None, {"limit": 10}, sorts)

    assert page.scores is not None and page.scores[0] == page.scores[1]
    assert [hit.title for hit in page.hits] == ["hello again", "hello world"]


async def _hub_with_limits(
    *, hub_limits: QueryFilterLimits | None, leg_limits: QueryFilterLimits | None
) -> MockHubSearchAdapter[_Item]:
    state = MockState()
    leg = SearchSpec(name="a", model_type=_Item, fields=["title"], filter_limits=leg_limits)
    await MockSearchCommandAdapter(state=state, spec=leg).upsert([_Item(id="1", title="hello")])
    hub = HubSearchSpec(name="hub", model_type=_Item, members=[leg], filter_limits=hub_limits)

    return MockHubSearchAdapter(
        hub_spec=hub, legs=[("a", MockSearchAdapter(state=state, spec=leg))]
    )


_WIDE = QueryFilterLimits(max_in_size=5_000)
_MANY_IDS = {"$values": {"id": [str(i) for i in range(1_500)]}}


@pytest.mark.asyncio
async def test_a_hub_parses_its_filters_under_its_own_limits() -> None:
    # As on Postgres, where the hub row's filter is parsed once, under the hub spec's limits.
    wide_hub = await _hub_with_limits(hub_limits=_WIDE, leg_limits=None)
    page = await wide_hub.search_page("hello", _MANY_IDS, pagination={"limit": 10})
    assert [h.id for h in page.hits] == ["1"]

    # A member's own limits do not widen the hub's.
    wide_leg = await _hub_with_limits(hub_limits=None, leg_limits=_WIDE)
    with pytest.raises(CoreException):
        await wide_leg.search_page("hello", _MANY_IDS, pagination={"limit": 10})


def _multi_leg_adapters(
    state: MockState,
) -> list[MockHubSearchAdapter[_Item] | MockFederatedSearchAdapter[_Item]]:
    leg_a = SearchSpec(name="a", model_type=_Item, fields=["title"])
    leg_b = SearchSpec(name="b", model_type=_Item, fields=["title"])

    def legs() -> list[tuple[str, MockSearchAdapter[_Item]]]:
        return [
            ("a", MockSearchAdapter(state=state, spec=leg_a)),
            ("b", MockSearchAdapter(state=state, spec=leg_b)),
        ]

    return [
        MockHubSearchAdapter(
            hub_spec=HubSearchSpec(name="hub", model_type=_Item, members=[leg_a, leg_b]),
            legs=legs(),
        ),
        MockFederatedSearchAdapter(
            federated_spec=FederatedSearchSpec(name="fed", members=[leg_a, leg_b]),
            legs=legs(),
        ),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["search", "search_page"])
@pytest.mark.parametrize(
    ("kwargs", "key"),
    [
        ({"options": {"fuzzzy": True}}, "fuzzzy"),
        ({"snapshot": {"idd": "run-1"}}, "idd"),
    ],
    ids=["option", "snapshot"],
)
async def test_a_hub_or_federated_search_refuses_a_key_no_type_defines(
    method: str, kwargs: dict[str, object], key: str
) -> None:
    for adapter in _multi_leg_adapters(MockState()):
        with pytest.raises(CoreException, match=key):
            await getattr(adapter, method)("a", pagination={"limit": 10}, **kwargs)


class _Stock(BaseModel):
    id: str
    title: str
    region: str
    quantity: int | None = None


class _HubStock(BaseModel):
    id: str
    title: str
    region: str


async def _stock_hub() -> MockHubSearchAdapter[_HubStock]:
    """``omega`` matches stocks 1, 2 and 4, which hold 1, 2 and 8 units, and stock 5, which
    holds none on record."""

    state = MockState()
    leg_a = SearchSpec(name="a", model_type=_Stock, fields=["title"])
    leg_b = SearchSpec(name="b", model_type=_Stock, fields=["title"])
    await MockSearchCommandAdapter(state=state, spec=leg_a).upsert(
        [
            _Stock(id="1", title="omega one", region="north", quantity=1),
            _Stock(id="2", title="omega two", region="south", quantity=2),
            _Stock(id="3", title="alpha", region="north", quantity=4),
        ]
    )
    await MockSearchCommandAdapter(state=state, spec=leg_b).upsert(
        [
            _Stock(id="4", title="omega four", region="north", quantity=8),
            _Stock(id="5", title="omega five", region="south"),
        ]
    )

    return MockHubSearchAdapter(
        hub_spec=HubSearchSpec(
            name="hub",
            model_type=_HubStock,
            members=[leg_a, leg_b],
            extra_aggregatable_fields=frozenset({"quantity"}),
        ),
        legs=[
            ("a", MockSearchAdapter(state=state, spec=leg_a)),
            ("b", MockSearchAdapter(state=state, spec=leg_b)),
        ],
    )


@pytest.mark.asyncio
async def test_a_hub_aggregate_measures_an_extra_field_across_its_legs() -> None:
    """As on Postgres: every merged match of a query and a filter, measured by a column the
    hub's read model does not carry."""

    hub = await _stock_hub()

    page = await hub.aggregate_search(
        {"$computed": {"n": {"$count": None}, "q": {"$sum": "quantity"}}},
        "omega",
        {"$values": {"region": {"$eq": "north"}}},
    )

    assert [(row["n"], row["q"]) for row in page.hits] == [(2, 9)]


@pytest.mark.asyncio
async def test_a_hub_metric_filter_names_an_extra_field() -> None:
    hub = await _stock_hub()

    page = await hub.aggregate_search(
        {
            "$groups": {"region": "region"},
            "$computed": {
                "total": {"$sum": "quantity"},
                "missing": {"$count": {"filter": {"$values": {"quantity": {"$null": True}}}}},
                "large": {"$count": {"filter": {"$values": {"quantity": {"$gte": 2}}}}},
                # A string bound casts to the stored value's type, as Postgres casts it to the
                # column's: 8 is at least "4", and 1 and 2 are not.
                "over": {"$count": {"filter": {"$values": {"quantity": {"$gte": "4"}}}}},
            },
        },
        "omega",
        sorts={"region": "asc"},
    )

    assert [
        (row["region"], row["total"], row["missing"], row["large"], row["over"])
        for row in page.hits
    ] == [("north", 9, 0, 1, 1), ("south", 2, 1, 1, 0)]


@pytest.mark.parametrize(
    ("op", "code"),
    [
        ({"$gte": "many"}, "core.precondition"),
        ({"$like": "8%"}, OPERATOR_TYPE_MISMATCH_CODE),
    ],
)
@pytest.mark.asyncio
async def test_a_hub_metric_filter_on_an_extra_field_is_typed_by_its_values(
    op: dict[str, Any], code: str
) -> None:
    """An int column takes no text operator, and a bound that is not a number is refused,
    never read as no match."""

    hub = await _stock_hub()

    with pytest.raises(CoreException) as refused:
        await hub.aggregate_search(
            {"$computed": {"n": {"$count": {"filter": {"$values": {"quantity": op}}}}}}, "omega"
        )

    assert refused.value.code == code


@pytest.mark.asyncio
async def test_an_extra_field_no_match_holds_goes_untyped() -> None:
    """Stock 5 alone matches and holds no quantity, so nothing types it: no row reaches 4."""

    hub = await _stock_hub()

    page = await hub.aggregate_search(
        {
            "$computed": {
                "bare": {"$count": {"filter": {"$values": {"quantity": {"$null": True}}}}},
                "over": {"$count": {"filter": {"$values": {"quantity": {"$gte": "4"}}}}},
            }
        },
        "five",
    )

    assert [(row["bare"], row["over"]) for row in page.hits] == [(1, 0)]


@pytest.mark.asyncio
async def test_federated_member_filters_narrow_their_own_leg_alone() -> None:
    state = MockState()
    leg_a = SearchSpec(name="a", model_type=_Item, fields=["title"])
    leg_b = SearchSpec(name="b", model_type=_Item, fields=["title"])

    for leg in (leg_a, leg_b):
        await MockSearchCommandAdapter(state=state, spec=leg).upsert(
            [_Item(id="1", title="alpha one"), _Item(id="2", title="alpha two")]
        )

    adapter = MockFederatedSearchAdapter(
        federated_spec=FederatedSearchSpec(name="fed", members=[leg_a, leg_b]),
        legs=[
            ("a", MockSearchAdapter(state=state, spec=leg_a)),
            ("b", MockSearchAdapter(state=state, spec=leg_b)),
        ],
    )

    page = await adapter.search_page(
        "alpha",
        {"$values": {"id": {"$in": ["1", "2"]}}},
        options={"member_filters": {"b": {"$values": {"id": "2"}}}},
    )

    assert sorted((h.member, h.hit.id) for h in page.hits) == [("a", "1"), ("a", "2"), ("b", "2")]

    with pytest.raises(CoreException, match="no member"):
        await adapter.search_page("alpha", options={"member_filters": {"c": {}}})


@pytest.mark.asyncio
async def test_hub_refuses_member_filters() -> None:
    state = MockState()
    leg_a = SearchSpec(name="a", model_type=_Item, fields=["title"])
    leg_b = SearchSpec(name="b", model_type=_Item, fields=["title"])
    adapter = MockHubSearchAdapter(
        hub_spec=HubSearchSpec(name="hub", model_type=_Item, members=[leg_a, leg_b]),
        legs=[
            ("a", MockSearchAdapter(state=state, spec=leg_a)),
            ("b", MockSearchAdapter(state=state, spec=leg_b)),
        ],
    )

    with pytest.raises(CoreException, match="member_filters") as ei:
        await adapter.search_page("alpha", options={"member_filters": {"a": {}}})

    assert ei.value.code == "query_feature_unsupported"
