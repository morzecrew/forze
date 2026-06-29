"""Mock hub + federated facets & highlights (RFC 0006, P6 multi-leg)."""

from pydantic import BaseModel

import pytest

from forze.application.contracts.search import (
    FacetBucket,
    FederatedSearchSpec,
    HubSearchSpec,
    SearchSpec,
)
from forze.base.exceptions import CoreException, ExceptionKind
from forze_mock.adapters.search.command import MockSearchCommandAdapter
from forze_mock.adapters.search.federated import MockFederatedSearchAdapter
from forze_mock.adapters.search.hub import MockHubSearchAdapter
from forze_mock.adapters.search.query import MockSearchAdapter
from forze_mock.state import MockState

# ----------------------- #


class _Item(BaseModel):
    id: str
    title: str
    category: str = "general"


async def _seed(state: MockState, leg: SearchSpec[_Item], items: list[_Item]) -> None:
    await MockSearchCommandAdapter(state=state, spec=leg).upsert(items)


# ....................... #


@pytest.mark.asyncio
async def test_hub_facets_and_highlights() -> None:
    state = MockState()
    leg_a = SearchSpec(name="a", model_type=_Item, fields=["title"])
    leg_b = SearchSpec(name="b", model_type=_Item, fields=["title"])
    await _seed(state, leg_a, [_Item(id="1", title="rust book", category="books")])
    await _seed(state, leg_b, [_Item(id="2", title="python book", category="books")])
    await _seed(state, leg_b, [_Item(id="3", title="gaming book", category="gear")])

    hub = HubSearchSpec(
        name="hub",
        model_type=_Item,
        members=[leg_a, leg_b],
        facetable_fields=frozenset({"category"}),
    )
    adapter = MockHubSearchAdapter(
        hub_spec=hub,
        legs=[
            ("a", MockSearchAdapter(state=state, spec=leg_a)),
            ("b", MockSearchAdapter(state=state, spec=leg_b)),
        ],
    )

    page = await adapter.search_page(
        "book", options={"facets": ["category"], "highlight": True}
    )

    # Hub facets are flat over the merged (deduped) rows.
    assert page.facets is not None
    assert page.facets["category"] == (
        FacetBucket(value="books", count=2),
        FacetBucket(value="gear", count=1),
    )
    # Highlights are per-hit, index-aligned, over the merged hub row.
    assert page.highlights is not None
    assert len(page.highlights) == len(page.hits)
    assert any("<em>book</em>" in hl.get("title", ("",))[0] for hl in page.highlights)


@pytest.mark.asyncio
async def test_hub_facet_on_non_facetable_field_refused() -> None:
    state = MockState()
    leg = SearchSpec(name="a", model_type=_Item, fields=["title"])
    await _seed(state, leg, [_Item(id="1", title="x")])
    hub = HubSearchSpec(name="hub", model_type=_Item, members=[leg])
    adapter = MockHubSearchAdapter(
        hub_spec=hub, legs=[("a", MockSearchAdapter(state=state, spec=leg))]
    )

    with pytest.raises(CoreException) as ei:
        await adapter.search_page("x", options={"facets": ["category"]})

    assert ei.value.kind is ExceptionKind.PRECONDITION


@pytest.mark.asyncio
async def test_federated_highlights_threaded_through_merge() -> None:
    state = MockState()
    leg_a = SearchSpec(name="a", model_type=_Item, fields=["title"])
    leg_b = SearchSpec(name="b", model_type=_Item, fields=["title"])
    await _seed(state, leg_a, [_Item(id="1", title="alpha book")])
    await _seed(state, leg_b, [_Item(id="2", title="beta book")])

    fed = FederatedSearchSpec(name="fed", members=[leg_a, leg_b])
    adapter = MockFederatedSearchAdapter(
        federated_spec=fed,
        legs=[
            ("a", MockSearchAdapter(state=state, spec=leg_a)),
            ("b", MockSearchAdapter(state=state, spec=leg_b)),
        ],
    )

    page = await adapter.search_page("book", options={"highlight": True})

    assert page.highlights is not None
    assert len(page.highlights) == len(page.hits)
    # Each surviving merged hit carries its originating leg's highlight.
    assert all("<em>book</em>" in hl["title"][0] for hl in page.highlights if "title" in hl)
    assert any("title" in hl for hl in page.highlights)


@pytest.mark.asyncio
async def test_federated_facets_fail_closed() -> None:
    state = MockState()
    leg_a = SearchSpec(name="a", model_type=_Item, fields=["title"])
    leg_b = SearchSpec(name="b", model_type=_Item, fields=["title"])
    await _seed(state, leg_a, [_Item(id="1", title="alpha")])
    await _seed(state, leg_b, [_Item(id="2", title="beta")])

    fed = FederatedSearchSpec(name="fed", members=[leg_a, leg_b])
    adapter = MockFederatedSearchAdapter(
        federated_spec=fed,
        legs=[
            ("a", MockSearchAdapter(state=state, spec=leg_a)),
            ("b", MockSearchAdapter(state=state, spec=leg_b)),
        ],
    )

    with pytest.raises(CoreException) as ei:
        await adapter.search_page("a", options={"facets": ["category"]})

    assert ei.value.kind is ExceptionKind.PRECONDITION
