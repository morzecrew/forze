"""Hub and federated mock search adapters."""

from pydantic import BaseModel

import pytest

from forze.application.contracts.search import (
    FederatedSearchSpec,
    HubSearchSpec,
    SearchSpec,
)
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
