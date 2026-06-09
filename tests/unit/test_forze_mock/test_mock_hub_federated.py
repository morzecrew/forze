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
