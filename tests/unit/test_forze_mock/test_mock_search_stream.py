"""Bounded-memory search streaming — the in-memory reference (RFC 0011)."""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import (
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

pytestmark = pytest.mark.unit


class _Item(BaseModel):
    id: str
    title: str


class _Lean(BaseModel):
    id: str


async def _seed(state: MockState, spec: SearchSpec, n: int) -> None:
    await MockSearchCommandAdapter(state=state, spec=spec).upsert(
        [_Item(id=str(i), title=f"alpha item {i}") for i in range(n)]
    )


def _adapter(state: MockState) -> MockSearchAdapter[_Item]:
    spec = SearchSpec(name="items", model_type=_Item, fields=["title"])
    return MockSearchAdapter(state=state, spec=spec)


# ....................... #


class TestSingleIndexStream:
    @pytest.mark.asyncio
    async def test_streams_all_hits_in_bounded_chunks(self) -> None:
        state = MockState()
        await _seed(state, SearchSpec(name="items", model_type=_Item, fields=["title"]), 23)
        adapter = _adapter(state)

        assert adapter.search_capabilities.supports_stream is True

        chunks = [chunk async for chunk in adapter.search_stream("alpha", chunk_size=10)]

        # Bounded chunks, complete coverage, no duplicates.
        assert [len(c) for c in chunks] == [10, 10, 3]
        ids = [h.id for chunk in chunks for h in chunk]
        assert len(ids) == 23
        assert len(set(ids)) == 23

    @pytest.mark.asyncio
    async def test_empty_result_yields_nothing(self) -> None:
        state = MockState()
        await _seed(state, SearchSpec(name="items", model_type=_Item, fields=["title"]), 5)
        adapter = _adapter(state)

        chunks = [c async for c in adapter.search_stream("zzznomatch", chunk_size=10)]
        assert chunks == []

    @pytest.mark.asyncio
    async def test_project_and_select_stream(self) -> None:
        state = MockState()
        await _seed(state, SearchSpec(name="items", model_type=_Item, fields=["title"]), 7)
        adapter = _adapter(state)

        proj = [row async for c in adapter.project_search_stream(["id"], "alpha", chunk_size=3) for row in c]
        assert len(proj) == 7
        assert all(set(row.keys()) == {"id"} for row in proj)

        sel = [row async for c in adapter.select_search_stream(_Lean, "alpha", chunk_size=3) for row in c]
        assert len(sel) == 7
        assert all(isinstance(row, _Lean) for row in sel)

    @pytest.mark.asyncio
    async def test_chunk_size_must_be_positive(self) -> None:
        state = MockState()
        await _seed(state, SearchSpec(name="items", model_type=_Item, fields=["title"]), 3)
        adapter = _adapter(state)

        with pytest.raises(CoreException):
            async for _ in adapter.search_stream("alpha", chunk_size=0):
                pass


# ....................... #


class TestFailClosed:
    """Offset-only mock adapters (hub/federated) do not serve keyset → no streaming."""

    async def _hub(self, state: MockState) -> MockHubSearchAdapter[_Item]:
        leg = SearchSpec(name="a", model_type=_Item, fields=["title"])
        await _seed(state, leg, 3)
        hub = HubSearchSpec(name="hub", model_type=_Item, members=[leg])
        return MockHubSearchAdapter(
            hub_spec=hub, legs=[("a", MockSearchAdapter(state=state, spec=leg))]
        )

    @pytest.mark.asyncio
    async def test_hub_refuses_stream(self) -> None:
        state = MockState()
        hub = await self._hub(state)
        assert hub.search_capabilities.supports_stream is False

        with pytest.raises(CoreException) as ei:
            async for _ in hub.search_stream("alpha", chunk_size=5):
                pass
        assert ei.value.kind is ExceptionKind.PRECONDITION

    @pytest.mark.asyncio
    async def test_hub_refuses_project_and_select_stream(self) -> None:
        state = MockState()
        hub = await self._hub(state)

        with pytest.raises(CoreException):
            async for _ in hub.project_search_stream(["title"], "alpha", chunk_size=5):
                pass

        with pytest.raises(CoreException):
            async for _ in hub.select_search_stream(_Item, "alpha", chunk_size=5):
                pass

    @pytest.mark.asyncio
    async def test_federated_refuses_stream(self) -> None:
        state = MockState()
        leg_a = SearchSpec(name="a", model_type=_Item, fields=["title"])
        leg_b = SearchSpec(name="b", model_type=_Item, fields=["title"])
        await _seed(state, leg_a, 3)
        await _seed(state, leg_b, 3)
        fed = FederatedSearchSpec(name="fed", members=[leg_a, leg_b])
        adapter = MockFederatedSearchAdapter(
            federated_spec=fed,
            legs=[
                ("a", MockSearchAdapter(state=state, spec=leg_a)),
                ("b", MockSearchAdapter(state=state, spec=leg_b)),
            ],
        )
        assert adapter.search_capabilities.supports_stream is False

        with pytest.raises(CoreException):
            async for _ in adapter.search_stream("alpha", chunk_size=5):
                pass
