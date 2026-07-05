"""The offset run fails closed when a window would cross Meilisearch's ``maxTotalHits``.

Meilisearch silently caps a query at ``maxTotalHits`` (default 1000): a deep window comes
back short with no signal. The adapter guards the window and raises instead, so deep
pagination and snapshot builds cannot quietly drop rows.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from forze.base.exceptions import CoreException
from forze_meilisearch.adapters.search._offset_run import _MeilisearchOffsetHooks

pytestmark = pytest.mark.unit


def _hooks(pagination_dict: dict, *, max_total_hits: int = 1000) -> _MeilisearchOffsetHooks:
    gw = MagicMock()
    gw.config = MagicMock(max_total_hits=max_total_hits)
    client = MagicMock()
    # A guard failure must fire *before* the search call.
    client.index = MagicMock(side_effect=AssertionError("must not query when over cap"))
    return _MeilisearchOffsetHooks(
        gw=gw,
        client=client,
        query_string="q",
        filter_str=None,
        attrs=None,
        sort_list=None,
        pagination_dict=pagination_dict,
        return_count=True,
        return_fields=None,
    )


@pytest.mark.asyncio
async def test_window_past_max_total_hits_fails_closed() -> None:
    hooks = _hooks({"offset": 990, "limit": 50})  # far edge 1040 > 1000

    with pytest.raises(CoreException) as ei:
        await hooks.fetch_rows(MagicMock(), want_snap=False)

    assert ei.value.code == "core.search.max_total_hits_exceeded"


@pytest.mark.asyncio
async def test_window_within_cap_does_not_trip_the_guard() -> None:
    # far edge 900 <= 1000; the guard passes and the (mock) search is reached.
    hooks = _hooks({"offset": 850, "limit": 50})
    index = MagicMock()
    index.search = AsyncMock(return_value=MagicMock(hits=[], estimated_total_hits=0))
    hooks.client.index = MagicMock(return_value=index)
    hooks.gw.physical_paths = MagicMock(return_value=[])
    hooks.gw.from_hit = MagicMock(side_effect=lambda h: h)
    hooks.gw._resolved_index_uid = AsyncMock(return_value="idx")

    result = await hooks.fetch_rows(MagicMock(), want_snap=False)

    assert result.rows == []
