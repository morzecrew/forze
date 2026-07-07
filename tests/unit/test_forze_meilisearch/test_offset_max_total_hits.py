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


def _count_hooks(*, exact: bool, total_hits: int = 42) -> _MeilisearchOffsetHooks:
    gw = MagicMock()
    gw.config = MagicMock(max_total_hits=1000, exact_total_count=exact)
    gw._resolved_index_uid = AsyncMock(return_value="idx")
    index = MagicMock()
    index.search = AsyncMock(return_value=MagicMock(total_hits=total_hits))
    client = MagicMock()
    client.index = MagicMock(return_value=index)
    return _MeilisearchOffsetHooks(
        gw=gw,
        client=client,
        query_string="q",
        filter_str=None,
        attrs=None,
        sort_list=None,
        pagination_dict={},
        return_count=True,
        return_fields=None,
    )


@pytest.mark.asyncio
async def test_fetch_count_none_when_not_exact() -> None:
    # Default: the total comes cheaply from the search's estimatedTotalHits, so no count query.
    assert await _count_hooks(exact=False).fetch_count() is None


@pytest.mark.asyncio
async def test_fetch_count_exact_uses_page_mode_total_hits() -> None:
    hooks = _count_hooks(exact=True, total_hits=42)

    assert await hooks.fetch_count() == 42

    index = hooks.client.index.return_value
    kwargs = index.search.await_args.kwargs
    assert kwargs["hits_per_page"] == 1 and kwargs["page"] == 1


@pytest.mark.asyncio
async def test_window_past_max_total_hits_fails_closed() -> None:
    hooks = _hooks({"offset": 990, "limit": 50})  # far edge 1040 > 1000

    with pytest.raises(CoreException) as ei:
        await hooks.fetch_rows(MagicMock(), want_snap=False)

    assert ei.value.code == "core.search.max_total_hits_exceeded"


@pytest.mark.asyncio
async def test_missing_limit_counts_meili_default_toward_window() -> None:
    # A window without an explicit ``limit`` still reads Meilisearch's default page (20), so
    # offset 990 actually reaches 1010 > 1000 — the guard must fail closed rather than treat the
    # missing limit as 0 and undercount the window.
    hooks = _hooks({"offset": 990})  # far edge 990 + default 20 = 1010 > 1000

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
