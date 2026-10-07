"""A snapshot window retrieves every attribute; only the page a request returns is projected.

The snapshot pool keys each hit by its whole record, so a projected request that writes a
snapshot must not narrow the windows that fill it.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from forze_meilisearch.adapters.search._offset_run import _MeilisearchOffsetHooks

pytestmark = pytest.mark.unit


def _hooks() -> tuple[_MeilisearchOffsetHooks, AsyncMock]:
    gw = MagicMock()
    gw.config = MagicMock(max_total_hits=1000)
    gw.physical_paths = MagicMock(side_effect=lambda fields: list(fields))
    gw.primary_key = "id"
    gw.from_hit = MagicMock(side_effect=lambda hit: hit)
    gw._resolved_index_uid = AsyncMock(return_value="idx")
    search = AsyncMock(return_value=MagicMock(hits=[], estimated_total_hits=0))
    client = MagicMock()
    client.index = MagicMock(return_value=MagicMock(search=search))
    hooks = _MeilisearchOffsetHooks(
        gw=gw,
        client=client,
        query_string="q",
        filter_str=None,
        attrs=None,
        sort_list=None,
        pagination_dict={"limit": 5},
        return_count=False,
        return_fields=("title",),
    )
    return hooks, search


@pytest.mark.asyncio
@pytest.mark.parametrize(("want_snap", "projected"), [(False, True), (True, False)])
async def test_only_the_page_is_projected(want_snap: bool, projected: bool) -> None:
    hooks, search = _hooks()

    await hooks.fetch_rows(MagicMock(fetch_offset=0, fetch_limit=5), want_snap=want_snap)

    assert ("attributes_to_retrieve" in search.call_args.kwargs) is projected
