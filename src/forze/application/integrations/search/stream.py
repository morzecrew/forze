"""Bounded-memory keyset streaming for search exports.

Shared by every keyset-capable search adapter (Postgres FTS/PGroonga/hub, Mongo text/Atlas,
the mock). Instead of loading a whole result set with a large ``limit`` (a latent OOM), this
loops the adapter's keyset cursor a chunk at a time — peak memory is one chunk. It is a thin
convenience over ``search_cursor``: the adapter gates on
:attr:`~forze.application.contracts.search.SearchCapabilities.supports_stream` first, so an
offset-only (Meilisearch) or top-k (vector) backend refuses up front rather than iterating.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator, Awaitable, Callable, Sequence
from typing import Any

from forze.application.contracts.querying import CursorPaginationExpression
from forze.application.contracts.search import SearchCursorPage
from forze.base.exceptions import exc

# ----------------------- #

DEFAULT_MAX_SEARCH_STREAM_PAGES = 100_000
"""Runaway guard: cap the internal cursor loop so a non-terminating cursor can't spin forever."""


async def stream_search_pages(
    fetch_page: Callable[[CursorPaginationExpression], Awaitable[SearchCursorPage[Any]]],
    *,
    chunk_size: int,
    max_pages: int | None = DEFAULT_MAX_SEARCH_STREAM_PAGES,
) -> AsyncGenerator[Sequence[Any]]:
    """Loop the keyset cursor, yielding each page's hits until the set is exhausted.

    *fetch_page* runs one keyset page for a given cursor (typically a lambda over the
    adapter's ``search_cursor`` / ``project_search_cursor`` / ``select_search_cursor``).
    Stops at the first empty page or a ``None`` continuation; guards a runaway loop with
    *max_pages* and a same-token non-advance check.
    """

    if chunk_size < 1:
        raise exc.validation("search stream chunk_size must be at least 1")

    cursor: CursorPaginationExpression = {"limit": chunk_size}
    prev_cursor: str | None = None
    pages = 0

    while True:
        if max_pages is not None and pages >= max_pages:
            raise exc.precondition(f"Search stream exceeded max_pages={max_pages}")

        page = await fetch_page(cursor)

        if not page.hits:
            return

        yield page.hits

        next_cursor = page.next_cursor

        if not page.has_more or next_cursor is None:
            return

        if prev_cursor is not None and next_cursor == prev_cursor:
            raise exc.internal("Search cursor pagination did not advance")

        prev_cursor = next_cursor
        cursor = {"limit": chunk_size, "after": next_cursor}
        pages += 1
