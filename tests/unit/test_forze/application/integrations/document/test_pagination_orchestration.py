"""Pure unit tests for document pagination orchestration.

These exercise :class:`DocumentPaginationMixin` directly through a lightweight
in-memory fake read gateway (no Docker, no ``MockState``). The fake only
implements the handful of gateway methods the mixin actually calls and returns
canned rows so we can drive offset paging, keyset cursor paging and streaming
through their cap/limit/empty/boundary branches.
"""

from __future__ import annotations

from typing import Any, Sequence

import pytest
from pydantic import BaseModel

from forze.application.contracts.base import CursorPage
from forze.application.integrations.document._pagination import (
    CursorQuery,
    DocumentPaginationMixin,
    OffsetQuery,
    StreamQuery,
)
from forze.base.exceptions import CoreException

# ----------------------- #


class _Row(BaseModel):
    """Minimal read model used to drive the ``return_model`` branches."""

    id: str


# ....................... #


class FakeReadGateway:
    """In-memory stand-in for ``DocumentReadGatewayPort``.

    Records calls and returns whatever canned rows the test stages. Only the
    methods the pagination mixin invokes are implemented.
    """

    def __init__(
        self,
        *,
        find_many_results: list[list[Any]] | None = None,
        cursor_results: list[list[Any]] | None = None,
        count_value: int = 0,
        aggregates_count_value: int = 0,
    ) -> None:
        self._find_many_results = list(find_many_results or [])
        self._cursor_results = list(cursor_results or [])
        self._count_value = count_value
        self._aggregates_count_value = aggregates_count_value
        self.find_many_calls: list[dict[str, Any]] = []
        self.find_many_aggregates_calls: list[dict[str, Any]] = []
        self.cursor_calls: list[dict[str, Any]] = []

    # ....................... #

    def compile_filters(self, filters: Any) -> Any:
        return ("parsed", filters)

    # ....................... #

    async def count(self, filters: Any, *, parsed: Any = None) -> int:
        return self._count_value

    # ....................... #

    async def count_aggregates(
        self,
        filters: Any,
        *,
        aggregates: Any,
        parsed: Any = None,
    ) -> int:
        return self._aggregates_count_value

    # ....................... #

    async def find_many(self, **kwargs: Any) -> list[Any]:
        self.find_many_calls.append(kwargs)
        if self._find_many_results:
            return self._find_many_results.pop(0)
        return []

    # ....................... #

    async def find_many_aggregates(self, **kwargs: Any) -> list[Any]:
        self.find_many_aggregates_calls.append(kwargs)
        if self._find_many_results:
            return self._find_many_results.pop(0)
        return []

    # ....................... #

    async def find_many_with_cursor(self, filters: Any, **kwargs: Any) -> list[Any]:
        self.cursor_calls.append({"filters": filters, **kwargs})
        if self._cursor_results:
            return self._cursor_results.pop(0)
        return []


# ....................... #


class PaginationHarness(DocumentPaginationMixin[_Row]):
    """Concrete mixin host wiring the abstract hooks to simple values."""

    def __init__(
        self,
        gateway: FakeReadGateway,
        *,
        read_fields: frozenset[str] = frozenset({"id"}),
        eff_batch_size: int = 2,
        max_scan_pages: int | None = None,
        max_stream_pages: int | None = None,
        enforce_primary_key_cursor_sort: bool = False,
        stream_chunk_override: int | None = None,
        default_sorts: dict[str, str] | None = None,
    ) -> None:
        self.read_gw = gateway  # type: ignore[assignment]
        self.enforce_primary_key_cursor_sort = enforce_primary_key_cursor_sort
        self._read_fields_value = read_fields
        self._eff_batch_size = eff_batch_size
        self._max_scan_pages = max_scan_pages
        self._max_stream_pages = max_stream_pages
        self._stream_chunk_override = stream_chunk_override
        self._default_sorts = default_sorts or {"id": "asc"}

    # ....................... #

    @property
    def _read_fields(self) -> frozenset[str]:
        return self._read_fields_value

    @property
    def eff_batch_size(self) -> int:
        return self._eff_batch_size

    @property
    def max_scan_pages(self) -> int | None:
        return self._max_scan_pages

    @property
    def max_stream_pages(self) -> int | None:
        return self._max_stream_pages

    def _eff_stream_chunk_size(self, chunk_size: int) -> int:
        if self._stream_chunk_override is not None:
            return self._stream_chunk_override
        return chunk_size

    def _resolve_sorts(self, sorts: Any) -> Any:
        return sorts if sorts else dict(self._default_sorts)


# ....................... #


def _offset_query(
    *,
    return_count: bool = False,
    aggregates: Any = None,
    return_model: type[Any] | None = None,
    return_fields: Sequence[str] | None = None,
) -> OffsetQuery:
    return OffsetQuery(
        return_count=return_count,
        aggregates=aggregates,
        return_model=return_model,
        return_fields=return_fields,
    )


# ----------------------- #
# _offset_page


@pytest.mark.asyncio
async def test_offset_page_rejects_aggregates_with_return_fields() -> None:
    harness = PaginationHarness(FakeReadGateway())

    with pytest.raises(CoreException, match="Aggregates cannot be combined"):
        await harness._offset_page(
            _offset_query(aggregates={"total": {"$sum": "amount"}}, return_fields=["id"]),
            filters=None,
            pagination=None,
            sorts=None,
        )


# ....................... #


@pytest.mark.asyncio
async def test_offset_page_count_zero_short_circuits() -> None:
    gateway = FakeReadGateway(count_value=0)
    harness = PaginationHarness(gateway)

    page = await harness._offset_page(
        _offset_query(return_count=True),
        filters=None,
        pagination={"limit": 5, "offset": 0},
        sorts=None,
    )

    assert page.hits == []
    assert page.count == 0
    # short-circuit: no row fetch happened
    assert gateway.find_many_calls == []


# ....................... #


@pytest.mark.asyncio
async def test_offset_page_with_limit_and_count() -> None:
    gateway = FakeReadGateway(
        find_many_results=[[{"id": "a"}, {"id": "b"}]],
        count_value=7,
    )
    harness = PaginationHarness(gateway)

    page = await harness._offset_page(
        _offset_query(return_count=True),
        filters={"k": "v"},
        pagination={"limit": 2, "offset": 4},
        sorts={"id": "asc"},
    )

    assert page.count == 7
    assert [r["id"] for r in page.hits] == ["a", "b"]
    # single windowed fetch, limit/offset forwarded verbatim
    assert len(gateway.find_many_calls) == 1
    assert gateway.find_many_calls[0]["limit"] == 2
    assert gateway.find_many_calls[0]["offset"] == 4


# ....................... #


@pytest.mark.asyncio
async def test_offset_page_with_limit_no_count() -> None:
    gateway = FakeReadGateway(find_many_results=[[{"id": "a"}]])
    harness = PaginationHarness(gateway)

    page = await harness._offset_page(
        _offset_query(return_count=False),
        filters=None,
        pagination={"limit": 10},
        sorts=None,
    )

    # CountlessPage has no count attribute
    assert not hasattr(page, "count")
    assert [r["id"] for r in page.hits] == ["a"]


# ....................... #


@pytest.mark.asyncio
async def test_offset_page_scan_loop_paginates_until_short_batch() -> None:
    # batch size 2: full batch, then short batch terminates the scan
    gateway = FakeReadGateway(
        find_many_results=[
            [{"id": "a"}, {"id": "b"}],
            [{"id": "c"}],
        ],
    )
    harness = PaginationHarness(gateway, eff_batch_size=2)

    page = await harness._offset_page(
        _offset_query(),
        filters=None,
        pagination=None,
        sorts=None,
    )

    assert [r["id"] for r in page.hits] == ["a", "b", "c"]
    assert len(gateway.find_many_calls) == 2
    assert gateway.find_many_calls[0]["offset"] == 0
    assert gateway.find_many_calls[1]["offset"] == 2


# ....................... #


@pytest.mark.asyncio
async def test_offset_page_scan_respects_max_scan_pages_cap() -> None:
    # Always returns a full batch -> would loop forever without the cap.
    gateway = FakeReadGateway(
        find_many_results=[[{"id": "x"}, {"id": "y"}]] * 5,
    )
    harness = PaginationHarness(gateway, eff_batch_size=2, max_scan_pages=2)

    with pytest.raises(CoreException, match="max_pages=2"):
        await harness._offset_page(
            _offset_query(),
            filters=None,
            pagination={"offset": 10},
            sorts=None,
        )


# ....................... #


@pytest.mark.asyncio
async def test_offset_page_scan_uses_initial_offset() -> None:
    gateway = FakeReadGateway(find_many_results=[[{"id": "a"}]])
    harness = PaginationHarness(gateway, eff_batch_size=2)

    await harness._offset_page(
        _offset_query(),
        filters=None,
        pagination={"offset": 6},
        sorts=None,
    )

    assert gateway.find_many_calls[0]["offset"] == 6


# ....................... #


@pytest.mark.asyncio
async def test_offset_page_aggregates_with_limit() -> None:
    gateway = FakeReadGateway(
        find_many_results=[[{"id": "a", "total": 5}]],
        aggregates_count_value=3,
    )
    harness = PaginationHarness(gateway)

    page = await harness._offset_page(
        _offset_query(
            return_count=True,
            aggregates={"total": {"$sum": "amount"}},
        ),
        filters=None,
        pagination={"limit": 5},
        sorts=None,
    )

    assert page.count == 3
    assert len(gateway.find_many_aggregates_calls) == 1


# ....................... #


@pytest.mark.asyncio
async def test_offset_page_aggregates_scan_loop() -> None:
    gateway = FakeReadGateway(
        find_many_results=[
            [{"id": "a"}, {"id": "b"}],
            [{"id": "c"}],
        ],
    )
    harness = PaginationHarness(gateway, eff_batch_size=2)

    page = await harness._offset_page(
        _offset_query(aggregates={"total": {"$sum": "amount"}}),
        filters=None,
        pagination=None,
        sorts=None,
    )

    assert [r["id"] for r in page.hits] == ["a", "b", "c"]
    assert len(gateway.find_many_aggregates_calls) == 2


# ----------------------- #
# _cursor_page


@pytest.mark.asyncio
async def test_cursor_page_rejects_return_model_with_return_fields() -> None:
    harness = PaginationHarness(FakeReadGateway())

    with pytest.raises(CoreException, match="cannot be combined"):
        await harness._cursor_page(
            CursorQuery(return_model=_Row, return_fields=["id"]),
            filters=None,
            cursor=None,
            sorts=None,
        )


# ....................... #


@pytest.mark.asyncio
async def test_cursor_page_strict_primary_key_rejects_non_id_sort() -> None:
    harness = PaginationHarness(
        FakeReadGateway(),
        read_fields=frozenset({"id", "name"}),
        enforce_primary_key_cursor_sort=True,
    )

    with pytest.raises(CoreException, match="strict"):
        await harness._cursor_page(
            CursorQuery(return_model=None, return_fields=None),
            filters=None,
            cursor=None,
            sorts={"name": "asc"},
        )


# ....................... #


@pytest.mark.asyncio
async def test_cursor_page_strict_primary_key_allows_id_sort() -> None:
    gateway = FakeReadGateway(cursor_results=[[{"id": "a"}]])
    harness = PaginationHarness(
        gateway,
        enforce_primary_key_cursor_sort=True,
    )

    page = await harness._cursor_page(
        CursorQuery(return_model=None, return_fields=None),
        filters=None,
        cursor={"limit": 5},
        sorts={"id": "asc"},
    )

    assert isinstance(page, CursorPage)
    assert [r["id"] for r in page.hits] == ["a"]
    assert page.has_more is False


# ....................... #


@pytest.mark.asyncio
async def test_cursor_page_return_model_branch() -> None:
    gateway = FakeReadGateway(cursor_results=[[_Row(id="a"), _Row(id="b")]])
    harness = PaginationHarness(gateway)

    page = await harness._cursor_page(
        CursorQuery(return_model=_Row, return_fields=None),
        filters=None,
        cursor={"limit": 5},
        sorts={"id": "asc"},
    )

    assert all(isinstance(h, _Row) for h in page.hits)
    assert [h.id for h in page.hits] == ["a", "b"]


# ....................... #


@pytest.mark.asyncio
async def test_cursor_page_return_model_dumps_for_cursor_token() -> None:
    # over-fetch (3 > limit 2) forces token encoding, which dumps the model row
    gateway = FakeReadGateway(
        cursor_results=[[_Row(id="a"), _Row(id="b"), _Row(id="c")]],
    )
    harness = PaginationHarness(gateway)

    page = await harness._cursor_page(
        CursorQuery(return_model=_Row, return_fields=None),
        filters=None,
        cursor={"limit": 2},
        sorts={"id": "asc"},
    )

    assert page.has_more is True
    assert page.next_cursor is not None
    assert [h.id for h in page.hits] == ["a", "b"]


# ....................... #


@pytest.mark.asyncio
async def test_cursor_page_return_fields_branch() -> None:
    gateway = FakeReadGateway(cursor_results=[[{"id": "a"}]])
    harness = PaginationHarness(gateway)

    page = await harness._cursor_page(
        CursorQuery(return_model=None, return_fields=["id"]),
        filters=None,
        cursor={"limit": 5},
        sorts={"id": "asc"},
    )

    assert page.hits == [{"id": "a"}]


# ....................... #


@pytest.mark.asyncio
async def test_cursor_page_has_more_emits_next_cursor() -> None:
    # over-fetch: limit=2 but 3 rows returned -> has_more True, next token set
    gateway = FakeReadGateway(
        cursor_results=[[{"id": "a"}, {"id": "b"}, {"id": "c"}]],
    )
    harness = PaginationHarness(gateway)

    page = await harness._cursor_page(
        CursorQuery(return_model=None, return_fields=None),
        filters=None,
        cursor={"limit": 2},
        sorts={"id": "asc"},
    )

    assert page.has_more is True
    assert page.next_cursor is not None
    assert [r["id"] for r in page.hits] == ["a", "b"]


# ----------------------- #
# _stream


async def _drain(gen: Any) -> list[Any]:
    chunks = []
    async for chunk in gen:
        chunks.append(chunk)
    return chunks


# ....................... #


@pytest.mark.asyncio
async def test_stream_empty_first_page_breaks_immediately() -> None:
    gateway = FakeReadGateway(cursor_results=[[]])
    harness = PaginationHarness(gateway)

    chunks = await _drain(
        harness._stream(
            StreamQuery(return_model=None, return_fields=None),
            filters=None,
            sorts={"id": "asc"},
            chunk_size=2,
        )
    )

    assert chunks == []


# ....................... #


@pytest.mark.asyncio
async def test_stream_single_page_no_more() -> None:
    # exactly chunk rows, no over-fetch -> has_more False -> one yield then stop
    gateway = FakeReadGateway(cursor_results=[[{"id": "a"}, {"id": "b"}]])
    harness = PaginationHarness(gateway)

    chunks = await _drain(
        harness._stream(
            StreamQuery(return_model=None, return_fields=None),
            filters=None,
            sorts={"id": "asc"},
            chunk_size=2,
        )
    )

    assert len(chunks) == 1
    assert [r["id"] for r in chunks[0]] == ["a", "b"]


# ....................... #


@pytest.mark.asyncio
async def test_stream_advances_cursor_across_pages() -> None:
    # page 1 over-fetches (3 > limit 2) -> has_more, advance; page 2 terminal
    gateway = FakeReadGateway(
        cursor_results=[
            [{"id": "a"}, {"id": "b"}, {"id": "c"}],
            [{"id": "c"}, {"id": "d"}],
        ],
    )
    harness = PaginationHarness(gateway)

    chunks = await _drain(
        harness._stream(
            StreamQuery(return_model=None, return_fields=None),
            filters=None,
            sorts={"id": "asc"},
            chunk_size=2,
        )
    )

    assert [r["id"] for chunk in chunks for r in chunk] == ["a", "b", "c", "d"]
    # second call carried an "after" token derived from the first page
    assert gateway.cursor_calls[1]["cursor"].get("after") is not None


# ....................... #


@pytest.mark.asyncio
async def test_stream_uses_eff_chunk_size_override() -> None:
    gateway = FakeReadGateway(cursor_results=[[{"id": "a"}]])
    harness = PaginationHarness(gateway, stream_chunk_override=7)

    await _drain(
        harness._stream(
            StreamQuery(return_model=None, return_fields=None),
            filters=None,
            sorts={"id": "asc"},
            chunk_size=2,
        )
    )

    # cursor limit reflects the overridden effective chunk size
    assert gateway.cursor_calls[0]["cursor"]["limit"] == 7


# ....................... #


@pytest.mark.asyncio
async def test_stream_return_model_branch() -> None:
    gateway = FakeReadGateway(cursor_results=[[_Row(id="a")]])
    harness = PaginationHarness(gateway)

    chunks = await _drain(
        harness._stream(
            StreamQuery(return_model=_Row, return_fields=None),
            filters=None,
            sorts={"id": "asc"},
            chunk_size=2,
        )
    )

    assert isinstance(chunks[0][0], _Row)


# ....................... #


@pytest.mark.asyncio
async def test_stream_return_fields_branch() -> None:
    gateway = FakeReadGateway(cursor_results=[[{"id": "a"}]])
    harness = PaginationHarness(gateway)

    chunks = await _drain(
        harness._stream(
            StreamQuery(return_model=None, return_fields=["id"]),
            filters=None,
            sorts={"id": "asc"},
            chunk_size=2,
        )
    )

    assert chunks[0] == [{"id": "a"}]


# ....................... #


@pytest.mark.asyncio
async def test_stream_respects_max_stream_pages_cap() -> None:
    # Each page over-fetches so the stream would never stop on its own.
    gateway = FakeReadGateway(
        cursor_results=[
            [{"id": "a"}, {"id": "b"}, {"id": "c"}],
            [{"id": "c"}, {"id": "d"}, {"id": "e"}],
            [{"id": "e"}, {"id": "f"}, {"id": "g"}],
        ],
    )
    harness = PaginationHarness(gateway, max_stream_pages=1)

    with pytest.raises(CoreException, match="max_pages=1"):
        await _drain(
            harness._stream(
                StreamQuery(return_model=None, return_fields=None),
                filters=None,
                sorts={"id": "asc"},
                chunk_size=2,
            )
        )


# ....................... #


@pytest.mark.asyncio
async def test_stream_detects_stalled_cursor() -> None:
    # Both pages over-fetch with identical rows -> identical next cursor ->
    # the stall guard must trip on the second advance.
    gateway = FakeReadGateway(
        cursor_results=[
            [{"id": "a"}, {"id": "b"}, {"id": "z"}],
            [{"id": "a"}, {"id": "b"}, {"id": "z"}],
        ],
    )
    harness = PaginationHarness(gateway)

    with pytest.raises(CoreException, match="did not advance"):
        await _drain(
            harness._stream(
                StreamQuery(return_model=None, return_fields=None),
                filters=None,
                sorts={"id": "asc"},
                chunk_size=2,
            )
        )
