"""Pure unit tests for document pagination orchestration.

These exercise :class:`DocumentPaginationMixin` directly through a lightweight
in-memory fake read gateway (no Docker, no ``MockState``). The fake only
implements the handful of gateway methods the mixin actually calls and returns
canned rows so we can drive offset paging, keyset cursor paging and streaming
through their cap/limit/empty/boundary branches.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pytest
from pydantic import BaseModel, Field, field_validator

from forze.application.contracts.base import CursorPage
from forze.application.contracts.querying import decode_keyset_v1
from forze.application.integrations.document._pagination import (
    CursorQuery,
    DocumentPaginationMixin,
    OffsetQuery,
    StreamQuery,
)
from forze.base.exceptions import CoreException, ExceptionKind

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

    @property
    def model_type(self) -> type[_Row]:
        return _Row

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
    # batch size 2: full batch, then short batch terminates the scan. A projection without
    # the sort key cannot seek, so the scan pages by offset.
    gateway = FakeReadGateway(
        find_many_results=[
            [{"id": "a"}, {"id": "b"}],
            [{"id": "c"}],
        ],
    )
    harness = PaginationHarness(gateway, eff_batch_size=2)

    page = await harness._offset_page(
        _offset_query(return_fields=("name",)),
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
            _offset_query(return_fields=("name",)),
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
        _offset_query(return_fields=("name",)),
        filters=None,
        pagination={"offset": 6},
        sorts=None,
    )

    assert gateway.find_many_calls[0]["offset"] == 6


# ....................... #


@pytest.mark.asyncio
async def test_offset_page_scan_seeks_past_each_batch() -> None:
    # page 1 over-fetches (3 > batch 2) -> has_more; page 2 seeks past "b" and ends
    gateway = FakeReadGateway(
        cursor_results=[
            [{"id": "a"}, {"id": "b"}, {"id": "c"}],
            [{"id": "c"}],
        ],
    )
    harness = PaginationHarness(gateway, eff_batch_size=2)

    page = await harness._offset_page(_offset_query(), filters=None, pagination=None, sorts=None)

    assert [r["id"] for r in page.hits] == ["a", "b", "c"]
    assert gateway.find_many_calls == []
    assert [call["cursor"].get("after") is not None for call in gateway.cursor_calls] == [
        False,
        True,
    ]


# ....................... #


@pytest.mark.asyncio
async def test_offset_page_scan_seeks_then_skips_the_offset() -> None:
    gateway = FakeReadGateway(cursor_results=[[{"id": "a"}, {"id": "b"}]])
    harness = PaginationHarness(gateway, eff_batch_size=2)

    page = await harness._offset_page(
        _offset_query(), filters=None, pagination={"offset": 1}, sorts=None
    )

    assert [r["id"] for r in page.hits] == ["b"]


# ....................... #


@pytest.mark.parametrize(
    "pagination",
    [{"offset": -1}, {"offset": -1, "limit": 5}, {"offset": "abc"}, {"offset": "abc", "limit": 5}],
    ids=["unbounded", "limited", "text-unbounded", "text-limited"],
)
@pytest.mark.asyncio
async def test_offset_page_refuses_a_negative_offset(pagination: dict[str, int]) -> None:
    # Sliced, it would count from the end; sent on, a backend answers with a server error.
    gateway = FakeReadGateway(cursor_results=[[{"id": "a"}]], find_many_results=[[{"id": "a"}]])
    harness = PaginationHarness(gateway, eff_batch_size=2)

    with pytest.raises(CoreException, match="non-negative integer") as ei:
        await harness._offset_page(
            _offset_query(), filters=None, pagination=pagination, sorts=None
        )

    assert ei.value.kind == ExceptionKind.PRECONDITION

    assert (gateway.find_many_calls, gateway.cursor_calls) == ([], [])


# ....................... #


@pytest.mark.asyncio
async def test_offset_page_scan_breaks_ties_by_id() -> None:
    gateway = FakeReadGateway(cursor_results=[[{"id": "a", "grp": 1}]])
    harness = PaginationHarness(gateway, read_fields=frozenset({"id", "grp"}))

    await harness._offset_page(
        _offset_query(), filters=None, pagination=None, sorts={"grp": "desc"}
    )

    assert gateway.cursor_calls[0]["sorts"] == {"grp": "desc", "id": "desc"}


# ....................... #


@pytest.mark.parametrize(
    ("read_fields", "sorts", "strict"),
    [
        # no id to break ties with: no key is unique
        (frozenset({"grp"}), {"grp": "asc"}, False),
        # a strict primary-key cursor refuses any other sort
        (frozenset({"id", "grp"}), {"grp": "asc"}, True),
    ],
)
@pytest.mark.asyncio
async def test_offset_page_scan_pages_by_offset_when_it_cannot_seek(
    read_fields: frozenset[str], sorts: dict[str, str], strict: bool
) -> None:
    gateway = FakeReadGateway(find_many_results=[[{"id": "a", "grp": 1}]])
    harness = PaginationHarness(
        gateway, read_fields=read_fields, enforce_primary_key_cursor_sort=strict
    )

    await harness._offset_page(_offset_query(), filters=None, pagination=None, sorts=sorts)

    assert (len(gateway.find_many_calls), gateway.cursor_calls) == (1, [])


# ....................... #


@pytest.mark.asyncio
async def test_offset_page_scan_seeks_on_id_alone_when_the_sort_starts_with_it() -> None:
    # `id` is unique, so the keys after it never decide; an id-only cursor serves the read.
    gateway = FakeReadGateway(cursor_results=[[{"id": "a", "grp": 1}]])
    harness = PaginationHarness(
        gateway, read_fields=frozenset({"id", "grp"}), enforce_primary_key_cursor_sort=True
    )

    await harness._offset_page(
        _offset_query(), filters=None, pagination=None, sorts={"id": "asc", "grp": "desc"}
    )

    assert (len(gateway.cursor_calls), gateway.find_many_calls) == (1, [])


# ....................... #


@pytest.mark.asyncio
async def test_offset_page_scan_seeks_when_the_returned_model_carries_the_keys() -> None:
    gateway = FakeReadGateway(cursor_results=[[_Row(id="a")]])
    harness = PaginationHarness(gateway)

    page = await harness._offset_page(
        _offset_query(return_model=_Row), filters=None, pagination=None, sorts=None
    )

    assert ([r.id for r in page.hits], gateway.find_many_calls) == (["a"], [])


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


# ....................... #


@pytest.mark.asyncio
async def test_offset_page_scan_takes_a_string_offset() -> None:
    gateway = FakeReadGateway(cursor_results=[[{"id": "a"}, {"id": "b"}]])
    harness = PaginationHarness(gateway, eff_batch_size=2)

    page = await harness._offset_page(
        _offset_query(), filters=None, pagination={"offset": "1"}, sorts=None  # type: ignore[typeddict-item]
    )

    assert [r["id"] for r in page.hits] == ["b"]


class _Lowered(BaseModel):
    id: str
    name: str

    @field_validator("name")
    @classmethod
    def _lower(cls, value: str) -> str:
        return value.lower()


@pytest.mark.asyncio
async def test_cursor_page_takes_token_values_from_the_store() -> None:
    # The model lowercases `name`; the store orders by what it holds, so the token must too.
    gateway = FakeReadGateway(
        cursor_results=[[_Lowered(id=i, name=i.upper()) for i in ("a", "b", "c")]],
        find_many_results=[[{"id": i, "name": i.upper()} for i in ("a", "b", "c")]],
    )
    harness = PaginationHarness(gateway, read_fields=frozenset({"id", "name"}))

    page = await harness._cursor_page(
        CursorQuery(return_model=_Lowered, return_fields=None),
        filters=None,
        cursor={"limit": 2},
        sorts={"name": "asc"},
    )

    assert decode_keyset_v1(page.next_cursor)[3] == ["B", "b"]  # type: ignore[arg-type]
    (lookup,) = gateway.find_many_calls
    assert lookup["filters"] == {"$values": {"id": {"$in": ["a", "b", "c"]}}}
    assert lookup["return_fields"] == ["name", "id"]


@pytest.mark.asyncio
async def test_cursor_page_refuses_a_page_whose_edge_row_vanished() -> None:
    gateway = FakeReadGateway(
        cursor_results=[[_Lowered(id=i, name=i) for i in ("a", "b", "c")]],
        find_many_results=[[]],
    )
    harness = PaginationHarness(gateway, read_fields=frozenset({"id", "name"}))

    with pytest.raises(CoreException) as ei:
        await harness._cursor_page(
            CursorQuery(return_model=_Lowered, return_fields=None),
            filters=None,
            cursor={"limit": 2},
            sorts={"name": "asc"},
        )

    assert ei.value.kind == ExceptionKind.CONCURRENCY


@pytest.mark.asyncio
async def test_cursor_page_refuses_a_keyed_sort_whose_model_has_no_id() -> None:
    class _Named(BaseModel):
        name: str

    gateway = FakeReadGateway(cursor_results=[[_Named(name=i) for i in ("a", "b", "c")]])
    harness = PaginationHarness(gateway, read_fields=frozenset({"id", "name"}))

    with pytest.raises(CoreException, match="carry id"):
        await harness._cursor_page(
            CursorQuery(return_model=_Named, return_fields=None),
            filters=None,
            cursor={"limit": 2},
            sorts={"name": "asc"},
        )

    assert not harness._seekable(_offset_query(return_model=_Named), {"name": "asc", "id": "asc"})
    assert harness._seekable(_offset_query(return_model=_Row), {"name": "asc", "id": "asc"})


@pytest.mark.asyncio
async def test_cursor_page_without_id_reads_the_fields_not_the_dump() -> None:
    # No id to look stored values up by: the token reads the model's fields, never its dump.
    # Past the model, a mapping holds what the backend stored; a null parent reads as null.
    class _Bag(BaseModel):
        id: str
        meta: dict[str, int] | None = Field(default=None, exclude=True)

    class _BagGateway(FakeReadGateway):
        @property
        def model_type(self) -> type[_Bag]:  # type: ignore[override]
            return _Bag

    rows = [_Bag(id="a", meta={"rank": 1}), _Bag(id="b"), _Bag(id="c")]
    harness = PaginationHarness(_BagGateway(), read_fields=frozenset({"meta"}))
    tokens = []

    for limit in (1, 2):
        harness.read_gw = _BagGateway(cursor_results=[rows[: limit + 1]])  # type: ignore[assignment]
        page = await harness._cursor_page(
            CursorQuery(return_model=_Bag, return_fields=None),
            filters=None,
            cursor={"limit": limit},
            sorts={"meta.rank": "asc"},
        )
        tokens.append(decode_keyset_v1(page.next_cursor)[3])  # type: ignore[arg-type]

    assert tokens == [[1], [None]]
