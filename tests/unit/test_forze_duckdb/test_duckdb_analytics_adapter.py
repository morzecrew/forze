"""Tests for DuckDbAnalyticsAdapter over a real in-process engine."""

from __future__ import annotations

from typing import Any, Callable

import pytest

from pydantic import BaseModel

from forze.application.contracts.base import CountlessPage, CursorPage, Page

from tests.unit.test_forze_duckdb.conftest import Params

# ----------------------- #

_AdapterFactory = Callable[..., Any]


def _sql(parquet: str) -> str:
    return (
        f"SELECT day, total FROM read_parquet('{parquet}') "
        "WHERE total >= $min_total ORDER BY day"
    )


# ....................... #


async def test_run_filters_and_types_rows(
    events_parquet: str,
    make_adapter: _AdapterFactory,
) -> None:
    adapter = make_adapter(_sql(events_parquet))

    page = await adapter.run("by_day", Params(min_total=25))

    assert isinstance(page, CountlessPage)
    assert [(r.day, r.total) for r in page.hits] == [("c", 30), ("d", 40)]
    assert all(isinstance(r, BaseModel) for r in page.hits)


# ....................... #


async def test_run_page_attaches_total(
    events_parquet: str,
    make_adapter: _AdapterFactory,
) -> None:
    adapter = make_adapter(_sql(events_parquet))

    page = await adapter.run_page("by_day", Params(min_total=0), {"limit": 2, "offset": 1})

    assert isinstance(page, Page)
    assert [(r.day, r.total) for r in page.hits] == [("b", 20), ("c", 30)]
    assert page.count == 4


# ....................... #


async def test_skip_total_returns_countless(
    events_parquet: str,
    make_adapter: _AdapterFactory,
) -> None:
    adapter = make_adapter(_sql(events_parquet), skip_total=True)

    page = await adapter.run_page("by_day", Params(min_total=0), {"limit": 2})

    assert isinstance(page, CountlessPage)
    assert not isinstance(page, Page)


# ....................... #


async def test_project_run_returns_field_subset(
    events_parquet: str,
    make_adapter: _AdapterFactory,
) -> None:
    adapter = make_adapter(_sql(events_parquet))

    page = await adapter.project_run(["day"], "by_day", Params(min_total=30))

    assert page.hits == [{"day": "c"}, {"day": "d"}]


# ....................... #


async def test_select_run_uses_alternate_model(
    events_parquet: str,
    make_adapter: _AdapterFactory,
) -> None:
    from pydantic import BaseModel

    class _Slim(BaseModel):
        day: str

    adapter = make_adapter(_sql(events_parquet))

    page = await adapter.select_run(_Slim, "by_day", Params(min_total=30))

    assert [r.day for r in page.hits] == ["c", "d"]
    assert all(isinstance(r, _Slim) for r in page.hits)


# ....................... #


async def test_cursor_pages_forward(
    events_parquet: str,
    make_adapter: _AdapterFactory,
) -> None:
    adapter = make_adapter(_sql(events_parquet))

    # 4 rows, page size 3: page 1 is full (has_more), page 2 is short (terminal).
    first = await adapter.run_cursor("by_day", Params(min_total=0), {"limit": 3})

    assert isinstance(first, CursorPage)
    assert [r.day for r in first.hits] == ["a", "b", "c"]
    assert first.has_more
    assert first.next_cursor is not None

    second = await adapter.run_cursor(
        "by_day",
        Params(min_total=0),
        {"limit": 3, "after": first.next_cursor},
    )

    assert [r.day for r in second.hits] == ["d"]
    assert not second.has_more
    assert second.next_cursor is None


# ....................... #


async def test_run_chunked_batches_rows(
    events_parquet: str,
    make_adapter: _AdapterFactory,
) -> None:
    adapter = make_adapter(_sql(events_parquet))

    chunks = [
        [(r.day, r.total) for r in chunk]
        async for chunk in adapter.run_chunked(
            "by_day", Params(min_total=0), fetch_batch_size=2
        )
    ]

    assert chunks == [[("a", 10), ("b", 20)], [("c", 30), ("d", 40)]]


# ....................... #


async def test_unknown_query_key_raises(
    events_parquet: str,
    make_adapter: _AdapterFactory,
) -> None:
    adapter = make_adapter(_sql(events_parquet))

    with pytest.raises(Exception, match="Unknown analytics query key"):
        await adapter.run("missing", Params())


# ....................... #


async def test_max_rows_option_caps_result(
    events_parquet: str,
    make_adapter: _AdapterFactory,
) -> None:
    adapter = make_adapter(_sql(events_parquet))

    page = await adapter.run("by_day", Params(min_total=0), options={"max_rows": 1})

    assert [r.day for r in page.hits] == ["a"]


# ....................... #


async def test_dry_run_skips_execution(
    events_parquet: str,
    make_adapter: _AdapterFactory,
) -> None:
    adapter = make_adapter(_sql(events_parquet))

    page = await adapter.run("by_day", Params(min_total=0), options={"dry_run": True})

    assert page.hits == []
