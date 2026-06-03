"""Unit tests for :mod:`forze.application.integrations.search.offset_executor`."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import SearchSpec
from forze.application.integrations.search.offset_executor import (
    OffsetFetchWindow,
    OffsetRowsResult,
    execute_simple_offset_search_with_snapshot,
    offset_from_dict,
)


class _Hit(BaseModel):
    id: UUID
    label: str


class _Hooks:
    def __init__(self, rows: list[dict[str, Any]], *, total: int | None = 2) -> None:
        self._rows = rows
        self._total = total
        self.fetch_count_calls = 0
        self.fetch_rows_calls = 0

    async def fetch_count(self) -> int | None:
        self.fetch_count_calls += 1
        return self._total

    async def fetch_rows(
        self,
        window: OffsetFetchWindow,
        *,
        want_snap: bool,
    ) -> OffsetRowsResult:
        self.fetch_rows_calls += 1
        _ = window, want_snap
        return OffsetRowsResult(rows=self._rows)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ({}, 0),
        ({"offset": 5}, 5),
        ({"offset": None}, 0),
    ],
)
def test_offset_from_dict(raw: dict[str, Any], expected: int) -> None:
    assert offset_from_dict(raw) == expected


@pytest.mark.asyncio
async def test_execute_simple_offset_search_empty_count_short_circuit() -> None:
    spec = SearchSpec(name="t", model_type=_Hit, fields=["id", "label"])
    hooks = _Hooks([], total=0)

    page = await execute_simple_offset_search_with_snapshot(
        query="q",
        filters=None,
        sorts=None,
        spec=spec,
        variant="v",
        fingerprint_extras=None,
        pagination={"limit": 10, "offset": 0},
        snapshot=None,
        return_count=True,
        return_type=None,
        return_fields=None,
        model_type=_Hit,
        codec=spec.resolved_read_codec,
        result_snapshot=None,
        hooks=hooks,
    )

    assert page.count == 0
    assert hooks.fetch_count_calls == 1
    assert hooks.fetch_rows_calls == 0


@pytest.mark.asyncio
async def test_execute_simple_offset_search_fetches_and_materializes() -> None:
    spec = SearchSpec(name="t", model_type=_Hit, fields=["id", "label"])
    rows = [
        {"id": "00000000-0000-0000-0000-000000000001", "label": "a"},
        {"id": "00000000-0000-0000-0000-000000000002", "label": "b"},
    ]
    hooks = _Hooks(rows)

    page = await execute_simple_offset_search_with_snapshot(
        query="q",
        filters=None,
        sorts=None,
        spec=spec,
        variant="v",
        fingerprint_extras=None,
        pagination={"limit": 10, "offset": 0},
        snapshot=None,
        return_count=True,
        return_type=None,
        return_fields=None,
        model_type=_Hit,
        codec=spec.resolved_read_codec,
        result_snapshot=None,
        hooks=hooks,
    )

    assert page.count == 2
    assert len(page.hits) == 2
    assert hooks.fetch_rows_calls == 1


@pytest.mark.asyncio
async def test_execute_simple_offset_search_with_nonzero_offset() -> None:
    spec = SearchSpec(name="t", model_type=_Hit, fields=["id", "label"])
    rows = [
        {"id": "00000000-0000-0000-0000-000000000001", "label": "a"},
        {"id": "00000000-0000-0000-0000-000000000002", "label": "b"},
    ]
    hooks = _Hooks(rows)

    async def fetch_rows(
        window: OffsetFetchWindow,
        *,
        want_snap: bool,
    ) -> OffsetRowsResult:
        hooks.fetch_rows_calls += 1
        assert window.fetch_offset == 1
        assert window.page_offset == 1
        assert window.page_limit == 1
        _ = want_snap
        return OffsetRowsResult(rows=rows[window.page_offset : window.page_offset + window.page_limit])

    hooks.fetch_rows = fetch_rows  # type: ignore[method-assign]

    page = await execute_simple_offset_search_with_snapshot(
        query="q",
        filters=None,
        sorts=None,
        spec=spec,
        variant="v",
        fingerprint_extras=None,
        pagination={"limit": 1, "offset": 1},
        snapshot=None,
        return_count=True,
        return_type=None,
        return_fields=None,
        model_type=_Hit,
        codec=spec.resolved_read_codec,
        result_snapshot=None,
        hooks=hooks,
    )

    assert page.count == 2
    assert len(page.hits) == 1
    assert page.hits[0].id == UUID("00000000-0000-0000-0000-000000000002")
    assert hooks.fetch_rows_calls == 1
