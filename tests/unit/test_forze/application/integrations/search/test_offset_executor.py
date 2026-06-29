"""Unit tests for :mod:`forze.application.integrations.search.offset_executor`."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import (
    SearchResultSnapshotSpec,
    SearchSpec,
)
from forze.application.integrations.search import SearchResultSnapshot
from forze.application.integrations.search.offset_executor import (
    OffsetFetchWindow,
    OffsetRowsResult,
    execute_simple_offset_search_with_snapshot,
    offset_from_dict,
)
from forze_mock.adapters.search.snapshot import MockSearchResultSnapshotAdapter
from forze_mock.state import MockState


class _Hit(BaseModel):
    id: UUID
    label: str


def _make_rows(n: int) -> list[dict[str, Any]]:
    return [
        {"id": f"00000000-0000-0000-0000-{i:012d}", "label": f"row-{i}"}
        for i in range(1, n + 1)
    ]


class _WindowedHooks:
    """Serves rows in honoured offset/limit windows, counting fetch calls."""

    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows
        self.fetch_rows_calls = 0
        self.windows: list[tuple[int, int | None]] = []

    async def fetch_count(self) -> int | None:
        return None

    async def fetch_rows(
        self,
        window: OffsetFetchWindow,
        *,
        want_snap: bool,
    ) -> OffsetRowsResult:
        self.fetch_rows_calls += 1
        self.windows.append((window.fetch_offset, window.fetch_limit))
        _ = want_snap
        end = (
            window.fetch_offset + window.fetch_limit
            if window.fetch_limit is not None
            else len(self._rows)
        )
        return OffsetRowsResult(rows=self._rows[window.fetch_offset : end])


def _snapshot_over_mock(chunk_size: int = 2, max_ids: int = 50_000) -> tuple[
    SearchResultSnapshot, SearchResultSnapshotSpec
]:
    rs_spec = SearchResultSnapshotSpec(
        name="snap", enabled=True, chunk_size=chunk_size, max_ids=max_ids
    )
    store = MockSearchResultSnapshotAdapter(state=MockState(), spec=rs_spec)
    return SearchResultSnapshot(store=store), rs_spec


async def _run_offset(
    *,
    spec: SearchSpec[Any],
    hooks: _WindowedHooks,
    result_snapshot: SearchResultSnapshot | None,
    pagination: dict[str, Any],
    snapshot: dict[str, Any] | None = None,
) -> Any:
    return await execute_simple_offset_search_with_snapshot(
        query="q",
        filters=None,
        sorts=None,
        spec=spec,
        variant="offset",
        fingerprint_extras=None,
        pagination=pagination,
        snapshot=snapshot,
        return_count=True,
        return_type=None,
        return_fields=None,
        model_type=_Hit,
        codec=spec.resolved_read_codec,
        result_snapshot=result_snapshot,
        hooks=hooks,
    )


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


# --- streaming snapshot build --------------------------------------------- #


@pytest.mark.parametrize("pool_size", [1, 3, 4, 5])
@pytest.mark.asyncio
async def test_streaming_snapshot_writes_full_pool_and_replays(pool_size: int) -> None:
    """Snapshot write streams windows into the store; replay returns the full pool in order."""

    spec = SearchSpec(
        name="t",
        model_type=_Hit,
        fields=["id", "label"],
        snapshot=SearchResultSnapshotSpec(name="snap", enabled=True, chunk_size=2),
    )
    rows = _make_rows(pool_size)
    result_snapshot, _ = _snapshot_over_mock(chunk_size=2)
    hooks = _WindowedHooks(rows)

    page = await _run_offset(
        spec=spec,
        hooks=hooks,
        result_snapshot=result_snapshot,
        pagination={"limit": 2, "offset": 0},
    )

    # Fetched in chunk_size windows (not one giant fetch) and the run is complete.
    assert hooks.fetch_rows_calls >= 1
    assert all(limit == 2 for _off, limit in hooks.windows)

    handle = page.snapshot
    assert handle is not None
    assert handle.total == pool_size
    assert len(page.hits) == min(2, pool_size)
    assert page.hits[0].label == "row-1"

    # The stored keys round-trip to the full ordered pool.
    stored = await result_snapshot.store.get_id_range(
        handle.id, 0, pool_size + 5, expected_fingerprint=handle.fingerprint
    )
    assert stored is not None
    expected = [
        SearchResultSnapshot.result_record_key_string(_Hit.model_validate(r))
        for r in rows
    ]
    assert stored == expected

    # Replaying the snapshot (second request carrying the handle) serves the same order.
    replay = await _run_offset(
        spec=spec,
        hooks=_WindowedHooks(rows),
        result_snapshot=result_snapshot,
        pagination={"limit": pool_size, "offset": 0},
        snapshot={"id": handle.id, "fingerprint": handle.fingerprint},
    )
    assert [h.label for h in replay.hits] == [f"row-{i}" for i in range(1, pool_size + 1)]


@pytest.mark.asyncio
async def test_streaming_snapshot_caps_at_max_ids() -> None:
    """The pool is bounded by ``max_ids`` even when the backend has more rows."""

    spec = SearchSpec(
        name="t",
        model_type=_Hit,
        fields=["id", "label"],
        snapshot=SearchResultSnapshotSpec(
            name="snap", enabled=True, chunk_size=2, max_ids=3
        ),
    )
    rows = _make_rows(10)
    result_snapshot, _ = _snapshot_over_mock(chunk_size=2, max_ids=3)
    hooks = _WindowedHooks(rows)

    page = await _run_offset(
        spec=spec,
        hooks=hooks,
        result_snapshot=result_snapshot,
        pagination={"limit": 2, "offset": 0},
    )

    handle = page.snapshot
    assert handle is not None
    assert handle.total == 3

    stored = await result_snapshot.store.get_id_range(
        handle.id, 0, 100, expected_fingerprint=handle.fingerprint
    )
    assert stored is not None
    assert len(stored) == 3
    # Never fetched beyond the cap.
    assert sum(len(rows[off : off + (lim or 0)]) for off, lim in hooks.windows) <= 3 + 2


@pytest.mark.asyncio
async def test_streaming_snapshot_deep_page_offset() -> None:
    """A page offset inside a later window still slices the correct hits."""

    spec = SearchSpec(
        name="t",
        model_type=_Hit,
        fields=["id", "label"],
        snapshot=SearchResultSnapshotSpec(name="snap", enabled=True, chunk_size=2),
    )
    rows = _make_rows(6)
    result_snapshot, _ = _snapshot_over_mock(chunk_size=2)
    hooks = _WindowedHooks(rows)

    page = await _run_offset(
        spec=spec,
        hooks=hooks,
        result_snapshot=result_snapshot,
        pagination={"limit": 2, "offset": 3},
    )

    assert page.snapshot is not None
    assert page.snapshot.total == 6
    assert [h.label for h in page.hits] == ["row-4", "row-5"]
