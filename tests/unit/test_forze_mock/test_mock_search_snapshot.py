"""Tests for chunked mock search result snapshots."""

import pytest

from forze.application.contracts.search import SearchResultSnapshotSpec
from forze_mock.adapters.search.snapshot import MockSearchResultSnapshotAdapter
from forze_mock.state import MockState

# ----------------------- #


@pytest.mark.asyncio
async def test_snapshot_put_run_and_get_id_range() -> None:
    state = MockState()
    spec = SearchResultSnapshotSpec(name="snap", chunk_size=2)
    port = MockSearchResultSnapshotAdapter(state=state, spec=spec)

    await port.put_run(
        run_id="run-1",
        fingerprint="fp-1",
        ordered_ids=["a", "b", "c", "d", "e"],
        chunk_size=2,
    )

    meta = await port.get_meta("run-1")
    assert meta is not None
    assert meta.complete is True
    assert meta.total == 5

    page = await port.get_id_range("run-1", 2, 2, expected_fingerprint="fp-1")
    assert page == ["c", "d"]

    await port.delete_run("run-1")
    assert await port.get_meta("run-1") is None
