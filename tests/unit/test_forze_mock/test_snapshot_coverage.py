"""Coverage tests for :class:`MockSearchResultSnapshotAdapter`."""

from __future__ import annotations

from datetime import timedelta

import pytest

from forze.application.contracts.search import SearchResultSnapshotSpec
from forze.base.exceptions import CoreException
from forze_mock.adapters.search.snapshot import MockSearchResultSnapshotAdapter
from forze_mock.state import MockState

# ----------------------- #


def _port(state: MockState | None = None) -> MockSearchResultSnapshotAdapter:
    return MockSearchResultSnapshotAdapter(
        state=state or MockState(),
        spec=SearchResultSnapshotSpec(name="snap", chunk_size=2),
    )


# ----------------------- #


async def test_put_run_invalid_chunk_size() -> None:
    port = _port()
    with pytest.raises(CoreException):
        await port.put_run(
            run_id="r", fingerprint="fp", ordered_ids=["a"], chunk_size=0
        )


async def test_put_run_empty_ordered_ids() -> None:
    port = _port()
    await port.put_run(run_id="r", fingerprint="fp", ordered_ids=[])
    meta = await port.get_meta("r")
    assert meta is not None
    assert meta.total == 0
    assert meta.complete is True


async def test_append_chunk_missing_begin_run() -> None:
    port = _port()
    with pytest.raises(CoreException):
        await port.append_chunk(
            run_id="r", chunk_index=0, ids=["a", "b"], is_last=True
        )


async def test_append_chunk_completed_run() -> None:
    port = _port()
    await port.put_run(run_id="r", fingerprint="fp", ordered_ids=["a", "b"])
    # Run is complete now; appending again must fail.
    with pytest.raises(CoreException):
        await port.append_chunk(
            run_id="r", chunk_index=1, ids=["c"], is_last=True
        )


async def test_append_chunk_wrong_index() -> None:
    port = _port()
    await port.begin_run(run_id="r", fingerprint="fp", chunk_size=2)
    with pytest.raises(CoreException):
        await port.append_chunk(
            run_id="r", chunk_index=5, ids=["a", "b"], is_last=True
        )


async def test_append_chunk_oversized() -> None:
    port = _port()
    await port.begin_run(run_id="r", fingerprint="fp", chunk_size=2)
    with pytest.raises(CoreException):
        await port.append_chunk(
            run_id="r", chunk_index=0, ids=["a", "b", "c"], is_last=True
        )


async def test_append_chunk_incomplete_non_final() -> None:
    port = _port()
    await port.begin_run(run_id="r", fingerprint="fp", chunk_size=2)
    with pytest.raises(CoreException):
        await port.append_chunk(
            run_id="r", chunk_index=0, ids=["a"], is_last=False
        )


async def test_append_chunk_multi_chunk_flow() -> None:
    port = _port()
    await port.begin_run(run_id="r", fingerprint="fp", chunk_size=2)
    await port.append_chunk(run_id="r", chunk_index=0, ids=["a", "b"], is_last=False)
    await port.append_chunk(run_id="r", chunk_index=1, ids=["c"], is_last=True)
    meta = await port.get_meta("r")
    assert meta is not None
    assert meta.total == 3


async def test_get_id_range_invalid_offset() -> None:
    port = _port()
    with pytest.raises(CoreException):
        await port.get_id_range("r", -1, 2)


async def test_get_id_range_invalid_limit() -> None:
    port = _port()
    with pytest.raises(CoreException):
        await port.get_id_range("r", 0, 0)


async def test_get_id_range_missing_run() -> None:
    port = _port()
    assert await port.get_id_range("missing", 0, 2) is None


async def test_get_id_range_incomplete_run() -> None:
    port = _port()
    await port.begin_run(run_id="r", fingerprint="fp", chunk_size=2)
    assert await port.get_id_range("r", 0, 2) is None


async def test_get_id_range_fingerprint_mismatch() -> None:
    port = _port()
    await port.put_run(run_id="r", fingerprint="fp", ordered_ids=["a", "b"])
    assert await port.get_id_range("r", 0, 2, expected_fingerprint="other") is None


async def test_get_id_range_offset_beyond_total() -> None:
    port = _port()
    await port.put_run(run_id="r", fingerprint="fp", ordered_ids=["a", "b"])
    assert await port.get_id_range("r", 99, 2) == []


async def test_get_id_range_spans_chunks() -> None:
    port = _port()
    await port.put_run(
        run_id="r", fingerprint="fp", ordered_ids=["a", "b", "c", "d", "e"]
    )
    assert await port.get_id_range("r", 1, 3) == ["b", "c", "d"]


async def test_put_run_explicit_ttl() -> None:
    """An explicit per-call ttl takes precedence (resolve_ttl explicit branch)."""
    port = _port()
    await port.put_run(
        run_id="r",
        fingerprint="fp",
        ordered_ids=["a", "b"],
        ttl=timedelta(seconds=10),
    )
    meta = await port.get_meta("r")
    assert meta is not None
    assert meta.total == 2


async def test_resolve_defaults_from_adapter() -> None:
    """default_ttl / default_chunk_size fallbacks (resolve helpers)."""
    state = MockState()
    port = MockSearchResultSnapshotAdapter(
        state=state,
        spec=SearchResultSnapshotSpec(name="snap", chunk_size=5),
        default_ttl=timedelta(minutes=1),
        default_chunk_size=2,
    )
    # No per-call ttl/chunk_size -> adapter defaults are used (chunk_size=2).
    await port.put_run(
        run_id="r", fingerprint="fp", ordered_ids=["a", "b", "c"]
    )
    meta = await port.get_meta("r")
    assert meta is not None
    assert meta.chunk_size == 2
    assert meta.total == 3


async def test_get_id_range_chunk_index_beyond_num_chunks() -> None:
    """Defensive break when meta.total overstates available chunks."""
    state = MockState()
    port = _port(state)
    # Inject a complete-but-inconsistent run: total claims 10 ids but only
    # one chunk index exists.
    port._meta_store()["r"] = {  # type: ignore[index]
        "fingerprint": "fp",
        "chunk_size": 2,
        "total": 10,
        "num_chunks": 1,
        "complete": True,
    }
    port._chunk_store()[("r", 0)] = ["a", "b"]  # type: ignore[index]
    # offset 4 -> ci = 2 >= num_chunks (1) -> break with what was collected.
    assert await port.get_id_range("r", 4, 4) == []


async def test_get_id_range_empty_chunk_break() -> None:
    """Defensive break when a referenced chunk is empty (take <= 0)."""
    state = MockState()
    port = _port(state)
    port._meta_store()["r"] = {  # type: ignore[index]
        "fingerprint": "fp",
        "chunk_size": 2,
        "total": 4,
        "num_chunks": 2,
        "complete": True,
    }
    port._chunk_store()[("r", 0)] = []  # type: ignore[index]
    assert await port.get_id_range("r", 0, 4) == []
