"""Unit tests for :mod:`forze_postgres.adapters.search.result_snapshot_ops`."""

from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import (
    SearchResultSnapshotMeta,
    SearchResultSnapshotSpec,
    SearchSpec,
)
from forze_postgres.adapters.search.result_snapshot_ops import (
    hydrate_result_row_key,
    hub_search_fingerprint,
    put_simple_result_snapshot,
    read_hub_result_snapshot,
    read_simple_result_snapshot,
    result_row_key_string,
    should_write_result_snapshot,
    simple_search_fingerprint,
    snapshot_sql_pagination,
)


class _Row(BaseModel):
    id: int
    name: str = "x"


def _rs_spec() -> SearchResultSnapshotSpec:
    return SearchResultSnapshotSpec(
        name="snap",
        enabled=True,
        ttl=timedelta(minutes=5),
        max_ids=100,
        chunk_size=10,
    )


def test_should_write_delegates_to_federation_rules() -> None:
    sp = _rs_spec()
    assert should_write_result_snapshot({"mode": True}, sp) is True
    assert should_write_result_snapshot({"mode": False}, sp) is False
    assert should_write_result_snapshot(None, sp) is True


def test_result_row_key_round_trip() -> None:
    r = _Row(id=1, name="a")
    key = result_row_key_string(r)
    assert "a" in key
    out = hydrate_result_row_key(key, _Row)
    assert out == r


def test_fingerprints_accepts_string_or_list_query() -> None:
    fp1 = simple_search_fingerprint("q", None, None, spec_name="s", variant="fts")
    fp2 = simple_search_fingerprint(["a", "b"], None, None, spec_name="s", variant="fts")
    assert fp1 != fp2
    h1 = hub_search_fingerprint(
        "q",
        None,
        None,
        spec_name="hub",
        members_weighted=[("a", 1.0)],
        score_merge="x",
        combine="y",
    )
    h2 = hub_search_fingerprint(
        ["a", "b"],
        None,
        None,
        spec_name="hub",
        members_weighted=[("a", 1.0)],
        score_merge="x",
        combine="y",
    )
    assert h1 != h2


def test_snapshot_sql_pagination() -> None:
    assert snapshot_sql_pagination(True, 500, None) == (500, 0, 20)
    assert snapshot_sql_pagination(True, 500, {"limit": 3}) == (500, 0, 3)
    assert snapshot_sql_pagination(False, 0, None) == (None, 0, 20)
    assert snapshot_sql_pagination(False, 0, {"limit": 7, "offset": 2}) == (7, 2, 7)


@pytest.mark.asyncio
async def test_read_simple_result_snapshot_no_options_or_id() -> None:
    sp = _rs_spec()
    spec = SearchSpec(name="t", model_type=_Row, fields=["id", "name"])
    assert (
        await read_simple_result_snapshot(
            store=MagicMock(),
            rs_spec=sp,
            snap_opt=None,
            fp_computed="fp",
            spec=spec,
            pagination={},
            return_type=None,
            return_fields=None,
            return_count=False,
        )
        is None
    )
    assert (
        await read_simple_result_snapshot(
            store=MagicMock(),
            rs_spec=sp,
            snap_opt={},
            fp_computed="fp",
            spec=spec,
            pagination={},
            return_type=None,
            return_fields=None,
            return_count=False,
        )
        is None
    )


@pytest.mark.asyncio
async def test_read_simple_result_snapshot_store_miss() -> None:
    store = MagicMock()
    store.get_id_range = AsyncMock(return_value=None)
    sp = _rs_spec()
    spec = SearchSpec(name="t", model_type=_Row, fields=["id", "name"])
    assert (
        await read_simple_result_snapshot(
            store=store,
            rs_spec=sp,
            snap_opt={"id": "r1"},
            fp_computed="fp",
            spec=spec,
            pagination={},
            return_type=None,
            return_fields=None,
            return_count=False,
        )
        is None
    )


@pytest.mark.asyncio
async def test_read_simple_result_snapshot_hydrate_paths() -> None:
    row = _Row(id=9, name="n")
    key = result_row_key_string(row)
    store = MagicMock()
    store.get_id_range = AsyncMock(return_value=[key])
    store.get_meta = AsyncMock(
        return_value=SearchResultSnapshotMeta(
            run_id="r1",
            fingerprint="meta-fp",
            total=1,
            chunk_size=1,
            complete=True,
        )
    )
    sp = _rs_spec()
    spec = SearchSpec(name="t", model_type=_Row, fields=["id", "name"])

    p_model = await read_simple_result_snapshot(
        store=store,
        rs_spec=sp,
        snap_opt={"id": "r1", "fingerprint": "meta-fp"},
        fp_computed="fallback",
        spec=spec,
        pagination={"offset": 0, "limit": 5},
        return_type=None,
        return_fields=None,
        return_count=True,
    )
    assert p_model.count == 1
    assert p_model.hits[0] == row

    class _Alt(BaseModel):
        id: int

    p_alt = await read_simple_result_snapshot(
        store=store,
        rs_spec=sp,
        snap_opt={"id": "r1"},
        fp_computed="fp",
        spec=spec,
        pagination={"offset": 0, "limit": 2},
        return_type=_Alt,
        return_fields=None,
        return_count=False,
    )
    assert p_alt.hits[0] == _Alt(id=9)

    p_fields = await read_simple_result_snapshot(
        store=store,
        rs_spec=sp,
        snap_opt={"id": "r1"},
        fp_computed="fp",
        spec=spec,
        pagination={},
        return_type=None,
        return_fields=["id"],
        return_count=True,
    )
    assert p_fields.hits == [{"id": 9}]

    store2 = MagicMock()
    store2.get_id_range = AsyncMock(return_value=[key])
    store2.get_meta = AsyncMock(return_value=None)
    p_incomplete = await read_simple_result_snapshot(
        store=store2,
        rs_spec=sp,
        snap_opt={"id": "r1"},
        fp_computed="fb",
        spec=spec,
        pagination={"offset": 1, "limit": 5},
        return_type=None,
        return_fields=None,
        return_count=False,
    )
    assert p_incomplete.snapshot is not None
    assert p_incomplete.snapshot.fingerprint == "fb"
    assert p_incomplete.snapshot.total == 2


@pytest.mark.asyncio
async def test_read_hub_result_snapshot_branches() -> None:
    row = _Row(id=3, name="h")
    key = result_row_key_string(row)
    store = MagicMock()
    store.get_id_range = AsyncMock(return_value=[key])
    store.get_meta = AsyncMock(
        return_value=SearchResultSnapshotMeta(
            run_id="h1",
            fingerprint="f",
            total=3,
            chunk_size=1,
            complete=False,
        )
    )
    sp = _rs_spec()

    p = await read_hub_result_snapshot(
        store=store,
        rs_spec=sp,
        snap_opt={"id": "h1"},
        fp_computed="fb",
        model_type=_Row,
        pagination={"offset": 0, "limit": 1},
        return_type=None,
        return_fields=None,
        return_count=True,
    )
    assert p is not None
    assert p.count == 1
    assert p.hits[0] == row

    p2 = await read_hub_result_snapshot(
        store=store,
        rs_spec=sp,
        snap_opt={"id": "h1"},
        fp_computed="fb",
        model_type=_Row,
        pagination={},
        return_type=None,
        return_fields=["name"],
        return_count=False,
    )
    assert p2.hits == [{"name": "h"}]

    assert (
        await read_hub_result_snapshot(
            store=store,
            rs_spec=sp,
            snap_opt=None,
            fp_computed="x",
            model_type=_Row,
            pagination={},
            return_type=None,
            return_fields=None,
            return_count=False,
        )
        is None
    )


@pytest.mark.asyncio
async def test_put_simple_result_snapshot() -> None:
    store = MagicMock()
    store.put_run = AsyncMock()
    sp = _rs_spec()
    hits = [_Row(id=i, name="z") for i in range(3)]
    h = await put_simple_result_snapshot(
        store,
        hits,
        snap_opt=None,
        rs_spec=sp,
        fp_computed="fp0",
        pool_len_before_cap=10,
    )
    assert h.capped is True
    assert h.total == 3
    store.put_run.assert_awaited_once()
    h2 = await put_simple_result_snapshot(
        store,
        hits,
        snap_opt=None,
        rs_spec=sp,
        fp_computed="fp0",
        pool_len_before_cap=3,
    )
    assert h2.capped is False
