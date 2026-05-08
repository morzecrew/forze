"""Tests for :class:`~forze.application.coordinators.SearchResultSnapshotCoordinator`."""

from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import (
    FederatedSearchSpec,
    SearchResultSnapshotMeta,
    SearchResultSnapshotSpec,
    SearchSpec,
)
from forze.application.coordinators import SearchResultSnapshotCoordinator
from forze.base.errors import CoreError


class _Row(BaseModel):
    id: int
    name: str = "x"


class _Hit(BaseModel):
    id: int
    t: str = ""


def _rs_spec() -> SearchResultSnapshotSpec:
    return SearchResultSnapshotSpec(
        name="snap",
        enabled=True,
        ttl=timedelta(minutes=5),
        max_ids=100,
        chunk_size=10,
    )


def _fed() -> FederatedSearchSpec[_Hit]:
    return FederatedSearchSpec(
        name="fed",
        members=(
            SearchSpec(name="a", model_type=_Hit, fields=["t"]),
            SearchSpec(name="b", model_type=_Hit, fields=["t"]),
        ),
    )


def test_should_write_delegates_to_snapshot_rules() -> None:
    sp = _rs_spec()
    coord = SearchResultSnapshotCoordinator(store=MagicMock())
    assert coord.should_write_result_snapshot({"mode": True}, sp) is True
    assert coord.should_write_result_snapshot({"mode": False}, sp) is False
    assert coord.should_write_result_snapshot(None, sp) is True


def test_result_record_key_round_trip() -> None:
    r = _Row(id=1, name="a")
    key = SearchResultSnapshotCoordinator.result_record_key_string(r)
    assert "a" in key
    out = SearchResultSnapshotCoordinator.hydrate_result_record_key(key, _Row)
    assert out == r


def test_fingerprints_accepts_string_or_list_query() -> None:
    fp1 = SearchResultSnapshotCoordinator.simple_search_fingerprint(
        "q", None, None, spec_name="s", variant="fts"
    )
    fp2 = SearchResultSnapshotCoordinator.simple_search_fingerprint(
        ["a", "b"], None, None, spec_name="s", variant="fts"
    )
    assert fp1 != fp2
    h1 = SearchResultSnapshotCoordinator.hub_search_fingerprint(
        "q",
        None,
        None,
        spec_name="hub",
        members_weighted=[("a", 1.0)],
        score_merge="x",
        combine="y",
    )
    h2 = SearchResultSnapshotCoordinator.hub_search_fingerprint(
        ["a", "b"],
        None,
        None,
        spec_name="hub",
        members_weighted=[("a", 1.0)],
        score_merge="x",
        combine="y",
    )
    assert h1 != h2


def test_snapshot_pagination() -> None:
    assert SearchResultSnapshotCoordinator.snapshot_pagination(True, 500, None) == (
        500,
        0,
        20,
    )
    assert SearchResultSnapshotCoordinator.snapshot_pagination(
        True, 500, {"limit": 3}
    ) == (500, 0, 3)
    assert SearchResultSnapshotCoordinator.snapshot_pagination(False, 0, None) == (
        None,
        0,
        20,
    )
    assert SearchResultSnapshotCoordinator.snapshot_pagination(
        False, 0, {"limit": 7, "offset": 2}
    ) == (7, 2, 7)


@pytest.mark.asyncio
async def test_read_simple_result_snapshot_no_options_or_id() -> None:
    sp = _rs_spec()
    spec = SearchSpec(name="t", model_type=_Row, fields=["id", "name"])
    coord = SearchResultSnapshotCoordinator(store=MagicMock())
    assert (
        await coord.read_simple_result_snapshot(
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
        await coord.read_simple_result_snapshot(
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
    coord = SearchResultSnapshotCoordinator(store=store)
    assert (
        await coord.read_simple_result_snapshot(
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
    key = SearchResultSnapshotCoordinator.result_record_key_string(row)
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
    coord = SearchResultSnapshotCoordinator(store=store)

    p_model = await coord.read_simple_result_snapshot(
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

    p_alt = await coord.read_simple_result_snapshot(
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

    p_fields = await coord.read_simple_result_snapshot(
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
    coord2 = SearchResultSnapshotCoordinator(store=store2)
    p_incomplete = await coord2.read_simple_result_snapshot(
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
    key = SearchResultSnapshotCoordinator.result_record_key_string(row)
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
    coord = SearchResultSnapshotCoordinator(store=store)

    p = await coord.read_hub_result_snapshot(
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

    p2 = await coord.read_hub_result_snapshot(
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
        await coord.read_hub_result_snapshot(
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
async def test_put_simple_ordered_hits() -> None:
    store = MagicMock()
    store.put_run = AsyncMock()
    sp = _rs_spec()
    hits = [_Row(id=i, name="z") for i in range(3)]
    coord = SearchResultSnapshotCoordinator(store=store)

    h = await coord.put_simple_ordered_hits(
        hits,
        snap_opt=None,
        rs_spec=sp,
        fp_computed="fp0",
        pool_len_before_cap=10,
    )
    assert h.capped is True
    assert h.total == 3
    store.put_run.assert_awaited_once()

    h2 = await coord.put_simple_ordered_hits(
        hits,
        snap_opt=None,
        rs_spec=sp,
        fp_computed="fp0",
        pool_len_before_cap=3,
    )
    assert h2.capped is False


def test_federated_fingerprint_list_query_differs() -> None:
    f1 = SearchResultSnapshotCoordinator.federated_fingerprint(
        "one", None, None, spec_name="s", rrf_k=10
    )
    f2 = SearchResultSnapshotCoordinator.federated_fingerprint(
        ["one", "two"], None, None, spec_name="s", rrf_k=10
    )
    assert f1 != f2


def test_federated_record_key_string_shape() -> None:
    h = _Hit(id=1, t="z")
    s = SearchResultSnapshotCoordinator.federated_record_key_string("a", h)
    assert s.startswith("a\0")


def test_effective_snapshot_overrides() -> None:
    base = SearchResultSnapshotSpec(
        name="b",
        enabled=True,
        ttl=timedelta(minutes=1),
        max_ids=7,
        chunk_size=3,
    )

    assert (
        SearchResultSnapshotCoordinator.effective_snapshot_max_ids({"max_ids": 2}, base)
        == 2
    )
    assert (
        SearchResultSnapshotCoordinator.effective_snapshot_chunk_size(
            {"chunk_size": 1}, base
        )
        == 1
    )
    assert SearchResultSnapshotCoordinator.effective_snapshot_ttl(
        {"ttl_seconds": 30}, base
    ) == timedelta(seconds=30)

    assert SearchResultSnapshotCoordinator.effective_snapshot_max_ids(None, base) == 7
    assert SearchResultSnapshotCoordinator.effective_snapshot_max_ids(
        {"other": 1}, None
    ) == (50_000)
    assert (
        SearchResultSnapshotCoordinator.effective_snapshot_chunk_size(None, None)
        == 5_000
    )
    assert SearchResultSnapshotCoordinator.effective_snapshot_ttl(
        None, None
    ) == timedelta(minutes=5)


def test_hydrate_federated_record_key_ok() -> None:
    h = _Hit(id=4, t="k")
    key = SearchResultSnapshotCoordinator.federated_record_key_string("a", h)
    out = SearchResultSnapshotCoordinator.hydrate_federated_record_key(key, _fed())

    assert out.member == "a"
    assert out.hit == h


def test_hydrate_federated_record_key_errors() -> None:
    with pytest.raises(CoreError, match="partition"):
        SearchResultSnapshotCoordinator.hydrate_federated_record_key(
            "no-null-byte", _fed()
        )

    with pytest.raises(CoreError, match="Unknown federated member"):
        SearchResultSnapshotCoordinator.hydrate_federated_record_key(
            'unknown\0{"id":1,"t":""}',
            _fed(),
        )
