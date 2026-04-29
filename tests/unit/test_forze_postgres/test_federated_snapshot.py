"""Unit tests for :mod:`forze_postgres.adapters.search.federated_snapshot`."""

from datetime import timedelta

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import (
    FederatedSearchSpec,
    SearchResultSnapshotSpec,
    SearchSpec,
)
from forze.base.errors import CoreError
from forze_postgres.adapters.search.federated_snapshot import (
    effective_snapshot_chunk_size,
    effective_snapshot_max_ids,
    effective_snapshot_ttl,
    federated_fingerprint,
    federated_row_key_string,
    hydrate_federated_row_key,
    should_write_federated_snapshot,
)


class _Hit(BaseModel):
    id: int
    t: str = ""


def _fed() -> FederatedSearchSpec[_Hit]:
    return FederatedSearchSpec(
        name="fed",
        members=(
            SearchSpec(name="a", model_type=_Hit, fields=["t"]),
            SearchSpec(name="b", model_type=_Hit, fields=["t"]),
        ),
    )


def test_federated_fingerprint_list_query_differs() -> None:
    f1 = federated_fingerprint("one", None, None, spec_name="s", rrf_k=10)
    f2 = federated_fingerprint(["one", "two"], None, None, spec_name="s", rrf_k=10)
    assert f1 != f2


def test_federated_row_key_string_shape() -> None:
    h = _Hit(id=1, t="z")
    s = federated_row_key_string("a", h)
    assert s.startswith("a\0")


@pytest.mark.parametrize(
    ("opt", "spec", "write"),
    [
        (None, None, False),
        (None, SearchResultSnapshotSpec(name="s", enabled=False), False),
        (None, SearchResultSnapshotSpec(name="s", enabled=True), True),
        ({"mode": False}, SearchResultSnapshotSpec(name="s", enabled=True), False),
        ({"mode": True}, SearchResultSnapshotSpec(name="s", enabled=False), True),
        ({"mode": "auto"}, SearchResultSnapshotSpec(name="s", enabled=True), True),
    ],
)
def test_should_write_federated_snapshot_modes(
    opt: dict[str, object] | None,
    spec: SearchResultSnapshotSpec | None,
    write: bool,
) -> None:
    assert should_write_federated_snapshot(opt, spec) is write


def test_effective_snapshot_overrides() -> None:
    base = SearchResultSnapshotSpec(
        name="b",
        enabled=True,
        ttl=timedelta(minutes=1),
        max_ids=7,
        chunk_size=3,
    )
    assert effective_snapshot_max_ids({"max_ids": 2}, base) == 2
    assert effective_snapshot_chunk_size({"chunk_size": 1}, base) == 1
    assert effective_snapshot_ttl({"ttl_seconds": 30}, base) == timedelta(seconds=30)

    assert effective_snapshot_max_ids(None, base) == 7
    assert effective_snapshot_max_ids({"other": 1}, None) == 50_000
    assert effective_snapshot_chunk_size(None, None) == 5_000
    assert effective_snapshot_ttl(None, None) == timedelta(minutes=5)


def test_hydrate_federated_row_key_ok() -> None:
    h = _Hit(id=4, t="k")
    key = federated_row_key_string("a", h)
    out = hydrate_federated_row_key(key, _fed())
    assert out.member == "a"
    assert out.hit == h


def test_hydrate_federated_row_key_errors() -> None:
    with pytest.raises(CoreError, match="partition"):
        hydrate_federated_row_key("no-null-byte", _fed())
    with pytest.raises(CoreError, match="Unknown federated member"):
        hydrate_federated_row_key('unknown\0{"id":1,"t":""}', _fed())
