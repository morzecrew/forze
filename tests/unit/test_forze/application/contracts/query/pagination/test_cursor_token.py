"""Tests for :mod:`forze.application.contracts.querying.pagination.cursor_token`."""

from datetime import date, datetime
from decimal import Decimal
from uuid import UUID

import pytest

from forze.application.contracts.querying.pagination.cursor_token import (
    compare_keyset_sort_values,
    decode_keyset_v1,
    encode_keyset_v1,
    row_passes_keyset_seek,
    row_value_for_sort_key,
)
from forze.application.contracts.querying.sort_resolution import (
    normalize_sorts_with_id,
)
from forze.base.exceptions import CoreException
from forze.domain.constants import ID_FIELD


def test_normalize_sorts_empty_defaults_id_asc() -> None:
    assert normalize_sorts_with_id(None) == [(ID_FIELD, "asc")]
    assert normalize_sorts_with_id({}) == [(ID_FIELD, "asc")]


def test_normalize_sorts_single_direction_appends_id_tiebreaker() -> None:
    assert normalize_sorts_with_id({"name": "asc"}) == [
        ("name", "asc"),
        (ID_FIELD, "asc"),
    ]
    assert normalize_sorts_with_id({"name": "desc", ID_FIELD: "desc"}) == [
        ("name", "desc"),
        (ID_FIELD, "desc"),
    ]


def test_normalize_sorts_mixed_directions_rejected() -> None:
    with pytest.raises(CoreException, match="all sort directions"):
        normalize_sorts_with_id({"a": "asc", "b": "desc"})


def test_normalize_sorts_invalid_direction() -> None:
    with pytest.raises(CoreException, match="Invalid sort direction"):
        normalize_sorts_with_id({"name": "sideways"})  # type: ignore[dict-item]


def test_encode_decode_roundtrip_json_types() -> None:
    u = UUID("12345678-1234-5678-1234-567812345678")
    dt = datetime(2026, 4, 24, 12, 0, 0)
    d = date(2026, 4, 24)
    keys = ["name", "ts", "d", "dec", "flag", "n", ID_FIELD]
    dirs = ["asc"] * len(keys)
    values: list[object] = ["x", dt, d, Decimal("1.5"), True, 7, u]
    token = encode_keyset_v1(sort_keys=keys, directions=dirs, values=values)
    k2, d2, v2 = decode_keyset_v1(token)
    assert k2 == keys
    assert d2 == ["asc"] * len(keys)
    assert v2[0] == "x"
    assert v2[1] == dt.isoformat()
    assert v2[2] == d.isoformat()
    assert v2[3] == "1.5"
    assert v2[4] is True
    assert v2[5] == 7
    assert v2[6] == str(u)


def test_encode_keyset_misaligned_raises() -> None:
    with pytest.raises(CoreException, match="aligned"):
        encode_keyset_v1(sort_keys=["a"], directions=["asc", "asc"], values=[1])
    with pytest.raises(CoreException, match="aligned"):
        encode_keyset_v1(sort_keys=[], directions=[], values=[])


def test_decode_keyset_invalid_base64() -> None:
    with pytest.raises(CoreException, match="Invalid cursor token"):
        decode_keyset_v1("not-valid-base64!!!")


def test_decode_keyset_wrong_version() -> None:
    import base64
    import json

    raw = json.dumps({"v": 99, "k": ["a"], "d": ["asc"], "x": [1]}).encode()
    token = base64.urlsafe_b64encode(raw).decode().rstrip("=")
    with pytest.raises(CoreException, match="Invalid cursor token"):
        decode_keyset_v1(token)


def test_decode_keyset_invalid_direction_in_payload() -> None:
    import base64
    import json

    raw = json.dumps({"v": 1, "k": ["a"], "d": ["sideways"], "x": [1]}).encode()
    token = base64.urlsafe_b64encode(raw).decode().rstrip("=")
    with pytest.raises(CoreException, match="Invalid cursor token"):
        decode_keyset_v1(token)


def test_row_value_for_sort_key_nested() -> None:
    row = {"meta": {"inner": {"k": 42}}}
    assert row_value_for_sort_key(row, "meta.inner.k") == 42
    assert row_value_for_sort_key(row, "meta.missing.leaf") is None
    assert row_value_for_sort_key({"meta": "scalar"}, "meta.inner") is None


def test_compare_keyset_sort_values_uuid_and_string() -> None:
    u = UUID("12345678-1234-5678-1234-567812345678")
    assert compare_keyset_sort_values(u, str(u)) == 0
    assert compare_keyset_sort_values(str(u), u) == 0


def test_row_passes_keyset_seek_uuid_after_desc() -> None:
    u1 = UUID("22222222-2222-2222-2222-222222222222")
    u2 = UUID("11111111-1111-1111-1111-111111111111")
    token = encode_keyset_v1(
        sort_keys=["id"],
        directions=["desc"],
        values=[u1],
    )
    _, _, cursor_vals = decode_keyset_v1(token)
    assert row_passes_keyset_seek(
        {"id": u2},
        sort_keys=["id"],
        directions=["desc"],
        cursor_values=cursor_vals,
        after=True,
    )
    assert not row_passes_keyset_seek(
        {"id": u1},
        sort_keys=["id"],
        directions=["desc"],
        cursor_values=cursor_vals,
        after=True,
    )
