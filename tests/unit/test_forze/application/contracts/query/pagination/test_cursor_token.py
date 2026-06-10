"""Tests for :mod:`forze.application.contracts.querying.pagination.cursor_token`."""

from datetime import date, datetime
from decimal import Decimal
from uuid import UUID

import pytest

from forze.application.contracts.querying.pagination.cursor_token import (
    compare_keyset_sort_values,
    keyset_canonical_value,
    decode_keyset_v1,
    encode_keyset_v1,
    keyset_page_bounds,
    row_passes_keyset_seek,
    row_value_for_sort_key,
    validate_cursor_token,
)
from forze.application.contracts.querying.sort_resolution import (
    normalize_sorts_with_id,
)
from forze.base.exceptions import CoreException, ExceptionKind
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
    with pytest.raises(CoreException, match="Invalid cursor token") as exc_info:
        decode_keyset_v1("not-valid-base64!!!")
    assert exc_info.value.kind == ExceptionKind.VALIDATION


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


# ----------------------- #
# Shared keyset-cursor token-tail helpers


def test_validate_cursor_token_roundtrip_returns_values() -> None:
    sort_keys = ["created_at", "id"]
    directions = ["desc", "asc"]
    token = encode_keyset_v1(
        sort_keys=sort_keys, directions=directions, values=["2024-01-01", "abc"]
    )

    assert validate_cursor_token(
        token, sort_keys=sort_keys, directions=directions
    ) == ["2024-01-01", "abc"]


def test_validate_cursor_token_rejects_key_mismatch() -> None:
    token = encode_keyset_v1(sort_keys=["a"], directions=["asc"], values=[1])

    with pytest.raises(CoreException, match="Cursor does not match") as exc_info:
        validate_cursor_token(token, sort_keys=["b"], directions=["asc"])
    assert exc_info.value.kind == ExceptionKind.VALIDATION


def test_validate_cursor_token_rejects_direction_mismatch() -> None:
    token = encode_keyset_v1(sort_keys=["a"], directions=["asc"], values=[1])

    with pytest.raises(CoreException, match="Cursor does not match") as exc_info:
        validate_cursor_token(token, sort_keys=["a"], directions=["desc"])
    assert exc_info.value.kind == ExceptionKind.VALIDATION


def _rows(n: int) -> list[dict[str, int]]:
    return [{"id": i} for i in range(n)]


def test_keyset_page_bounds_after_trims_and_emits_next() -> None:
    # over-fetched limit+1 rows -> has_more, next cursor from last kept row, no prev on first page
    rows, has_more, nxt, prv = keyset_page_bounds(
        _rows(4), 3, sort_keys=["id"], directions=["asc"], use_after=False, use_before=False
    )
    assert [r["id"] for r in rows] == [0, 1, 2]
    assert has_more is True
    assert nxt is not None
    assert prv is None  # first page (no after/before) emits no prev


def test_keyset_page_bounds_after_page_emits_prev() -> None:
    _, has_more, nxt, prv = keyset_page_bounds(
        _rows(4), 3, sort_keys=["id"], directions=["asc"], use_after=True, use_before=False
    )
    assert has_more is True
    assert nxt is not None
    assert prv is not None  # an 'after' page can page back


def test_keyset_page_bounds_before_reverses_then_trims() -> None:
    # 'before' fetches in flipped order; the helper reverses then trims to the window.
    raw = [{"id": i} for i in (3, 2, 1, 0)]
    rows, has_more, _nxt, prv = keyset_page_bounds(
        raw, 3, sort_keys=["id"], directions=["asc"], use_after=False, use_before=True
    )
    assert [r["id"] for r in rows] == [0, 1, 2]  # reversed([3,2,1,0])[:3]
    assert has_more is True
    assert prv is not None  # paging 'before' with more remaining emits a prev cursor


def test_keyset_page_bounds_exact_fit_has_no_more() -> None:
    rows, has_more, nxt, _ = keyset_page_bounds(
        _rows(3), 3, sort_keys=["id"], directions=["asc"], use_after=False, use_before=False
    )
    assert [r["id"] for r in rows] == [0, 1, 2]
    assert has_more is False
    assert nxt is None


# ----------------------- #
# Canonicalization + comparison branch coverage


class _Weird:
    def __str__(self) -> str:
        return "weird"


@pytest.mark.parametrize(
    "value,expected",
    [
        ([1, 2], [1, 2]),  # list passthrough
        ({"a": 1}, {"a": 1}),  # dict passthrough
        (_Weird(), "weird"),  # fallback str()
        ("s", "s"),
        (3, 3),
    ],
)
def test_keyset_canonical_value(value: object, expected: object) -> None:
    assert keyset_canonical_value(value) == expected


@pytest.mark.parametrize(
    "left,right,expected",
    [
        (None, 1, -1),  # lc is None
        (1, None, 1),  # rc is None
        (1, 1, 0),  # equal
        (1, 2, -1),  # lc < rc
        (2, 1, 1),  # lc > rc
        (None, None, 0),  # both None -> equal
    ],
)
def test_compare_keyset_sort_values(left: object, right: object, expected: int) -> None:
    assert compare_keyset_sort_values(left, right) == expected


def test_decode_keyset_rejects_container_values() -> None:
    # Tampered token: client-controlled values must be JSON scalars only.
    import base64
    import json

    for bad in ({"a": 1}, [1, 2]):
        raw = json.dumps({"v": 1, "k": ["a"], "d": ["asc"], "x": [bad]}).encode()
        token = base64.urlsafe_b64encode(raw).decode().rstrip("=")
        with pytest.raises(CoreException, match="Invalid cursor token") as exc_info:
            decode_keyset_v1(token)
        assert exc_info.value.kind == ExceptionKind.VALIDATION


def test_compare_keyset_mixed_types_raises_validation_not_type_error() -> None:
    # A tampered cursor can put an int next to a str row value; that must surface
    # as an invalid-cursor validation error, never a raw TypeError (500).
    with pytest.raises(CoreException, match="Invalid cursor token") as exc_info:
        compare_keyset_sort_values(1, "abc")
    assert exc_info.value.kind == ExceptionKind.VALIDATION


def test_well_formed_token_round_trips_unchanged() -> None:
    sort_keys = ["name", ID_FIELD]
    directions = ["asc", "asc"]
    values = ["alice", "a1"]
    token = encode_keyset_v1(
        sort_keys=sort_keys, directions=directions, values=values
    )

    assert (
        validate_cursor_token(token, sort_keys=sort_keys, directions=directions)
        == values
    )


def test_decode_keyset_rejects_non_list_payload() -> None:
    import base64
    import json

    raw = json.dumps({"v": 1, "k": "notlist", "d": ["asc"], "x": [1]}).encode()
    token = base64.urlsafe_b64encode(raw).decode().rstrip("=")
    with pytest.raises(CoreException, match="Invalid cursor token"):
        decode_keyset_v1(token)


def test_decode_keyset_rejects_length_mismatch() -> None:
    import base64
    import json

    raw = json.dumps({"v": 1, "k": ["a", "b"], "d": ["asc"], "x": [1]}).encode()
    token = base64.urlsafe_b64encode(raw).decode().rstrip("=")
    with pytest.raises(CoreException, match="Invalid cursor token"):
        decode_keyset_v1(token)
