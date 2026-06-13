"""Tests for :mod:`forze.pagination` cursor v1 token helpers."""

from __future__ import annotations

from forze.application.contracts.querying import (
    decode_keyset_v1,
    encode_keyset_v1,
    normalize_sorts_with_id,
)
from forze.domain.constants import ID_FIELD


def test_encode_decode_keyset_round_trip() -> None:
    t = encode_keyset_v1(
        sort_keys=[ID_FIELD, "name"],
        directions=["asc", "asc"],
        values=["a1", "b2"],
    )
    k, d, n, v = decode_keyset_v1(t)
    assert k == [ID_FIELD, "name"]
    assert d == ["asc", "asc"]
    assert n == ["first", "first"]
    assert v == ["a1", "b2"]


def test_normalize_sorts_with_id_appends_tiebreak() -> None:
    # Each key carries its null placement (desc → last).
    s = normalize_sorts_with_id({"m": "desc"})
    assert s == [("m", "desc", "last"), (ID_FIELD, "desc", "last")]


def test_normalize_allows_mixed_directions() -> None:
    # Mixed asc/desc is supported; the tie-breaker (here id, given explicitly) keeps its
    # own direction, and a mixed sort with an auto tie-breaker defaults that key to asc.
    assert normalize_sorts_with_id({ID_FIELD: "asc", "m": "desc"}) == [
        ("m", "desc", "last"),
        (ID_FIELD, "asc", "first"),
    ]
    assert normalize_sorts_with_id({"a": "asc", "b": "desc"}) == [
        ("a", "asc", "first"),
        ("b", "desc", "last"),
        (ID_FIELD, "asc", "first"),
    ]
