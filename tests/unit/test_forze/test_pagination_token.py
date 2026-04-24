"""Tests for :mod:`forze.pagination` cursor v1 token helpers."""

from __future__ import annotations

import pytest

from forze.application.contracts.query import (
    decode_keyset_v1,
    encode_keyset_v1,
    normalize_sorts_with_id,
)
from forze.base.errors import CoreError
from forze.domain.constants import ID_FIELD


def test_encode_decode_keyset_round_trip() -> None:
    t = encode_keyset_v1(
        sort_keys=[ID_FIELD, "name"],
        directions=["asc", "asc"],
        values=["a1", "b2"],
    )
    k, d, v = decode_keyset_v1(t)
    assert k == [ID_FIELD, "name"]
    assert d == ["asc", "asc"]
    assert v == ["a1", "b2"]


def test_normalize_sorts_with_id_appends_tiebreak() -> None:
    s = normalize_sorts_with_id({"m": "desc"})
    assert s == [("m", "desc"), (ID_FIELD, "desc")]


def test_normalize_rejects_mixed_directions() -> None:
    with pytest.raises(CoreError, match="all sort directions"):
        normalize_sorts_with_id({ID_FIELD: "asc", "m": "desc"})
