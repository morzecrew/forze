"""Tests for analytics cursor helpers in ``adapter_common``."""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from forze.application.integrations.analytics.adapter_common import (
    encode_keyset_cursor_next,
    encode_offset_cursor_next_prev,
    merge_forze_after_params,
    parse_analytics_cursor_limit,
    parse_keyset_cursor_after,
    parse_offset_cursor_after,
)
from forze.base.codecs import B64UrlJsonCodec
from forze.base.exceptions import CoreException, ExceptionKind

_CODEC = B64UrlJsonCodec()


def test_parse_analytics_cursor_limit_default() -> None:
    assert parse_analytics_cursor_limit(None) == 10


def test_parse_analytics_cursor_limit_rejects_both_tokens() -> None:
    with pytest.raises(CoreException, match="at most one"):
        parse_analytics_cursor_limit({"after": "a", "before": "b"})


def test_parse_offset_cursor_after_first_page() -> None:
    start, lim = parse_offset_cursor_after(None)
    assert start == 0
    assert lim == 10


def test_parse_offset_cursor_after_decodes_offset() -> None:
    token = _CODEC.dumps({"o": 5})
    start, lim = parse_offset_cursor_after({"after": token, "limit": 3})
    assert start == 5
    assert lim == 3


def test_parse_offset_cursor_rejects_keyset_token() -> None:
    token = _CODEC.dumps({"kc": "id", "kv": 1})
    with pytest.raises(CoreException, match="Offset cursor token"):
        parse_offset_cursor_after({"after": token})


def test_parse_offset_cursor_rejects_before() -> None:
    with pytest.raises(CoreException, match="Backward analytics cursors"):
        parse_offset_cursor_after({"before": "x"})


def test_parse_keyset_cursor_after_no_token() -> None:
    after, lim = parse_keyset_cursor_after(None)
    assert after is None
    assert lim == 10


def test_parse_keyset_cursor_after_decodes_kv() -> None:
    token = _CODEC.dumps({"kc": "id", "kv": 42})
    after, lim = parse_keyset_cursor_after({"after": token})
    assert after == 42
    assert lim == 10


def test_encode_offset_cursor_next_prev() -> None:
    nxt, prv = encode_offset_cursor_next_prev(start=0, page_len=10, limit=10)
    assert nxt is not None
    assert prv is None
    start, _ = parse_offset_cursor_after({"after": nxt})
    assert start == 10


def test_encode_keyset_cursor_next_from_model() -> None:
    class _Row(BaseModel):
        id: int

    token = encode_keyset_cursor_next(column="id", hits=[_Row(id=1)], limit=1)
    assert token is not None
    after, _ = parse_keyset_cursor_after({"after": token})
    assert after == 1


def test_merge_forze_after_params() -> None:
    base = {"day": "2026-01-01"}
    assert merge_forze_after_params(base, None) == base
    merged = merge_forze_after_params(base, 99)
    assert merged["forze_after"] == 99


class TestCursorLimitHardening:
    """The analytics limit parse is the SAME clamp every keyset path uses — the fork
    it replaced sent ``int(limit)`` unguarded to DuckDB/BigQuery (a raw ValueError
    500 on 'abc', ``LIMIT 10**20`` on an absurd value)."""

    def test_non_integer_limit_is_a_clean_validation_error(self) -> None:
        with pytest.raises(CoreException) as caught:
            parse_analytics_cursor_limit({"limit": "abc"})

        assert caught.value.kind is ExceptionKind.VALIDATION

    def test_oversized_limit_is_clamped(self) -> None:
        from forze.application.contracts.querying.pagination.cursor_page import (
            MAX_CURSOR_LIMIT,
        )

        assert parse_analytics_cursor_limit({"limit": 10**20}) == MAX_CURSOR_LIMIT

    def test_non_positive_limit_is_a_clean_validation_error(self) -> None:
        for bad in (0, -1):
            with pytest.raises(CoreException) as caught:
                parse_analytics_cursor_limit({"limit": bad})

            assert caught.value.kind is ExceptionKind.VALIDATION

    def test_non_finite_limit_is_a_clean_validation_error(self) -> None:
        with pytest.raises(CoreException) as caught:
            parse_analytics_cursor_limit({"limit": float("inf")})

        assert caught.value.kind is ExceptionKind.VALIDATION
