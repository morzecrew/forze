"""Direct coverage of the offset-token + keyset cursor helpers."""

from __future__ import annotations

import pytest

from forze.base.exceptions import CoreException
from forze_mock.query.cursors import (
    _b64url_json_dumps,  # type: ignore[reportPrivateUsage]
    _b64url_json_loads_dict,  # type: ignore[reportPrivateUsage]
    _mock_cursor_start_and_limit,  # type: ignore[reportPrivateUsage]
    _mock_cursor_tokens,  # type: ignore[reportPrivateUsage]
    _mock_keyset_window,  # type: ignore[reportPrivateUsage]
)

pytestmark = pytest.mark.unit

# ----------------------- #


def test_b64url_roundtrip() -> None:
    token = _b64url_json_dumps({"s": 7})
    assert "=" not in token  # padding stripped
    assert _b64url_json_loads_dict(token) == {"s": 7}


def test_b64url_decode_rejects_non_dict() -> None:
    # ``[]`` base64url-encoded decodes to a list, not a ``{"s": ...}`` dict → ValueError.
    with pytest.raises(ValueError):
        _b64url_json_loads_dict("W10")


def test_cursor_start_and_limit_after() -> None:
    after = _b64url_json_dumps({"s": 5})
    start, lim = _mock_cursor_start_and_limit({"after": after, "limit": 3})
    assert (start, lim) == (5, 3)


def test_cursor_start_and_limit_before_clamps_to_zero() -> None:
    before = _b64url_json_dumps({"s": 2})
    start, lim = _mock_cursor_start_and_limit({"before": before, "limit": 10})
    # page_start - lim = 2 - 10 → clamped to 0.
    assert (start, lim) == (0, 10)


def test_cursor_start_and_limit_before_nonzero() -> None:
    before = _b64url_json_dumps({"s": 12})
    start, lim = _mock_cursor_start_and_limit({"before": before, "limit": 4})
    assert (start, lim) == (8, 4)


def test_cursor_default_limit_and_no_cursor() -> None:
    # No cursor -> start 0, the shared default page size.
    start, lim = _mock_cursor_start_and_limit(None)
    assert (start, lim) == (0, 10)


def test_cursor_limit_clamped_and_coerced() -> None:
    # A huge limit is clamped (not an unbounded scan); a non-integer is a clean 400.
    _, lim = _mock_cursor_start_and_limit({"limit": 10**9})
    assert lim == 10_000
    with pytest.raises(CoreException, match="integer"):
        _mock_cursor_start_and_limit({"limit": "abc"})


def test_cursor_dual_token_rejected() -> None:
    a = _b64url_json_dumps({"s": 1})
    b = _b64url_json_dumps({"s": 2})
    with pytest.raises(CoreException, match="at most one"):
        _mock_cursor_start_and_limit({"after": a, "before": b})


def test_cursor_limit_must_be_positive() -> None:
    with pytest.raises(CoreException, match="positive"):
        _mock_cursor_start_and_limit({"limit": 0})


def test_cursor_invalid_after_token() -> None:
    with pytest.raises(CoreException, match="Invalid cursor token"):
        _mock_cursor_start_and_limit({"after": "not-base64-$$$"})


def test_cursor_invalid_before_token() -> None:
    with pytest.raises(CoreException, match="Invalid cursor token"):
        _mock_cursor_start_and_limit({"before": "@@@bad@@@"})


def test_cursor_tokens_first_page_has_no_prev() -> None:
    next_c, prev_c = _mock_cursor_tokens(0, 3, has_more=True)
    assert prev_c is None  # start == 0
    assert next_c is not None
    assert _b64url_json_loads_dict(next_c) == {"s": 3}


def test_cursor_tokens_middle_page_has_both() -> None:
    next_c, prev_c = _mock_cursor_tokens(5, 3, has_more=True)
    assert prev_c is not None and _b64url_json_loads_dict(prev_c) == {"s": 5}
    assert next_c is not None and _b64url_json_loads_dict(next_c) == {"s": 8}


def test_cursor_tokens_last_page_no_next() -> None:
    next_c, prev_c = _mock_cursor_tokens(5, 2, has_more=False)
    assert next_c is None
    assert prev_c is not None


# ----------------------- #
# keyset window

_DOCS = [
    {"id": "a", "score": 1},
    {"id": "b", "score": 2},
    {"id": "c", "score": 3},
    {"id": "d", "score": 4},
]


def _window(cursor: dict[str, object] | None) -> tuple[list[dict[str, object]], bool, str | None, str | None]:
    return _mock_keyset_window(
        list(_DOCS),
        cursor=cursor,
        sort_keys=["score"],
        directions=["asc"],
        nulls=["first"],
    )


def test_keyset_window_first_page() -> None:
    page, has_more, next_c, prev_c = _window({"limit": 2})
    assert [d["score"] for d in page] == [1, 2]
    assert has_more is True
    assert next_c is not None
    assert prev_c is None


def test_keyset_window_after_token() -> None:
    _, _, next_c, _ = _window({"limit": 2})
    assert next_c is not None
    page, has_more, _, prev_c = _window({"limit": 2, "after": next_c})
    assert [d["score"] for d in page] == [3, 4]
    assert has_more is False
    assert prev_c is not None


def test_keyset_window_dual_token_rejected() -> None:
    _, _, next_c, _ = _window({"limit": 2})
    assert next_c is not None
    with pytest.raises(CoreException, match="at most one"):
        _window({"after": next_c, "before": next_c})


def test_keyset_window_limit_must_be_positive() -> None:
    with pytest.raises(CoreException, match="positive"):
        _window({"limit": 0})


def test_keyset_window_tie_on_sort_key_is_stable() -> None:
    # Two docs share the sort value, so the per-key comparator returns 0 (tie path).
    tied = [
        {"id": "x", "score": 1},
        {"id": "y", "score": 1},
        {"id": "z", "score": 2},
    ]
    page, _, _, _ = _mock_keyset_window(
        tied,
        cursor={"limit": 5},
        sort_keys=["score"],
        directions=["asc"],
        nulls=["first"],
    )
    assert [d["score"] for d in page] == [1, 1, 2]


def test_keyset_window_before_token() -> None:
    # Walk forward to a token, then page backwards with ``before``.
    _, _, next_c, _ = _window({"limit": 2})
    page2, _, _, _ = _window({"limit": 2, "after": next_c})
    # build a before token from second page's prev cursor
    _, _, _, prev2 = _window({"limit": 2, "after": next_c})
    assert prev2 is not None
    back_page, _, _, _ = _window({"limit": 2, "before": prev2})
    assert [d["score"] for d in back_page] == [1, 2]
    _ = page2
