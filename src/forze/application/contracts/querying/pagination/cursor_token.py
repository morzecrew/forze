"""Base64-JSON cursors and sort normalization (agnostic of SQL vs Mongo)."""

from typing import Any, Sequence

from forze.base.codecs import B64UrlJsonCodec
from forze.base.exceptions import exc

# ----------------------- #

_KEYSET_V1 = 1
_DIRECTIONS = ("asc", "desc")
_CODEC = B64UrlJsonCodec()

# ....................... #


def _jsonify_value(v: Any) -> Any:
    if v is None:
        return None

    t = type(v).__name__

    if t in ("UUID", "uuid"):
        return str(v)

    if t in ("datetime", "date"):
        return v.isoformat()

    if t == "Decimal":
        return str(v)

    if isinstance(v, (str, int, float, bool)):
        return v

    if isinstance(v, (list, dict)):
        return v  # type: ignore[return-value]

    return str(v)


# ....................... #


def keyset_canonical_value(v: Any) -> Any:
    """Normalize a sort-key value to the wire form used in cursor tokens."""

    return _jsonify_value(v)


# ....................... #


def compare_keyset_sort_values(left: Any, right: Any) -> int:
    """Compare two sort-key values (-1, 0, 1) using cursor wire canonicalization."""

    lc = keyset_canonical_value(left)
    rc = keyset_canonical_value(right)

    if lc == rc:
        return 0

    if lc is None:
        return -1

    if rc is None:
        return 1

    # Cursor values are client-controlled: a tampered token can put a value of
    # the wrong type next to a row value (e.g. ``int < str``) — surface that as
    # an invalid-cursor validation error instead of a raw TypeError (500).
    try:
        if lc < rc:
            return -1

    except TypeError as e:
        raise exc.validation("Invalid cursor token") from e

    return 1


# ....................... #


def ordered_compare(
    left: Any,
    right: Any,
    *,
    direction: str,
    nulls: str,
) -> int:
    """Sort-order comparison for one key: ``-1`` if *left* sorts before *right*, else.

    Null placement is *absolute* — ``nulls="first"`` puts nulls at the start, ``"last"``
    at the end, independent of *direction*; only the non-null comparison flips with
    direction. This is the canonical keyset order every backend conforms to.
    """

    lc = keyset_canonical_value(left)
    rc = keyset_canonical_value(right)
    l_null = lc is None
    r_null = rc is None

    if l_null and r_null:
        return 0

    if l_null:
        return -1 if nulls == "first" else 1

    if r_null:
        return 1 if nulls == "first" else -1

    if lc == rc:
        return 0

    # Cursor values are client-controlled: a tampered token can put a value of the wrong
    # type next to a row value — surface as an invalid-cursor error, not a raw TypeError.
    try:
        ordered = -1 if lc < rc else 1

    except TypeError as e:
        raise exc.validation("Invalid cursor token") from e

    return -ordered if direction == "desc" else ordered


# ....................... #


def _resolved_nulls(
    directions: Sequence[str],
    nulls: Sequence[str] | None,
) -> list[str]:
    """The explicit *nulls* placement, or the canonical default per direction."""

    if nulls is None:
        return [_canonical_nulls(d) for d in directions]

    return list(nulls)


def row_passes_keyset_seek(
    row: dict[str, Any],
    *,
    sort_keys: Sequence[str],
    directions: Sequence[str],
    cursor_values: Sequence[Any],
    after: bool,
    nulls: Sequence[str] | None = None,
) -> bool:
    """Return whether *row* is strictly after/before the cursor tuple (composite keyset)."""

    for key, direction, null_order, cursor_value in zip(
        sort_keys,
        directions,
        _resolved_nulls(directions, nulls),
        cursor_values,
        strict=True,
    ):
        cmp = ordered_compare(
            row_value_for_sort_key(row, key),
            cursor_value,
            direction=direction,
            nulls=null_order,
        )

        if cmp == 0:
            continue

        return cmp > 0 if after else cmp < 0

    return False


# ....................... #


def _parse_value(v: Any) -> Any:
    if v is None:
        return None

    if isinstance(v, (int, float, str, bool)):
        return v

    # Token values are client-controlled: only JSON scalars are valid sort-key
    # values; containers (and anything else) are rejected as a tampered cursor.
    raise exc.validation("Invalid cursor token")


# ....................... #


def _canonical_nulls(direction: str) -> str:
    """Default null placement for *direction* (asc → first, desc → last)."""

    return "first" if direction == "asc" else "last"


def encode_keyset_v1(
    *,
    sort_keys: Sequence[str],
    directions: Sequence[str],
    values: Sequence[Any],
    nulls: Sequence[str] | None = None,
) -> str:
    null_order = _resolved_nulls(directions, nulls)

    if (
        len(sort_keys) != len(values)
        or len(sort_keys) != len(directions)
        or len(sort_keys) != len(null_order)
        or not sort_keys
    ):
        raise exc.internal(
            "Keyset token fields must be aligned in length and non-empty"
        )

    payload: dict[str, Any] = {
        "v": _KEYSET_V1,
        "k": list(sort_keys),
        "d": list(directions),
        "n": list(null_order),
        "x": [_jsonify_value(x) for x in values],
    }
    return _CODEC.dumps(payload)


# ....................... #


def row_value_for_sort_key(row: dict[str, Any], key: str) -> Any:
    if "." not in key:
        return row.get(key)
    cur: Any = row
    for part in key.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)  # type: ignore[assignment, misc]

    return cur  # type: ignore[return-value]


# ....................... #


def decode_keyset_v1(token: str) -> tuple[list[str], list[str], list[str], list[Any]]:
    """Decode a keyset token to ``(keys, directions, nulls, values)``.

    A token written before per-key null placement existed carries no ``n`` field; its
    nulls default to the canonical placement for each direction, so old cursors stay
    valid as long as the active sort uses that default.
    """

    try:
        data: Any = _CODEC.loads(token)

    except ValueError as e:
        raise exc.validation("Invalid cursor token") from e

    if not isinstance(data, dict) or int(data.get("v", 0)) != _KEYSET_V1:  # type: ignore[arg-type]
        raise exc.validation("Invalid cursor token")

    k = data.get("k")  # type: ignore[assignment, misc]
    d = data.get("d")  # type: ignore[assignment, misc]
    x = data.get("x")  # type: ignore[assignment, misc]

    if not isinstance(k, list) or not isinstance(d, list) or not isinstance(x, list):
        raise exc.validation("Invalid cursor token")

    if len(k) != len(d) or len(k) != len(x):  # type: ignore[arg-type]
        raise exc.validation("Invalid cursor token")

    keys = [str(a) for a in k]  # type: ignore[arg-type]
    dirs = [str(a).lower() for a in d]  # type: ignore[arg-type]

    for dr in dirs:
        if dr not in _DIRECTIONS:
            raise exc.validation("Invalid cursor token")

    n = data.get("n")  # type: ignore[assignment, misc]

    if n is None:
        nulls = [_canonical_nulls(dr) for dr in dirs]

    else:
        if not isinstance(n, list) or len(n) != len(k):  # type: ignore[arg-type]
            raise exc.validation("Invalid cursor token")

        nulls = [str(a).lower() for a in n]  # type: ignore[arg-type]

        for nn in nulls:
            if nn not in ("first", "last"):
                raise exc.validation("Invalid cursor token")

    vals = [_parse_value(v) for v in x]  # type: ignore[arg-type]

    return keys, dirs, nulls, vals


# ....................... #


def validate_cursor_token(
    token: str,
    *,
    sort_keys: Sequence[str],
    directions: Sequence[str],
    nulls: Sequence[str] | None = None,
) -> list[Any]:
    """Decode a keyset *token* and verify it matches the active sort; return its values.

    Raises :func:`~forze.base.exceptions.exc.validation` when the token's keys,
    directions, or null placement do not align with the current search sort (a stale or
    mismatched cursor). When *nulls* is omitted the canonical placement is assumed.
    Shared by every keyset-cursor search path so the validation is identical.
    """

    null_order = _resolved_nulls(directions, nulls)
    tk, td, tn, tv = decode_keyset_v1(token)

    if (
        list(tk) != list(sort_keys)
        or len(td) != len(directions)
        or len(tn) != len(null_order)
    ):
        raise exc.validation("Cursor does not match current search sort")

    for i, di in enumerate(directions):
        if (td[i] or "").lower() != di or (tn[i] or "").lower() != null_order[i]:
            raise exc.validation("Cursor does not match current search sort")

    return list(tv)


# ....................... #


def keyset_page_bounds(
    raw_rows: list[dict[str, Any]],
    limit: int,
    *,
    sort_keys: Sequence[str],
    directions: Sequence[str],
    use_after: bool,
    use_before: bool,
    nulls: Sequence[str] | None = None,
) -> tuple[list[dict[str, Any]], bool, str | None, str | None]:
    """Trim an over-fetched keyset result to one page and compute next/prev cursors.

    *raw_rows* holds up to ``limit + 1`` rows — the extra row signals more pages. For a
    ``before`` page the rows are reversed back into ascending order first. Returns
    ``(rows, has_more, next_cursor, prev_cursor)``. Shared by the keyset-cursor search
    paths so the page-boundary and token-emission logic is single-sourced.
    """

    if use_before:
        raw_rows = list(reversed(raw_rows))

    has_more = len(raw_rows) > limit
    rows = raw_rows[:limit]

    def _row_token_vals(row: dict[str, Any]) -> list[Any]:
        return [row_value_for_sort_key(row, k) for k in sort_keys]

    nxt = (
        encode_keyset_v1(
            sort_keys=sort_keys,
            directions=directions,
            nulls=nulls,
            values=_row_token_vals(rows[-1]),
        )
        if has_more and rows
        else None
    )

    prv = (
        encode_keyset_v1(
            sort_keys=sort_keys,
            directions=directions,
            nulls=nulls,
            values=_row_token_vals(rows[0]),
        )
        if rows and (use_after or (use_before and has_more))
        else None
    )

    return rows, has_more, nxt, prv
