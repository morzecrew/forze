"""Base64-JSON cursors and sort normalization (agnostic of SQL vs Mongo)."""

import base64
import json
from typing import Any, Sequence

from forze.base.exceptions import exc

# ----------------------- #

_KEYSET_V1 = 1
_DIRECTIONS = ("asc", "desc")

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


def row_passes_keyset_seek(
    row: dict[str, Any],
    *,
    sort_keys: Sequence[str],
    directions: Sequence[str],
    cursor_values: Sequence[Any],
    after: bool,
) -> bool:
    """Return whether *row* is strictly after/before the cursor tuple (composite keyset)."""

    for key, direction, cursor_value in zip(
        sort_keys,
        directions,
        cursor_values,
        strict=True,
    ):
        cmp = compare_keyset_sort_values(
            row_value_for_sort_key(row, key),
            cursor_value,
        )

        if cmp == 0:
            continue

        if direction == "asc":
            return cmp > 0 if after else cmp < 0

        return cmp < 0 if after else cmp > 0

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


def encode_keyset_v1(
    *,
    sort_keys: Sequence[str],
    directions: Sequence[str],
    values: Sequence[Any],
) -> str:
    if (
        len(sort_keys) != len(values)
        or len(sort_keys) != len(directions)
        or not sort_keys
    ):
        raise exc.internal(
            "Keyset token fields must be aligned in length and non-empty"
        )

    payload: dict[str, Any] = {
        "v": _KEYSET_V1,
        "k": list(sort_keys),
        "d": list(directions),
        "x": [_jsonify_value(x) for x in values],
    }
    raw = json.dumps(
        payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


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


def decode_keyset_v1(token: str) -> tuple[list[str], list[str], list[Any]]:
    pad = "=" * (-len(token) % 4)

    try:
        raw = base64.urlsafe_b64decode(token + pad)
        data: Any = json.loads(raw.decode("utf-8"))

    except (ValueError, json.JSONDecodeError) as e:
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

    vals = [_parse_value(v) for v in x]  # type: ignore[arg-type]

    return keys, dirs, vals


# ....................... #


def validate_cursor_token(
    token: str,
    *,
    sort_keys: Sequence[str],
    directions: Sequence[str],
) -> list[Any]:
    """Decode a keyset *token* and verify it matches the active sort; return its values.

    Raises :func:`~forze.base.exceptions.exc.validation` when the token's keys or
    directions do not align with the current search sort (a stale or mismatched
    cursor). Shared by every keyset-cursor search path so the validation is identical.
    """

    tk, td, tv = decode_keyset_v1(token)

    if list(tk) != list(sort_keys) or len(td) != len(directions):
        raise exc.validation("Cursor does not match current search sort")

    for i, di in enumerate(directions):
        if (td[i] or "").lower() != di:
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
            values=_row_token_vals(rows[-1]),
        )
        if has_more and rows
        else None
    )

    prv = (
        encode_keyset_v1(
            sort_keys=sort_keys,
            directions=directions,
            values=_row_token_vals(rows[0]),
        )
        if rows and (use_after or (use_before and has_more))
        else None
    )

    return rows, has_more, nxt, prv
