"""Base64-JSON cursors and sort normalization (agnostic of SQL vs Mongo)."""

import base64
import json
from collections.abc import Sequence
from typing import Any

from forze.application.contracts.query import QuerySortExpression
from forze.base.errors import CoreError
from forze.domain.constants import ID_FIELD

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


def _parse_value(v: Any) -> Any:
    if v is None:
        return None

    if isinstance(v, (int, float, str, bool)):
        return v

    if isinstance(v, (list, dict)):

        return v  # type: ignore[return-value]
    return v


# ....................... #


def normalize_sorts_with_id(
    sorts: QuerySortExpression | None,
) -> list[tuple[str, str]]:
    """Uniform-direction sorts with *id* as final tie-breaker."""
    s = dict(sorts) if sorts else {}
    if not s:
        return [(ID_FIELD, "asc")]

    dirs: set[str] = {s[k] for k in s}  # type: ignore[assignment, operator]

    if len(dirs) != 1:
        raise CoreError(
            "Keyset (cursor) pagination requires all sort directions to match "
            "(all ``asc`` or all ``desc``).",
        )
    direction = next(iter(dirs))

    if direction not in _DIRECTIONS:
        raise CoreError("Invalid sort direction in sorts expression")

    order_keys: list[str] = [k for k in s if k != ID_FIELD]

    if ID_FIELD in s:
        order_keys.append(ID_FIELD)

    else:
        order_keys.append(ID_FIELD)

    return [
        (k, s[k] if k in s else direction)  # type: ignore[dict-item, misc]
        for k in order_keys
    ]


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
        raise CoreError("Keyset token fields must be aligned in length and non-empty")

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


def row_value_for_sort_key(row: dict[str, Any], key: str) -> Any:
    if "." not in key:
        return row.get(key)
    cur: Any = row
    for part in key.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)  # type: ignore[assignment, misc]

    return cur  # type: ignore[return-value]


def decode_keyset_v1(token: str) -> tuple[list[str], list[str], list[Any]]:
    pad = "=" * (-len(token) % 4)

    try:
        raw = base64.urlsafe_b64decode(token + pad)
        data: Any = json.loads(raw.decode("utf-8"))

    except (ValueError, json.JSONDecodeError) as e:
        raise CoreError("Invalid cursor token") from e

    if not isinstance(data, dict) or int(data.get("v", 0)) != _KEYSET_V1:  # type: ignore[arg-type]
        raise CoreError("Invalid cursor token")

    k = data.get("k")  # type: ignore[assignment, misc]
    d = data.get("d")  # type: ignore[assignment, misc]
    x = data.get("x")  # type: ignore[assignment, misc]

    if not isinstance(k, list) or not isinstance(d, list) or not isinstance(x, list):
        raise CoreError("Invalid cursor token")

    if len(k) != len(d) or len(k) != len(x):  # type: ignore[arg-type]
        raise CoreError("Invalid cursor token")

    keys = [str(a) for a in k]  # type: ignore[arg-type]
    dirs = [str(a).lower() for a in d]  # type: ignore[arg-type]

    for dr in dirs:
        if dr not in _DIRECTIONS:
            raise CoreError("Invalid cursor token")

    vals = [_parse_value(v) for v in x]  # type: ignore[arg-type]

    return keys, dirs, vals
